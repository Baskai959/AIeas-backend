package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/tracing"

	redisgo "github.com/redis/go-redis/v9"
)

// AuctionRealtimeStore 是 RT 路径上拍卖状态 / 出价 / 落槌 / 排名的 Redis 实现。
//
// v2 起按 auctionID 走分片：所有 auction:<id>:* key 通过 sharded.ForAuction 路由到
// 同一 shard，确保 Lua EVAL（multi-key）成立。脚本注册由 ScriptRegistry 在每个
// shard 上 LoadAll 并按 shard 索引返回 SHA。
type AuctionRealtimeStore struct {
	sharded *ShardedRTClient
	scripts *ScriptRegistry
	keys    KeyBuilder
}

func NewAuctionRealtimeStore(sharded *ShardedRTClient, scripts *ScriptRegistry, keys KeyBuilder) *AuctionRealtimeStore {
	if scripts == nil {
		scripts = NewShardedScriptRegistry(sharded, DefaultScripts())
	}
	return &AuctionRealtimeStore{sharded: sharded, scripts: scripts, keys: keys}
}

// shardForAuction 返回该 auctionID 落到的 shard 客户端 + 索引。
func (s *AuctionRealtimeStore) shardForAuction(auctionID uint64) (*RedisRTClient, int) {
	idx := s.sharded.IndexAuction(auctionID)
	return s.sharded.ForIndex(idx), idx
}

func (s *AuctionRealtimeStore) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	if minIncrement <= 0 {
		minIncrement = 1
	}
	rule, err := domain.ParseIncrementRule(auction.IncrementRule)
	if err != nil {
		rule = domain.IncrementRule{Type: domain.IncrementRuleTypeFixed, Amount: minIncrement, MaxBidSteps: 1}
	} else {
		if amount := rule.AmountForPrice(auction.StartPrice); amount > 0 {
			minIncrement = amount
		}
	}
	incrementAmount := rule.AmountForPrice(auction.StartPrice)
	if incrementAmount <= 0 {
		incrementAmount = minIncrement
	}
	state := domain.AuctionState{
		AuctionID:    auction.AuctionID,
		Status:       auction.Status,
		CurrentPrice: auction.StartPrice,
		StartTime:    auction.StartTime,
		EndTime:      auction.EndTime,
		Version:      time.Now().UTC().UnixMilli(),
		Source:       "redis",
	}
	client, _ := s.shardForAuction(auction.AuctionID)
	pipe := client.Pipeline()
	pipe.Del(ctx,
		s.keys.AuctionBids(auction.AuctionID),
		s.keys.AuctionUserBids(auction.AuctionID),
		s.keys.AuctionStream(auction.AuctionID),
		s.keys.AuctionSeq(auction.AuctionID),
		s.keys.AuctionCloseLock(auction.AuctionID),
	)
	pipe.SRem(ctx, s.keys.ActiveStreams(), strconv.FormatUint(auction.AuctionID, 10))
	pipe.HSet(ctx, s.keys.AuctionState(auction.AuctionID),
		"auction_id", auction.AuctionID,
		"status", string(auction.Status),
		"start_price", auction.StartPrice,
		"current_price", auction.StartPrice,
		"cap_price", auction.CapPrice,
		"leader_bidder_id", "",
		"start_ts_ms", auction.StartTime.UnixMilli(),
		"end_ts_ms", auction.EndTime.UnixMilli(),
		"last_bid_ts_ms", 0,
		"extend_count", 0,
		"version", state.Version,
		"min_increment", minIncrement,
		"increment_amount", incrementAmount,
		"max_bid_steps", rule.MaxBidSteps,
		"increment_rule", string(auction.IncrementRule),
		"anti_extend_mode", string(domain.NormalizeAuctionExtendMode(auction.AntiExtendMode)),
	)
	_, err = pipe.Exec(ctx)
	return state, err
}

func (s *AuctionRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	client, _ := s.shardForAuction(auctionID)
	values, err := client.HGetAll(ctx, s.keys.AuctionState(auctionID)).Result()
	if err != nil {
		return domain.AuctionState{}, false, err
	}
	if len(values) == 0 {
		return domain.AuctionState{}, false, nil
	}
	state := domain.AuctionState{
		AuctionID:      parseUint(values["auction_id"], auctionID),
		Status:         domain.AuctionStatus(values["status"]),
		CurrentPrice:   parseInt(values["current_price"], 0),
		LeaderBidderID: values["leader_bidder_id"],
		StartTime:      time.UnixMilli(parseInt(values["start_ts_ms"], 0)).UTC(),
		EndTime:        time.UnixMilli(parseInt(values["end_ts_ms"], 0)).UTC(),
		LastBidTSMS:    parseInt(values["last_bid_ts_ms"], 0),
		ExtendCount:    int(parseInt(values["extend_count"], 0)),
		Version:        parseInt(values["version"], 0),
		Source:         "redis",
	}
	return state, true, nil
}

func (s *AuctionRealtimeStore) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	client, _ := s.shardForAuction(auctionID)
	pipe := client.Pipeline()
	pipe.SAdd(ctx, s.keys.AuctionEnrolled(auctionID), userID)
	pipe.SAdd(ctx, s.keys.AuctionDeposits(auctionID), userID)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *AuctionRealtimeStore) BidResultByRequestID(ctx context.Context, auctionID uint64, requestID string) (domain.BidResult, bool, error) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return domain.BidResult{}, false, nil
	}
	client, _ := s.shardForAuction(auctionID)
	raw, err := client.Get(ctx, s.keys.AuctionIdempotency(auctionID, requestID)).Result()
	if err != nil {
		if err == redisgo.Nil {
			return domain.BidResult{}, false, nil
		}
		return domain.BidResult{}, false, err
	}
	result, err := decodeBidResult(raw)
	if err != nil {
		return domain.BidResult{}, false, err
	}
	return result, true, nil
}

func (s *AuctionRealtimeStore) BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error) {
	client, _ := s.shardForAuction(auctionID)
	pipe := client.Pipeline()
	enrolled := pipe.SIsMember(ctx, s.keys.AuctionEnrolled(auctionID), userID)
	depositReady := pipe.SIsMember(ctx, s.keys.AuctionDeposits(auctionID), userID)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, false, err
	}
	return enrolled.Val(), depositReady.Val(), nil
}

func (s *AuctionRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	now := input.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if input.IdempotencyTTL <= 0 {
		input.IdempotencyTTL = 24 * time.Hour
	}
	expectedCurrentPrice := ""
	if input.ExpectedCurrentPrice != nil {
		expectedCurrentPrice = strconv.FormatInt(*input.ExpectedCurrentPrice, 10)
	}
	keys := []string{
		s.keys.AuctionState(input.AuctionID),
		s.keys.AuctionBids(input.AuctionID),
		s.keys.AuctionIdempotency(input.AuctionID, input.RequestID),
		s.keys.AuctionEnrolled(input.AuctionID),
		s.keys.AuctionDeposits(input.AuctionID),
		s.keys.BidFrequency(input.BidderID, input.AuctionID),
		s.keys.AuctionUserBids(input.AuctionID),
		s.keys.AuctionStream(input.AuctionID),
		s.keys.AuctionSeq(input.AuctionID),
		s.keys.ActiveStreams(),
	}
	traceCarrier := map[string]string{}
	tracing.InjectMap(ctx, traceCarrier)
	traceParent := traceCarrier["traceparent"]
	traceState := traceCarrier["tracestate"]
	_, shardIdx := s.shardForAuction(input.AuctionID)
	raw, err := s.scripts.EvalOnShard(ctx, shardIdx, ScriptBidPlace, keys,
		input.RequestID,
		input.AuctionID,
		input.BidderID,
		input.Price,
		now.UnixMilli(),
		input.MinIncrement,
		input.AntiSnipingMS,
		input.AntiExtendMS,
		input.MaxExtendCount,
		input.FreqLimitCount,
		input.FreqWindowMS,
		input.IdempotencyTTL.Milliseconds(),
		input.Source,
		string(domain.NormalizeAuctionExtendMode(input.AntiExtendMode)),
		expectedCurrentPrice,
		traceParent,
		traceState,
	)
	if err != nil {
		return domain.BidResult{}, err
	}
	return decodeBidResult(raw)
}

func (s *AuctionRealtimeStore) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	now := input.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if input.IdempotencyTTL <= 0 {
		input.IdempotencyTTL = 24 * time.Hour
	}
	_, shardIdx := s.shardForAuction(input.AuctionID)
	raw, err := s.scripts.EvalOnShard(ctx, shardIdx, ScriptHammer, []string{
		s.keys.AuctionState(input.AuctionID),
		s.keys.AuctionBids(input.AuctionID),
		s.keys.AuctionCloseLock(input.AuctionID),
	}, input.RequestID, input.AuctionID, now.UnixMilli(), input.IdempotencyTTL.Milliseconds(), input.ReservePrice, input.Force)
	if err != nil {
		return domain.HammerResult{}, err
	}
	result, err := decodeHammerResult(raw)
	if err != nil {
		return domain.HammerResult{}, err
	}
	if result.Status == domain.AuctionStatus("NOT_FOUND") {
		return domain.HammerResult{}, domain.ErrNotFound
	}
	if result.Status == domain.AuctionStatus("NOT_ENDED") {
		return domain.HammerResult{}, domain.ErrInvalidState
	}
	return result, nil
}

func (s *AuctionRealtimeStore) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	client, _ := s.shardForAuction(auctionID)
	rows, err := client.ZRevRangeWithScores(ctx, s.keys.AuctionBids(auctionID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]domain.RankingEntry, 0, len(rows))
	for i, row := range rows {
		bidderID, _ := row.Member.(string)
		price := int64(row.Score)
		if first := strings.IndexByte(bidderID, ':'); first > 0 {
			if parsed, err := strconv.ParseInt(bidderID[:first], 10, 64); err == nil {
				price = parsed
			}
			if last := strings.LastIndexByte(bidderID, ':'); last >= 0 && last+1 < len(bidderID) {
				bidderID = bidderID[last+1:]
			}
		}
		entries = append(entries, domain.RankingEntry{Rank: i + 1, BidderID: bidderID, Price: price})
	}
	return entries, nil
}

type luaBidResult struct {
	RequestID      string               `json:"requestId"`
	AuctionID      uint64               `json:"auctionId"`
	BidderID       string               `json:"bidderId"`
	Price          int64                `json:"price"`
	Accepted       bool                 `json:"accepted"`
	Duplicate      bool                 `json:"duplicate"`
	Reason         string               `json:"reason"`
	CurrentPrice   int64                `json:"currentPrice"`
	LeaderBidderID string               `json:"leaderBidderId"`
	EndTSMS        int64                `json:"endTsMs"`
	Extended       bool                 `json:"extended"`
	ExtendCount    int                  `json:"extendCount"`
	Version        int64                `json:"version"`
	Seq            int64                `json:"seq"`
	StreamID       string               `json:"streamId"`
	Event          string               `json:"event"`
	RiskResult     domain.BidRiskResult `json:"riskResult"`
	AuctionStatus  domain.AuctionStatus `json:"auctionStatus"`
	AutoClosed     bool                 `json:"autoClosed"`
}

func decodeBidResult(raw interface{}) (domain.BidResult, error) {
	text, ok := raw.(string)
	if !ok {
		if bytes, ok := raw.([]byte); ok {
			text = string(bytes)
		}
	}
	var decoded luaBidResult
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return domain.BidResult{}, err
	}
	return domain.BidResult{
		RequestID:      decoded.RequestID,
		AuctionID:      decoded.AuctionID,
		BidderID:       decoded.BidderID,
		Price:          decoded.Price,
		Accepted:       decoded.Accepted,
		Duplicate:      decoded.Duplicate,
		Reason:         decoded.Reason,
		CurrentPrice:   decoded.CurrentPrice,
		LeaderBidderID: decoded.LeaderBidderID,
		EndTime:        time.UnixMilli(decoded.EndTSMS).UTC(),
		Extended:       decoded.Extended,
		ExtendCount:    decoded.ExtendCount,
		Version:        decoded.Version,
		Seq:            decoded.Seq,
		StreamID:       decoded.StreamID,
		Event:          decoded.Event,
		RiskResult:     decoded.RiskResult,
		AuctionStatus:  decoded.AuctionStatus,
		AutoClosed:     decoded.AutoClosed,
	}, nil
}

func (s *AuctionRealtimeStore) StreamEnabled() bool {
	return s != nil && s.sharded != nil && s.sharded.Len() > 0
}

type luaHammerResult struct {
	RequestID  string               `json:"requestId"`
	AuctionID  uint64               `json:"auctionId"`
	Status     domain.AuctionStatus `json:"status"`
	WinnerID   string               `json:"winnerId"`
	Price      int64                `json:"price"`
	Duplicate  bool                 `json:"duplicate"`
	ClosedAtMS int64                `json:"closedAtMs"`
	Version    int64                `json:"version"`
}

func decodeHammerResult(raw interface{}) (domain.HammerResult, error) {
	text, ok := raw.(string)
	if !ok {
		if bytes, ok := raw.([]byte); ok {
			text = string(bytes)
		}
	}
	var decoded luaHammerResult
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return domain.HammerResult{}, err
	}
	return domain.HammerResult{
		RequestID: decoded.RequestID,
		AuctionID: decoded.AuctionID,
		Status:    decoded.Status,
		WinnerID:  decoded.WinnerID,
		Price:     decoded.Price,
		Duplicate: decoded.Duplicate,
		ClosedAt:  time.UnixMilli(decoded.ClosedAtMS).UTC(),
		Version:   decoded.Version,
	}, nil
}

func parseInt(raw string, fallback int64) int64 {
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func parseUint(raw string, fallback uint64) uint64 {
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return value
}
