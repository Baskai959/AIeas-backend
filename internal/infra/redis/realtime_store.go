package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"

	redisgo "github.com/redis/go-redis/v9"
)

type AuctionRealtimeStore struct {
	client  *redisgo.Client
	scripts *ScriptRegistry
	keys    KeyBuilder
}

func NewAuctionRealtimeStore(client *redisgo.Client, scripts *ScriptRegistry, keys KeyBuilder) *AuctionRealtimeStore {
	if scripts == nil {
		scripts = NewScriptRegistry(client, DefaultScripts())
	}
	return &AuctionRealtimeStore{client: client, scripts: scripts, keys: keys}
}

func (s *AuctionRealtimeStore) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	if minIncrement <= 0 {
		minIncrement = 1
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
	err := s.client.HSet(ctx, s.keys.AuctionState(auction.AuctionID),
		"auction_id", auction.AuctionID,
		"status", string(auction.Status),
		"current_price", auction.StartPrice,
		"leader_bidder_id", "",
		"start_ts_ms", auction.StartTime.UnixMilli(),
		"end_ts_ms", auction.EndTime.UnixMilli(),
		"last_bid_ts_ms", 0,
		"extend_count", 0,
		"version", state.Version,
		"min_increment", minIncrement,
		"increment_rule", string(auction.IncrementRule),
	).Err()
	return state, err
}

func (s *AuctionRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	values, err := s.client.HGetAll(ctx, s.keys.AuctionState(auctionID)).Result()
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
	pipe := s.client.Pipeline()
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
	raw, err := s.client.Get(ctx, s.keys.AuctionIdempotency(auctionID, requestID)).Result()
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
	pipe := s.client.Pipeline()
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
	keys := []string{
		s.keys.AuctionState(input.AuctionID),
		s.keys.AuctionBids(input.AuctionID),
		s.keys.AuctionIdempotency(input.AuctionID, input.RequestID),
		s.keys.AuctionEnrolled(input.AuctionID),
		s.keys.AuctionDeposits(input.AuctionID),
		s.keys.UserBlacklist(),
		s.keys.BidFrequency(input.BidderID, input.AuctionID),
		s.keys.AuctionUserBids(input.AuctionID),
		s.keys.AuctionStream(input.AuctionID),
		s.keys.AuctionSeq(input.AuctionID),
		s.keys.ActiveStreams(),
	}
	raw, err := s.scripts.Eval(ctx, ScriptBidPlace, keys,
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
	raw, err := s.scripts.Eval(ctx, ScriptHammer, []string{
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
	rows, err := s.client.ZRevRangeWithScores(ctx, s.keys.AuctionBids(auctionID), 0, int64(limit-1)).Result()
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

func (s *AuctionRealtimeStore) IsBlacklisted(ctx context.Context, userID string) (bool, error) {
	return s.client.SIsMember(ctx, s.keys.UserBlacklist(), userID).Result()
}

func (s *AuctionRealtimeStore) SetBlacklisted(ctx context.Context, userID string, blacklisted bool) error {
	if blacklisted {
		return s.client.SAdd(ctx, s.keys.UserBlacklist(), userID).Err()
	}
	return s.client.SRem(ctx, s.keys.UserBlacklist(), userID).Err()
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
	}, nil
}

func (s *AuctionRealtimeStore) StreamEnabled() bool {
	return s != nil && s.client != nil
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
