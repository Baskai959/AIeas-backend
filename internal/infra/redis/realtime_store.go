package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	sharded        *ShardedRTClient
	publishSharded *ShardedRTClient
	rankingSharded *ShardedRTClient
	scripts        *ScriptRegistry
	keys           KeyBuilder
}

func NewAuctionRealtimeStore(sharded *ShardedRTClient, scripts *ScriptRegistry, keys KeyBuilder) *AuctionRealtimeStore {
	if scripts == nil {
		scripts = NewShardedScriptRegistry(sharded, DefaultScripts())
	}
	return &AuctionRealtimeStore{sharded: sharded, publishSharded: sharded, rankingSharded: sharded, scripts: scripts, keys: keys}
}

// SetPublishShardedRT 注入专用于 best-effort PubSub fanout 的 Redis client/pool。
// 它不影响 Lua / state / stream 等主出价路径。
func (s *AuctionRealtimeStore) SetPublishShardedRT(sharded *ShardedRTClient) {
	if s != nil && sharded != nil {
		s.publishSharded = sharded
	}
}

// SetRankingShardedRT 注入专用于异步排行榜读写的 Redis client/pool。
// 它不影响 Lua / state / stream 等主出价路径。
func (s *AuctionRealtimeStore) SetRankingShardedRT(sharded *ShardedRTClient) {
	if s != nil && sharded != nil {
		s.rankingSharded = sharded
	}
}

// shardForAuction 返回该 auctionID 落到的 shard 客户端 + 索引。
func (s *AuctionRealtimeStore) shardForAuction(auctionID uint64) (*RedisRTClient, int) {
	idx := s.sharded.IndexAuction(auctionID)
	return s.sharded.ForIndex(idx), idx
}

func (s *AuctionRealtimeStore) rankingShardForAuction(auctionID uint64) *RedisRTClient {
	if s == nil {
		return nil
	}
	if s.rankingSharded != nil {
		return s.rankingSharded.ForAuction(auctionID)
	}
	if s.sharded != nil {
		return s.sharded.ForAuction(auctionID)
	}
	return nil
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
	ruleType := strings.ToLower(strings.TrimSpace(rule.Type))
	var incrementFixedAmount int64
	if ruleType == domain.IncrementRuleTypeFixed && rule.Amount > 0 {
		incrementFixedAmount = rule.Amount
	}
	var liveSessionID uint64
	if auction.LiveSessionID != nil {
		liveSessionID = *auction.LiveSessionID
	}
	client, _ := s.shardForAuction(auction.AuctionID)
	participantCount, err := client.SCard(ctx, s.keys.AuctionEnrolled(auction.AuctionID)).Result()
	if err != nil {
		return domain.AuctionState{}, err
	}
	state := domain.AuctionState{
		AuctionID:        auction.AuctionID,
		LiveSessionID:    liveSessionID,
		Status:           auction.Status,
		StartPrice:       auction.StartPrice,
		CapPrice:         auction.CapPrice,
		IncrementRule:    append([]byte(nil), auction.IncrementRule...),
		CurrentPrice:     auction.StartPrice,
		ParticipantCount: int(participantCount),
		StartTime:        auction.StartTime,
		EndTime:          auction.EndTime,
		Version:          time.Now().UTC().UnixMilli(),
		Source:           "redis",
	}
	pipe := client.Pipeline()
	pipe.Del(ctx,
		s.keys.AuctionBids(auction.AuctionID),
		s.keys.AuctionUserBids(auction.AuctionID),
		s.keys.AuctionUserLatestSeq(auction.AuctionID),
		s.keys.AuctionStream(auction.AuctionID),
		s.keys.AuctionSeq(auction.AuctionID),
		s.keys.AuctionCloseLock(auction.AuctionID),
	)
	pipe.SRem(ctx, s.keys.ActiveStreams(), strconv.FormatUint(auction.AuctionID, 10))
	pipe.SAdd(ctx, s.keys.ActiveStreams(), strconv.FormatUint(auction.AuctionID, 10))
	pipe.HSet(ctx, s.keys.AuctionState(auction.AuctionID),
		"auction_id", auction.AuctionID,
		"live_session_id", liveSessionID,
		"status", string(auction.Status),
		"start_price", auction.StartPrice,
		"current_price", auction.StartPrice,
		"cap_price", auction.CapPrice,
		"leader_bidder_id", "",
		"bid_count", 0,
		"participant_count", participantCount,
		"start_ts_ms", auction.StartTime.UnixMilli(),
		"end_ts_ms", auction.EndTime.UnixMilli(),
		"last_bid_ts_ms", 0,
		"extend_count", 0,
		"version", state.Version,
		"min_increment", minIncrement,
		"increment_amount", incrementAmount,
		"max_bid_steps", rule.MaxBidSteps,
		"increment_rule", string(auction.IncrementRule),
		"increment_rule_type", ruleType,
		"increment_fixed_amount", incrementFixedAmount,
		"anti_extend_mode", string(domain.NormalizeAuctionExtendMode(auction.AntiExtendMode)),
	)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return state, err
	}
	if rankingClient := s.rankingShardForAuction(auction.AuctionID); rankingClient != nil && rankingClient != client {
		err = rankingClient.Del(ctx,
			s.keys.AuctionBids(auction.AuctionID),
			s.keys.AuctionUserBids(auction.AuctionID),
			s.keys.AuctionUserLatestSeq(auction.AuctionID),
		).Err()
	}
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
	participantCount := int(parseInt(values["participant_count"], 0))
	if enrolledCount, countErr := client.SCard(ctx, s.keys.AuctionEnrolled(auctionID)).Result(); countErr == nil && int(enrolledCount) > participantCount {
		participantCount = int(enrolledCount)
		_ = client.HSet(ctx, s.keys.AuctionState(auctionID), "participant_count", participantCount).Err()
	}
	state := domain.AuctionState{
		AuctionID:        parseUint(values["auction_id"], auctionID),
		LiveSessionID:    parseUint(values["live_session_id"], 0),
		Status:           domain.AuctionStatus(values["status"]),
		StartPrice:       parseInt(values["start_price"], 0),
		CapPrice:         parseInt(values["cap_price"], 0),
		IncrementRule:    parseRawJSON(values["increment_rule"]),
		CurrentPrice:     parseInt(values["current_price"], 0),
		LeaderBidderID:   values["leader_bidder_id"],
		BidCount:         int(parseInt(values["bid_count"], 0)),
		ParticipantCount: participantCount,
		StartTime:        time.UnixMilli(parseInt(values["start_ts_ms"], 0)).UTC(),
		EndTime:          time.UnixMilli(parseInt(values["end_ts_ms"], 0)).UTC(),
		LastBidTSMS:      parseInt(values["last_bid_ts_ms"], 0),
		ExtendCount:      int(parseInt(values["extend_count"], 0)),
		Version:          parseInt(values["version"], 0),
		Source:           "redis",
	}
	return state, true, nil
}

func (s *AuctionRealtimeStore) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return domain.ErrInvalidArgument
	}
	_, shardIdx := s.shardForAuction(auctionID)
	_, err := s.scripts.EvalOnShard(ctx, shardIdx, ScriptMarkEnrollment, []string{
		s.keys.AuctionEnrolled(auctionID),
		s.keys.AuctionDeposits(auctionID),
		s.keys.AuctionState(auctionID),
	}, userID)
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
		input.IdempotencyTTL = 30 * time.Second
	}
	expectedCurrentPrice := ""
	if input.ExpectedCurrentPrice != nil {
		expectedCurrentPrice = strconv.FormatInt(*input.ExpectedCurrentPrice, 10)
	}
	keys := []string{
		s.keys.AuctionState(input.AuctionID),
		s.keys.AuctionBids(input.AuctionID),
		s.keys.AuctionIdempotency(input.AuctionID, input.RequestID),
		s.keys.BidFrequency(input.BidderID, input.AuctionID),
		s.keys.AuctionUserBids(input.AuctionID),
		s.keys.AuctionStream(input.AuctionID),
		s.keys.AuctionSeq(input.AuctionID),
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
		input.LiveSessionID,
		input.BidderNickname,
		input.BidderAvatarURL,
	)
	if err != nil {
		return domain.BidResult{}, err
	}
	result, err := decodeBidResult(raw)
	if err != nil {
		return domain.BidResult{}, err
	}
	if result.BidderAvatarURL == "" {
		result.BidderAvatarURL = strings.TrimSpace(input.BidderAvatarURL)
		result.AvatarURL = result.BidderAvatarURL
	}
	if result.Nickname == "" {
		result.Nickname = result.BidderNickname
	}
	if result.Accepted && !result.Duplicate {
		s.publishAcceptedResult(ctx, input, result, now)
	}
	return result, nil
}

// publishAcceptedResult publishes the accepted bid payload to the auction PubSub
// channel asynchronously on the publish pool. Failures are logged and ignored so
// the bidder ACK is never blocked.
func (s *AuctionRealtimeStore) publishAcceptedResult(ctx context.Context, input domain.BidInput, result domain.BidResult, bidAt time.Time) {
	auctionID := input.AuctionID
	shard := s.publishShardForAuction(auctionID)
	if shard == nil {
		return
	}
	source := input.Source
	if source == "" {
		source = "live_ws"
	}
	event := result.Event
	if event == "" {
		event = "bid.accepted"
	}
	channel := fmt.Sprintf("auction:%d:events", auctionID)
	go func() {
		pubCtx, cancel := realtimePublishContext(ctx)
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				slog.Warn("auction realtime publish panic", "auctionId", auctionID, "panic", r)
			}
		}()
		payload := bidAcceptedPublishPayload{
			RequestID:       result.RequestID,
			AuctionID:       result.AuctionID,
			LiveSessionID:   result.LiveSessionID,
			BidderID:        result.BidderID,
			BidderNickname:  result.BidderNickname,
			Nickname:        result.BidderNickname,
			BidderAvatarURL: result.BidderAvatarURL,
			AvatarURL:       result.BidderAvatarURL,
			Price:           result.Price,
			Accepted:        result.Accepted,
			Duplicate:       result.Duplicate,
			Reason:          result.Reason,
			CurrentPrice:    result.CurrentPrice,
			LeaderBidderID:  result.LeaderBidderID,
			EndTime:         result.EndTime,
			Extended:        result.Extended,
			ExtendCount:     result.ExtendCount,
			Version:         result.Version,
			Seq:             result.Seq,
			StreamID:        result.StreamID,
			CreatedAtMS:     bidAt.UnixMilli(),
			BidTSMS:         bidAt.UnixMilli(),
			ServerTime:      bidAt.UTC(),
			Source:          source,
			Event:           event,
			RiskResult:      result.RiskResult,
			AuctionStatus:   result.AuctionStatus,
			AutoClosed:      result.AutoClosed,
		}
		rawPayload, err := json.Marshal(payload)
		if err != nil {
			slog.Debug("auction realtime publish marshal failed", "auctionId", auctionID, "err", err)
			return
		}
		if err := shard.Publish(pubCtx, channel, rawPayload).Err(); err != nil {
			slog.Debug("auction realtime publish failed", "auctionId", auctionID, "err", err)
		}
	}()
}

func (s *AuctionRealtimeStore) publishShardForAuction(auctionID uint64) *RedisRTClient {
	if s == nil {
		return nil
	}
	if s.publishSharded != nil {
		return s.publishSharded.ForAuction(auctionID)
	}
	if s.sharded != nil {
		return s.sharded.ForAuction(auctionID)
	}
	return nil
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
	client := s.rankingShardForAuction(auctionID)
	if client == nil {
		return nil, fmt.Errorf("redis ranking store is not configured")
	}
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
	RequestID       string               `json:"requestId"`
	AuctionID       uint64               `json:"auctionId"`
	LiveSessionID   uint64               `json:"liveSessionId"`
	BidderID        string               `json:"bidderId"`
	BidderNickname  string               `json:"bidderNickname"`
	BidderAvatarURL string               `json:"bidderAvatarUrl"`
	Price           int64                `json:"price"`
	Accepted        bool                 `json:"accepted"`
	Duplicate       bool                 `json:"duplicate"`
	Reason          string               `json:"reason"`
	CurrentPrice    int64                `json:"currentPrice"`
	LeaderBidderID  string               `json:"leaderBidderId"`
	EndTSMS         int64                `json:"endTsMs"`
	Extended        bool                 `json:"extended"`
	ExtendCount     int                  `json:"extendCount"`
	Version         int64                `json:"version"`
	Seq             int64                `json:"seq"`
	StreamID        string               `json:"streamId"`
	Event           string               `json:"event"`
	RiskResult      domain.BidRiskResult `json:"riskResult"`
	AuctionStatus   domain.AuctionStatus `json:"auctionStatus"`
	AutoClosed      bool                 `json:"autoClosed"`
}

type bidAcceptedPublishPayload struct {
	RequestID       string               `json:"requestId"`
	AuctionID       uint64               `json:"auctionId"`
	LiveSessionID   uint64               `json:"liveSessionId"`
	BidderID        string               `json:"bidderId"`
	BidderNickname  string               `json:"bidderNickname"`
	Nickname        string               `json:"nickname,omitempty"`
	BidderAvatarURL string               `json:"bidderAvatarUrl,omitempty"`
	AvatarURL       string               `json:"avatarUrl,omitempty"`
	Price           int64                `json:"price"`
	Accepted        bool                 `json:"accepted"`
	Duplicate       bool                 `json:"duplicate"`
	Reason          string               `json:"reason"`
	CurrentPrice    int64                `json:"currentPrice"`
	LeaderBidderID  string               `json:"leaderBidderId"`
	EndTime         time.Time            `json:"endTime"`
	Extended        bool                 `json:"extended"`
	ExtendCount     int                  `json:"extendCount"`
	Version         int64                `json:"version"`
	Seq             int64                `json:"seq"`
	StreamID        string               `json:"streamId"`
	CreatedAtMS     int64                `json:"createdAtMs"`
	BidTSMS         int64                `json:"bidTsMs"`
	ServerTime      time.Time            `json:"serverTime"`
	Source          string               `json:"source"`
	Event           string               `json:"event"`
	RiskResult      domain.BidRiskResult `json:"riskResult"`
	AuctionStatus   domain.AuctionStatus `json:"auctionStatus"`
	AutoClosed      bool                 `json:"autoClosed"`
}

func decodeBidResult(raw interface{}) (domain.BidResult, error) {
	switch v := raw.(type) {
	case []interface{}:
		return decodeBidResultArray(v)
	case string:
		return decodeBidResultString(v)
	case []byte:
		return decodeBidResultString(string(v))
	}
	return domain.BidResult{}, fmt.Errorf("decode bid result: unsupported redis result type %T", raw)
}

func decodeBidResultString(text string) (domain.BidResult, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return domain.BidResult{}, fmt.Errorf("decode bid result: empty payload")
	}
	if strings.HasPrefix(text, "[") {
		var fields []interface{}
		if err := json.Unmarshal([]byte(text), &fields); err != nil {
			return domain.BidResult{}, err
		}
		return decodeBidResultArray(fields)
	}
	var decoded luaBidResult
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return domain.BidResult{}, err
	}
	return bidResultFromLuaObject(decoded), nil
}

func bidResultFromLuaObject(decoded luaBidResult) domain.BidResult {
	return domain.BidResult{
		RequestID:       decoded.RequestID,
		AuctionID:       decoded.AuctionID,
		LiveSessionID:   decoded.LiveSessionID,
		BidderID:        decoded.BidderID,
		BidderNickname:  decoded.BidderNickname,
		Nickname:        decoded.BidderNickname,
		BidderAvatarURL: decoded.BidderAvatarURL,
		AvatarURL:       decoded.BidderAvatarURL,
		Price:           decoded.Price,
		Accepted:        decoded.Accepted,
		Duplicate:       decoded.Duplicate,
		Reason:          decoded.Reason,
		CurrentPrice:    decoded.CurrentPrice,
		LeaderBidderID:  decoded.LeaderBidderID,
		EndTime:         time.UnixMilli(decoded.EndTSMS).UTC(),
		Extended:        decoded.Extended,
		ExtendCount:     decoded.ExtendCount,
		Version:         decoded.Version,
		Seq:             decoded.Seq,
		StreamID:        decoded.StreamID,
		Event:           decoded.Event,
		RiskResult:      decoded.RiskResult,
		AuctionStatus:   decoded.AuctionStatus,
		AutoClosed:      decoded.AutoClosed,
	}
}

func decodeBidResultArray(fields []interface{}) (domain.BidResult, error) {
	if len(fields) < 21 {
		return domain.BidResult{}, fmt.Errorf("decode bid result array: got %d fields, want at least 21", len(fields))
	}
	return domain.BidResult{
		RequestID:       arrayString(fields, 0),
		AuctionID:       uint64(arrayInt64(fields, 1)),
		LiveSessionID:   uint64(arrayInt64(fields, 2)),
		BidderID:        arrayString(fields, 3),
		BidderNickname:  arrayString(fields, 4),
		Nickname:        arrayString(fields, 4),
		BidderAvatarURL: arrayString(fields, 23),
		AvatarURL:       arrayString(fields, 23),
		Price:           arrayInt64(fields, 5),
		Accepted:        arrayBool(fields, 6),
		Duplicate:       arrayBool(fields, 7),
		Reason:          arrayString(fields, 8),
		CurrentPrice:    arrayInt64(fields, 9),
		LeaderBidderID:  arrayString(fields, 10),
		EndTime:         time.UnixMilli(arrayInt64(fields, 11)).UTC(),
		Extended:        arrayBool(fields, 12),
		ExtendCount:     int(arrayInt64(fields, 13)),
		Version:         arrayInt64(fields, 14),
		Seq:             arrayInt64(fields, 15),
		StreamID:        arrayString(fields, 16),
		Event:           arrayString(fields, 17),
		RiskResult:      domain.BidRiskResult(arrayString(fields, 18)),
		AuctionStatus:   domain.AuctionStatus(arrayString(fields, 19)),
		AutoClosed:      arrayBool(fields, 20),
	}, nil
}

func arrayString(fields []interface{}, idx int) string {
	if idx < 0 || idx >= len(fields) || fields[idx] == nil {
		return ""
	}
	switch v := fields[idx].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func arrayInt64(fields []interface{}, idx int) int64 {
	if idx < 0 || idx >= len(fields) || fields[idx] == nil {
		return 0
	}
	switch v := fields[idx].(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0
		}
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return parsed
	case []byte:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(v)), 10, 64)
		return parsed
	}
}

func arrayBool(fields []interface{}, idx int) bool {
	if idx < 0 || idx >= len(fields) || fields[idx] == nil {
		return false
	}
	switch v := fields[idx].(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	case string:
		v = strings.ToLower(strings.TrimSpace(v))
		return v == "1" || v == "true" || v == "yes"
	case []byte:
		text := strings.ToLower(strings.TrimSpace(string(v)))
		return text == "1" || text == "true" || text == "yes"
	default:
		return arrayInt64(fields, idx) != 0
	}
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

func parseRawJSON(raw string) json.RawMessage {
	if strings.TrimSpace(raw) == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	return append([]byte(nil), raw...)
}
