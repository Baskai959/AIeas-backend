package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"

	redisgo "github.com/redis/go-redis/v9"
)

const BidRecordConsumerGroup = "bid-record-writers"

type BidEvent struct {
	AuctionID      uint64
	StreamID       string
	Seq            int64
	RequestID      string
	BidderID       string
	BidPrice       int64
	BidTSMS        int64
	Source         string
	RiskResult     domain.BidRiskResult
	RejectReason   string
	Accepted       bool
	CurrentPrice   int64
	LeaderBidderID string
	EndTSMS        int64
	Extended       bool
	ExtendCount    int
	CreatedAtMS    int64
	EventType      string
	AuctionStatus  domain.AuctionStatus
	AutoClosed     bool
	Deliveries     int64
	TraceParent    string
	TraceState     string
	Raw            map[string]string
}

// TraceCarrier 返回事件携带的 W3C trace context，便于消费端通过
// tracing.ExtractMap 续上 trace。无 traceparent 时返回 nil。
func (e BidEvent) TraceCarrier() map[string]string {
	if strings.TrimSpace(e.TraceParent) == "" {
		return nil
	}
	carrier := map[string]string{"traceparent": e.TraceParent}
	if strings.TrimSpace(e.TraceState) != "" {
		carrier["tracestate"] = e.TraceState
	}
	return carrier
}

// EventLog 把竞价事件写入 Redis Stream / DLQ。
//
// v2 起按 auctionID 走分片：每个 auction:<id>:stream / seq / active_streams 都
// 在 ForAuction(id) 这个 shard 上读写。`auction:active_streams` 是 per-shard
// 的：每个 shard 上只记录"当前 shard 上"出过价的 auction，ActiveAuctions 时
// 把所有 shard 的 active_streams 合并返回。DLQ (`bid_record:dlq`) 是全局 key，
// 固定在 ForGlobal()（shard 0）。
type EventLog struct {
	sharded *ShardedRTClient
	keys    KeyBuilder
}

func NewEventLog(sharded *ShardedRTClient, keys KeyBuilder) *EventLog {
	return &EventLog{sharded: sharded, keys: keys}
}

func (l *EventLog) Enabled() bool { return l != nil && l.sharded != nil && l.sharded.Len() > 0 }

func (l *EventLog) shardForAuction(auctionID uint64) *RedisRTClient {
	if !l.Enabled() {
		return nil
	}
	return l.sharded.ForAuction(auctionID)
}

func (l *EventLog) globalShard() *RedisRTClient {
	if !l.Enabled() {
		return nil
	}
	return l.sharded.ForGlobal()
}

func (l *EventLog) ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]BidEvent, bool, error) {
	if !l.Enabled() {
		return nil, false, fmt.Errorf("redis event log is not configured")
	}
	if limit <= 0 {
		limit = 256
	}
	stream := l.keys.AuctionStream(auctionID)
	start := fmt.Sprintf("%d-1", lastSeq)
	client := l.shardForAuction(auctionID)
	entries, err := client.XRangeN(ctx, stream, start, "+", limit).Result()
	if err != nil {
		return nil, false, err
	}
	if len(entries) == 0 {
		return nil, true, nil
	}
	firstSeq := parseStreamSeq(entries[0].ID)
	if lastSeq > 0 && firstSeq > lastSeq+1 {
		return nil, false, nil
	}
	events := make([]BidEvent, 0, len(entries))
	for _, entry := range entries {
		events = append(events, bidEventFromXMessage(entry, 0))
	}
	return events, true, nil
}

// ActiveAuctions 合并每个 shard 上 active_streams 集合的所有 auctionID。
// 同一 auctionID 一定只在 ForAuction(id) 那个 shard 上出现，因此结果天然不重复，
// 但这里仍然做一次 dedup 兜底。
func (l *EventLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	if !l.Enabled() {
		return nil, fmt.Errorf("redis event log is not configured")
	}
	seen := make(map[uint64]struct{})
	auctions := make([]uint64, 0)
	for _, shard := range l.sharded.Shards() {
		members, err := shard.SMembers(ctx, l.keys.ActiveStreams()).Result()
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			auctionID, err := strconv.ParseUint(member, 10, 64)
			if err != nil || auctionID == 0 {
				continue
			}
			if _, dup := seen[auctionID]; dup {
				continue
			}
			seen[auctionID] = struct{}{}
			auctions = append(auctions, auctionID)
		}
	}
	return auctions, nil
}

// ActiveAuctionsOnShard 只返回某个 shard 上 active_streams 集合中的 auctionID。
// 这是 per-shard 工作循环的入口：每个 BidRecordWriter goroutine 只巡检自己那一片，
// 避免跨 shard 抖动相互放大。
func (l *EventLog) ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error) {
	if !l.Enabled() {
		return nil, fmt.Errorf("redis event log is not configured")
	}
	if shardIdx < 0 || shardIdx >= l.sharded.Len() {
		return nil, fmt.Errorf("redis event log: shard index %d out of range [0,%d)", shardIdx, l.sharded.Len())
	}
	shard := l.sharded.ForIndex(shardIdx)
	members, err := shard.SMembers(ctx, l.keys.ActiveStreams()).Result()
	if err != nil {
		return nil, err
	}
	auctions := make([]uint64, 0, len(members))
	for _, member := range members {
		auctionID, err := strconv.ParseUint(member, 10, 64)
		if err != nil || auctionID == 0 {
			continue
		}
		auctions = append(auctions, auctionID)
	}
	return auctions, nil
}

// ShardCount 返回当前配置的 RT shard 数量；ServerWiring / 工作循环用它决定起多少 goroutine。
func (l *EventLog) ShardCount() int {
	if !l.Enabled() {
		return 0
	}
	return l.sharded.Len()
}

func (l *EventLog) EnsureBidRecordGroup(ctx context.Context, auctionID uint64) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	err := client.XGroupCreateMkStream(ctx, l.keys.AuctionStream(auctionID), BidRecordConsumerGroup, "0-0").Err()
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return err
	}
	return nil
}

func (l *EventLog) ReadBidRecordGroup(ctx context.Context, auctionID uint64, consumer string, count int64, block time.Duration) ([]BidEvent, error) {
	if err := l.EnsureBidRecordGroup(ctx, auctionID); err != nil {
		return nil, err
	}
	client := l.shardForAuction(auctionID)
	streams, err := client.XReadGroup(ctx, &redisgo.XReadGroupArgs{
		Group:    BidRecordConsumerGroup,
		Consumer: consumer,
		Streams:  []string{l.keys.AuctionStream(auctionID), ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if err == redisgo.Nil {
			return nil, nil
		}
		return nil, err
	}
	return bidEventsFromStreams(streams, 0), nil
}

func (l *EventLog) ClaimStaleBidRecordEvents(ctx context.Context, auctionID uint64, consumer string, minIdle time.Duration, max int64) ([]BidEvent, error) {
	if !l.Enabled() {
		return nil, fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	pending, err := client.XPendingExt(ctx, &redisgo.XPendingExtArgs{
		Stream: l.keys.AuctionStream(auctionID), Group: BidRecordConsumerGroup, Start: "-", End: "+", Count: max,
	}).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(pending))
	deliveries := make(map[string]int64, len(pending))
	for _, item := range pending {
		if item.Idle >= minIdle {
			ids = append(ids, item.ID)
			deliveries[item.ID] = item.RetryCount
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	claimed, err := client.XClaim(ctx, &redisgo.XClaimArgs{
		Stream: l.keys.AuctionStream(auctionID), Group: BidRecordConsumerGroup, Consumer: consumer, MinIdle: minIdle, Messages: ids,
	}).Result()
	if err != nil {
		return nil, err
	}
	events := make([]BidEvent, 0, len(claimed))
	for _, msg := range claimed {
		events = append(events, bidEventFromXMessage(msg, deliveries[msg.ID]))
	}
	return events, nil
}

func (l *EventLog) AckBidRecord(ctx context.Context, auctionID uint64, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	return client.XAck(ctx, l.keys.AuctionStream(auctionID), BidRecordConsumerGroup, ids...).Err()
}

// WriteBidRecordDLQ 把死信写到全局 shard 上的 DLQ，便于运维统一巡检。
func (l *EventLog) WriteBidRecordDLQ(ctx context.Context, event BidEvent, reason string) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	client := l.globalShard()
	values := map[string]interface{}{
		"auction_id": event.AuctionID, "stream_id": event.StreamID, "seq": event.Seq,
		"request_id": event.RequestID, "event_type": event.EventType, "reason": reason,
		"created_at_ms": time.Now().UTC().UnixMilli(),
	}
	return client.XAdd(ctx, &redisgo.XAddArgs{Stream: l.keys.BidRecordDLQ(), Values: values}).Err()
}

func (l *EventLog) ReconcileCheckpoint(ctx context.Context, auctionID uint64) (int64, error) {
	if !l.Enabled() {
		return 0, fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	value, err := client.Get(ctx, l.keys.BidRecordReconcileCheckpoint(auctionID)).Result()
	if err == redisgo.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func (l *EventLog) SetReconcileCheckpoint(ctx context.Context, auctionID uint64, seq int64) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	return client.Set(ctx, l.keys.BidRecordReconcileCheckpoint(auctionID), seq, 0).Err()
}

func (e BidEvent) ToBidRecord() domain.BidRecord {
	return domain.BidRecord{RequestID: e.RequestID, AuctionID: e.AuctionID, BidderID: e.BidderID, BidPrice: e.BidPrice, BidTSMS: e.BidTSMS, Source: e.Source, RiskResult: e.RiskResult, RejectReason: e.RejectReason, CreatedAt: time.UnixMilli(e.CreatedAtMS).UTC()}
}

func (e BidEvent) PayloadJSON() []byte {
	payload := map[string]interface{}{
		"requestId": e.RequestID, "auctionId": e.AuctionID, "bidderId": e.BidderID, "price": e.BidPrice,
		"accepted": e.Accepted, "reason": e.RejectReason, "currentPrice": e.CurrentPrice, "leaderBidderId": e.LeaderBidderID,
		"endTsMs": e.EndTSMS, "extended": e.Extended, "extendCount": e.ExtendCount, "seq": e.Seq,
		"streamId": e.StreamID, "createdAtMs": e.CreatedAtMS, "bidTsMs": e.BidTSMS, "source": e.Source,
		"event": e.EventType, "riskResult": e.RiskResult, "auctionStatus": e.AuctionStatus, "autoClosed": e.AutoClosed,
	}
	raw, _ := json.Marshal(payload)
	return raw
}

func bidEventsFromStreams(streams []redisgo.XStream, deliveries int64) []BidEvent {
	events := make([]BidEvent, 0)
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			events = append(events, bidEventFromXMessage(msg, deliveries))
		}
	}
	return events
}

func bidEventFromXMessage(msg redisgo.XMessage, deliveries int64) BidEvent {
	raw := make(map[string]string, len(msg.Values))
	for key, value := range msg.Values {
		raw[key] = fmt.Sprint(value)
	}
	auctionID, _ := strconv.ParseUint(raw["auction_id"], 10, 64)
	seq := parseInt64(raw["seq"], parseStreamSeq(msg.ID))
	return BidEvent{
		AuctionID: auctionID, StreamID: msg.ID, Seq: seq, RequestID: raw["request_id"], BidderID: raw["bidder_id"],
		BidPrice: parseInt64(raw["bid_price"], 0), BidTSMS: parseInt64(raw["bid_ts_ms"], 0), Source: raw["source"],
		RiskResult: domain.BidRiskResult(raw["risk_result"]), RejectReason: raw["reject_reason"], Accepted: raw["accepted"] == "1" || raw["accepted"] == "true",
		CurrentPrice: parseInt64(raw["current_price"], 0), LeaderBidderID: raw["leader_bidder_id"], EndTSMS: parseInt64(raw["end_ts_ms"], 0),
		Extended: raw["extended"] == "1" || raw["extended"] == "true", ExtendCount: int(parseInt64(raw["extend_count"], 0)),
		CreatedAtMS: parseInt64(raw["created_at_ms"], 0), EventType: raw["event_type"],
		AuctionStatus: domain.AuctionStatus(raw["auction_status"]), AutoClosed: raw["auto_closed"] == "1" || raw["auto_closed"] == "true",
		Deliveries: deliveries, TraceParent: raw["traceparent"], TraceState: raw["tracestate"], Raw: raw,
	}
}

func parseStreamSeq(id string) int64 {
	if dash := strings.IndexByte(id, '-'); dash > 0 {
		id = id[:dash]
	}
	return parseInt64(id, 0)
}

func parseInt64(raw string, fallback int64) int64 {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return value
}
