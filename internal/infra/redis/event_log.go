package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"

	redisgo "github.com/redis/go-redis/v9"
)

const BidRecordConsumerGroup = "bid-record-writers"
const BidKafkaBridgeConsumerGroup = "bid-kafka-bridge"
const BidRankingConsumerGroup = "bid-ranking-updaters"

var updateAcceptedRankingScript = redisgo.NewScript(`
local ranking_key = KEYS[1]
local user_bids_key = KEYS[2]
local user_seq_key = KEYS[3]

local bidder_id = ARGV[1]
local price = tonumber(ARGV[2])
local bid_ts_ms = tonumber(ARGV[3])
local seq = tonumber(ARGV[4])
local new_member = ARGV[5]

if bidder_id == nil or bidder_id == '' or price == nil or price <= 0 then
	return redis.error_reply('invalid ranking input')
end

local old_member = redis.call('HGET', user_bids_key, bidder_id)
if old_member ~= false and old_member ~= nil and old_member ~= '' then
	local old_seq = tonumber(redis.call('HGET', user_seq_key, bidder_id)) or 0
	local old_price_raw, old_inverted_raw = string.match(old_member, '^(%d+):(%d+):')
	local old_price = tonumber(old_price_raw) or 0
	local old_inverted = tonumber(old_inverted_raw) or 9999999999999
	local old_bid_ts_ms = 9999999999999 - old_inverted

	local newer = false
	if price ~= old_price then
		newer = price > old_price
	elseif bid_ts_ms ~= old_bid_ts_ms then
		newer = bid_ts_ms > old_bid_ts_ms
	else
		newer = seq > old_seq
	end

	if not newer then
		return 0
	end
	redis.call('ZREM', ranking_key, old_member)
end

redis.call('ZADD', ranking_key, 0, new_member)
redis.call('HSET', user_bids_key, bidder_id, new_member)
redis.call('HSET', user_seq_key, bidder_id, seq)
return 1
`)

type BidEvent struct {
	AuctionID       uint64
	LiveSessionID   uint64
	StreamID        string
	Seq             int64
	RequestID       string
	BidderID        string
	BidderNickname  string
	BidderAvatarURL string
	BidPrice        int64
	BidTSMS         int64
	Source          string
	RiskResult      domain.BidRiskResult
	RejectReason    string
	Accepted        bool
	CurrentPrice    int64
	LeaderBidderID  string
	EndTSMS         int64
	Extended        bool
	ExtendCount     int
	CreatedAtMS     int64
	EventType       string
	AuctionStatus   domain.AuctionStatus
	AutoClosed      bool
	Deliveries      int64
	TraceParent     string
	TraceState      string
	Raw             map[string]string
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
	sharded        *ShardedRTClient
	rankingSharded *ShardedRTClient
	keys           KeyBuilder

	ensuredGroups sync.Map
}

type ensureGroupKey struct {
	shardIdx  int
	auctionID uint64
	group     string
}

func NewEventLog(sharded *ShardedRTClient, keys KeyBuilder) *EventLog {
	return &EventLog{sharded: sharded, rankingSharded: sharded, keys: keys}
}

// SetRankingShardedRT 注入专用于异步排行榜更新的 Redis client/pool。
// Stream 消费和 ACK 仍然使用 EventLog 的 worker RT pool。
func (l *EventLog) SetRankingShardedRT(sharded *ShardedRTClient) {
	if l != nil && sharded != nil {
		l.rankingSharded = sharded
	}
}

func (l *EventLog) Enabled() bool { return l != nil && l.sharded != nil && l.sharded.Len() > 0 }

func (l *EventLog) shardForAuction(auctionID uint64) *RedisRTClient {
	if !l.Enabled() {
		return nil
	}
	return l.sharded.ForAuction(auctionID)
}

func (l *EventLog) rankingShardForAuction(auctionID uint64) *RedisRTClient {
	if l == nil {
		return nil
	}
	if l.rankingSharded != nil {
		return l.rankingSharded.ForAuction(auctionID)
	}
	if l.sharded != nil {
		return l.sharded.ForAuction(auctionID)
	}
	return nil
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
	return l.EnsureConsumerGroup(ctx, auctionID, BidRecordConsumerGroup)
}

func (l *EventLog) EnsureConsumerGroup(ctx context.Context, auctionID uint64, group string) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	group = strings.TrimSpace(group)
	if group == "" {
		return fmt.Errorf("redis event log consumer group is required")
	}
	shardIdx := l.sharded.IndexAuction(auctionID)
	cacheKey := ensureGroupKey{shardIdx: shardIdx, auctionID: auctionID, group: group}
	if _, ok := l.ensuredGroups.Load(cacheKey); ok {
		return nil
	}
	client := l.sharded.ForIndex(shardIdx)
	err := client.XGroupCreateMkStream(ctx, l.keys.AuctionStream(auctionID), group, "0-0").Err()
	if err == nil || isBusyGroupErr(err) {
		l.ensuredGroups.Store(cacheKey, struct{}{})
		return nil
	}
	return err
}

func isBusyGroupErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP")
}

func isNoGroupErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToUpper(err.Error()), "NOGROUP")
}

func (l *EventLog) invalidateEnsuredGroup(auctionID uint64, group string) {
	if !l.Enabled() {
		return
	}
	shardIdx := l.sharded.IndexAuction(auctionID)
	l.ensuredGroups.Delete(ensureGroupKey{shardIdx: shardIdx, auctionID: auctionID, group: group})
}

func (l *EventLog) ReadBidRecordGroup(ctx context.Context, auctionID uint64, consumer string, count int64, block time.Duration) ([]BidEvent, error) {
	return l.ReadConsumerGroup(ctx, auctionID, BidRecordConsumerGroup, consumer, count, block)
}

func (l *EventLog) ReadConsumerGroup(ctx context.Context, auctionID uint64, group, consumer string, count int64, block time.Duration) ([]BidEvent, error) {
	if err := l.EnsureConsumerGroup(ctx, auctionID, group); err != nil {
		return nil, err
	}
	client := l.shardForAuction(auctionID)
	streams, err := client.XReadGroup(ctx, &redisgo.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{l.keys.AuctionStream(auctionID), ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if err == redisgo.Nil {
			return nil, nil
		}
		if isNoGroupErr(err) {
			l.invalidateEnsuredGroup(auctionID, group)
			if err := l.EnsureConsumerGroup(ctx, auctionID, group); err != nil {
				return nil, err
			}
			streams, err = client.XReadGroup(ctx, &redisgo.XReadGroupArgs{
				Group:    group,
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
		return nil, err
	}
	return bidEventsFromStreams(streams, 0), nil
}

func (l *EventLog) ClaimStaleBidRecordEvents(ctx context.Context, auctionID uint64, consumer string, minIdle time.Duration, max int64) ([]BidEvent, error) {
	return l.ClaimStaleConsumerEvents(ctx, auctionID, BidRecordConsumerGroup, consumer, minIdle, max)
}

func (l *EventLog) ClaimStaleConsumerEvents(ctx context.Context, auctionID uint64, group, consumer string, minIdle time.Duration, max int64) ([]BidEvent, error) {
	if !l.Enabled() {
		return nil, fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	pending, err := client.XPendingExt(ctx, &redisgo.XPendingExtArgs{
		Stream: l.keys.AuctionStream(auctionID), Group: group, Start: "-", End: "+", Count: max,
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
		Stream: l.keys.AuctionStream(auctionID), Group: group, Consumer: consumer, MinIdle: minIdle, Messages: ids,
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
	return l.AckConsumerGroup(ctx, auctionID, BidRecordConsumerGroup, ids...)
}

func (l *EventLog) AckConsumerGroup(ctx context.Context, auctionID uint64, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	client := l.shardForAuction(auctionID)
	return client.XAck(ctx, l.keys.AuctionStream(auctionID), group, ids...).Err()
}

// TrimAuctionStream 对单个拍卖的 bid stream 做近似长度裁剪。
// 裁剪从 bid.lua 主原子段移到后台 worker，避免每次 accepted bid 都在热 Lua 里执行 MAXLEN。
func (l *EventLog) TrimAuctionStream(ctx context.Context, auctionID uint64, maxLen int64) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	if maxLen <= 0 {
		return nil
	}
	client := l.shardForAuction(auctionID)
	return client.XTrimMaxLenApprox(ctx, l.keys.AuctionStream(auctionID), maxLen, 0).Err()
}

// UpdateAcceptedRanking 更新拍卖的 ranking ZSet 与 user_bids HASH。
// 使用 Lua 原子脚本 + (price, bidTSMS, seq) 三元组比较防乱序：仅当新事件比旧事件
// 严格"靠后"时才替换（旧 / 同 / 更早事件 → no-op，返回 nil），避免 WATCH 在
// 热点拍卖 user_bids / user_latest_seq 整个 hash key 上产生事务冲突。
// seq 单独存于 user_latest_seq hash，不入 ranking_member 编码，hammer.lua 零改动。
// 失败由调用方决定是否致命；本方法不会自己写日志/指标。
func (l *EventLog) UpdateAcceptedRanking(ctx context.Context, auctionID uint64, bidderID string, price, bidTSMS, seq int64) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	if bidderID == "" || price <= 0 {
		return fmt.Errorf("redis event log: invalid ranking input")
	}
	client := l.rankingShardForAuction(auctionID)
	if client == nil {
		return fmt.Errorf("redis event log: ranking store is not configured")
	}
	rankingKey := l.keys.AuctionBids(auctionID)
	userBidsKey := l.keys.AuctionUserBids(auctionID)
	userSeqKey := l.keys.AuctionUserLatestSeq(auctionID)
	newMember := FormatRankingMember(price, bidTSMS, bidderID)
	return updateAcceptedRankingScript.Run(ctx, client, []string{rankingKey, userBidsKey, userSeqKey},
		bidderID, price, bidTSMS, seq, newMember,
	).Err()
}

// rankingNewerThan 在三元组 (price, bidTSMS, seq) 上比较：新事件比旧事件严格"靠后"
// 才返回 true。次序：price 大者优先；相同则 bidTSMS 晚者优先；都相同则 seq 大者优先。
func rankingNewerThan(newPrice, newTS, newSeq, oldPrice, oldTS, oldSeq int64) bool {
	if newPrice != oldPrice {
		return newPrice > oldPrice
	}
	if newTS != oldTS {
		return newTS > oldTS
	}
	return newSeq > oldSeq
}

// parseRankingMember 解析 FormatRankingMember 编码：%019d:%013d:%s。
// 第二段是 9999999999999-bidTSMS（用于 ZSet 排序），还原回 bidTSMS。
func parseRankingMember(member string) (price, bidTSMS int64) {
	if member == "" {
		return 0, 0
	}
	parts := strings.SplitN(member, ":", 3)
	if len(parts) < 2 {
		return 0, 0
	}
	price = parseInt64(parts[0], 0)
	inverted := parseInt64(parts[1], 0)
	bidTSMS = int64(9999999999999) - inverted
	return price, bidTSMS
}

// FormatRankingMember 与 bid.lua 历史实现保持字节一致：
//
//	%019d:%013d:%s   (price, 9999999999999-bidTSMS, bidderID)
//
// 这是 ZSet 排序的 source of truth；hammer.lua 也按这一格式解析。
func FormatRankingMember(price, bidTSMS int64, bidderID string) string {
	return fmt.Sprintf("%019d:%013d:%s", price, int64(9999999999999)-bidTSMS, bidderID)
}

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
	var liveSessionID *uint64
	if e.LiveSessionID != 0 {
		id := e.LiveSessionID
		liveSessionID = &id
	}
	return domain.BidRecord{RequestID: e.RequestID, AuctionID: e.AuctionID, LiveSessionID: liveSessionID, BidderID: e.BidderID, BidderNickname: e.BidderNickname, BidPrice: e.BidPrice, BidTSMS: e.BidTSMS, Source: e.Source, RiskResult: e.RiskResult, RejectReason: e.RejectReason, CreatedAt: time.UnixMilli(e.CreatedAtMS).UTC()}
}

func (e BidEvent) PayloadJSON() []byte {
	payload := map[string]interface{}{
		"requestId": e.RequestID, "auctionId": e.AuctionID, "liveSessionId": e.LiveSessionID, "bidderId": e.BidderID, "bidderNickname": e.BidderNickname,
		"nickname": e.BidderNickname, "bidderAvatarUrl": e.BidderAvatarURL, "avatarUrl": e.BidderAvatarURL, "price": e.BidPrice,
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
		AuctionID: auctionID, LiveSessionID: uint64(parseInt64(raw["live_session_id"], 0)), StreamID: msg.ID, Seq: seq, RequestID: raw["request_id"], BidderID: raw["bidder_id"], BidderNickname: raw["bidder_nickname"], BidderAvatarURL: raw["bidder_avatar_url"],
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
