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
	Deliveries     int64
	Raw            map[string]string
}

type EventLog struct {
	client *redisgo.Client
	keys   KeyBuilder
}

func NewEventLog(client *redisgo.Client, keys KeyBuilder) *EventLog {
	return &EventLog{client: client, keys: keys}
}

func (l *EventLog) Enabled() bool { return l != nil && l.client != nil }

func (l *EventLog) ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]BidEvent, bool, error) {
	if !l.Enabled() {
		return nil, false, fmt.Errorf("redis event log is not configured")
	}
	if limit <= 0 {
		limit = 256
	}
	stream := l.keys.AuctionStream(auctionID)
	start := fmt.Sprintf("%d-1", lastSeq)
	entries, err := l.client.XRangeN(ctx, stream, start, "+", limit).Result()
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

func (l *EventLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	if !l.Enabled() {
		return nil, fmt.Errorf("redis event log is not configured")
	}
	members, err := l.client.SMembers(ctx, l.keys.ActiveStreams()).Result()
	if err != nil {
		return nil, err
	}
	auctions := make([]uint64, 0, len(members))
	for _, member := range members {
		auctionID, err := strconv.ParseUint(member, 10, 64)
		if err == nil && auctionID > 0 {
			auctions = append(auctions, auctionID)
		}
	}
	return auctions, nil
}

func (l *EventLog) EnsureBidRecordGroup(ctx context.Context, auctionID uint64) error {
	if !l.Enabled() {
		return fmt.Errorf("redis event log is not configured")
	}
	err := l.client.XGroupCreateMkStream(ctx, l.keys.AuctionStream(auctionID), BidRecordConsumerGroup, "0-0").Err()
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return err
	}
	return nil
}

func (l *EventLog) ReadBidRecordGroup(ctx context.Context, auctionID uint64, consumer string, count int64, block time.Duration) ([]BidEvent, error) {
	if err := l.EnsureBidRecordGroup(ctx, auctionID); err != nil {
		return nil, err
	}
	streams, err := l.client.XReadGroup(ctx, &redisgo.XReadGroupArgs{
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
	pending, err := l.client.XPendingExt(ctx, &redisgo.XPendingExtArgs{
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
	claimed, err := l.client.XClaim(ctx, &redisgo.XClaimArgs{
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
	return l.client.XAck(ctx, l.keys.AuctionStream(auctionID), BidRecordConsumerGroup, ids...).Err()
}

func (l *EventLog) WriteBidRecordDLQ(ctx context.Context, event BidEvent, reason string) error {
	values := map[string]interface{}{
		"auction_id": event.AuctionID, "stream_id": event.StreamID, "seq": event.Seq,
		"request_id": event.RequestID, "event_type": event.EventType, "reason": reason,
		"created_at_ms": time.Now().UTC().UnixMilli(),
	}
	return l.client.XAdd(ctx, &redisgo.XAddArgs{Stream: l.keys.BidRecordDLQ(), Values: values}).Err()
}

func (l *EventLog) ReconcileCheckpoint(ctx context.Context, auctionID uint64) (int64, error) {
	value, err := l.client.Get(ctx, l.keys.BidRecordReconcileCheckpoint(auctionID)).Result()
	if err == redisgo.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func (l *EventLog) SetReconcileCheckpoint(ctx context.Context, auctionID uint64, seq int64) error {
	return l.client.Set(ctx, l.keys.BidRecordReconcileCheckpoint(auctionID), seq, 0).Err()
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
		"event": e.EventType, "riskResult": e.RiskResult,
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
		CreatedAtMS: parseInt64(raw["created_at_ms"], 0), EventType: raw["event_type"], Deliveries: deliveries, Raw: raw,
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
