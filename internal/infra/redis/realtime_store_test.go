package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"aieas_backend/internal/domain"

	miniredis "github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

func TestAuctionRealtimeStorePlaceBidWritesStreamAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10001)
	now := time.UnixMilli(1700000000000).UTC()
	mustInitRunningAuction(t, ctx, client, keys, auctionID, now)
	if err := client.SAdd(ctx, keys.AuctionEnrolled(auctionID), "u_1001").Err(); err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if err := client.SAdd(ctx, keys.AuctionDeposits(auctionID), "u_1001").Err(); err != nil {
		t.Fatalf("deposit: %v", err)
	}

	result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "req-1", AuctionID: auctionID, BidderID: "u_1001", Price: 1100, Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("place accepted bid: %v", err)
	}
	if !result.Accepted || result.Seq != 1 || result.StreamID != "1-0" || result.Event != "bid.accepted" {
		t.Fatalf("unexpected accepted result: %+v", result)
	}
	entries, err := client.XRange(ctx, keys.AuctionStream(auctionID), "-", "+").Result()
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "1-0" || entries[0].Values["event_type"] != "bid.accepted" || entries[0].Values["seq"] != "1" {
		t.Fatalf("unexpected accepted stream entries: %+v", entries)
	}

	duplicate, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "req-1", AuctionID: auctionID, BidderID: "u_1001", Price: 1500, Now: now.Add(time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("duplicate bid: %v", err)
	}
	if duplicate.Seq != result.Seq || duplicate.StreamID != result.StreamID || duplicate.Price != result.Price {
		t.Fatalf("expected duplicate original seq/stream/price, first=%+v duplicate=%+v", result, duplicate)
	}
	entries, _ = client.XRange(ctx, keys.AuctionStream(auctionID), "-", "+").Result()
	if len(entries) != 1 {
		t.Fatalf("idempotent retry should not append another stream entry: %+v", entries)
	}
}

func TestAuctionRealtimeStoreRejectedBidWritesStreamAndReplayGap(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10002)
	now := time.UnixMilli(1700000000000).UTC()
	mustInitRunningAuction(t, ctx, client, keys, auctionID, now)

	rejected, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "req-reject", AuctionID: auctionID, BidderID: "u_missing", Price: 1100, Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("place rejected bid: %v", err)
	}
	if rejected.Accepted || rejected.Reason != "NOT_ENROLLED" || rejected.Seq != 1 || rejected.StreamID != "1-0" || rejected.Event != "bid.rejected" {
		t.Fatalf("unexpected rejected result: %+v", rejected)
	}

	log := NewEventLog(client, keys)
	events, complete, err := log.ReplayBidEvents(ctx, auctionID, 0, 10)
	if err != nil || !complete || len(events) != 1 || events[0].Seq != 1 || events[0].EventType != "bid.rejected" || events[0].RejectReason != "NOT_ENROLLED" {
		t.Fatalf("unexpected replay events=%+v complete=%v err=%v", events, complete, err)
	}
	gapAuctionID := uint64(10003)
	if err := client.XAdd(ctx, &redisgo.XAddArgs{Stream: keys.AuctionStream(gapAuctionID), ID: "4-0", Values: map[string]interface{}{"auction_id": gapAuctionID, "seq": 4, "event_type": "bid.accepted"}}).Err(); err != nil {
		t.Fatalf("seed gap stream: %v", err)
	}
	if events, complete, err = log.ReplayBidEvents(ctx, gapAuctionID, 2, 10); err != nil || complete || len(events) != 0 {
		t.Fatalf("expected incomplete replay gap, events=%+v complete=%v err=%v", events, complete, err)
	}
}

func TestAuctionRealtimeStoreTieredIncrementRule(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	auctionID := uint64(10004)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:     auctionID,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		StartTime:     now.Add(-time.Minute),
		EndTime:       now.Add(time.Hour),
		IncrementRule: json.RawMessage(`{"type":"ladder","steps":[{"min":0,"max":5000,"amount":500},{"min":5000,"max":10000,"amount":800},{"min":10000,"amount":1000}]}`),
	}
	if _, err := store.InitAuction(ctx, auction, 100); err != nil {
		t.Fatalf("init auction: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark enrollment: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1002"); err != nil {
		t.Fatalf("mark enrollment 2: %v", err)
	}

	lowFirstTier, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "tier-low-1", AuctionID: auctionID, BidderID: "u_1001", Price: 1400, Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("first tier low bid: %v", err)
	}
	if lowFirstTier.Accepted || lowFirstTier.Reason != "BELOW_MIN_INCREMENT" {
		t.Fatalf("expected first tier rejection, got %+v", lowFirstTier)
	}
	firstTier, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "tier-ok-1", AuctionID: auctionID, BidderID: "u_1001", Price: 1500, Now: now.Add(time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !firstTier.Accepted || firstTier.CurrentPrice != 1500 {
		t.Fatalf("expected first tier accept, result=%+v err=%v", firstTier, err)
	}
	jump, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "tier-jump", AuctionID: auctionID, BidderID: "u_1002", Price: 5000, Now: now.Add(2 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !jump.Accepted || jump.CurrentPrice != 5000 {
		t.Fatalf("expected jump accept, result=%+v err=%v", jump, err)
	}
	lowSecondTier, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "tier-low-2", AuctionID: auctionID, BidderID: "u_1001", Price: 5700, Now: now.Add(3 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("second tier low bid: %v", err)
	}
	if lowSecondTier.Accepted || lowSecondTier.Reason != "BELOW_MIN_INCREMENT" {
		t.Fatalf("expected second tier rejection, got %+v", lowSecondTier)
	}
	secondTier, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "tier-ok-2", AuctionID: auctionID, BidderID: "u_1001", Price: 5800, Now: now.Add(4 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !secondTier.Accepted || secondTier.CurrentPrice != 5800 {
		t.Fatalf("expected second tier accept, result=%+v err=%v", secondTier, err)
	}
}

func newMiniredisStore(t *testing.T) (*AuctionRealtimeStore, *redisgo.Client, KeyBuilder) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys := NewKeyBuilder("test")
	scripts := NewScriptRegistry(client, DefaultScripts())
	if err := scripts.LoadAll(context.Background()); err != nil {
		t.Fatalf("load scripts: %v", err)
	}
	return NewAuctionRealtimeStore(client, scripts, keys), client, keys
}

func mustInitRunningAuction(t *testing.T, ctx context.Context, client *redisgo.Client, keys KeyBuilder, auctionID uint64, now time.Time) {
	t.Helper()
	if err := client.HSet(ctx, keys.AuctionState(auctionID),
		"auction_id", auctionID,
		"status", string(domain.AuctionStatusRunning),
		"current_price", 1000,
		"leader_bidder_id", "",
		"start_ts_ms", now.Add(-time.Minute).UnixMilli(),
		"end_ts_ms", now.Add(time.Hour).UnixMilli(),
		"last_bid_ts_ms", 0,
		"extend_count", 0,
		"version", 1,
		"min_increment", 100,
	).Err(); err != nil {
		t.Fatalf("init auction state: %v", err)
	}
}
