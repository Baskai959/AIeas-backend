package redis

import (
	"context"
	"encoding/json"
	"strconv"
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

	log := NewEventLog(NewShardedRTClientFromShards([]*RedisRTClient{{Client: client}}), keys)
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

func TestAuctionRealtimeStoreFixedRuleValidatesStepAndCap(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	auctionID := uint64(10004)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:     auctionID,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		ReservePrice:  1000,
		CapPrice:      2000,
		StartTime:     now.Add(-time.Minute),
		EndTime:       now.Add(time.Hour),
		IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":3}`),
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

	mismatch, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-mismatch", AuctionID: auctionID, BidderID: "u_1001", Price: 1150, Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("step mismatch bid: %v", err)
	}
	if mismatch.Accepted || mismatch.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected step mismatch rejection, got %+v", mismatch)
	}
	tooHigh, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-too-high", AuctionID: auctionID, BidderID: "u_1001", Price: 1500, Now: now.Add(time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("too high bid: %v", err)
	}
	if tooHigh.Accepted || tooHigh.Reason != domain.BidRejectAboveMaxBidSteps {
		t.Fatalf("expected max steps rejection, got %+v", tooHigh)
	}
	okBid, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-ok", AuctionID: auctionID, BidderID: "u_1001", Price: 1300, Now: now.Add(2 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !okBid.Accepted || okBid.CurrentPrice != 1300 {
		t.Fatalf("expected fixed bid accept, result=%+v err=%v", okBid, err)
	}
	aboveCap, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-above-cap", AuctionID: auctionID, BidderID: "u_1002", Price: 2100, Now: now.Add(3 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("above cap bid: %v", err)
	}
	if aboveCap.Accepted || aboveCap.Reason != domain.BidRejectAboveCapPrice {
		t.Fatalf("expected cap rejection, got %+v", aboveCap)
	}
	capBid, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-cap", AuctionID: auctionID, BidderID: "u_1002", Price: 1600, Now: now.Add(4 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !capBid.Accepted || capBid.AutoClosed {
		t.Fatalf("expected non-cap bid accept, result=%+v err=%v", capBid, err)
	}
}

func TestAuctionRealtimeStoreLadderRuleUsesCurrentPriceBand(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	auctionID := uint64(10014)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:     auctionID,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		ReservePrice:  1000,
		CapPrice:      5000,
		StartTime:     now.Add(-time.Minute),
		EndTime:       now.Add(time.Hour),
		IncrementRule: json.RawMessage(`{"type":"ladder","maxBidSteps":3,"steps":[{"min":0,"max":2000,"amount":100},{"min":2000,"amount":500}]}`),
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
	for i, price := range []int64{1300, 1600, 1900, 2200} {
		result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "ladder-ok-" + strconv.Itoa(i), AuctionID: auctionID, BidderID: "u_1001", Price: price, Now: now.Add(time.Duration(i) * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
		if err != nil || !result.Accepted || result.CurrentPrice != price {
			t.Fatalf("expected ladder bid %d accepted, result=%+v err=%v", price, result, err)
		}
	}
	mismatch, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "ladder-mismatch", AuctionID: auctionID, BidderID: "u_1002", Price: 2300, Now: now.Add(5 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("ladder mismatch bid: %v", err)
	}
	if mismatch.Accepted || mismatch.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected ladder step mismatch, got %+v", mismatch)
	}
	okBid, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "ladder-second-band-ok", AuctionID: auctionID, BidderID: "u_1002", Price: 3700, Now: now.Add(6 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !okBid.Accepted || okBid.CurrentPrice != 3700 {
		t.Fatalf("expected second band bid accept, result=%+v err=%v", okBid, err)
	}
}

func TestAuctionRealtimeStoreCapBidAutoCloses(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	auctionID := uint64(10005)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:     auctionID,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		ReservePrice:  1000,
		CapPrice:      1950,
		StartTime:     now.Add(-time.Minute),
		EndTime:       now.Add(time.Hour),
		IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
	}
	if _, err := store.InitAuction(ctx, auction, 100); err != nil {
		t.Fatalf("init auction: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark enrollment: %v", err)
	}
	result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "cap-ok", AuctionID: auctionID, BidderID: "u_1001", Price: 1950, Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("cap bid: %v", err)
	}
	if !result.Accepted || !result.AutoClosed || result.AuctionStatus != domain.AuctionStatusClosedWon {
		t.Fatalf("expected accepted auto close, got %+v", result)
	}
	state, ok, err := store.GetAuctionState(ctx, auctionID)
	if err != nil || !ok || state.Status != domain.AuctionStatusClosedWon || state.CurrentPrice != 1950 {
		t.Fatalf("expected closed redis state, state=%+v ok=%v err=%v", state, ok, err)
	}
}

func TestAuctionRealtimeStoreRejectsStaleExpectedState(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	auctionID := uint64(10007)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:     auctionID,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		ReservePrice:  1000,
		CapPrice:      2000,
		StartTime:     now.Add(-time.Minute),
		EndTime:       now.Add(time.Hour),
		IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
	}
	if _, err := store.InitAuction(ctx, auction, 100); err != nil {
		t.Fatalf("init auction: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark enrollment: %v", err)
	}
	expected := int64(900)
	result, err := store.PlaceBid(ctx, domain.BidInput{
		RequestID:            "stale-bid",
		AuctionID:            auctionID,
		BidderID:             "u_1001",
		Price:                1100,
		ExpectedCurrentPrice: &expected,
		Now:                  now,
		Source:               "live_ws",
		MinIncrement:         100,
		IdempotencyTTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("stale bid: %v", err)
	}
	if result.Accepted || result.Reason != domain.BidRejectStaleAuctionState || result.CurrentPrice != 1000 {
		t.Fatalf("expected stale state rejection, got %+v", result)
	}
}

func TestAuctionRealtimeStoreAntiExtendResetMode(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	auctionID := uint64(10006)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:      auctionID,
		Status:         domain.AuctionStatusRunning,
		StartPrice:     1000,
		ReservePrice:   1000,
		CapPrice:       5000,
		StartTime:      now.Add(-time.Minute),
		EndTime:        now.Add(10 * time.Second),
		IncrementRule:  json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		AntiExtendMode: domain.AuctionExtendModeReset,
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
	}
	if _, err := store.InitAuction(ctx, auction, 100); err != nil {
		t.Fatalf("init auction: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark enrollment: %v", err)
	}
	result, err := store.PlaceBid(ctx, domain.BidInput{
		RequestID:      "reset-extend",
		AuctionID:      auctionID,
		BidderID:       "u_1001",
		Price:          1100,
		Now:            now,
		Source:         "live_ws",
		MinIncrement:   100,
		AntiSnipingMS:  15 * 1000,
		AntiExtendMS:   30 * 1000,
		AntiExtendMode: domain.AuctionExtendModeReset,
		MaxExtendCount: 10,
		IdempotencyTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("reset extend bid: %v", err)
	}
	if !result.Accepted || !result.Extended || !result.EndTime.Equal(now.Add(30*time.Second)) {
		t.Fatalf("expected reset-mode extension to now+30s, got %+v", result)
	}
}

func newMiniredisStore(t *testing.T) (*AuctionRealtimeStore, *redisgo.Client, KeyBuilder) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys := NewKeyBuilder("test")
	sharded := NewShardedRTClientFromShards([]*RedisRTClient{{Client: client}})
	scripts := NewShardedScriptRegistry(sharded, DefaultScripts())
	if err := scripts.LoadAll(context.Background()); err != nil {
		t.Fatalf("load scripts: %v", err)
	}
	return NewAuctionRealtimeStore(sharded, scripts, keys), client, keys
}

func mustInitRunningAuction(t *testing.T, ctx context.Context, client *redisgo.Client, keys KeyBuilder, auctionID uint64, now time.Time) {
	t.Helper()
	if err := client.HSet(ctx, keys.AuctionState(auctionID),
		"auction_id", auctionID,
		"status", string(domain.AuctionStatusRunning),
		"current_price", 1000,
		"start_price", 1000,
		"cap_price", 2000,
		"leader_bidder_id", "",
		"start_ts_ms", now.Add(-time.Minute).UnixMilli(),
		"end_ts_ms", now.Add(time.Hour).UnixMilli(),
		"last_bid_ts_ms", 0,
		"extend_count", 0,
		"version", 1,
		"min_increment", 100,
		"increment_amount", 100,
		"max_bid_steps", 10,
		"increment_rule", `{"type":"fixed","amount":100,"maxBidSteps":10}`,
	).Err(); err != nil {
		t.Fatalf("init auction state: %v", err)
	}
}
