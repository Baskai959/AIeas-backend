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

	result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "req-1", AuctionID: auctionID, BidderID: "u_1001", Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("place accepted bid: %v", err)
	}
	if !result.Accepted || result.Seq != 1 || result.StreamID != "1-0" || result.Event != "bid.accepted" {
		t.Fatalf("unexpected accepted result: %+v", result)
	}
	state, ok, err := store.GetAuctionState(ctx, auctionID)
	if err != nil || !ok {
		t.Fatalf("read state: ok=%v err=%v", ok, err)
	}
	if state.BidCount != 1 || state.CurrentPrice != 1100 {
		t.Fatalf("unexpected realtime state after bid: %+v", state)
	}
	if state.StartPrice != 1000 || state.CapPrice != 2000 || string(state.IncrementRule) != `{"type":"fixed","amount":100,"maxBidSteps":10}` {
		t.Fatalf("realtime state should include pricing rule, got %+v", state)
	}
	entries, err := client.XRange(ctx, keys.AuctionStream(auctionID), "-", "+").Result()
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "1-0" || entries[0].Values["event_type"] != "bid.accepted" || entries[0].Values["seq"] != "1" {
		t.Fatalf("unexpected accepted stream entries: %+v", entries)
	}

	duplicate, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "req-1", AuctionID: auctionID, BidderID: "u_1001", Price: 1500, ExpectedCurrentPrice: expectedCurrentPrice(1100), Now: now.Add(time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("duplicate bid: %v", err)
	}
	if !duplicate.Duplicate || duplicate.Seq != result.Seq || duplicate.StreamID != result.StreamID || duplicate.Price != result.Price {
		t.Fatalf("expected duplicate original seq/stream/price, first=%+v duplicate=%+v", result, duplicate)
	}
	entries, _ = client.XRange(ctx, keys.AuctionStream(auctionID), "-", "+").Result()
	if len(entries) != 1 {
		t.Fatalf("idempotent retry should not append another stream entry: %+v", entries)
	}
}

func TestAuctionRealtimeStoreRejectedBidDoesNotWriteStreamAndReplayGap(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10002)
	now := time.UnixMilli(1700000000000).UTC()
	mustInitRunningAuction(t, ctx, client, keys, auctionID, now)
	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark enrollment: %v", err)
	}

	rejected, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "req-reject", AuctionID: auctionID, BidderID: "u_1001", Price: 1150, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("place rejected bid: %v", err)
	}
	if rejected.Accepted || rejected.Reason != domain.BidRejectStepMismatch || rejected.Seq != 0 || rejected.StreamID != "" || rejected.Event != "bid.rejected" {
		t.Fatalf("unexpected rejected result: %+v", rejected)
	}

	log := NewEventLog(NewShardedRTClientFromShards([]*RedisRTClient{{Client: client}}), keys)
	events, complete, err := log.ReplayBidEvents(ctx, auctionID, 0, 10)
	if err != nil || !complete || len(events) != 0 {
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

func TestAuctionRealtimeStoreMarkEnrollmentUpdatesParticipantCount(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10012)
	now := time.UnixMilli(1700000000000).UTC()
	mustInitRunningAuction(t, ctx, client, keys, auctionID, now)

	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark enrollment 1: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("mark duplicate enrollment: %v", err)
	}
	if err := store.MarkEnrollment(ctx, auctionID, "u_1002"); err != nil {
		t.Fatalf("mark enrollment 2: %v", err)
	}

	state, ok, err := store.GetAuctionState(ctx, auctionID)
	if err != nil || !ok {
		t.Fatalf("read state: ok=%v err=%v", ok, err)
	}
	if state.ParticipantCount != 2 {
		t.Fatalf("expected 2 participants after unique enrollments, got %+v", state)
	}
	enrolled, depositReady, err := store.BidPrerequisites(ctx, auctionID, "u_1002")
	if err != nil {
		t.Fatalf("bid prerequisites: %v", err)
	}
	if !enrolled || !depositReady {
		t.Fatalf("expected u_1002 ready, enrolled=%v depositReady=%v", enrolled, depositReady)
	}
}

func TestAuctionRealtimeStoreInitAuctionRestoresParticipantCountFromEnrollmentSet(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10013)
	now := time.UnixMilli(1700000000000).UTC()
	if err := client.SAdd(ctx, keys.AuctionEnrolled(auctionID), "u_1001", "u_1002").Err(); err != nil {
		t.Fatalf("seed enrollment set: %v", err)
	}
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
	state, err := store.InitAuction(ctx, auction, 100)
	if err != nil {
		t.Fatalf("init auction: %v", err)
	}
	if state.ParticipantCount != 2 {
		t.Fatalf("expected init state participant count from enrollment set, got %+v", state)
	}
	stored, ok, err := store.GetAuctionState(ctx, auctionID)
	if err != nil || !ok {
		t.Fatalf("read state: ok=%v err=%v", ok, err)
	}
	if stored.ParticipantCount != 2 {
		t.Fatalf("expected stored participant count 2, got %+v", stored)
	}
}

func TestAuctionRealtimeStoreGetAuctionStateRepairsParticipantCount(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10014)
	now := time.UnixMilli(1700000000000).UTC()
	mustInitRunningAuction(t, ctx, client, keys, auctionID, now)
	if err := client.HSet(ctx, keys.AuctionState(auctionID), "participant_count", 0).Err(); err != nil {
		t.Fatalf("seed stale participant count: %v", err)
	}
	if err := client.SAdd(ctx, keys.AuctionEnrolled(auctionID), "u_1001", "u_1002").Err(); err != nil {
		t.Fatalf("seed enrollment set: %v", err)
	}

	state, ok, err := store.GetAuctionState(ctx, auctionID)
	if err != nil || !ok {
		t.Fatalf("read state: ok=%v err=%v", ok, err)
	}
	if state.ParticipantCount != 2 {
		t.Fatalf("expected repaired participant count 2, got %+v", state)
	}
	storedCount, err := client.HGet(ctx, keys.AuctionState(auctionID), "participant_count").Int()
	if err != nil {
		t.Fatalf("read stored participant count: %v", err)
	}
	if storedCount != 2 {
		t.Fatalf("expected stored participant count repaired to 2, got %d", storedCount)
	}
}

func TestAuctionRealtimeStoreHammerUsesStateLeaderWhenRankingIsAsync(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10003)
	now := time.UnixMilli(1700000000000).UTC()
	mustInitRunningAuction(t, ctx, client, keys, auctionID, now)

	result, err := store.PlaceBid(ctx, domain.BidInput{
		RequestID:            "hammer-state-bid",
		AuctionID:            auctionID,
		BidderID:             "u_1001",
		Price:                1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
		Now:                  now,
		Source:               "live_ws",
		MinIncrement:         100,
		IdempotencyTTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("place bid: %v", err)
	}
	if !result.Accepted {
		t.Fatalf("expected accepted bid, got %+v", result)
	}
	if zcard, err := client.ZCard(ctx, keys.AuctionBids(auctionID)).Result(); err != nil || zcard != 0 {
		t.Fatalf("ranking should be async and empty before worker update, zcard=%d err=%v", zcard, err)
	}

	closed, err := store.Hammer(ctx, domain.HammerInput{
		RequestID:      "hammer-state-close",
		AuctionID:      auctionID,
		Now:            now.Add(time.Second),
		IdempotencyTTL: time.Hour,
		ReservePrice:   1000,
		Force:          true,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedWon || closed.WinnerID != "u_1001" || closed.Price != 1100 {
		t.Fatalf("hammer should close from state leader/current price, got %+v", closed)
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

	mismatch, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-mismatch", AuctionID: auctionID, BidderID: "u_1001", Price: 1150, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("step mismatch bid: %v", err)
	}
	if mismatch.Accepted || mismatch.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected step mismatch rejection, got %+v", mismatch)
	}
	tooHigh, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-too-high", AuctionID: auctionID, BidderID: "u_1001", Price: 1500, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now.Add(time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("too high bid: %v", err)
	}
	if tooHigh.Accepted || tooHigh.Reason != domain.BidRejectAboveMaxBidSteps {
		t.Fatalf("expected max steps rejection, got %+v", tooHigh)
	}
	okBid, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-ok", AuctionID: auctionID, BidderID: "u_1001", Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now.Add(2 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !okBid.Accepted || okBid.CurrentPrice != 1300 {
		t.Fatalf("expected fixed bid accept, result=%+v err=%v", okBid, err)
	}
	aboveCap, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-above-cap", AuctionID: auctionID, BidderID: "u_1002", Price: 2100, ExpectedCurrentPrice: expectedCurrentPrice(1300), Now: now.Add(3 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("above cap bid: %v", err)
	}
	if aboveCap.Accepted || aboveCap.Reason != domain.BidRejectAboveCapPrice {
		t.Fatalf("expected cap rejection, got %+v", aboveCap)
	}
	capBid, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "fixed-cap", AuctionID: auctionID, BidderID: "u_1002", Price: 1600, ExpectedCurrentPrice: expectedCurrentPrice(1300), Now: now.Add(4 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
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
	currentPrice := int64(1000)
	for i, price := range []int64{1300, 1600, 1900, 2200} {
		result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "ladder-ok-" + strconv.Itoa(i), AuctionID: auctionID, BidderID: "u_1001", Price: price, ExpectedCurrentPrice: expectedCurrentPrice(currentPrice), Now: now.Add(time.Duration(i) * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
		if err != nil || !result.Accepted || result.CurrentPrice != price {
			t.Fatalf("expected ladder bid %d accepted, result=%+v err=%v", price, result, err)
		}
		currentPrice = price
	}
	mismatch, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "ladder-mismatch", AuctionID: auctionID, BidderID: "u_1002", Price: 2300, ExpectedCurrentPrice: expectedCurrentPrice(currentPrice), Now: now.Add(5 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("ladder mismatch bid: %v", err)
	}
	if mismatch.Accepted || mismatch.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected ladder step mismatch, got %+v", mismatch)
	}
	okBid, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "ladder-second-band-ok", AuctionID: auctionID, BidderID: "u_1002", Price: 3700, ExpectedCurrentPrice: expectedCurrentPrice(currentPrice), Now: now.Add(6 * time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
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
	result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "cap-ok", AuctionID: auctionID, BidderID: "u_1001", Price: 1950, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
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
	expected := int64(1100)
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
		RequestID:            "reset-extend",
		AuctionID:            auctionID,
		BidderID:             "u_1001",
		Price:                1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
		Now:                  now,
		Source:               "live_ws",
		MinIncrement:         100,
		AntiSnipingMS:        15 * 1000,
		AntiExtendMS:         30 * 1000,
		AntiExtendMode:       domain.AuctionExtendModeReset,
		MaxExtendCount:       10,
		IdempotencyTTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("reset extend bid: %v", err)
	}
	if !result.Accepted || !result.Extended || !result.EndTime.Equal(now.Add(30*time.Second)) {
		t.Fatalf("expected reset-mode extension to now+30s, got %+v", result)
	}
}

func TestAuctionRealtimeStoreInitAuctionRegistersActiveStream(t *testing.T) {
	ctx := context.Background()
	store, client, keys := newMiniredisStore(t)
	auctionID := uint64(10100)
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
	members, err := client.SMembers(ctx, keys.ActiveStreams()).Result()
	if err != nil {
		t.Fatalf("smembers active streams: %v", err)
	}
	want := strconv.FormatUint(auctionID, 10)
	found := false
	for _, m := range members {
		if m == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected active_streams to contain %s after InitAuction, got %v", want, members)
	}
	stateValues, err := client.HGetAll(ctx, keys.AuctionState(auctionID)).Result()
	if err != nil {
		t.Fatalf("hgetall state: %v", err)
	}
	if stateValues["increment_rule_type"] != "fixed" {
		t.Fatalf("expected increment_rule_type=fixed, got %q", stateValues["increment_rule_type"])
	}
	if stateValues["increment_fixed_amount"] != "100" {
		t.Fatalf("expected increment_fixed_amount=100, got %q", stateValues["increment_fixed_amount"])
	}
}

func TestAuctionRealtimeStorePlaceBidPublishesRawPayload(t *testing.T) {
	ctx := context.Background()
	store, client, _ := newMiniredisStore(t)
	auctionID := uint64(10101)
	liveSessionID := uint64(90101)
	now := time.UnixMilli(1700000000000).UTC()
	auction := domain.AuctionLot{
		AuctionID:     auctionID,
		LiveSessionID: &liveSessionID,
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
	channel := "auction:" + strconv.FormatUint(auctionID, 10) + ":events"
	pubsub := client.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "pubsub-req", AuctionID: auctionID, BidderID: "u_1001", Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !result.Accepted {
		t.Fatalf("place bid: result=%+v err=%v", result, err)
	}
	if result.LiveSessionID != liveSessionID || result.CurrentPrice != 1100 {
		t.Fatalf("accepted result should include live session/current price, got %+v", result)
	}
	select {
	case msg := <-ch:
		if msg == nil {
			t.Fatalf("nil message from pubsub")
		}
		var payload struct {
			Event         string    `json:"event"`
			Seq           int64     `json:"seq"`
			AuctionID     uint64    `json:"auctionId"`
			LiveSessionID uint64    `json:"liveSessionId"`
			CurrentPrice  int64     `json:"currentPrice"`
			EndTime       string    `json:"endTime"`
			ServerTime    time.Time `json:"serverTime"`
		}
		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			t.Fatalf("unmarshal published payload: %v (%s)", err, msg.Payload)
		}
		if payload.Event != "bid.accepted" || payload.Seq != result.Seq || payload.AuctionID != auctionID {
			t.Fatalf("unexpected published payload: %+v raw=%s", payload, msg.Payload)
		}
		if payload.LiveSessionID != liveSessionID || payload.CurrentPrice != 1100 {
			t.Fatalf("published payload must include liveSessionId/currentPrice, got %+v raw=%s", payload, msg.Payload)
		}
		if payload.EndTime == "" {
			t.Fatalf("published payload must keep endTime field, raw=%s", msg.Payload)
		}
		if payload.ServerTime.IsZero() {
			t.Fatalf("published payload must include serverTime, raw=%s", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected published event on %s within timeout", channel)
	}
}

func TestAuctionRealtimeStorePlaceBidUsesDedicatedPublishClient(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newMiniredisStore(t)
	pubMR := miniredis.RunT(t)
	pubClient := redisgo.NewClient(&redisgo.Options{Addr: pubMR.Addr()})
	t.Cleanup(func() { _ = pubClient.Close() })
	store.SetPublishShardedRT(NewShardedRTClientFromShards([]*RedisRTClient{{Client: pubClient}}))

	auctionID := uint64(10103)
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

	channel := "auction:" + strconv.FormatUint(auctionID, 10) + ":events"
	pubsub := pubClient.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	result, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "pub-dedicated-req", AuctionID: auctionID, BidderID: "u_1001", Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !result.Accepted {
		t.Fatalf("place bid: result=%+v err=%v", result, err)
	}
	select {
	case msg := <-ch:
		if msg == nil || msg.Channel != channel {
			t.Fatalf("unexpected dedicated publish message: %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected published event on dedicated publish client")
	}
}

func TestAuctionRealtimeStoreTopNUsesDedicatedRankingClient(t *testing.T) {
	ctx := context.Background()
	store, mainClient, keys := newMiniredisStore(t)
	rankingMR := miniredis.RunT(t)
	rankingClient := redisgo.NewClient(&redisgo.Options{Addr: rankingMR.Addr()})
	t.Cleanup(func() { _ = rankingClient.Close() })
	store.SetRankingShardedRT(NewShardedRTClientFromShards([]*RedisRTClient{{Client: rankingClient}}))

	auctionID := uint64(10104)
	if err := mainClient.ZAdd(ctx, keys.AuctionBids(auctionID), redisgo.Z{Score: 0, Member: FormatRankingMember(9999, 1700000009000, "u_rt")}).Err(); err != nil {
		t.Fatalf("seed main ranking: %v", err)
	}
	wantMember := FormatRankingMember(1500, 1700000001000, "u_cache")
	if err := rankingClient.ZAdd(ctx, keys.AuctionBids(auctionID), redisgo.Z{Score: 0, Member: wantMember}).Err(); err != nil {
		t.Fatalf("seed dedicated ranking: %v", err)
	}

	top, err := store.TopN(ctx, auctionID, 10)
	if err != nil {
		t.Fatalf("topn: %v", err)
	}
	if len(top) != 1 || top[0].BidderID != "u_cache" || top[0].Price != 1500 {
		t.Fatalf("TopN should read dedicated ranking client, got %+v want member %s", top, wantMember)
	}
}

func TestAuctionRealtimeStorePlaceBidDuplicateDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	store, client, _ := newMiniredisStore(t)
	auctionID := uint64(10102)
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
	if _, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "dedup-req", AuctionID: auctionID, BidderID: "u_1001", Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000), Now: now, Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour}); err != nil {
		t.Fatalf("first bid: %v", err)
	}
	channel := "auction:" + strconv.FormatUint(auctionID, 10) + ":events"
	pubsub := client.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	dup, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "dedup-req", AuctionID: auctionID, BidderID: "u_1001", Price: 1500, ExpectedCurrentPrice: expectedCurrentPrice(1100), Now: now.Add(time.Second), Source: "live_ws", MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatalf("dup bid: %v", err)
	}
	if !dup.Duplicate || !dup.Accepted {
		t.Fatalf("expected duplicate accepted, got %+v", dup)
	}
	select {
	case msg := <-ch:
		t.Fatalf("duplicate request must not publish, got %+v", msg)
	case <-time.After(150 * time.Millisecond):
	}
}

func expectedCurrentPrice(price int64) *int64 {
	return &price
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
