package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/tests/repository"
)

func TestBidServiceLoadsAuctionSnapshotFromRedisCacheBeforeDB(t *testing.T) {
	ctx := context.Background()
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	liveSessionID := uint64(90001)
	auction := domain.AuctionLot{
		AuctionID:      10001,
		SellerID:       "u_2001",
		LiveSessionID:  &liveSessionID,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		CapPrice:       2000,
		IncrementRule:  rule,
		AntiSnipingSec: 60,
		AntiExtendSec:  30,
		Status:         domain.AuctionStatusRunning,
		StartTime:      time.Now().Add(-time.Minute),
		EndTime:        time.Now().Add(time.Hour),
	}
	auctionRepo := &countingAuctionRepo{AuctionRepository: repository.NewMemoryAuctionRepository()}
	snapshots := &fakeAuctionSnapshotCache{
		getSnapshot: AuctionRuntimeSnapshotFromLot(auction),
		getOK:       true,
		getSource:   "l2",
	}
	realtime := &streamEnabledRealtime{
		result: domain.BidResult{
			AuctionID:    auction.AuctionID,
			Accepted:     true,
			CurrentPrice: 1100,
			Event:        "bid.accepted",
		},
	}
	svc := NewBidServiceWithDeps(BidServiceDeps{
		Bids:             &trackingBidRepo{findErr: domain.ErrNotFound},
		Auctions:         auctionRepo,
		Realtime:         realtime,
		Config:           appconfig.Default().Auction,
		AuctionSnapshots: snapshots,
	})

	result, err := svc.Place(ctx, PlaceBidInput{
		RequestID:            "cache-snapshot-bid",
		AuctionID:            auction.AuctionID,
		BidderID:             "u_1001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if !result.Accepted {
		t.Fatalf("expected accepted result, got %+v", result)
	}
	if auctionRepo.findCalls != 0 {
		t.Fatalf("auction snapshot cache hit should skip DB, FindByID calls=%d", auctionRepo.findCalls)
	}
	if snapshots.getCalls != 1 {
		t.Fatalf("expected one snapshot cache get, got %d", snapshots.getCalls)
	}
	if realtime.lastInput.LiveSessionID != liveSessionID || realtime.lastInput.StartPrice != 1000 || realtime.lastInput.CapPrice != 2000 {
		t.Fatalf("bid input did not use cached auction snapshot: %+v", realtime.lastInput)
	}
	if realtime.lastInput.IncrementRule.Type != domain.IncrementRuleTypeFixed || realtime.lastInput.IncrementRule.Amount != 100 {
		t.Fatalf("bid input did not parse cached increment rule: %+v", realtime.lastInput.IncrementRule)
	}
}

func TestAuctionServiceStartWritesAuctionSnapshotCache(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	snapshots := &fakeAuctionSnapshotCache{}
	cfg := appconfig.Default().Auction
	cfg.MaxExtendCount = 3
	svc := NewAuctionServiceWithDeps(AuctionServiceDeps{
		Auctions:         auctionRepo,
		Tx:               repository.NoopTxManager{},
		Realtime:         repository.NoopRealtimeStore{},
		AuctionConfig:    cfg,
		AuctionSnapshots: snapshots,
	})
	liveSessionID := uint64(90002)
	auction := domain.AuctionLot{
		AuctionID:      10002,
		SellerID:       "u_2001",
		LiveSessionID:  &liveSessionID,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   1000,
		CapPrice:       2000,
		IncrementRule:  json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		AntiSnipingSec: 60,
		AntiExtendSec:  30,
		DepositAmount:  100,
		Status:         domain.AuctionStatusReady,
		StartTime:      time.Now().Add(-time.Minute),
		EndTime:        time.Now().Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}

	start := time.Now().UTC().Truncate(time.Second)
	end := start.Add(time.Minute)
	started, err := svc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, start, end)
	if err != nil {
		t.Fatalf("start with timing: %v", err)
	}
	if started.Status != domain.AuctionStatusRunning {
		t.Fatalf("expected running auction, got %s", started.Status)
	}
	if len(snapshots.setSnapshots) != 1 {
		t.Fatalf("expected one cache set, got %d", len(snapshots.setSnapshots))
	}
	cached := snapshots.setSnapshots[0]
	if cached.AuctionID != auction.AuctionID || cached.SellerID != "u_2001" || cached.LiveSessionID != liveSessionID {
		t.Fatalf("unexpected cached snapshot identity: %+v", cached)
	}
	if cached.Status != domain.AuctionStatusRunning || !cached.EndTime.Equal(end) {
		t.Fatalf("unexpected cached snapshot lifecycle fields: %+v", cached)
	}
	if len(snapshots.setTTLs) != 1 || snapshots.setTTLs[0] <= 0 {
		t.Fatalf("expected positive snapshot cache ttl, got %+v", snapshots.setTTLs)
	}
}

type fakeAuctionSnapshotCache struct {
	getSnapshot AuctionRuntimeSnapshot
	getSource   string
	getOK       bool
	getErr      error
	getCalls    int

	setSnapshots []AuctionRuntimeSnapshot
	setTTLs      []time.Duration
	setErr       error

	invalidated   []uint64
	invalidateErr error
}

func (c *fakeAuctionSnapshotCache) Get(ctx context.Context, auctionID uint64) (AuctionRuntimeSnapshot, string, bool, error) {
	_ = ctx
	_ = auctionID
	c.getCalls++
	if c.getErr != nil {
		return AuctionRuntimeSnapshot{}, c.getSource, false, c.getErr
	}
	return c.getSnapshot, c.getSource, c.getOK, nil
}

func (c *fakeAuctionSnapshotCache) Set(ctx context.Context, snapshot AuctionRuntimeSnapshot, ttl time.Duration) error {
	_ = ctx
	if c.setErr != nil {
		return c.setErr
	}
	c.setSnapshots = append(c.setSnapshots, snapshot)
	c.setTTLs = append(c.setTTLs, ttl)
	return nil
}

func (c *fakeAuctionSnapshotCache) Invalidate(ctx context.Context, auctionID uint64) error {
	_ = ctx
	if c.invalidateErr != nil {
		return c.invalidateErr
	}
	c.invalidated = append(c.invalidated, auctionID)
	return nil
}
