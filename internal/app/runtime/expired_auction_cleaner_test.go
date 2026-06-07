package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
)

func TestExpiredAuctionCleanerClosesExpiredActiveAuctions(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	auctions := &fakeExpiredAuctionLister{batches: [][]domain.AuctionLot{
		{
			{AuctionID: 1001, Status: domain.AuctionStatusRunning, EndTime: now.Add(-time.Second)},
			{AuctionID: 1002, Status: domain.AuctionStatusExtended, EndTime: now.Add(-2 * time.Second)},
		},
		{},
	}}
	hammer := &fakeExpiredAuctionHammer{}
	cleaner := NewExpiredAuctionCleaner(auctions, hammer, 10)

	result, err := cleaner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if result.Scanned != 2 || result.Closed != 2 || result.Skipped != 0 || result.Failed != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := hammer.auctionIDs; len(got) != 2 || got[0] != 1001 || got[1] != 1002 {
		t.Fatalf("hammer auction ids = %v, want [1001 1002]", got)
	}
}

func TestExpiredAuctionCleanerSkipsInvalidStateWithoutLooping(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	auctions := &fakeExpiredAuctionLister{batches: [][]domain.AuctionLot{
		{
			{AuctionID: 1001, Status: domain.AuctionStatusRunning, EndTime: now.Add(-time.Second)},
		},
		{
			{AuctionID: 1001, Status: domain.AuctionStatusRunning, EndTime: now.Add(-time.Second)},
		},
	}}
	hammer := &fakeExpiredAuctionHammer{err: domain.ErrInvalidState}
	cleaner := NewExpiredAuctionCleaner(auctions, hammer, 1)

	result, err := cleaner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if result.Scanned != 1 || result.Closed != 0 || result.Skipped != 1 || result.Failed != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if auctions.calls != 1 {
		t.Fatalf("lister calls = %d, want 1", auctions.calls)
	}
}

func TestExpiredAuctionCleanerCountsNonRetryableFailures(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	auctions := &fakeExpiredAuctionLister{batches: [][]domain.AuctionLot{
		{
			{AuctionID: 1001, Status: domain.AuctionStatusRunning, EndTime: now.Add(-time.Second)},
		},
	}}
	hammer := &fakeExpiredAuctionHammer{err: errors.New("boom")}
	cleaner := NewExpiredAuctionCleaner(auctions, hammer, 10)

	result, err := cleaner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if result.Scanned != 1 || result.Closed != 0 || result.Skipped != 0 || result.Failed != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

type fakeExpiredAuctionLister struct {
	batches [][]domain.AuctionLot
	calls   int
}

func (f *fakeExpiredAuctionLister) ListExpiredActive(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error) {
	_ = ctx
	_ = now
	_ = limit
	if f.calls >= len(f.batches) {
		f.calls++
		return nil, nil
	}
	batch := f.batches[f.calls]
	f.calls++
	return batch, nil
}

type fakeExpiredAuctionHammer struct {
	err        error
	auctionIDs []uint64
}

func (f *fakeExpiredAuctionHammer) Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	_ = ctx
	f.auctionIDs = append(f.auctionIDs, in.AuctionID)
	if f.err != nil {
		return domain.HammerResult{}, nil, f.err
	}
	return domain.HammerResult{
		RequestID: in.RequestID,
		AuctionID: in.AuctionID,
		Status:    domain.AuctionStatusClosedFailed,
		ClosedAt:  in.Now,
	}, nil, nil
}
