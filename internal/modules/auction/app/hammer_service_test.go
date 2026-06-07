package app

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
	orderrepo "aieas_backend/internal/modules/order/repository"
)

type missingHammerRealtimeStore struct {
	noopRealtimeStore
}

func (missingHammerRealtimeStore) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	_ = ctx
	_ = input
	return domain.HammerResult{}, domain.ErrNotFound
}

func TestHammerFallsBackToBidRecordsWhenRealtimeStateMissing(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	auctionRepo := auctionrepo.NewMemoryAuctionRepository()
	bidRepo := auctionrepo.NewMemoryBidRepository()
	orderRepo := orderrepo.NewMemoryOrderRepository()
	auction := domain.AuctionLot{
		AuctionID:    1001,
		SellerID:     "seller-1",
		Title:        "lot",
		AuctionType:  domain.AuctionTypeEnglish,
		StartPrice:   1000,
		ReservePrice: 1200,
		Status:       domain.AuctionStatusRunning,
		StartTime:    now.Add(-2 * time.Hour),
		EndTime:      now.Add(-time.Minute),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	bid := domain.BidRecord{
		RequestID:  "bid-1",
		AuctionID:  auction.AuctionID,
		BidderID:   "buyer-1",
		BidPrice:   1300,
		BidTSMS:    now.Add(-30 * time.Minute).UnixMilli(),
		RiskResult: domain.BidRiskAllow,
	}
	if err := bidRepo.Create(ctx, &bid); err != nil {
		t.Fatalf("create bid: %v", err)
	}
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions: auctionRepo,
		Bids:     bidRepo,
		Orders:   orderRepo,
		Realtime: missingHammerRealtimeStore{},
	})

	result, order, err := svc.Hammer(ctx, domain.HammerInput{
		RequestID: "cleanup-1001",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "system",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if result.Status != domain.AuctionStatusClosedWon {
		t.Fatalf("status = %s, want %s", result.Status, domain.AuctionStatusClosedWon)
	}
	if result.WinnerID != "buyer-1" || result.Price != 1300 {
		t.Fatalf("winner/price = %q/%d, want buyer-1/1300", result.WinnerID, result.Price)
	}
	if order == nil || order.AuctionID != auction.AuctionID || order.WinnerID != "buyer-1" {
		t.Fatalf("unexpected order: %#v", order)
	}
	closed, err := auctionRepo.FindByID(ctx, auction.AuctionID)
	if err != nil {
		t.Fatalf("find closed auction: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedWon {
		t.Fatalf("closed status = %s, want %s", closed.Status, domain.AuctionStatusClosedWon)
	}
	if closed.WinnerID == nil || *closed.WinnerID != "buyer-1" {
		t.Fatalf("closed winner = %#v, want buyer-1", closed.WinnerID)
	}
}
