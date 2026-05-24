package service

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type fixedAuctionIDGenerator struct {
	id uint64
}

func (g fixedAuctionIDGenerator) NextAuctionID() (uint64, error) {
	return g.id, nil
}

func (g fixedAuctionIDGenerator) NextOrderID() (uint64, error) {
	return g.id, nil
}

func TestAuctionServiceCreateGeneratesAuctionID(t *testing.T) {
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})
	svc.SetIDGenerator(fixedAuctionIDGenerator{id: 123456789})

	item := domain.Item{
		SellerID:       "u_2001",
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Status:         domain.ItemStatusReady,
	}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}

	start := time.Now().UTC().Add(time.Minute)
	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:       "u_2001",
		ActorRole:     domain.RoleMerchant,
		ItemID:        item.ID,
		StartPrice:    1000,
		ReservePrice:  5000,
		Status:        domain.AuctionStatusReady,
		StartTime:     start,
		EndTime:       start.Add(time.Hour),
		AuctionType:   domain.AuctionTypeEnglish,
		DepositAmount: 100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if auction.AuctionID != 123456789 {
		t.Fatalf("expected generated auction ID, got %d", auction.AuctionID)
	}

	stored, err := auctionRepo.FindByID(ctx, 123456789)
	if err != nil {
		t.Fatalf("find generated auction: %v", err)
	}
	if stored.AuctionID != auction.AuctionID {
		t.Fatalf("stored ID mismatch: got %d want %d", stored.AuctionID, auction.AuctionID)
	}
}

func TestAuctionServiceCreatePreservesProvidedAuctionID(t *testing.T) {
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})
	svc.SetIDGenerator(fixedAuctionIDGenerator{id: 123456789})

	item := domain.Item{
		SellerID:       "u_2001",
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Status:         domain.ItemStatusReady,
	}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}

	start := time.Now().UTC().Add(time.Minute)
	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:       "u_2001",
		ActorRole:     domain.RoleMerchant,
		AuctionID:     987654321,
		ItemID:        item.ID,
		StartPrice:    1000,
		ReservePrice:  5000,
		Status:        domain.AuctionStatusReady,
		StartTime:     start,
		EndTime:       start.Add(time.Hour),
		AuctionType:   domain.AuctionTypeEnglish,
		DepositAmount: 100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if auction.AuctionID != 987654321 {
		t.Fatalf("expected provided auction ID, got %d", auction.AuctionID)
	}
}
