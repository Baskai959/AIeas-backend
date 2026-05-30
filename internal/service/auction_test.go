package service

import (
	"context"
	"errors"
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
		CapPrice:      6000,
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
	if auction.Status != domain.AuctionStatusReady {
		t.Fatalf("expected new auction to be READY, got %s", auction.Status)
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
		CapPrice:      6000,
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

func TestAuctionServiceCreateAllowsOptionalTiming(t *testing.T) {
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})

	item := domain.Item{SellerID: "u_2001", Title: "Watch", Category: "luxury", ConditionGrade: domain.ConditionNew, Status: domain.ItemStatusReady}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}

	auction, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:       "u_2001",
		ActorRole:     domain.RoleMerchant,
		ItemID:        item.ID,
		StartPrice:    1000,
		ReservePrice:  5000,
		CapPrice:      6000,
		AuctionType:   domain.AuctionTypeEnglish,
		DepositAmount: 100,
		DurationSec:   600,
	})
	if err != nil {
		t.Fatalf("create auction without start/end: %v", err)
	}
	if !auction.StartTime.IsZero() || !auction.EndTime.IsZero() || auction.DurationSec != 600 {
		t.Fatalf("expected optional schedule to remain unset with stored duration, got %+v", auction)
	}

	start := time.Now().UTC().Add(time.Hour)
	scheduled, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:       "u_2001",
		ActorRole:     domain.RoleMerchant,
		ItemID:        item.ID,
		StartPrice:    1000,
		ReservePrice:  5000,
		CapPrice:      6000,
		StartTime:     start,
		DurationSec:   300,
		AuctionType:   domain.AuctionTypeEnglish,
		DepositAmount: 100,
	})
	if err != nil {
		t.Fatalf("create scheduled auction: %v", err)
	}
	if !scheduled.EndTime.Equal(start.Add(300 * time.Second)) {
		t.Fatalf("expected end time derived from duration, got %s", scheduled.EndTime)
	}
}

func TestAuctionServiceCreateRejectsSystemStatus(t *testing.T) {
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})

	item := domain.Item{SellerID: "u_2001", Title: "Watch", Category: "luxury", ConditionGrade: domain.ConditionNew, Status: domain.ItemStatusReady}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}

	start := time.Now().UTC().Add(time.Minute)
	_, err := svc.Create(ctx, CreateAuctionInput{
		ActorID:       "u_2001",
		ActorRole:     domain.RoleMerchant,
		ItemID:        item.ID,
		StartPrice:    1000,
		ReservePrice:  5000,
		CapPrice:      6000,
		Status:        domain.AuctionStatusRunning,
		StartTime:     start,
		EndTime:       start.Add(time.Hour),
		AuctionType:   domain.AuctionTypeEnglish,
		DepositAmount: 100,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAuctionServiceUpdateAllowsReadyAndRejectsSystemStatus(t *testing.T) {
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})

	item := domain.Item{SellerID: "u_2001", Title: "Watch", Category: "luxury", ConditionGrade: domain.ConditionNew, Status: domain.ItemStatusReady}
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
		CapPrice:      6000,
		Status:        domain.AuctionStatusDraft,
		StartTime:     start,
		EndTime:       start.Add(time.Hour),
		AuctionType:   domain.AuctionTypeEnglish,
		DepositAmount: 100,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}

	ready := domain.AuctionStatusReady
	updated, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{ActorID: "u_2001", ActorRole: domain.RoleMerchant, Status: &ready})
	if err != nil {
		t.Fatalf("set ready: %v", err)
	}
	if updated.Status != domain.AuctionStatusReady {
		t.Fatalf("expected READY, got %s", updated.Status)
	}

	running := domain.AuctionStatusRunning
	if _, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{ActorID: "u_2001", ActorRole: domain.RoleMerchant, Status: &running}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}
