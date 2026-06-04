package service

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func TestMarketplaceSearchLotsOnlyReturnsPublicVisibleLots(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	svc := NewMarketplaceService(auctionRepo, repository.NewMemoryLiveSessionRepository(), depositRepo, repository.NewMemoryOrderRepository(), repository.NewSeedUserRepository())
	now := time.Now().UTC()
	ready := domain.AuctionLot{
		AuctionID:      10001,
		SellerID:       "u_2001",
		Title:          "翡翠手镯",
		Description:    "冰种翡翠",
		Category:       "珠宝玉石",
		ImageURLs:      []string{"https://cdn.example.com/lot.jpg"},
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     10000,
		DepositAmount:  5000,
		Status:         domain.AuctionStatusReady,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
		IncrementRule:  domain.DefaultIncrementRule(),
		ConditionGrade: domain.ConditionGood,
	}
	draft := ready
	draft.AuctionID = 10002
	draft.Title = "草稿拍品"
	draft.Status = domain.AuctionStatusDraft
	if err := auctionRepo.Create(ctx, &ready); err != nil {
		t.Fatalf("create ready lot: %v", err)
	}
	if err := auctionRepo.Create(ctx, &draft); err != nil {
		t.Fatalf("create draft lot: %v", err)
	}
	if err := depositRepo.Create(ctx, &domain.DepositLedger{AuctionID: ready.AuctionID, UserID: "u_1001", Amount: 5000, Status: domain.DepositStatusReady}); err != nil {
		t.Fatalf("create deposit: %v", err)
	}
	lots, total, err := svc.SearchLots(ctx, domain.AuctionSearchFilter{Keyword: "翡翠", CategoryID: "jewelry", Limit: 20})
	if err != nil {
		t.Fatalf("search lots: %v", err)
	}
	if total != 1 || len(lots) != 1 || lots[0].AuctionID != ready.AuctionID {
		t.Fatalf("expected only ready public lot, total=%d lots=%+v", total, lots)
	}
	if lots[0].CategoryID != "jewelry" || lots[0].ImageURL == "" || lots[0].ParticipantCount != 1 || lots[0].CurrentPrice != ready.StartPrice {
		t.Fatalf("expected enriched public lot fields, got %+v", lots[0])
	}
	lots, total, err = svc.SearchLots(ctx, domain.AuctionSearchFilter{Status: domain.AuctionStatusPendingAudit, Limit: 20})
	if err != nil {
		t.Fatalf("search hidden status: %v", err)
	}
	if total != 0 || len(lots) != 0 {
		t.Fatalf("hidden status should not be visible, total=%d lots=%+v", total, lots)
	}
}

func TestMarketplaceMyParticipationsAggregatesDepositLotRoomAndOrder(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	svc := NewMarketplaceService(auctionRepo, sessionRepo, depositRepo, orderRepo, repository.NewSeedUserRepository())
	now := time.Now().UTC()
	session := domain.LiveSession{ID: 70001, MerchantID: "u_2001", Title: "珠宝直播", Status: domain.LiveSessionStatusLive, ActiveAuctionID: 10001}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	sessionID := session.ID
	lot := domain.AuctionLot{
		AuctionID:      10001,
		SellerID:       "u_2001",
		LiveSessionID:  &sessionID,
		Title:          "和田玉牌",
		Category:       "collectible",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     10000,
		DepositAmount:  5000,
		Status:         domain.AuctionStatusRunning,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
		IncrementRule:  domain.DefaultIncrementRule(),
		ConditionGrade: domain.ConditionGood,
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	deposit := domain.DepositLedger{AuctionID: lot.AuctionID, UserID: "u_1001", Amount: 5000, Status: domain.DepositStatusReady}
	if err := depositRepo.Create(ctx, &deposit); err != nil {
		t.Fatalf("create deposit: %v", err)
	}
	order := domain.OrderDeal{AuctionID: lot.AuctionID, LiveSessionID: &sessionID, WinnerID: "u_1001", SellerID: "u_2001", DealPrice: 12000, DepositAmount: 5000, Status: domain.OrderStatusCreated, PayStatus: domain.PayStatusUnpaid}
	if _, _, err := orderRepo.CreateIfAbsentByAuction(ctx, &order); err != nil {
		t.Fatalf("create order: %v", err)
	}
	records, err := svc.MyParticipations(ctx, "u_1001", domain.RoleBuyer, 20, 0)
	if err != nil {
		t.Fatalf("my participations: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one participation, got %+v", records)
	}
	record := records[0]
	if record.Lot == nil || record.Lot.AuctionID != lot.AuctionID || record.Room == nil || record.Room.ID != session.ID || record.Order == nil || record.Order.AuctionID != lot.AuctionID {
		t.Fatalf("expected aggregate lot room order, got %+v", record)
	}
	if record.DepositAmount != 5000 || record.DepositStatus != domain.DepositStatusReady || record.EnrolledAt.IsZero() {
		t.Fatalf("expected deposit fields, got %+v", record)
	}
}

func TestMarketplaceMerchantViewIncludesCurrentLiveSession(t *testing.T) {
	ctx := context.Background()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	svc := NewMarketplaceService(repository.NewMemoryAuctionRepository(), sessionRepo, repository.NewMemoryDepositRepository(), repository.NewMemoryOrderRepository(), repository.NewSeedUserRepository())
	session := domain.LiveSession{ID: 70001, MerchantID: "u_2001", Title: "当前直播", Status: domain.LiveSessionStatusLive}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	merchant, err := svc.GetMerchant(ctx, "u_2001")
	if err != nil {
		t.Fatalf("get merchant: %v", err)
	}
	if merchant.ID != "u_2001" || merchant.Name != "商家001" || merchant.LiveSessionID != session.ID || merchant.LiveRoomID != "70001" || merchant.CurrentLiveSession == nil {
		t.Fatalf("unexpected merchant view: %+v", merchant)
	}
	if merchant.CurrentLiveSession.MerchantName != "商家001" {
		t.Fatalf("expected current live session merchant name, got %+v", merchant.CurrentLiveSession)
	}
}
