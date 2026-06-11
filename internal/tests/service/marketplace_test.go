package service

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	marketplaceapp "aieas_backend/internal/modules/marketplace/app"
	"aieas_backend/internal/tests/repository"
)

func TestMarketplaceSearchLotsOnlyReturnsPublicVisibleLots(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	userRepo := repository.NewSeedUserRepository()
	svc := marketplaceapp.NewMarketplaceService(auctionRepo, sessionRepo, depositRepo, repository.NewMemoryOrderRepository(), userRepo, userRepo)
	now := time.Now().UTC()
	liveSession := domain.LiveSession{ID: 70001, MerchantID: "u_2001", Title: "开播直播间", Status: domain.LiveSessionStatusLive}
	endedSession := domain.LiveSession{ID: 70002, MerchantID: "u_2001", Title: "已结束直播间", Status: domain.LiveSessionStatusEnded}
	if err := sessionRepo.Create(ctx, &liveSession); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	if err := sessionRepo.Create(ctx, &endedSession); err != nil {
		t.Fatalf("create ended session: %v", err)
	}
	liveSessionID := liveSession.ID
	endedSessionID := endedSession.ID
	ready := domain.AuctionLot{
		AuctionID:      10001,
		SellerID:       "u_2001",
		LiveSessionID:  &liveSessionID,
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
	ended := ready
	ended.AuctionID = 10003
	ended.Title = "已结束直播间翡翠"
	ended.LiveSessionID = &endedSessionID
	unmounted := ready
	unmounted.AuctionID = 10004
	unmounted.Title = "未上架翡翠"
	unmounted.LiveSessionID = nil
	hammerPending := ready
	hammerPending.AuctionID = 10005
	hammerPending.Title = "落槌确认翡翠"
	hammerPending.Status = domain.AuctionStatusHammerPending
	closedWon := ready
	closedWon.AuctionID = 10006
	closedWon.Title = "已成交翡翠"
	closedWon.Status = domain.AuctionStatusClosedWon
	settled := ready
	settled.AuctionID = 10007
	settled.Title = "已结算翡翠"
	settled.Status = domain.AuctionStatusSettled
	for _, lot := range []*domain.AuctionLot{&ready, &draft, &ended, &unmounted, &hammerPending, &closedWon, &settled} {
		if err := auctionRepo.Create(ctx, lot); err != nil {
			t.Fatalf("create lot %d: %v", lot.AuctionID, err)
		}
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
	lots, total, err = svc.SearchLots(ctx, domain.AuctionSearchFilter{Status: domain.AuctionStatusHammerPending, Limit: 20})
	if err != nil {
		t.Fatalf("search hammer pending status: %v", err)
	}
	if total != 0 || len(lots) != 0 {
		t.Fatalf("hammer pending status should not be visible in discover, total=%d lots=%+v", total, lots)
	}
}

func TestMarketplaceSearchLotsTreatsExtendedAsRunningForBuyer(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	userRepo := repository.NewSeedUserRepository()
	svc := marketplaceapp.NewMarketplaceService(auctionRepo, sessionRepo, depositRepo, repository.NewMemoryOrderRepository(), userRepo, userRepo)
	now := time.Now().UTC()
	liveSession := domain.LiveSession{ID: 71001, MerchantID: "u_2001", Title: "开播直播间", Status: domain.LiveSessionStatusLive}
	if err := sessionRepo.Create(ctx, &liveSession); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	liveSessionID := liveSession.ID
	base := domain.AuctionLot{
		SellerID:       "u_2001",
		LiveSessionID:  &liveSessionID,
		Title:          "直播竞拍拍品",
		Description:    "用户端竞拍中筛选",
		Category:       "珠宝玉石",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     10000,
		DepositAmount:  5000,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
		IncrementRule:  domain.DefaultIncrementRule(),
		ConditionGrade: domain.ConditionGood,
	}
	running := base
	running.AuctionID = 11001
	running.Status = domain.AuctionStatusRunning
	extended := base
	extended.AuctionID = 11002
	extended.Status = domain.AuctionStatusExtended
	ready := base
	ready.AuctionID = 11003
	ready.Status = domain.AuctionStatusReady
	for _, lot := range []*domain.AuctionLot{&running, &extended, &ready} {
		if err := auctionRepo.Create(ctx, lot); err != nil {
			t.Fatalf("create lot %d: %v", lot.AuctionID, err)
		}
	}
	lots, total, err := svc.SearchLots(ctx, domain.AuctionSearchFilter{Status: domain.AuctionStatusRunning, Limit: 20})
	if err != nil {
		t.Fatalf("search running lots: %v", err)
	}
	got := make(map[uint64]domain.AuctionStatus, len(lots))
	for _, lot := range lots {
		got[lot.AuctionID] = lot.Status
	}
	if total != 2 || len(lots) != 2 || got[running.AuctionID] != domain.AuctionStatusRunning || got[extended.AuctionID] != domain.AuctionStatusExtended {
		t.Fatalf("expected running and extended lots, total=%d lots=%+v", total, lots)
	}
	if _, ok := got[ready.AuctionID]; ok {
		t.Fatalf("ready lot should not match running filter: %+v", lots)
	}
}

func TestMarketplaceCategoriesMatchDiscoverFilterCategories(t *testing.T) {
	userRepo := repository.NewSeedUserRepository()
	svc := marketplaceapp.NewMarketplaceService(repository.NewMemoryAuctionRepository(), repository.NewMemoryLiveSessionRepository(), repository.NewMemoryDepositRepository(), repository.NewMemoryOrderRepository(), userRepo, userRepo)
	categories := svc.Categories(context.Background())
	expected := []domain.Category{
		{ID: "jewelry", Name: "珠宝玉石", IconName: "gem"},
		{ID: "watch", Name: "腕表钟表", IconName: "watch"},
		{ID: "craft", Name: "工艺收藏", IconName: "sparkles"},
		{ID: "fashion", Name: "潮流配饰", IconName: "shopping-bag"},
		{ID: "tea", Name: "茶酒滋补", IconName: "leaf"},
		{ID: "digital", Name: "数码潮玩", IconName: "badge"},
		{ID: "painting", Name: "书画篆刻", IconName: "sparkles"},
		{ID: "ceramic", Name: "瓷器陶艺", IconName: "badge"},
		{ID: "wine", Name: "名酒陈酿", IconName: "leaf"},
		{ID: "bag", Name: "箱包皮具", IconName: "shopping-bag"},
		{ID: "coin", Name: "钱币邮票", IconName: "badge"},
		{ID: "furniture", Name: "古典家具", IconName: "sparkles"},
		{ID: "camera", Name: "影像器材", IconName: "badge"},
		{ID: "music", Name: "乐器音响", IconName: "sparkles"},
		{ID: "outdoor", Name: "户外收藏", IconName: "badge"},
	}
	if len(categories) != len(expected) {
		t.Fatalf("expected %d categories, got %d: %+v", len(expected), len(categories), categories)
	}
	for i := range expected {
		if categories[i] != expected[i] {
			t.Fatalf("category %d mismatch: expected %+v, got %+v", i, expected[i], categories[i])
		}
	}
}

func TestMarketplaceMyParticipationsAggregatesDepositLotRoomAndOrder(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	userRepo := repository.NewSeedUserRepository()
	svc := marketplaceapp.NewMarketplaceService(auctionRepo, sessionRepo, depositRepo, orderRepo, userRepo, userRepo)
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
	userRepo := repository.NewSeedUserRepository()
	merchantUser, err := userRepo.FindByID("u_2001")
	if err != nil {
		t.Fatalf("find merchant: %v", err)
	}
	merchantUser.Location = "杭州"
	if err := userRepo.Update(&merchantUser); err != nil {
		t.Fatalf("update merchant location: %v", err)
	}
	svc := marketplaceapp.NewMarketplaceService(repository.NewMemoryAuctionRepository(), sessionRepo, repository.NewMemoryDepositRepository(), repository.NewMemoryOrderRepository(), userRepo, userRepo)
	session := domain.LiveSession{ID: 70001, MerchantID: "u_2001", Title: "当前直播", Status: domain.LiveSessionStatusLive}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	merchant, err := svc.GetMerchant(ctx, "u_1001", domain.RoleBuyer, "u_2001")
	if err != nil {
		t.Fatalf("get merchant: %v", err)
	}
	if merchant.ID != "u_2001" || merchant.Name != "商家001" || merchant.Location != "杭州" || merchant.LiveSessionID != session.ID || merchant.LiveRoomID != "70001" || merchant.CurrentLiveSession == nil {
		t.Fatalf("unexpected merchant view: %+v", merchant)
	}
	if merchant.CurrentLiveSession.MerchantName != "商家001" {
		t.Fatalf("expected current live session merchant name, got %+v", merchant.CurrentLiveSession)
	}
}

func TestMarketplaceFollowMerchantUpdatesFollowerCountAndFollowingList(t *testing.T) {
	ctx := context.Background()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	userRepo := repository.NewSeedUserRepository()
	svc := marketplaceapp.NewMarketplaceService(repository.NewMemoryAuctionRepository(), sessionRepo, repository.NewMemoryDepositRepository(), repository.NewMemoryOrderRepository(), userRepo, userRepo)
	session := domain.LiveSession{ID: 70001, MerchantID: "u_2001", Title: "当前直播", Status: domain.LiveSessionStatusLive}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	merchant, err := svc.FollowMerchant(ctx, "u_1001", domain.RoleBuyer, "2001")
	if err != nil {
		t.Fatalf("follow merchant: %v", err)
	}
	if !merchant.IsFollowed || merchant.FollowerCount != 1 {
		t.Fatalf("expected followed merchant with one follower, got %+v", merchant)
	}
	merchant, err = svc.FollowMerchant(ctx, "u_1001", domain.RoleBuyer, "u_2001")
	if err != nil {
		t.Fatalf("follow merchant again: %v", err)
	}
	if merchant.FollowerCount != 1 {
		t.Fatalf("repeat follow should not duplicate follower count, got %+v", merchant)
	}
	view := svc.LiveSessionView(ctx, session)
	if view.MerchantFollowerCount != 1 {
		t.Fatalf("expected live session merchant follower count, got %+v", view)
	}
	follows, total, err := svc.MyFollowedMerchants(ctx, "1001", domain.RoleBuyer, 20, 0)
	if err != nil {
		t.Fatalf("my followed merchants: %v", err)
	}
	if total != 1 || len(follows) != 1 || follows[0].Merchant.ID != "u_2001" || follows[0].Merchant.CurrentLiveSession == nil {
		t.Fatalf("expected followed merchant with current live session, total=%d follows=%+v", total, follows)
	}

	merchant, err = svc.UnfollowMerchant(ctx, "u_1001", domain.RoleBuyer, "u_2001")
	if err != nil {
		t.Fatalf("unfollow merchant: %v", err)
	}
	if merchant.IsFollowed || merchant.FollowerCount != 0 {
		t.Fatalf("expected unfollowed merchant with zero followers, got %+v", merchant)
	}
}
