package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func TestDepositEnrollIsIdempotent(t *testing.T) {
	ctx := context.Background()
	fixture := newRealtimeAuctionFixture(t, appconfig.Default().Auction)

	first, err := fixture.deposits.Enroll(ctx, EnrollInput{AuctionID: fixture.auctionID, UserID: "u_1001", UserRole: domain.RoleBuyer})
	if err != nil {
		t.Fatalf("first enroll: %v", err)
	}
	second, err := fixture.deposits.Enroll(ctx, EnrollInput{AuctionID: fixture.auctionID, UserID: "u_1001", UserRole: domain.RoleBuyer})
	if err != nil {
		t.Fatalf("second enroll: %v", err)
	}
	if first.ID != second.ID || second.Status != domain.DepositStatusReady {
		t.Fatalf("expected idempotent ready deposit, first=%+v second=%+v", first, second)
	}
}

func TestBidServiceIdempotencyMinimumIncrementAndTopN(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")

	tooLow, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "bid-low", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1050})
	if err != nil {
		t.Fatalf("low bid: %v", err)
	}
	if tooLow.Accepted || tooLow.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected PRICE_STEP_MISMATCH rejection, got %+v", tooLow)
	}

	accepted, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "bid-ok", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100})
	if err != nil {
		t.Fatalf("accepted bid: %v", err)
	}
	if !accepted.Accepted || accepted.CurrentPrice != 1100 {
		t.Fatalf("expected accepted 1100 bid, got %+v", accepted)
	}
	duplicate, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "bid-ok", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1300})
	if err != nil {
		t.Fatalf("duplicate bid: %v", err)
	}
	if !duplicate.Duplicate || !duplicate.Accepted || duplicate.Price != 1100 {
		t.Fatalf("expected duplicate original result, got %+v", duplicate)
	}
	top, err := fixture.bids.TopN(ctx, fixture.auctionID, 3)
	if err != nil {
		t.Fatalf("topn: %v", err)
	}
	if len(top) != 1 || top[0].BidderID != "u_1001" || top[0].Price != 1100 || top[0].Rank != 1 {
		t.Fatalf("unexpected topn: %+v", top)
	}
}

func TestBidServiceRejectsStaleExpectedState(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")

	expected := int64(900)
	result, err := fixture.bids.Place(ctx, PlaceBidInput{
		RequestID:            "bid-stale",
		AuctionID:            fixture.auctionID,
		BidderID:             "u_1001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: &expected,
	})
	if err != nil {
		t.Fatalf("stale bid: %v", err)
	}
	if result.Accepted || result.Reason != domain.BidRejectStaleAuctionState || result.CurrentPrice != 1000 {
		t.Fatalf("expected stale state rejection, got %+v", result)
	}
}

func TestBidServiceRejectsEqualPriceAfterLeader(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")
	mustEnroll(t, fixture, "u_1002")
	mustEnroll(t, fixture, "u_1003")

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "tie-first", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100})
	if err != nil || !first.Accepted || first.LeaderBidderID != "u_1001" {
		t.Fatalf("first bid result=%+v err=%v", first, err)
	}
	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "tie-second", AuctionID: fixture.auctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 1100})
	if err != nil {
		t.Fatalf("second equal-price bid: %v", err)
	}
	if second.Accepted || second.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected equal-price bid rejected, result=%+v", second)
	}
	third, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "tie-higher", AuctionID: fixture.auctionID, BidderID: "u_1003", UserRole: domain.RoleBuyer, Price: 1300})
	if err != nil || !third.Accepted || third.LeaderBidderID != "u_1003" {
		t.Fatalf("higher bid result=%+v err=%v", third, err)
	}

	top, err := fixture.bids.TopN(ctx, fixture.auctionID, 3)
	if err != nil {
		t.Fatalf("topn: %v", err)
	}
	want := []struct {
		bidder string
		price  int64
		rank   int
	}{{"u_1003", 1300, 1}, {"u_1001", 1100, 2}}
	if len(top) != len(want) {
		t.Fatalf("expected %d entries, got %+v", len(want), top)
	}
	for i := range want {
		if top[i].BidderID != want[i].bidder || top[i].Price != want[i].price || top[i].Rank != want[i].rank {
			t.Fatalf("unexpected top[%d], got %+v want=%+v all=%+v", i, top[i], want[i], top)
		}
	}
}

func TestBidServiceFixedRulePrecheck(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":3}`)
	fixture := newRealtimeAuctionFixtureWithRule(t, cfg, -2*time.Minute, 1000, rule)
	mustEnroll(t, fixture, "u_1001")
	mustEnroll(t, fixture, "u_1002")

	mismatch, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-mismatch", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1150})
	if err != nil {
		t.Fatalf("step mismatch bid: %v", err)
	}
	if mismatch.Accepted || mismatch.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected step mismatch, got %+v", mismatch)
	}
	tooHigh, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-too-high", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1500})
	if err != nil {
		t.Fatalf("too high bid: %v", err)
	}
	if tooHigh.Accepted || tooHigh.Reason != domain.BidRejectAboveMaxBidSteps {
		t.Fatalf("expected max-steps rejection, got %+v", tooHigh)
	}
	okBid, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-ok", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1300})
	if err != nil || !okBid.Accepted || okBid.CurrentPrice != 1300 {
		t.Fatalf("expected fixed bid accepted, result=%+v err=%v", okBid, err)
	}
	aboveCap, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-above-cap", AuctionID: fixture.auctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 10100})
	if err != nil {
		t.Fatalf("above cap bid: %v", err)
	}
	if aboveCap.Accepted || aboveCap.Reason != domain.BidRejectAboveCapPrice {
		t.Fatalf("expected cap rejection, got %+v", aboveCap)
	}
}

func TestBidServiceCapPriceAutoClosesAndCreatesOrder(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	fixture := newRealtimeAuctionFixtureWithRule(t, cfg, -2*time.Minute, 1000, rule)
	fixture.hammers.SetOrderIDGenerator(fixedAuctionIDGenerator{id: 567890123})
	mustEnroll(t, fixture, "u_1001")

	result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "cap-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 2000})
	if err != nil {
		t.Fatalf("cap bid: %v", err)
	}
	if !result.Accepted || !result.AutoClosed || result.AuctionStatus != domain.AuctionStatusClosedWon {
		t.Fatalf("expected cap bid accepted and auto closed, got %+v", result)
	}
	order, err := fixture.orderRepo.FindByAuctionID(ctx, fixture.auctionID)
	if err != nil {
		t.Fatalf("expected cap bid to create order: %v", err)
	}
	if order.ID != 567890123 || order.WinnerID != "u_1001" || order.DealPrice != 2000 {
		t.Fatalf("unexpected cap order: %+v", order)
	}
}

func TestBidServiceFrequencyLimitRecordsRiskRejection(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 1
	cfg.FreqWindowMs = int64(time.Minute / time.Millisecond)
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "freq-1", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100})
	if err != nil {
		t.Fatalf("first bid: %v", err)
	}
	if !first.Accepted {
		t.Fatalf("expected first bid accepted, got %+v", first)
	}
	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "freq-2", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1300})
	if err != nil {
		t.Fatalf("second bid: %v", err)
	}
	if second.Accepted || second.Reason != "FREQ_LIMIT" {
		t.Fatalf("expected FREQ_LIMIT rejection, got %+v", second)
	}
}

func TestHammerAndOrderPayAreIdempotent(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "hammer-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100}); err != nil || !result.Accepted {
		t.Fatalf("place bid before hammer result=%+v err=%v", result, err)
	}

	closed, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-1", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedWon || closed.WinnerID != "u_1001" || order == nil || order.ID == 0 {
		t.Fatalf("unexpected hammer result=%+v order=%+v", closed, order)
	}
	again, orderAgain, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-1", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer duplicate: %v", err)
	}
	if !again.Duplicate || orderAgain == nil || orderAgain.ID != order.ID {
		t.Fatalf("expected duplicate hammer with same order, result=%+v order=%+v", again, orderAgain)
	}

	paid, err := fixture.orders.Pay(ctx, order.ID, "u_1001", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("pay: %v", err)
	}
	paidAgain, err := fixture.orders.Pay(ctx, order.ID, "u_1001", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("pay duplicate: %v", err)
	}
	if paid.PayStatus != domain.PayStatusPaid || paidAgain.PayStatus != domain.PayStatusPaid || paidAgain.PaidAt == nil {
		t.Fatalf("expected idempotent paid order, paid=%+v paidAgain=%+v", paid, paidAgain)
	}
}

func TestHammerGeneratesOrderID(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	fixture.hammers.SetOrderIDGenerator(fixedAuctionIDGenerator{id: 345678901})
	mustEnroll(t, fixture, "u_1001")
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "order-id-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100}); err != nil || !result.Accepted {
		t.Fatalf("place bid: result=%+v err=%v", result, err)
	}

	_, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "order-id-hammer", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if order == nil || order.ID != 345678901 {
		t.Fatalf("expected generated order ID, got %+v", order)
	}
}

func TestHammerRejectsBeforeEndAndForceBypasses(t *testing.T) {
	ctx := context.Background()
	fixture := newRealtimeAuctionFixtureWithTiming(t, appconfig.Default().Auction, time.Hour, 1000)

	_, _, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-too-early", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != domain.ErrInvalidState {
		t.Fatalf("expected early hammer invalid state, got %v", err)
	}
	forced, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-force", AuctionID: fixture.auctionID, ActorID: "u_9001", ActorRole: domain.RoleAdmin, ClosedBy: "u_9001", Force: true})
	if err != nil {
		t.Fatalf("force hammer: %v", err)
	}
	if forced.Status != domain.AuctionStatusClosedFailed || order != nil {
		t.Fatalf("expected forced no-bid close failed without order, result=%+v order=%+v", forced, order)
	}
}

func TestHammerNoBidClosesFailedWithoutOrderAndReleasesDeposits(t *testing.T) {
	ctx := context.Background()
	fixture := newRealtimeAuctionFixtureWithTiming(t, appconfig.Default().Auction, -2*time.Minute, 1000)
	mustEnroll(t, fixture, "u_1001")

	closed, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-no-bid", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer no bid: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedFailed || order != nil {
		t.Fatalf("expected failed no-order close, result=%+v order=%+v", closed, order)
	}
	if _, err := fixture.orderRepo.FindByAuctionID(ctx, fixture.auctionID); err != domain.ErrNotFound {
		t.Fatalf("expected no order, got err=%v", err)
	}
	deposit, err := fixture.depositRepo.FindByAuctionUser(ctx, fixture.auctionID, "u_1001")
	if err != nil || deposit.Status != domain.DepositStatusReleased {
		t.Fatalf("expected released deposit, deposit=%+v err=%v", deposit, err)
	}
}

func TestHammerReserveNotMetClosesFailedWithoutOrder(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixtureWithTiming(t, cfg, -2*time.Minute, 2000)
	mustEnroll(t, fixture, "u_1001")
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "reserve-low-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100}); err != nil || !result.Accepted {
		t.Fatalf("place reserve-low bid result=%+v err=%v", result, err)
	}

	closed, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-reserve-low", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer reserve low: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedFailed || closed.WinnerID != "" || order != nil {
		t.Fatalf("expected failed reserve close without order, result=%+v order=%+v", closed, order)
	}
}

func TestHammerCapturesWinnerAndReleasesNonWinners(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixtureWithTiming(t, cfg, -2*time.Minute, 1000)
	mustEnroll(t, fixture, "u_1001")
	mustEnroll(t, fixture, "u_1002")
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "winner-bid-1", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100}); err != nil || !result.Accepted {
		t.Fatalf("first bid result=%+v err=%v", result, err)
	}
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "winner-bid-2", AuctionID: fixture.auctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 1300}); err != nil || !result.Accepted {
		t.Fatalf("second bid result=%+v err=%v", result, err)
	}

	closed, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-with-loser", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer with loser: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedWon || closed.WinnerID != "u_1002" || order == nil {
		t.Fatalf("expected won close with order, result=%+v order=%+v", closed, order)
	}
	winnerDeposit, err := fixture.depositRepo.FindByAuctionUser(ctx, fixture.auctionID, "u_1002")
	if err != nil || winnerDeposit.Status != domain.DepositStatusCaptured || winnerDeposit.RelatedOrderID == nil || *winnerDeposit.RelatedOrderID != order.ID {
		t.Fatalf("expected captured winner deposit, deposit=%+v err=%v", winnerDeposit, err)
	}
	loserDeposit, err := fixture.depositRepo.FindByAuctionUser(ctx, fixture.auctionID, "u_1001")
	if err != nil || loserDeposit.Status != domain.DepositStatusReleased {
		t.Fatalf("expected released loser deposit, deposit=%+v err=%v", loserDeposit, err)
	}
}

type realtimeAuctionFixture struct {
	auctionID   uint64
	deposits    *DepositService
	bids        *BidService
	hammers     *HammerService
	orders      *OrderService
	depositRepo *repository.MemoryDepositRepository
	orderRepo   *repository.MemoryOrderRepository
}

func newRealtimeAuctionFixture(t *testing.T, cfg appconfig.AuctionConfig) realtimeAuctionFixture {
	return newRealtimeAuctionFixtureWithTiming(t, cfg, -2*time.Minute, 1000)
}

func newRealtimeAuctionFixtureWithTiming(t *testing.T, cfg appconfig.AuctionConfig, endOffset time.Duration, reservePrice int64) realtimeAuctionFixture {
	t.Helper()
	rule, _ := json.Marshal(map[string]interface{}{"type": "fixed", "amount": 100, "maxBidSteps": 10})
	return newRealtimeAuctionFixtureWithRule(t, cfg, endOffset, reservePrice, rule)
}

func newRealtimeAuctionFixtureWithRule(t *testing.T, cfg appconfig.AuctionConfig, endOffset time.Duration, reservePrice int64, rule json.RawMessage) realtimeAuctionFixture {
	t.Helper()
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	realtime := repository.NewMemoryRealtimeStore()
	riskSvc := NewRiskService(riskRepo, realtime, nil)
	depositSvc := NewDepositService(depositRepo, auctionRepo, realtime, riskSvc, repository.NoopTxManager{})
	orderSvc := NewOrderService(orderRepo, repository.NoopTxManager{})
	hammerSvc := NewHammerService(auctionRepo, orderRepo, depositRepo, realtime, repository.NoopTxManager{}, nil)
	auctionSvc := NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})
	auctionSvc.SetRealtime(realtime)
	auctionSvc.SetAuctionConfig(cfg)
	bidSvc := NewBidService(bidRepo, auctionRepo, realtime, riskSvc, nil, cfg)
	bidSvc.SetHammerService(hammerSvc)

	item := domain.Item{SellerID: "u_2001", Title: "Watch", Category: "luxury", ConditionGrade: domain.ConditionNew, Status: domain.ItemStatusReady}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}
	now := time.Now().UTC()
	endTime := now.Add(endOffset)
	startTime := now.Add(-time.Minute)
	if !endTime.After(startTime) {
		startTime = endTime.Add(-time.Minute)
	}
	auction, err := auctionSvc.Create(ctx, CreateAuctionInput{
		ActorID:           "u_2001",
		ActorRole:         domain.RoleMerchant,
		ItemID:            item.ID,
		AuctionType:       domain.AuctionTypeEnglish,
		StartPrice:        1000,
		ReservePrice:      reservePrice,
		CapPrice:          2000,
		IncrementRule:     rule,
		AntiSnipingSec:    60,
		AntiExtendSec:     30,
		DepositAmount:     100,
		Status:            domain.AuctionStatusReady,
		StartTime:         startTime,
		EndTime:           endTime,
		allowSystemStatus: true,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	startEndTime := endTime
	if !startEndTime.After(now) {
		startEndTime = now.Add(time.Hour)
	}
	if _, err := auctionSvc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, startTime, startEndTime); err != nil {
		t.Fatalf("start auction: %v", err)
	}
	if !endTime.After(now) {
		auction.Status = domain.AuctionStatusRunning
		auction.StartTime = startTime
		auction.EndTime = endTime
		if err := auctionRepo.Update(ctx, &auction); err != nil {
			t.Fatalf("expire auction: %v", err)
		}
		if _, err := realtime.InitAuction(ctx, auction, 100); err != nil {
			t.Fatalf("expire realtime auction: %v", err)
		}
	}
	return realtimeAuctionFixture{auctionID: auction.AuctionID, deposits: depositSvc, bids: bidSvc, hammers: hammerSvc, orders: orderSvc, depositRepo: depositRepo, orderRepo: orderRepo}
}

func mustEnroll(t *testing.T, fixture realtimeAuctionFixture, userID string) {
	t.Helper()
	if _, err := fixture.deposits.Enroll(context.Background(), EnrollInput{AuctionID: fixture.auctionID, UserID: userID, UserRole: domain.RoleBuyer}); err != nil {
		t.Fatalf("enroll %s: %v", userID, err)
	}
}
