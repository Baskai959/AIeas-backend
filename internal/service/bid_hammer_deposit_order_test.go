package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
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

	tooLow, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "bid-low", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1050, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("low bid: %v", err)
	}
	if tooLow.Accepted || tooLow.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected PRICE_STEP_MISMATCH rejection, got %+v", tooLow)
	}

	accepted, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "bid-ok", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("accepted bid: %v", err)
	}
	if !accepted.Accepted || accepted.CurrentPrice != 1100 {
		t.Fatalf("expected accepted 1100 bid, got %+v", accepted)
	}
	duplicate, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "bid-ok", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1100)})
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

	expected := int64(1100)
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

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "tie-first", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil || !first.Accepted || first.LeaderBidderID != "u_1001" {
		t.Fatalf("first bid result=%+v err=%v", first, err)
	}
	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "tie-second", AuctionID: fixture.auctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1100)})
	if err != nil {
		t.Fatalf("second equal-price bid: %v", err)
	}
	if second.Accepted || second.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected equal-price bid rejected, result=%+v", second)
	}
	third, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "tie-higher", AuctionID: fixture.auctionID, BidderID: "u_1003", UserRole: domain.RoleBuyer, Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1100)})
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

	mismatch, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-mismatch", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1150, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("step mismatch bid: %v", err)
	}
	if mismatch.Accepted || mismatch.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected step mismatch, got %+v", mismatch)
	}
	tooHigh, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-too-high", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1500, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("too high bid: %v", err)
	}
	if tooHigh.Accepted || tooHigh.Reason != domain.BidRejectAboveMaxBidSteps {
		t.Fatalf("expected max-steps rejection, got %+v", tooHigh)
	}
	okBid, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-ok", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil || !okBid.Accepted || okBid.CurrentPrice != 1300 {
		t.Fatalf("expected fixed bid accepted, result=%+v err=%v", okBid, err)
	}
	aboveCap, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "fixed-above-cap", AuctionID: fixture.auctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 10100, ExpectedCurrentPrice: expectedCurrentPrice(1300)})
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

	result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "cap-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 2000, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
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

// TestBidServiceCapPriceDuplicateDoesNotInvokeHammer 验证 P1-C：
// 当同一 RequestID 在 cap-price 命中后被重放时，BidService 必须依靠
// `result.Accepted && !result.Duplicate && result.AutoClosed` 守卫直接复用幂等
// 结果，不应再次触发 Hammer。否则 cap-price 重放会引发重复 Hammer 调用，造成
// hammer_duplicate_total 抖动并污染 RT/MySQL 终态短路链路。
func TestBidServiceCapPriceDuplicateDoesNotInvokeHammer(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	fixture := newRealtimeAuctionFixtureWithRule(t, cfg, -2*time.Minute, 1000, rule)
	fixture.hammers.SetOrderIDGenerator(fixedAuctionIDGenerator{id: 567890123})
	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "test"})
	fixture.hammers.SetMetrics(reg)
	mustEnroll(t, fixture, "u_1001")

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "cap-dup", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 2000, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("first cap bid: %v", err)
	}
	if !first.Accepted || !first.AutoClosed || first.Duplicate || first.AuctionStatus != domain.AuctionStatusClosedWon {
		t.Fatalf("expected first cap bid accepted+auto-closed+!duplicate, got %+v", first)
	}
	if got := counterValue(t, reg, "test_auction_hammer_total"); got != 1 {
		t.Fatalf("expected hammer_total=1 after first cap bid, got %v", got)
	}

	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "cap-dup", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 2000, ExpectedCurrentPrice: expectedCurrentPrice(2000)})
	if err != nil {
		t.Fatalf("duplicate cap bid: %v", err)
	}
	if !second.Duplicate {
		t.Fatalf("expected duplicate cap bid replay, got %+v", second)
	}
	// 守卫存在时：BidService 在见到 Duplicate=true 后短路，不会再次进入 Hammer 流程。
	// 因此 hammer_total 仍为 1；如守卫被移除，第二次 Hammer 会被识别为终态重放并落入
	// hammer_duplicate_total，hammer_total 也会变成 2。
	if got := counterValue(t, reg, "test_auction_hammer_total"); got != 1 {
		t.Fatalf("expected hammer_total still=1 after duplicate cap bid, got %v", got)
	}
	if got := counterValue(t, reg, "test_auction_hammer_duplicate_total"); got != 0 {
		t.Fatalf("expected hammer_duplicate_total=0 (BidService should short-circuit before Hammer), got %v", got)
	}
	// 订单仍然只有一条：cap-price 单据 ID 与首次落槌一致，未被覆盖也未新增。
	order, err := fixture.orderRepo.FindByAuctionID(ctx, fixture.auctionID)
	if err != nil {
		t.Fatalf("expected single order to remain after duplicate, err=%v", err)
	}
	if order.ID != 567890123 || order.WinnerID != "u_1001" || order.DealPrice != 2000 {
		t.Fatalf("unexpected order after duplicate cap bid: %+v", order)
	}
}

func TestBidServiceFrequencyLimitRecordsRiskRejection(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 1
	cfg.FreqWindowMs = int64(time.Minute / time.Millisecond)
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "freq-1", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("first bid: %v", err)
	}
	if !first.Accepted {
		t.Fatalf("expected first bid accepted, got %+v", first)
	}
	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "freq-2", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1100)})
	if err != nil {
		t.Fatalf("second bid: %v", err)
	}
	if second.Accepted || second.Reason != "FREQ_LIMIT" {
		t.Fatalf("expected FREQ_LIMIT rejection, got %+v", second)
	}
}

func TestBidServiceBlacklistStrategyMissingDeposit(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	setBidBlacklistStrategy(t, fixture.bids, domain.BlacklistStrategyConfig{
		Enabled:               true,
		FrequencyEnabled:      false,
		MissingDepositEnabled: true,
	})

	result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "auto-blacklist-no-deposit", AuctionID: fixture.auctionID, BidderID: "u_1009", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("bid without deposit: %v", err)
	}
	if result.Accepted || result.Reason != "NOT_ENROLLED" {
		t.Fatalf("expected no-enrollment rejection, got %+v", result)
	}
	waitForBlacklisted(t, fixture.risk, "u_1009")
}

func TestBidServiceBlacklistStrategyUnreasonablePrice(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":3}`)
	fixture := newRealtimeAuctionFixtureWithRule(t, cfg, time.Hour, 1000, rule)
	setBidBlacklistStrategy(t, fixture.bids, domain.BlacklistStrategyConfig{
		Enabled:                  true,
		FrequencyEnabled:         false,
		UnreasonablePriceEnabled: true,
	})
	mustEnroll(t, fixture, "u_1010")

	result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "auto-blacklist-price", AuctionID: fixture.auctionID, BidderID: "u_1010", UserRole: domain.RoleBuyer, Price: 1350, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("unreasonable bid: %v", err)
	}
	if result.Accepted || result.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected step mismatch rejection, got %+v", result)
	}
	waitForBlacklisted(t, fixture.risk, "u_1010")
}

func TestBidServiceBlacklistStrategyFrequencyLimit(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	setBidBlacklistStrategy(t, fixture.bids, domain.BlacklistStrategyConfig{
		Enabled:              true,
		FrequencyEnabled:     true,
		FrequencyWindowMs:    1000,
		FrequencyMaxRequests: 1,
	})
	mustEnroll(t, fixture, "u_1011")

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "auto-blacklist-freq-1", AuctionID: fixture.auctionID, BidderID: "u_1011", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil || !first.Accepted {
		t.Fatalf("first bid result=%+v err=%v", first, err)
	}
	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "auto-blacklist-freq-2", AuctionID: fixture.auctionID, BidderID: "u_1011", UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100)})
	if err != nil {
		t.Fatalf("second bid: %v", err)
	}
	if second.Accepted || second.Reason != "FREQ_LIMIT" {
		t.Fatalf("expected freq limit rejection, got %+v", second)
	}
	waitForBlacklisted(t, fixture.risk, "u_1011")
}

func TestBidServiceAutoBlacklistFailureDoesNotBlockRejection(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	failingRisk := NewRiskService(failingCreateBlacklistRepo{MemoryRiskRepository: repository.NewMemoryRiskRepository()}, nil, nil)
	fixture.risk = failingRisk
	fixture.bids.risk = failingRisk
	setBidBlacklistStrategy(t, fixture.bids, domain.BlacklistStrategyConfig{
		Enabled:               true,
		FrequencyEnabled:      false,
		MissingDepositEnabled: true,
	})

	result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "auto-blacklist-fail", AuctionID: fixture.auctionID, BidderID: "u_1014", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("auto blacklist failure must not fail bid path: %v", err)
	}
	if result.Accepted || result.Reason != "NOT_ENROLLED" {
		t.Fatalf("expected normal no-enrollment rejection, got %+v", result)
	}
}

func TestBidServiceRiskControlDisablesBidFrequencyLimit(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 1
	cfg.FreqWindowMs = int64(time.Minute / time.Millisecond)
	fixture := newRealtimeAuctionFixture(t, cfg)
	setRiskControl(t, fixture.controls, domain.RiskControlConfig{Enabled: false})
	mustEnroll(t, fixture, "u_1012")

	first, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "risk-control-freq-1", AuctionID: fixture.auctionID, BidderID: "u_1012", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil || !first.Accepted {
		t.Fatalf("first bid result=%+v err=%v", first, err)
	}
	second, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "risk-control-freq-2", AuctionID: fixture.auctionID, BidderID: "u_1012", UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100)})
	if err != nil || !second.Accepted {
		t.Fatalf("expected frequency limiter disabled, second=%+v err=%v", second, err)
	}
}

func TestBidServiceRiskControlDisablesBlacklistCheck(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	setRiskControl(t, fixture.controls, domain.RiskControlConfig{Enabled: false})
	if err := fixture.risk.AddBlacklist(ctx, "u_1013", "manual", "u_9999", nil); err != nil {
		t.Fatalf("add blacklist: %v", err)
	}
	mustEnroll(t, fixture, "u_1013")

	result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "risk-control-blacklist", AuctionID: fixture.auctionID, BidderID: "u_1013", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil || !result.Accepted {
		t.Fatalf("expected blacklist check disabled, result=%+v err=%v", result, err)
	}
}

func TestHammerAndOrderPayAreIdempotent(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "hammer-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)}); err != nil || !result.Accepted {
		t.Fatalf("place bid before hammer result=%+v err=%v", result, err)
	}

	closed, order, err := fixture.hammers.Hammer(ctx, domain.HammerInput{RequestID: "hammer-1", AuctionID: fixture.auctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, ClosedBy: "u_2001"})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if closed.Status != domain.AuctionStatusClosedWon || closed.WinnerID != "u_1001" || order == nil || order.ID == 0 {
		t.Fatalf("unexpected hammer result=%+v order=%+v", closed, order)
	}
	if order.PayDeadline == nil || !order.PayDeadline.Equal(order.CreatedAt.Add(DefaultOrderPayTimeout)) {
		t.Fatalf("expected %s pay deadline, got order=%+v", DefaultOrderPayTimeout, order)
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
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "order-id-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)}); err != nil || !result.Accepted {
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
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "reserve-low-bid", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)}); err != nil || !result.Accepted {
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
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "winner-bid-1", AuctionID: fixture.auctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)}); err != nil || !result.Accepted {
		t.Fatalf("first bid result=%+v err=%v", result, err)
	}
	if result, err := fixture.bids.Place(ctx, PlaceBidInput{RequestID: "winner-bid-2", AuctionID: fixture.auctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1100)}); err != nil || !result.Accepted {
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
	risk        *RiskService
	controls    *RiskControlService
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
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	configRepo := repository.NewMemoryConfigRepository()
	realtime := repository.NewMemoryRealtimeStore()
	riskSvc := NewRiskService(riskRepo, realtime, nil)
	riskControlSvc := NewRiskControlService(domain.DefaultRiskControlConfig())
	depositSvc := NewDepositService(depositRepo, auctionRepo, realtime, riskSvc, repository.NoopTxManager{})
	depositSvc.SetRiskControlService(riskControlSvc)
	orderSvc := NewOrderService(orderRepo, repository.NoopTxManager{})
	hammerSvc := NewHammerService(auctionRepo, orderRepo, depositRepo, realtime, repository.NoopTxManager{}, nil)
	auctionSvc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	auctionSvc.SetRealtime(realtime)
	auctionSvc.SetAuctionConfig(cfg)
	bidSvc := NewBidService(bidRepo, auctionRepo, realtime, riskSvc, nil, cfg)
	bidSvc.SetHammerService(hammerSvc)
	bidSvc.SetConfigRepository(configRepo)
	bidSvc.SetRiskControlService(riskControlSvc)

	now := time.Now().UTC()
	endTime := now.Add(endOffset)
	startTime := now.Add(-time.Minute)
	if !endTime.After(startTime) {
		startTime = endTime.Add(-time.Minute)
	}
	auction, err := auctionSvc.Create(ctx, CreateAuctionInput{
		ActorID:           "u_2001",
		ActorRole:         domain.RoleMerchant,
		Title:             "Watch",
		Category:          "luxury",
		ConditionGrade:    domain.ConditionNew,
		Description:       "rare watch",
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
	return realtimeAuctionFixture{auctionID: auction.AuctionID, deposits: depositSvc, bids: bidSvc, hammers: hammerSvc, orders: orderSvc, risk: riskSvc, controls: riskControlSvc, depositRepo: depositRepo, orderRepo: orderRepo}
}

func mustEnroll(t *testing.T, fixture realtimeAuctionFixture, userID string) {
	t.Helper()
	if _, err := fixture.deposits.Enroll(context.Background(), EnrollInput{AuctionID: fixture.auctionID, UserID: userID, UserRole: domain.RoleBuyer}); err != nil {
		t.Fatalf("enroll %s: %v", userID, err)
	}
}

func expectedCurrentPrice(price int64) *int64 {
	return &price
}

func setBidBlacklistStrategy(t *testing.T, bids *BidService, cfg domain.BlacklistStrategyConfig) {
	t.Helper()
	repo := repository.NewMemoryConfigRepository()
	if _, err := upsertBlacklistStrategyConfig(context.Background(), repo, cfg, "u_9999"); err != nil {
		t.Fatalf("upsert blacklist strategy: %v", err)
	}
	bids.SetConfigRepository(repo)
}

func setRiskControl(t *testing.T, controls *RiskControlService, cfg domain.RiskControlConfig) {
	t.Helper()
	if controls == nil {
		t.Fatal("risk control service is nil")
	}
	controls.cfg = cfg
}

func waitForBlacklisted(t *testing.T, risk *RiskService, userID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := risk.IsBlacklisted(context.Background(), userID)
		if err == nil && ok {
			return
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	ok, err := risk.IsBlacklisted(context.Background(), userID)
	if err != nil {
		lastErr = err
	}
	t.Fatalf("expected user auto blacklisted, ok=%v err=%v lastErr=%v", ok, err, lastErr)
}

var errCreateBlacklistFailed = errors.New("create blacklist failed")

type failingCreateBlacklistRepo struct {
	*repository.MemoryRiskRepository
}

func (r failingCreateBlacklistRepo) CreateBlacklist(ctx context.Context, item *domain.Blacklist) error {
	return errCreateBlacklistFailed
}
