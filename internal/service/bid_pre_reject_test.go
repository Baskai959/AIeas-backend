package service

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func newFixedRule(amount int64, maxBidSteps int) domain.IncrementRule {
	return domain.IncrementRule{Type: domain.IncrementRuleTypeFixed, Amount: amount, MaxBidSteps: maxBidSteps}
}

func newFixedRuleRaw(amount int64, maxBidSteps int) json.RawMessage {
	raw, _ := json.Marshal(domain.IncrementRule{Type: domain.IncrementRuleTypeFixed, Amount: amount, MaxBidSteps: maxBidSteps})
	return raw
}

func newLadderRuleRaw() json.RawMessage {
	max1 := int64(2000)
	rule := domain.IncrementRule{
		Type:        domain.IncrementRuleTypeLadder,
		MaxBidSteps: 5,
		Steps: []domain.IncrementStep{
			{Min: 0, Max: &max1, Amount: 100},
			{Min: 2000, Amount: 200},
		},
	}
	raw, _ := json.Marshal(rule)
	return raw
}

func TestSnapshotFloorPreRejectReason_BelowMinIncrement(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	expected := int64(1100)
	in := PlaceBidInput{AuctionID: 1, Price: 1100, ExpectedCurrentPrice: &expected}
	reason, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10))
	if !ok || reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected pre-reject BELOW_MIN_INCREMENT, got reason=%q ok=%v", reason, ok)
	}
}

func TestSnapshotFloorPreRejectReason_BelowStartPrice(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1000}
	expected := int64(1000)
	in := PlaceBidInput{AuctionID: 1, Price: 900, ExpectedCurrentPrice: &expected}
	reason, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10))
	if !ok || reason != domain.BidRejectBelowStartPrice {
		t.Fatalf("expected pre-reject BELOW_START_PRICE, got reason=%q ok=%v", reason, ok)
	}
}

func TestSnapshotFloorPreRejectReason_ExactlyFloorPasses(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	expected := int64(1100)
	in := PlaceBidInput{AuctionID: 1, Price: 1200, ExpectedCurrentPrice: &expected}
	if _, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10)); ok {
		t.Fatalf("price exactly at floor should pass to Lua, got pre-reject")
	}
}

func TestSnapshotFloorPreRejectReason_LadderSkipsPreReject(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	expected := int64(1100)
	in := PlaceBidInput{AuctionID: 1, Price: 1100, ExpectedCurrentPrice: &expected}
	ladder := domain.IncrementRule{Type: domain.IncrementRuleTypeLadder, MaxBidSteps: 5, Steps: []domain.IncrementStep{{Min: 0, Amount: 100}}}
	if _, ok := snapshotFloorPreRejectReason(in, state, true, auction, ladder); ok {
		t.Fatalf("ladder rule must skip pre-reject")
	}
}

func TestSnapshotFloorPreRejectReason_StateMissingSkips(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{}
	expected := int64(1100)
	in := PlaceBidInput{AuctionID: 1, Price: 1100, ExpectedCurrentPrice: &expected}
	if _, ok := snapshotFloorPreRejectReason(in, state, false, auction, newFixedRule(100, 10)); ok {
		t.Fatalf("state miss must skip pre-reject")
	}
	stateZero := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 0}
	if _, ok := snapshotFloorPreRejectReason(in, stateZero, true, auction, newFixedRule(100, 10)); ok {
		t.Fatalf("zero current price must skip pre-reject")
	}
	auctionZero := bidAuctionSnapshot{StartPrice: 0, CapPrice: 2000}
	stateOK := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	if _, ok := snapshotFloorPreRejectReason(in, stateOK, true, auctionZero, newFixedRule(100, 10)); ok {
		t.Fatalf("zero start price must skip pre-reject")
	}
	if _, ok := snapshotFloorPreRejectReason(in, stateOK, true, bidAuctionSnapshot{StartPrice: 1000}, newFixedRule(0, 10)); ok {
		t.Fatalf("zero amount fixed rule must skip pre-reject")
	}
}

func TestSnapshotFloorPreRejectReason_NonRunningStatusSkips(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	expected := int64(1100)
	in := PlaceBidInput{AuctionID: 1, Price: 1100, ExpectedCurrentPrice: &expected}
	rule := newFixedRule(100, 10)
	for _, st := range []domain.AuctionStatus{
		domain.AuctionStatusReady,
		domain.AuctionStatusWarmingUp,
		domain.AuctionStatusHammerPending,
		domain.AuctionStatusClosedWon,
	} {
		state := domain.AuctionState{AuctionID: 1, Status: st, CurrentPrice: 1100}
		if _, ok := snapshotFloorPreRejectReason(in, state, true, auction, rule); ok {
			t.Fatalf("status %s must skip pre-reject", st)
		}
	}
	for _, st := range []domain.AuctionStatus{domain.AuctionStatusRunning, domain.AuctionStatusExtended} {
		state := domain.AuctionState{AuctionID: 1, Status: st, CurrentPrice: 1100}
		if _, ok := snapshotFloorPreRejectReason(in, state, true, auction, rule); !ok {
			t.Fatalf("status %s must allow pre-reject when price too low", st)
		}
	}
}

func TestSnapshotFloorPreRejectReason_StepMismatchSkips(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	expected := int64(1100)
	// 1150 不与 step 100 对齐，应交给 Lua 区分 STEP_MISMATCH，而不是预拒。
	in := PlaceBidInput{AuctionID: 1, Price: 1150, ExpectedCurrentPrice: &expected}
	if _, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10)); ok {
		t.Fatalf("misaligned step must skip pre-reject")
	}
}

// recordingRealtimeStore wraps MemoryRealtimeStore and counts PlaceBid invocations.
type recordingRealtimeStore struct {
	*repository.MemoryRealtimeStore
	placeBidCalls        int64
	getAuctionStateCalls int64
}

func (r *recordingRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	atomic.AddInt64(&r.placeBidCalls, 1)
	return r.MemoryRealtimeStore.PlaceBid(ctx, input)
}

func (r *recordingRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	atomic.AddInt64(&r.getAuctionStateCalls, 1)
	return r.MemoryRealtimeStore.GetAuctionState(ctx, auctionID)
}

func (r *recordingRealtimeStore) calls() int64 {
	return atomic.LoadInt64(&r.placeBidCalls)
}

func (r *recordingRealtimeStore) stateCalls() int64 {
	return atomic.LoadInt64(&r.getAuctionStateCalls)
}

type preRejectFixture struct {
	auctionID uint64
	bids      *BidService
	realtime  *recordingRealtimeStore
	deposits  *DepositService
}

func newPreRejectFixture(t *testing.T, rule json.RawMessage, status domain.AuctionStatus) preRejectFixture {
	t.Helper()
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100

	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	configRepo := repository.NewMemoryConfigRepository()
	memRT := repository.NewMemoryRealtimeStore()
	rt := &recordingRealtimeStore{MemoryRealtimeStore: memRT}

	riskSvc := NewRiskService(riskRepo, rt, nil)
	riskControlSvc := NewRiskControlService(domain.DefaultRiskControlConfig())
	depositSvc := NewDepositService(depositRepo, auctionRepo, rt, riskSvc, repository.NoopTxManager{})
	depositSvc.SetRiskControlService(riskControlSvc)
	hammerSvc := NewHammerService(auctionRepo, orderRepo, depositRepo, rt, repository.NoopTxManager{}, nil)
	auctionSvc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	auctionSvc.SetRealtime(rt)
	auctionSvc.SetAuctionConfig(cfg)
	bidSvc := NewBidService(bidRepo, auctionRepo, rt, riskSvc, nil, cfg)
	bidSvc.SetHammerService(hammerSvc)
	bidSvc.SetConfigRepository(configRepo)
	bidSvc.SetRiskControlService(riskControlSvc)

	now := time.Now().UTC()
	endTime := now.Add(time.Hour)
	startTime := now.Add(-time.Minute)
	auction, err := auctionSvc.Create(ctx, CreateAuctionInput{
		ActorID:           "u_2001",
		ActorRole:         domain.RoleMerchant,
		Title:             "Watch",
		Category:          "luxury",
		ConditionGrade:    domain.ConditionNew,
		Description:       "rare watch",
		AuctionType:       domain.AuctionTypeEnglish,
		StartPrice:        1000,
		ReservePrice:      1000,
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
	if _, err := auctionSvc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, startTime, endTime); err != nil {
		t.Fatalf("start auction: %v", err)
	}
	if status != "" && status != domain.AuctionStatusRunning {
		auction.Status = status
		auction.StartTime = startTime
		auction.EndTime = endTime
		if err := auctionRepo.Update(ctx, &auction); err != nil {
			t.Fatalf("update auction status: %v", err)
		}
		if _, err := rt.InitAuction(ctx, auction, 100); err != nil {
			t.Fatalf("re-init realtime: %v", err)
		}
	}
	if _, err := depositSvc.Enroll(ctx, EnrollInput{AuctionID: auction.AuctionID, UserID: "u_1001", UserRole: domain.RoleBuyer}); err != nil {
		t.Fatalf("enroll: %v", err)
	}
	return preRejectFixture{auctionID: auction.AuctionID, bids: bidSvc, realtime: rt, deposits: depositSvc}
}

func TestBidServicePreRejectsBelowFloorWithoutCallingLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "ok-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !first.Accepted {
		t.Fatalf("seed accepted bid: %+v err=%v", first, err)
	}
	callsBefore := fx.realtime.calls()

	rejected, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "low-2", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("low bid err: %v", err)
	}
	if rejected.Accepted || rejected.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected BELOW_MIN_INCREMENT pre-reject, got %+v", rejected)
	}
	if rejected.Event != "bid.rejected" || rejected.RiskResult != domain.BidRiskReject {
		t.Fatalf("pre-reject result must mirror Lua reject shape: %+v", rejected)
	}
	if callsAfter := fx.realtime.calls(); callsAfter != callsBefore {
		t.Fatalf("expected PlaceBid NOT invoked on pre-reject, calls before=%d after=%d", callsBefore, callsAfter)
	}
}

func TestBidServicePreRejectAtFloorReachesLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	callsBefore := fx.realtime.calls()
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "floor-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("floor bid err: %v", err)
	}
	if !res.Accepted || res.CurrentPrice != 1100 {
		t.Fatalf("expected accepted floor bid, got %+v", res)
	}
	if fx.realtime.calls() != callsBefore+1 {
		t.Fatalf("expected exactly one PlaceBid invocation when price meets floor")
	}
}

// 对 ladder 规则，pre-reject 必须直接跳过（行级单测见
// TestSnapshotFloorPreRejectReason_LadderSkipsPreReject）。这里再用 e2e 形式确认
// 一笔接近 floor 的 ladder 出价能正常被 Lua/service 接受（即未被 pre-reject 误拦）。
func TestBidServicePreRejectSkippedForLadderRule(t *testing.T) {
	fx := newPreRejectFixture(t, newLadderRuleRaw(), domain.AuctionStatusRunning)
	ctx := context.Background()

	first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "ladder-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !first.Accepted {
		t.Fatalf("ladder seed bid: %+v err=%v", first, err)
	}
}

// TestBidServicePreRejectsLocalCachedFloorBeforeRedisState 验证：当本地 cachedBidRealtimeState
// 命中且报价低于 floor 时，预拒应当直接返回，不再调用 Redis 的 GetAuctionState 或 PlaceBid。
func TestBidServicePreRejectsLocalCachedFloorBeforeRedisState(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "seed-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !first.Accepted {
		t.Fatalf("seed accepted bid: %+v err=%v", first, err)
	}
	stateBefore := fx.realtime.stateCalls()
	placeBefore := fx.realtime.calls()

	rejected, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "low-2", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("low bid err: %v", err)
	}
	if rejected.Accepted || rejected.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected BELOW_MIN_INCREMENT pre-reject from local cache, got %+v", rejected)
	}
	if delta := fx.realtime.stateCalls() - stateBefore; delta != 0 {
		t.Fatalf("expected zero GetAuctionState calls during local pre-reject, got delta=%d", delta)
	}
	if delta := fx.realtime.calls() - placeBefore; delta != 0 {
		t.Fatalf("expected zero PlaceBid calls during local pre-reject, got delta=%d", delta)
	}
}

// TestBidServiceLocalCachedMissedGoesDirectlyToLua 验证：cachedBidRealtimeState 缺失时，
// 主路径不再调用 GetAuctionState，而是直接进入 Lua（PlaceBid）。
func TestBidServiceLocalCachedMissedGoesDirectlyToLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	stateBefore := fx.realtime.stateCalls()
	placeBefore := fx.realtime.calls()
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "first-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("first bid err: %v", err)
	}
	if !res.Accepted {
		t.Fatalf("expected accepted first bid, got %+v", res)
	}
	if delta := fx.realtime.stateCalls() - stateBefore; delta != 0 {
		t.Fatalf("main path must not invoke GetAuctionState even on cache miss, got delta=%d", delta)
	}
	if delta := fx.realtime.calls() - placeBefore; delta != 1 {
		t.Fatalf("cache miss should go directly to Lua exactly once, got delta=%d", delta)
	}
}

// TestBidServiceMainPathNeverCallsGetAuctionState 验证：无论 cache 命中还是 miss，
// 主路径整体上从不调用 GetAuctionState。
func TestBidServiceMainPathNeverCallsGetAuctionState(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	if delta := fx.realtime.stateCalls(); delta != 0 {
		t.Fatalf("baseline GetAuctionState calls expected 0, got %d", delta)
	}
	first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "main-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !first.Accepted {
		t.Fatalf("first accepted bid: %+v err=%v", first, err)
	}
	if got := fx.realtime.stateCalls(); got != 0 {
		t.Fatalf("after cache-miss bid, GetAuctionState calls expected 0, got %d", got)
	}
	second, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "main-2", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil || !second.Accepted {
		t.Fatalf("second accepted bid: %+v err=%v", second, err)
	}
	if got := fx.realtime.stateCalls(); got != 0 {
		t.Fatalf("after cache-hit bid, GetAuctionState calls expected 0, got %d", got)
	}
}

// TestBidServiceLuaResultUpdatesLocalCache 验证：Lua 返回（accepted/rejected/duplicate）
// 后立即更新本地 cachedBidRealtimeState，下一次同 auction 出价价格不够时被本地预拒。
func TestBidServiceLuaResultUpdatesLocalCache(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "cache-update-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !first.Accepted {
		t.Fatalf("seed accepted bid: %+v err=%v", first, err)
	}
	cached, ok := fx.bids.cachedBidRealtimeState(fx.auctionID, time.Now())
	if !ok {
		t.Fatalf("expected local cache populated after accepted Lua result")
	}
	if cached.CurrentPrice != 1100 || cached.AuctionID != fx.auctionID {
		t.Fatalf("local cache not refreshed by accepted Lua result: %+v", cached)
	}

	placeBefore := fx.realtime.calls()
	rejected, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "cache-update-2", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("second bid: %v", err)
	}
	if rejected.Accepted || rejected.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected pre-reject from cache populated by Lua, got %+v", rejected)
	}
	if delta := fx.realtime.calls() - placeBefore; delta != 0 {
		t.Fatalf("second bid should pre-reject locally without calling Lua, got delta=%d", delta)
	}
}

// TestBidServiceLuaRejectUpdatesLocalCache 验证：Lua 拒绝路径返回的 currentPrice/version
// 也会更新本地 cache，下一次出价继续保鲜。
func TestBidServiceLuaRejectUpdatesLocalCache(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	// 用未注册的用户触发 Lua 内 NOT_ENROLLED 拒绝；此时 Lua 应返回当时的 currentPrice/version。
	rejected, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "lua-reject-1", AuctionID: fx.auctionID, BidderID: "u_2002",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("lua reject bid: %v", err)
	}
	if rejected.Accepted || rejected.Reason == "" {
		t.Fatalf("expected Lua-side rejection, got %+v", rejected)
	}
	cached, ok := fx.bids.cachedBidRealtimeState(fx.auctionID, time.Now())
	if !ok {
		t.Fatalf("expected local cache populated by Lua reject result")
	}
	if cached.AuctionID != fx.auctionID || cached.Status == "" && cached.Version == 0 && cached.CurrentPrice == 0 {
		t.Fatalf("local cache not meaningfully populated by Lua reject: %+v", cached)
	}
}
