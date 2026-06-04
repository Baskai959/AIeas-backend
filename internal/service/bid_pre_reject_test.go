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

func TestSnapshotFloorPreRejectReason_StepMismatchPreRejected(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	expected := int64(1100)
	// 1150 不与 step 100 对齐，且基于 cached current 做模运算结果不依赖真实 current
	// (差值必为 amount 整数倍，模值不变)，可在本地必然拒为 PRICE_STEP_MISMATCH。
	in := PlaceBidInput{AuctionID: 1, Price: 1150, ExpectedCurrentPrice: &expected}
	reason, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10))
	if !ok || reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected pre-reject PRICE_STEP_MISMATCH, got reason=%q ok=%v", reason, ok)
	}
}

// recordingRealtimeStore wraps MemoryRealtimeStore and counts PlaceBid invocations.
type recordingRealtimeStore struct {
	*repository.MemoryRealtimeStore
	placeBidCalls        int64
	getAuctionStateCalls int64
	bidPrereqCalls       int64
}

func (r *recordingRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	atomic.AddInt64(&r.placeBidCalls, 1)
	return r.MemoryRealtimeStore.PlaceBid(ctx, input)
}

func (r *recordingRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	atomic.AddInt64(&r.getAuctionStateCalls, 1)
	return r.MemoryRealtimeStore.GetAuctionState(ctx, auctionID)
}

func (r *recordingRealtimeStore) BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error) {
	atomic.AddInt64(&r.bidPrereqCalls, 1)
	return r.MemoryRealtimeStore.BidPrerequisites(ctx, auctionID, userID)
}

func (r *recordingRealtimeStore) calls() int64 {
	return atomic.LoadInt64(&r.placeBidCalls)
}

func (r *recordingRealtimeStore) stateCalls() int64 {
	return atomic.LoadInt64(&r.getAuctionStateCalls)
}

func (r *recordingRealtimeStore) prereqCalls() int64 {
	return atomic.LoadInt64(&r.bidPrereqCalls)
}

type preRejectFixture struct {
	auctionID uint64
	bids      *BidService
	realtime  *recordingRealtimeStore
	deposits  *DepositService
	controls  *RiskControlService
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
	return preRejectFixture{auctionID: auction.AuctionID, bids: bidSvc, realtime: rt, deposits: depositSvc, controls: riskControlSvc}
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

func TestBidServicePreRejectsMissingEnrollmentWithoutCallingLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	callsBefore := fx.realtime.calls()
	prereqBefore := fx.realtime.prereqCalls()

	rejected, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "missing-enrollment", AuctionID: fx.auctionID, BidderID: "u_missing",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("missing enrollment bid err: %v", err)
	}
	if rejected.Accepted || rejected.Reason != "NOT_ENROLLED" {
		t.Fatalf("expected NOT_ENROLLED pre-reject, got %+v", rejected)
	}
	if rejected.Event != "bid.rejected" || rejected.RiskResult != domain.BidRiskReject {
		t.Fatalf("pre-reject result must mirror Lua reject shape: %+v", rejected)
	}
	if callsAfter := fx.realtime.calls(); callsAfter != callsBefore {
		t.Fatalf("expected PlaceBid NOT invoked on prerequisite pre-reject, calls before=%d after=%d", callsBefore, callsAfter)
	}
	if prereqAfter := fx.realtime.prereqCalls(); prereqAfter != prereqBefore+1 {
		t.Fatalf("expected exactly one prerequisite check, before=%d after=%d", prereqBefore, prereqAfter)
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

func TestBidServicePrerequisitePositiveCacheAvoidsRepeatedRedisCheck(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "prereq-cache-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !first.Accepted {
		t.Fatalf("first bid result=%+v err=%v", first, err)
	}
	prereqAfterFirst := fx.realtime.prereqCalls()
	if prereqAfterFirst == 0 {
		t.Fatalf("expected first bid to query prerequisites")
	}

	second, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "prereq-cache-2", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil || !second.Accepted {
		t.Fatalf("second bid result=%+v err=%v", second, err)
	}
	if got := fx.realtime.prereqCalls(); got != prereqAfterFirst {
		t.Fatalf("expected prerequisite cache hit, calls before=%d after=%d", prereqAfterFirst, got)
	}
}

func TestBidServiceRiskControlDisabledSkipsBidPrerequisites(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	setRiskControl(t, fx.controls, domain.RiskControlConfig{Enabled: false})
	ctx := context.Background()

	result, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "prereq-disabled", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil || !result.Accepted {
		t.Fatalf("bid result=%+v err=%v", result, err)
	}
	if got := fx.realtime.prereqCalls(); got != 0 {
		t.Fatalf("expected risk-control disabled to skip prerequisites, got calls=%d", got)
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

	// 用已报名用户的步长错误触发 Lua 内 PRICE_STEP_MISMATCH 拒绝；此时 Lua 应返回当时的 currentPrice/version。
	rejected, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "lua-reject-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1150, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("lua reject bid: %v", err)
	}
	if rejected.Accepted || rejected.Reason != domain.BidRejectStepMismatch {
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

func TestSnapshotFloorPreRejectReason_AboveCapPrice(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1100}
	expected := int64(1100)
	in := PlaceBidInput{AuctionID: 1, Price: 2100, ExpectedCurrentPrice: &expected}
	reason, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10))
	if !ok || reason != domain.BidRejectAboveCapPrice {
		t.Fatalf("expected pre-reject ABOVE_CAP_PRICE, got reason=%q ok=%v", reason, ok)
	}
}

func TestSnapshotFloorPreRejectReason_AboveExpectedMaxBidSteps(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 0}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1500}
	expected := int64(1100)
	// expected=1100, maxBidSteps=2, amount=100 → expectedMaxAllowed=1300。price=1400 超过。
	in := PlaceBidInput{AuctionID: 1, Price: 1400, ExpectedCurrentPrice: &expected}
	reason, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 2))
	if !ok || reason != domain.BidRejectAboveExpectedMaxBidSteps {
		t.Fatalf("expected pre-reject ABOVE_EXPECTED_MAX_BID_STEPS, got reason=%q ok=%v", reason, ok)
	}
}

func TestSnapshotFloorPreRejectReason_StaleExpectedBelowCachedRejectsAsBelowMin(t *testing.T) {
	auction := bidAuctionSnapshot{StartPrice: 1000, CapPrice: 2000}
	state := domain.AuctionState{AuctionID: 1, Status: domain.AuctionStatusRunning, CurrentPrice: 1300}
	expected := int64(1100)
	// expected<cached, price=1300 < cached(1300)+amount(100)=1400 → BELOW_MIN_INCREMENT (P1 safe stale reject 路径)。
	in := PlaceBidInput{AuctionID: 1, Price: 1300, ExpectedCurrentPrice: &expected}
	reason, ok := snapshotFloorPreRejectReason(in, state, true, auction, newFixedRule(100, 10))
	if !ok || reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected stale-expected pre-reject as BELOW_MIN_INCREMENT, got reason=%q ok=%v", reason, ok)
	}
}

func TestBidServicePreRejectsAboveCapWithoutCallingLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()

	// 先用一笔合法出价把 cache 灌起来。
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "above-cap-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	callsBefore := fx.realtime.calls()
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "above-cap", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 2500, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if res.Accepted || res.Reason != domain.BidRejectAboveCapPrice {
		t.Fatalf("expected ABOVE_CAP_PRICE pre-reject, got %+v", res)
	}
	if delta := fx.realtime.calls() - callsBefore; delta != 0 {
		t.Fatalf("pre-reject must skip Lua, got delta=%d", delta)
	}
}

func TestBidServicePreRejectsStepMismatchWithoutCallingLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "step-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	callsBefore := fx.realtime.calls()
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "step-mismatch", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1250, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if res.Accepted || res.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected PRICE_STEP_MISMATCH pre-reject, got %+v", res)
	}
	if delta := fx.realtime.calls() - callsBefore; delta != 0 {
		t.Fatalf("pre-reject must skip Lua, got delta=%d", delta)
	}
}

func TestBidServicePreRejectsAboveExpectedMaxStepsWithoutCallingLua(t *testing.T) {
	// maxBidSteps=2 → expected=1000 时 expectedMaxAllowed=1200；price=1500 必超。
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 2), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "max-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	if second, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "max-seed-2", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	}); err != nil || !second.Accepted {
		t.Fatalf("seed bid 2: %+v err=%v", second, err)
	}
	callsBefore := fx.realtime.calls()
	// 现在 cached.CurrentPrice=1200, expected=1000 (stale), expectedMaxAllowed=1000+100*2=1200，price=1500 > 1200。
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "above-expected-max", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1500, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if res.Accepted || res.Reason != domain.BidRejectAboveExpectedMaxBidSteps {
		t.Fatalf("expected ABOVE_EXPECTED_MAX_BID_STEPS pre-reject, got %+v", res)
	}
	if delta := fx.realtime.calls() - callsBefore; delta != 0 {
		t.Fatalf("pre-reject must skip Lua, got delta=%d", delta)
	}
}

func TestBidServiceLowestValidPriceStillReachesLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "low-valid-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	callsBefore := fx.realtime.calls()
	// price=1200 == cached(1100)+amount(100) 是最低有效价，必须放行进 Lua。
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "low-valid", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil || !res.Accepted {
		t.Fatalf("expected lowest-valid bid accepted via Lua, got %+v err=%v", res, err)
	}
	if delta := fx.realtime.calls() - callsBefore; delta != 1 {
		t.Fatalf("lowest-valid bid must reach Lua exactly once, got delta=%d", delta)
	}
}

func TestBidServiceSamePriceInflightGateRejectsBeyondLimit(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "gate-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	// 直接占住 in-flight 槽位，模拟正在等待 Lua 的并发请求。
	key := samePriceGateKey(fx.auctionID, 1100, 1200)
	counter := fx.bids.samePriceGateCounter(key)
	if counter == nil {
		t.Fatalf("expected gate counter")
	}
	counter.Add(int32(samePriceInflightLimit))

	prereqBefore := fx.realtime.prereqCalls()
	placeBefore := fx.realtime.calls()
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "gate-busy", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if res.Accepted || res.Reason != domain.BidRejectAuctionBusy {
		t.Fatalf("expected AUCTION_BUSY when gate full, got %+v", res)
	}
	if res.Event != "bid.rejected" || res.RiskResult != domain.BidRiskReject {
		t.Fatalf("gate reject must mirror Lua reject shape: %+v", res)
	}
	if delta := fx.realtime.calls() - placeBefore; delta != 0 {
		t.Fatalf("gate reject must skip Lua, got delta=%d", delta)
	}
	if delta := fx.realtime.prereqCalls() - prereqBefore; delta != 0 {
		t.Fatalf("gate reject must skip prerequisite check, got delta=%d", delta)
	}
	// gate 拒不写幂等：相同 RequestID 的下一笔仍应能命中 gate 拒（counter 仍满）。
	res2, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "gate-busy", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("place dup: %v", err)
	}
	if res2.Duplicate {
		t.Fatalf("gate reject must not write idempotency, got duplicate=%+v", res2)
	}
}

func TestBidServiceInflightHighestPriceGateRejectsLowerPrice(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "highest-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	// 模拟已有 1300 的请求正在 Lua 内排队/执行，此时 1200 虽然是本地视角的下一口价，
	// 但低于当前 in-flight 最高价，应直接返回 AUCTION_BUSY。
	highKey := samePriceGateKey(fx.auctionID, 1100, 1300)
	highCounter := fx.bids.samePriceGateCounter(highKey)
	if highCounter == nil {
		t.Fatalf("expected high price gate counter")
	}
	highCounter.Add(1)

	placeBefore := fx.realtime.calls()
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "highest-low", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if res.Accepted || res.Reason != domain.BidRejectAuctionBusy {
		t.Fatalf("expected AUCTION_BUSY for lower-than-highest in-flight price, got %+v", res)
	}
	if delta := fx.realtime.calls() - placeBefore; delta != 0 {
		t.Fatalf("highest-price gate reject must skip Lua, got delta=%d", delta)
	}
}

func TestBidServiceSamePriceGateReleasesAfterLua(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "release-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	// 一笔最低有效价正常进入 Lua → 之后 counter 必须归零。
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "release-1", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil || !res.Accepted {
		t.Fatalf("expected accepted, got %+v err=%v", res, err)
	}
	key := samePriceGateKey(fx.auctionID, 1100, 1200)
	counter := fx.bids.samePriceGateCounter(key)
	if counter == nil || counter.Load() != 0 {
		var v int32 = -1
		if counter != nil {
			v = counter.Load()
		}
		t.Fatalf("counter must be released to 0 after Lua, got %d", v)
	}
}

func TestBidServiceSamePriceGateAllowsNonLowestPrice(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "non-lowest-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	// 非最低价 (1300>1100+100) 也会经过最高价 gate；无其它更高 in-flight 时应放行。
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "non-lowest", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1300, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil || !res.Accepted {
		t.Fatalf("expected accepted, got %+v err=%v", res, err)
	}
}

func TestBidServiceSamePriceGateAllowsCapBid(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	ctx := context.Background()
	if first, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "cap-seed", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !first.Accepted {
		t.Fatalf("seed bid: %+v err=%v", first, err)
	}
	// price == capPrice(2000) 时也允许进入最高价 gate；无其它更高 in-flight 时应放行。
	res, err := fx.bids.Place(ctx, PlaceBidInput{
		RequestID: "cap-bid", AuctionID: fx.auctionID, BidderID: "u_1001",
		UserRole: domain.RoleBuyer, Price: 2000, ExpectedCurrentPrice: expectedCurrentPrice(1100),
	})
	if err != nil || !res.Accepted {
		t.Fatalf("expected cap bid accepted, got %+v err=%v", res, err)
	}
}

func TestBidServiceSamePriceGateSweepsIdleZeroCounters(t *testing.T) {
	fx := newPreRejectFixture(t, newFixedRuleRaw(100, 10), domain.AuctionStatusRunning)
	key := samePriceGateKey(fx.auctionID, 1100, 1200)
	counter := fx.bids.samePriceGateCounter(key)
	if counter == nil {
		t.Fatalf("expected gate counter")
	}
	if !fx.bids.samePriceGateExists(key) {
		t.Fatalf("expected gate entry to exist before sweep")
	}
	fx.bids.samePriceGateMu.Lock()
	if entry := fx.bids.samePriceInflight[samePriceGateAuctionKey(fx.auctionID)]; entry != nil {
		entry.lastUsed = time.Now().Add(-samePriceGateIdleTTL - time.Second)
	}
	fx.bids.samePriceGateNextSweep = time.Time{}
	fx.bids.sweepSamePriceGateLocked(time.Now())
	fx.bids.samePriceGateMu.Unlock()
	if fx.bids.samePriceGateExists(key) {
		t.Fatalf("expected idle zero counter to be swept")
	}
}
