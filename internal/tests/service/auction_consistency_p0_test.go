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
	"aieas_backend/internal/tests/repository"
)

// failingInitRealtime 用于 P0-2 测试：InitAuction 必失败，其余方法走 Noop。
type failingInitRealtime struct {
	repository.NoopRealtimeStore
	err error
}

func (f *failingInitRealtime) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	_ = auction
	_ = minIncrement
	return domain.AuctionState{}, f.err
}

// TestAuctionStartTCCKeepsWarmingUpOnInitFailure 验证 P0-2：
// startWithTiming 的 TCC 中间态——InitAuction 失败时拍品状态停留在 WARMING_UP，
// 不回滚到 READY，也不前进到 RUNNING；error 上抛供监控告警观察。
func TestAuctionStartTCCKeepsWarmingUpOnInitFailure(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	initErr := errors.New("init auction failure")
	svc.SetRealtime(&failingInitRealtime{err: initErr})

	now := time.Now().UTC()
	auction, err := svc.Create(ctx, CreateAuctionInput{
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
		IncrementRule:     json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		AntiSnipingSec:    60,
		AntiExtendSec:     30,
		DepositAmount:     100,
		Status:            domain.AuctionStatusReady,
		StartTime:         now.Add(time.Minute),
		EndTime:           now.Add(time.Hour),
		AllowSystemStatus: true,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}

	startTime := now
	endTime := now.Add(time.Hour)
	if _, err := svc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, startTime, endTime); !errors.Is(err, initErr) {
		t.Fatalf("expected InitAuction error to propagate, got %v", err)
	}

	stored, err := auctionRepo.FindByID(ctx, auction.AuctionID)
	if err != nil {
		t.Fatalf("find auction: %v", err)
	}
	if stored.Status != domain.AuctionStatusWarmingUp {
		t.Fatalf("expected status WARMING_UP after InitAuction failure, got %s", stored.Status)
	}
	if !stored.StartTime.Equal(startTime.UTC()) || !stored.EndTime.Equal(endTime.UTC()) {
		t.Fatalf("expected warming-up to persist start/end time, got %+v", stored)
	}
}

// TestAuctionStartTCCAdvancesToRunningOnSuccess 验证 P0-2 happy path：
// startWithTiming 成功时状态推进 READY → WARMING_UP → RUNNING，最终落到 RUNNING。
func TestAuctionStartTCCAdvancesToRunningOnSuccess(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	svc.SetRealtime(repository.NewMemoryRealtimeStore())

	now := time.Now().UTC()
	auction, err := svc.Create(ctx, CreateAuctionInput{
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
		IncrementRule:     json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		AntiSnipingSec:    60,
		AntiExtendSec:     30,
		DepositAmount:     100,
		Status:            domain.AuctionStatusReady,
		StartTime:         now.Add(time.Minute),
		EndTime:           now.Add(time.Hour),
		AllowSystemStatus: true,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}

	startTime := now
	endTime := now.Add(time.Hour)
	final, err := svc.StartWithTiming(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant, startTime, endTime)
	if err != nil {
		t.Fatalf("start with timing: %v", err)
	}
	if final.Status != domain.AuctionStatusRunning {
		t.Fatalf("expected final status RUNNING, got %s", final.Status)
	}

	stored, err := auctionRepo.FindByID(ctx, auction.AuctionID)
	if err != nil {
		t.Fatalf("find auction: %v", err)
	}
	if stored.Status != domain.AuctionStatusRunning {
		t.Fatalf("expected stored status RUNNING, got %s", stored.Status)
	}
	if !stored.StartTime.Equal(startTime.UTC()) || !stored.EndTime.Equal(endTime.UTC()) {
		t.Fatalf("expected stored timing match, got %+v", stored)
	}
}

// conflictAuctionRepo 是 AuctionRepository 的薄包装：在 CloseWithVersion 第一次
// 被调用时把"期望版本"故意改坏（-1），制造 CAS 冲突，用于验证 P0-1 的告警路径。
type conflictAuctionRepo struct {
	repository.AuctionRepository
	conflictOnce bool
}

func (r *conflictAuctionRepo) CloseWithVersion(ctx context.Context, auction *domain.AuctionLot, expectedVersion int64, allowedFromStatuses []domain.AuctionStatus) error {
	if r.conflictOnce {
		r.conflictOnce = false
		// 把期望版本故意减一，必命中 ErrOptimisticConflict。
		return r.AuctionRepository.CloseWithVersion(ctx, auction, expectedVersion-1, allowedFromStatuses)
	}
	return r.AuctionRepository.CloseWithVersion(ctx, auction, expectedVersion, allowedFromStatuses)
}

// TestHammerOptimisticConflictEmitsMetric 验证 P0-1：
// 落槌 commit 阶段命中 CAS 冲突时返回 domain.ErrOptimisticConflict，
// 且 hammer_optimistic_conflict_total 计数器+1。
func TestHammerOptimisticConflictEmitsMetric(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	fixture := newRealtimeAuctionFixture(t, cfg)
	mustEnroll(t, fixture, "u_1001")

	if result, err := fixture.bids.Place(ctx, PlaceBidInput{
		RequestID: "hammer-cas-bid", AuctionID: fixture.auctionID,
		BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil || !result.Accepted {
		t.Fatalf("place bid: result=%+v err=%v", result, err)
	}

	// 复制当前 auction 到独立 repo 以隔离冲突注入；version 从 0 开始与原 fixture 解耦。
	seed, err := fixture.bids.AuctionRepository().FindByID(ctx, fixture.auctionID)
	if err != nil {
		t.Fatalf("snapshot auction: %v", err)
	}
	seed.Version = 0
	seed.Status = domain.AuctionStatusRunning
	base := repository.NewMemoryAuctionRepository()
	if err := base.Create(ctx, &seed); err != nil {
		t.Fatalf("seed clone: %v", err)
	}
	wrapped := &conflictAuctionRepo{AuctionRepository: base, conflictOnce: true}
	hammerSvc := NewHammerService(
		wrapped,
		fixture.orderRepo,
		fixture.depositRepo,
		fixture.bids.RealtimeStore(),
		repository.NoopTxManager{},
		nil,
	)
	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "test"})
	hammerSvc.SetMetrics(reg)

	_, _, err = hammerSvc.Hammer(ctx, domain.HammerInput{
		RequestID: "hammer-cas-conflict",
		AuctionID: seed.AuctionID,
		ActorID:   "u_2001",
		ActorRole: domain.RoleMerchant,
		ClosedBy:  "u_2001",
		Now:       time.Now().UTC(),
	})
	if !errors.Is(err, domain.ErrOptimisticConflict) {
		t.Fatalf("expected ErrOptimisticConflict, got %v", err)
	}
	if got := counterValue(t, reg, "test_auction_hammer_optimistic_conflict_total"); got != 1 {
		t.Fatalf("expected hammer_optimistic_conflict_total=1, got %v", got)
	}
}

// counterValue 读取指定 counter 当前值，找不到时返回 0。命名空间已包含在 name 里。
func counterValue(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return 0
}

// TestBidServiceStreamEnabledSkipsMySQLFindByRequestID 验证 P0-3：
// bidStreamEnabled=true 时 BidService.place 直接走 Redis 幂等链路，不再调用
// bid_record 仓的 FindByRequestID。
func TestBidServiceStreamEnabledSkipsMySQLFindByRequestID(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{
		AuctionID: 20001, SellerID: "u_2001",
		AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000,
		IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		Status:        domain.AuctionStatusRunning,
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now().Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	bids := &countingFindByRequestIDRepo{}
	realtime := &streamEnabledRealtime{result: domain.BidResult{
		RequestID: "stream-skip", AuctionID: auction.AuctionID,
		BidderID: "u_1001", Price: 1100, Accepted: true, CurrentPrice: 1100,
		Seq: 1, StreamID: "1-0", Event: "bid.accepted",
	}}
	svc := NewBidService(bids, auctionRepo, realtime, nil, nil, appconfig.Default().Auction)

	if _, err := svc.Place(ctx, PlaceBidInput{
		RequestID: "stream-skip", AuctionID: auction.AuctionID,
		BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
	}); err != nil {
		t.Fatalf("place: %v", err)
	}
	if bids.findCalls != 0 {
		t.Fatalf("stream-enabled bid service must not call MySQL FindByRequestID, got %d", bids.findCalls)
	}
}

// TestBidServiceStreamDisabledStillCallsMySQLFindByRequestID 反向验证 P0-3：
// bidStreamEnabled=false（默认 NoopRealtimeStore）时仍走 MySQL 前置幂等查询，
// 保证未启用 stream 的链路行为不退化。
func TestBidServiceStreamDisabledStillCallsMySQLFindByRequestID(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{
		AuctionID: 20002, SellerID: "u_2001",
		AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000,
		IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		Status:        domain.AuctionStatusRunning,
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now().Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	bids := &countingFindByRequestIDRepo{}
	// 显式传入 nil realtime，让 NewBidService 兜底 NoopRealtimeStore（不实现 StreamEnabled）。
	svc := NewBidService(bids, auctionRepo, nil, nil, nil, appconfig.Default().Auction)

	_, _ = svc.Place(ctx, PlaceBidInput{
		RequestID: "no-stream", AuctionID: auction.AuctionID,
		BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if bids.findCalls != 1 {
		t.Fatalf("stream-disabled bid service must call MySQL FindByRequestID exactly once, got %d", bids.findCalls)
	}
}

// countingFindByRequestIDRepo 是 BidRepository 的最小实现：仅记录 FindByRequestID
// 调用次数，其余方法 Noop。
type countingFindByRequestIDRepo struct {
	findCalls int
}

func (r *countingFindByRequestIDRepo) Create(ctx context.Context, bid *domain.BidRecord) error {
	_ = ctx
	_ = bid
	return nil
}

func (r *countingFindByRequestIDRepo) CreateIgnoreBatch(ctx context.Context, records []domain.BidRecord) error {
	_ = ctx
	_ = records
	return nil
}

func (r *countingFindByRequestIDRepo) FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error) {
	_ = ctx
	_ = requestID
	r.findCalls++
	return domain.BidRecord{}, domain.ErrNotFound
}

func (r *countingFindByRequestIDRepo) ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	return nil, nil
}

func (r *countingFindByRequestIDRepo) CountByAuction(ctx context.Context, auctionID uint64) (int, error) {
	_ = ctx
	_ = auctionID
	return 0, nil
}

func (r *countingFindByRequestIDRepo) ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error) {
	_ = ctx
	_ = sessionID
	_ = sortBy
	_ = limit
	_ = offset
	return nil, nil
}
