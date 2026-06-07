package runtime

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/tests/repository"
)

// newDepositReconcilerFixture 构造一个最小内存夹具：1 个 RUNNING 拍品 + 内存
// auctionRepo / depositRepo / realtime。返回 reconciler 与底层仓便于断言。
func newDepositReconcilerFixture(t *testing.T) (*DepositReconciler, *repository.MemoryAuctionRepository, *repository.MemoryDepositRepository, *repository.MemoryRealtimeStore, *metrics.Registry, uint64) {
	t.Helper()
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	realtime := repository.NewMemoryRealtimeStore()

	auction := domain.AuctionLot{
		AuctionID:   30001,
		SellerID:    "u_2001",
		AuctionType: domain.AuctionTypeEnglish,
		StartPrice:  1000,
		CapPrice:    2000,
		Status:      domain.AuctionStatusRunning,
		StartTime:   time.Now().Add(-time.Minute).UTC(),
		EndTime:     time.Now().Add(time.Hour).UTC(),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}

	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "test"})
	rec := NewDepositReconciler(auctionRepo, depositRepo, realtime, 30*time.Second)
	rec.SetMetrics(reg)
	return rec, auctionRepo, depositRepo, realtime, reg, auction.AuctionID
}

// TestDepositReconcilerFixesMissingEnrollment：账本里 READY，但 RT 集合
// 缺失（模拟 RT crash 导致 enrolled/deposits 漂移），ReconcileOnce 应回填，
// 之后 BidPrerequisites 返回 (true, true)。
func TestDepositReconcilerFixesMissingEnrollment(t *testing.T) {
	ctx := context.Background()
	rec, _, depositRepo, realtime, reg, auctionID := newDepositReconcilerFixture(t)

	deposit := domain.DepositLedger{
		AuctionID: auctionID,
		UserID:    "u_1001",
		Amount:    100,
		Status:    domain.DepositStatusReady,
	}
	if err := depositRepo.Create(ctx, &deposit); err != nil {
		t.Fatalf("create deposit: %v", err)
	}

	// 巡检前 RT 是空的。
	if enrolled, depositReady, _ := realtime.BidPrerequisites(ctx, auctionID, "u_1001"); enrolled || depositReady {
		t.Fatalf("pre-reconcile RT should be empty, got enrolled=%v deposit=%v", enrolled, depositReady)
	}

	if err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	enrolled, depositReady, err := realtime.BidPrerequisites(ctx, auctionID, "u_1001")
	if err != nil {
		t.Fatalf("BidPrerequisites: %v", err)
	}
	if !enrolled || !depositReady {
		t.Fatalf("expected RT to be backfilled, got enrolled=%v deposit=%v", enrolled, depositReady)
	}
	if got := counterValue(t, reg, "test_auction_deposit_reconcile_total"); got != 1 {
		t.Fatalf("expected deposit_reconcile_total=1, got %v", got)
	}
}

// TestDepositReconcilerNoOpWhenConsistent：账本 READY 且 RT 已有该用户，
// ReconcileOnce 不应再重复 MarkEnrollment（ok 计数++）。
func TestDepositReconcilerNoOpWhenConsistent(t *testing.T) {
	ctx := context.Background()
	rec, _, depositRepo, realtime, reg, auctionID := newDepositReconcilerFixture(t)

	if err := depositRepo.Create(ctx, &domain.DepositLedger{
		AuctionID: auctionID, UserID: "u_1001", Amount: 100, Status: domain.DepositStatusReady,
	}); err != nil {
		t.Fatalf("create deposit: %v", err)
	}
	if err := realtime.MarkEnrollment(ctx, auctionID, "u_1001"); err != nil {
		t.Fatalf("seed RT enrollment: %v", err)
	}

	if err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	// 仍然一致。
	enrolled, depositReady, _ := realtime.BidPrerequisites(ctx, auctionID, "u_1001")
	if !enrolled || !depositReady {
		t.Fatalf("RT should remain enrolled, got enrolled=%v deposit=%v", enrolled, depositReady)
	}
	// fixed=0 这一轮记 ok。
	if got := counterValue(t, reg, "test_auction_deposit_reconcile_total"); got != 1 {
		t.Fatalf("expected deposit_reconcile_total=1, got %v", got)
	}
}

// TestDepositReconcilerSkipsTerminalStatus：CLOSED_WON 不在扫描列表内，
// 即使账本里有未同步行也不应触发回填。
func TestDepositReconcilerSkipsTerminalStatus(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	realtime := repository.NewMemoryRealtimeStore()

	auction := domain.AuctionLot{
		AuctionID: 30002, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish,
		StartPrice: 1000, Status: domain.AuctionStatusClosedWon,
		StartTime: time.Now().Add(-time.Hour).UTC(), EndTime: time.Now().Add(-time.Minute).UTC(),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if err := depositRepo.Create(ctx, &domain.DepositLedger{
		AuctionID: auction.AuctionID, UserID: "u_1001", Amount: 100, Status: domain.DepositStatusReady,
	}); err != nil {
		t.Fatalf("create deposit: %v", err)
	}
	rec := NewDepositReconciler(auctionRepo, depositRepo, realtime, time.Minute)

	if err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if enrolled, depositReady, _ := realtime.BidPrerequisites(ctx, auction.AuctionID, "u_1001"); enrolled || depositReady {
		t.Fatalf("CLOSED_WON should be skipped, got enrolled=%v deposit=%v", enrolled, depositReady)
	}
}

// TestDepositReconcilerSkipsNonReadyDeposits：PENDING/RELEASED/FAILED 不回填，
// 由原链路自然收敛；RT 不应被本巡检改写。
func TestDepositReconcilerSkipsNonReadyDeposits(t *testing.T) {
	ctx := context.Background()
	rec, _, depositRepo, realtime, _, auctionID := newDepositReconcilerFixture(t)

	statuses := []struct {
		userID string
		status domain.DepositStatus
	}{
		{"u_pending", domain.DepositStatusPending},
		{"u_released", domain.DepositStatusReleased},
		{"u_failed", domain.DepositStatusFailed},
	}
	for _, s := range statuses {
		if err := depositRepo.Create(ctx, &domain.DepositLedger{
			AuctionID: auctionID, UserID: s.userID, Amount: 100, Status: s.status,
		}); err != nil {
			t.Fatalf("create deposit %s: %v", s.userID, err)
		}
	}

	if err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	for _, s := range statuses {
		if enrolled, depositReady, _ := realtime.BidPrerequisites(ctx, auctionID, s.userID); enrolled || depositReady {
			t.Fatalf("status=%s should not be backfilled, got enrolled=%v deposit=%v", s.status, enrolled, depositReady)
		}
	}
}

// TestDepositReconcilerCapturedStillBackfills：CAPTURED 也应回填——
// 落槌路径会把胜者 deposit 改成 CAPTURED，此时 RT 仍可能被外部清理动作清空，
// 巡检需保证胜者 enrolled/deposits 集合一致，避免之后重复出价被错杀。
func TestDepositReconcilerCapturedStillBackfills(t *testing.T) {
	ctx := context.Background()
	rec, _, depositRepo, realtime, _, auctionID := newDepositReconcilerFixture(t)

	if err := depositRepo.Create(ctx, &domain.DepositLedger{
		AuctionID: auctionID, UserID: "u_winner", Amount: 100, Status: domain.DepositStatusCaptured,
	}); err != nil {
		t.Fatalf("create deposit: %v", err)
	}
	if err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	enrolled, depositReady, _ := realtime.BidPrerequisites(ctx, auctionID, "u_winner")
	if !enrolled || !depositReady {
		t.Fatalf("CAPTURED winner should be backfilled, got enrolled=%v deposit=%v", enrolled, depositReady)
	}
}

// TestDepositReconcilerStartStop：Start 后 Stop 必须在合理时间内返回，
// 验证后台 goroutine 能被优雅退出（不挂起）。
func TestDepositReconcilerStartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec, _, _, _, _, _ := newDepositReconcilerFixture(t)
	rec.interval = 10 * time.Millisecond
	rec.Start(ctx)

	done := make(chan struct{})
	go func() {
		rec.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Stop did not return within 2s")
	}
	// 重复 Stop 必须 idempotent。
	rec.Stop()
}

func counterValue(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	metricFamilies, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range metricFamilies {
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
