package service

import (
	"context"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// depositReconcilePrerequisiteReader 是 RT store 上读取 (enrolled, depositReady)
// 状态的最小接口；MemoryRealtimeStore 与 redis.AuctionRealtimeStore 都实现了它。
// DepositReconciler 周期性巡检"押金账本(MySQL)"与"RT enrolled/deposits 集合"
// 的最终一致性：当账本里某 user 是 READY/CAPTURED 但 RT 集合缺失时，调
// MarkEnrollment 回填；其他状态（RELEASED/PENDING/FAILED）不回填，由原链路
// 自然收敛。仅扫描"还可能影响出价"的活跃状态拍品（WARMING_UP/RUNNING/
// EXTENDED/HAMMER_PENDING），避免把已 CLOSED/SETTLED 的历史数据反复拉回 RT。
//
// 设计要点：
//   - 与 BidRecordReconciler 行为一致：构造时保留 stop channel + WaitGroup，
//     Stop 后保证巡检 goroutine 结束，便于在 server shutdown 时优雅停机。
//   - 指标：每次 ReconcileOnce 结束按 result(ok|error|fixed) 触发
//     IncDepositReconcile，整体耗时回写 ObserveDepositReconcileLag。
//     "fixed" 表示本轮修正了至少一个 entry；"ok" 表示无需修正且无错误；
//     "error" 表示中途遇到错误（只统计一次，本轮提前结束）。
//   - 复用 RealtimeStore.MarkEnrollment（同时把 user 加进 enrolled 与
//     deposits 集合），不会引入新 RT 接口。
type DepositReconciler struct {
	auctions repository.AuctionRepository
	deposits repository.DepositRepository
	realtime repository.AuctionRealtimeStore
	metrics  *metrics.Registry
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewDepositReconciler 构造一个押金一致性巡检器。
// interval<=0 时回退到 30s（与 BidRecordReconciler 默认间隔同量级）。
// realtime 为 nil 时回退到 NoopRealtimeStore，整体 ReconcileOnce 退化为 noop。
func NewDepositReconciler(auctions repository.AuctionRepository, deposits repository.DepositRepository, realtime repository.AuctionRealtimeStore, interval time.Duration) *DepositReconciler {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	return &DepositReconciler{
		auctions: auctions,
		deposits: deposits,
		realtime: realtime,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// SetMetrics 注入 Prometheus Registry。nil 安全。
func (r *DepositReconciler) SetMetrics(reg *metrics.Registry) {
	if r == nil {
		return
	}
	r.metrics = reg
}

// Start 开启后台巡检 goroutine。
// 当 ctx 关闭或 Stop 被调用时退出；多次调用 Start 行为未定义（与
// BidRecordReconciler 一致，不做重入保护）。
func (r *DepositReconciler) Start(ctx context.Context) {
	if r == nil || r.auctions == nil || r.deposits == nil {
		return
	}
	r.wg.Add(1)
	go r.loop(ctx)
}

// Stop 关闭 stopCh 并等待巡检 goroutine 结束。允许重复调用：
// 第二次 close 会 panic，故用 sync.Once 思路改成 select 容错。
func (r *DepositReconciler) Stop() {
	if r == nil {
		return
	}
	select {
	case <-r.stopCh:
		// already stopped
	default:
		close(r.stopCh)
	}
	r.wg.Wait()
}

func (r *DepositReconciler) loop(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	// 启动后立即跑一次，避免冷启动后第一轮要等到 interval 之后才发现漂移。
	_ = r.ReconcileOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			_ = r.ReconcileOnce(ctx)
		}
	}
}

// ReconcileOnce 跑一轮巡检。返回值 fixed 仅供测试断言使用：
// 生产 Start->loop 路径不读它（指标与日志已经覆盖关键事件）。
func (r *DepositReconciler) ReconcileOnce(ctx context.Context) error {
	if r == nil || r.auctions == nil || r.deposits == nil {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx, "deposit.reconcile")
	defer span.End()
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.ObserveDepositReconcileLag(time.Since(start))
		}
	}()

	statuses := []domain.AuctionStatus{
		domain.AuctionStatusWarmingUp,
		domain.AuctionStatusRunning,
		domain.AuctionStatusExtended,
		domain.AuctionStatusHammerPending,
	}
	fixed := 0
	for _, status := range statuses {
		auctions, err := r.auctions.List(ctx, domain.AuctionFilter{Status: status, Limit: 100})
		if err != nil {
			r.observeResult("error")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		for _, auction := range auctions {
			n, err := r.reconcileAuction(ctx, auction.AuctionID)
			if err != nil {
				r.observeResult("error")
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}
			fixed += n
		}
	}
	span.SetAttributes(attribute.Int("deposit.reconcile.fixed", fixed))
	if fixed > 0 {
		r.observeResult("fixed")
	} else {
		r.observeResult("ok")
	}
	return nil
}

// reconcileAuction 拉单个拍品的 deposit ledger，对每条 READY/CAPTURED 行
// 走 RT 校验：若 enrolled 或 deposits 集合任一缺失，则调 MarkEnrollment 回填。
// 返回本拍品修正条目数。
func (r *DepositReconciler) reconcileAuction(ctx context.Context, auctionID uint64) (int, error) {
	ledgers, err := r.deposits.ListByAuction(ctx, auctionID)
	if err != nil {
		return 0, err
	}
	fixed := 0
	for _, ledger := range ledgers {
		if ledger.Status != domain.DepositStatusReady && ledger.Status != domain.DepositStatusCaptured {
			continue
		}
		enrolled, depositReady, err := r.realtime.BidPrerequisites(ctx, auctionID, ledger.UserID)
		if err != nil {
			return fixed, err
		}
		if enrolled && depositReady {
			continue
		}
		if err := r.realtime.MarkEnrollment(ctx, auctionID, ledger.UserID); err != nil {
			return fixed, err
		}
		fixed++
	}
	return fixed, nil
}

func (r *DepositReconciler) observeResult(result string) {
	if r.metrics == nil {
		return
	}
	r.metrics.IncDepositReconcile(result)
}
