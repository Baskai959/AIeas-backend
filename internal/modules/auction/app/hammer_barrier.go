package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

// ErrHammerPending 是 publisher 闸门拒绝在 HAMMER_PENDING 期间入队的新出价命令时返回的 sentinel。
// ws 异步分支收到后回 bid.ack(mode=ASYNC,status=REJECTED,reason=AUCTION_HAMMER_PENDING)。
var ErrHammerPending = errors.New("auction is in HAMMER_PENDING; new bid commands are rejected")

// HammerDrainCoordinator 暴露屏障判断 in-flight 排空所需的最小读端口，
// 由 *ws.BidAsyncCoordinator 实现：
//   - PendingForAuction 返回该 auction 当前 pending 命令数（包含 publish 后未裁决、worker 处理中、已 deliver 待 ack）。
type HammerDrainCoordinator interface {
	PendingForAuction(auctionID uint64) int
}

// HammerBarrierMetrics 是屏障所需的最小指标接口，nil 安全。
type HammerBarrierMetrics interface {
	ObserveHammerDrain(elapsed time.Duration)
	IncHammerDrainTimeout()
}

// HammerPublisherGate 维护 per-auction 的 publisher 闸门：
//   - Close(auctionID)：标记该 auction 进入 HAMMER_PENDING，后续 PublishBidCommand 拒绝该 auctionId 的新命令；
//   - Open(auctionID)：清理（在 hammer 真正完成或 fallback 后调用，主要给测试与极端 fallback 使用）；
//   - IsClosed(auctionID)：闸门状态查询，给 publisher 适配器使用。
//
// 进程内单例，sync.Map 不必要；用 RWMutex + map 显式表达即可。
type HammerPublisherGate struct {
	mu       sync.RWMutex
	closed   map[uint64]time.Time // auctionID -> closedAt
	graceMu  sync.RWMutex
	graceFor time.Duration
}

// NewHammerPublisherGate 构造闸门。grace 用于屏障 B 的兜底宽限期（pending=0 后再等 grace ms 才视为追上）。
func NewHammerPublisherGate(grace time.Duration) *HammerPublisherGate {
	if grace < 0 {
		grace = 0
	}
	return &HammerPublisherGate{
		closed:   make(map[uint64]time.Time),
		graceFor: grace,
	}
}

// Close 标记某 auction 进入 HAMMER_PENDING。重复 Close 幂等。
func (g *HammerPublisherGate) Close(auctionID uint64) {
	if g == nil || auctionID == 0 {
		return
	}
	g.mu.Lock()
	if _, ok := g.closed[auctionID]; !ok {
		g.closed[auctionID] = time.Now()
	}
	g.mu.Unlock()
}

// Open 清理 auction 闸门。一般在 hammer 完成或 fallback 之后调用。
func (g *HammerPublisherGate) Open(auctionID uint64) {
	if g == nil || auctionID == 0 {
		return
	}
	g.mu.Lock()
	delete(g.closed, auctionID)
	g.mu.Unlock()
}

// IsClosed 查询闸门是否关闭。供 publisher 适配器在 PublishBidCommand 入口判断。
func (g *HammerPublisherGate) IsClosed(auctionID uint64) bool {
	if g == nil || auctionID == 0 {
		return false
	}
	g.mu.RLock()
	_, ok := g.closed[auctionID]
	g.mu.RUnlock()
	return ok
}

// ClosedSince 返回闸门关闭时刻；未关闭返回 zero、ok=false。供屏障 B 判断 grace 是否已过。
func (g *HammerPublisherGate) ClosedSince(auctionID uint64) (time.Time, bool) {
	if g == nil || auctionID == 0 {
		return time.Time{}, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	t, ok := g.closed[auctionID]
	return t, ok
}

// Grace 返回当前宽限时长。
func (g *HammerPublisherGate) Grace() time.Duration {
	if g == nil {
		return 0
	}
	g.graceMu.RLock()
	defer g.graceMu.RUnlock()
	return g.graceFor
}

// SetGrace 调整宽限时长（主要给测试用）。
func (g *HammerPublisherGate) SetGrace(grace time.Duration) {
	if g == nil || grace < 0 {
		return
	}
	g.graceMu.Lock()
	g.graceFor = grace
	g.graceMu.Unlock()
}

// InFlightBarrier 等待某 auction 的 in-flight 命令排空。
//
// 屏障 A：coordinator.PendingForAuction(auctionID) == 0；
// 屏障 B：HammerPublisherGate 已 Close 且距离 Close 时刻已过宽限期（防止与 publish 入队竞态）。
//
// 单进程实现：与 ws_handler 同进程；多实例 Kafka offset 跨进程协调留 TODO（spec 允许）。
type InFlightBarrier struct {
	coord   HammerDrainCoordinator
	gate    *HammerPublisherGate
	pollMs  int
	metrics HammerBarrierMetrics
}

// NewInFlightBarrier 构造屏障。pollMs<=0 时回退到 50ms。
func NewInFlightBarrier(coord HammerDrainCoordinator, gate *HammerPublisherGate, pollMs int) *InFlightBarrier {
	if pollMs <= 0 {
		pollMs = 50
	}
	return &InFlightBarrier{coord: coord, gate: gate, pollMs: pollMs}
}

// SetMetrics 注入指标实现。nil 安全。
func (b *InFlightBarrier) SetMetrics(m HammerBarrierMetrics) {
	if b == nil {
		return
	}
	b.metrics = m
}

// WaitDrain 关闭闸门并等待 in-flight 排空，返回 ok=true 表示已排空，false 表示 maxWait 超时（应走 fallback finalize）。
func (b *InFlightBarrier) WaitDrain(ctx context.Context, auctionID uint64, maxWait time.Duration) bool {
	if b == nil {
		return true
	}
	start := time.Now()
	if b.gate != nil {
		b.gate.Close(auctionID)
	}
	if maxWait <= 0 {
		maxWait = 5 * time.Second
	}
	pollInterval := time.Duration(b.pollMs) * time.Millisecond
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	deadline := start.Add(maxWait)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	check := func() bool {
		if b.coord != nil && b.coord.PendingForAuction(auctionID) > 0 {
			return false
		}
		if b.gate != nil {
			closedAt, ok := b.gate.ClosedSince(auctionID)
			if !ok {
				return false
			}
			grace := b.gate.Grace()
			if grace > 0 && time.Since(closedAt) < grace {
				return false
			}
		}
		return true
	}
	if check() {
		elapsed := time.Since(start)
		if b.metrics != nil {
			b.metrics.ObserveHammerDrain(elapsed)
		}
		return true
	}
	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			if b.metrics != nil {
				b.metrics.ObserveHammerDrain(elapsed)
			}
			return false
		case now := <-ticker.C:
			if check() {
				elapsed := time.Since(start)
				if b.metrics != nil {
					b.metrics.ObserveHammerDrain(elapsed)
				}
				return true
			}
			if !now.Before(deadline) {
				elapsed := time.Since(start)
				if b.metrics != nil {
					b.metrics.ObserveHammerDrain(elapsed)
					b.metrics.IncHammerDrainTimeout()
				}
				return false
			}
		}
	}
}

// HammerTriggerFromInput 从 HammerInput 推断 trigger 维度，用于 metrics（低基数）。
func HammerTriggerFromInput(in domain.HammerInput) string {
	closedBy := in.ClosedBy
	if closedBy == "" {
		closedBy = ""
	}
	switch closedBy {
	case "CAP_PRICE":
		return "cap_price"
	case "system":
		return "timer"
	case "expired":
		return "expired"
	}
	if in.ActorRole == domain.RoleAdmin || in.ActorRole == domain.RoleMerchant {
		return "manual"
	}
	return "system"
}
