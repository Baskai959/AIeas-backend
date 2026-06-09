package app

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	realtimeinfra "aieas_backend/internal/infra/realtime"
	auctionports "aieas_backend/internal/modules/auction/ports"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
	orderrepo "aieas_backend/internal/modules/order/repository"
)

// recordingHammerPublisher 是一个收集所有广播事件的测试 publisher，用于断言 auction.state / auction.closed 帧。
type recordingHammerPublisher struct {
	mu     sync.Mutex
	events []recordedHammerEvent
}

type recordedHammerEvent struct {
	auctionID uint64
	envType   string
	payload   json.RawMessage
}

func (p *recordingHammerPublisher) Broadcast(auctionID uint64, env auctionports.EventEnvelope) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, recordedHammerEvent{auctionID: auctionID, envType: env.Type, payload: env.Payload})
	return 1
}

func (p *recordingHammerPublisher) snapshot() []recordedHammerEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]recordedHammerEvent, len(p.events))
	copy(out, p.events)
	return out
}

// fakePendingCoordinator 实现 HammerDrainCoordinator，在测试里手动控制 PendingForAuction 返回值。
type fakePendingCoordinator struct {
	pending atomic.Int32
}

func (c *fakePendingCoordinator) PendingForAuction(auctionID uint64) int {
	_ = auctionID
	return int(c.pending.Load())
}

func (c *fakePendingCoordinator) set(v int32) { c.pending.Store(v) }

func newRunningAuctionFixture(t *testing.T, auctionID uint64) (*auctionrepo.MemoryAuctionRepository, *auctionrepo.MemoryBidRepository, *orderrepo.MemoryOrderRepository, *realtimeinfra.MemoryRealtimeStore, domain.AuctionLot) {
	t.Helper()
	auctionRepo := auctionrepo.NewMemoryAuctionRepository()
	bidRepo := auctionrepo.NewMemoryBidRepository()
	orderRepo := orderrepo.NewMemoryOrderRepository()
	realtime := realtimeinfra.NewMemoryRealtimeStore()
	now := time.Now().UTC()
	auction := domain.AuctionLot{
		AuctionID:    auctionID,
		SellerID:     "seller-1",
		Title:        "lot",
		AuctionType:  domain.AuctionTypeEnglish,
		StartPrice:   1000,
		ReservePrice: 1000,
		Status:       domain.AuctionStatusRunning,
		StartTime:    now.Add(-time.Hour),
		EndTime:      now.Add(-time.Minute),
	}
	if err := auctionRepo.Create(context.Background(), &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := realtime.InitAuction(context.Background(), auction, 100); err != nil {
		t.Fatalf("realtime init: %v", err)
	}
	return auctionRepo, bidRepo, orderRepo, realtime, auction
}

// TestHammerSyncModeUnchanged 同步模式（async 关）：Hammer 行为完全不变 —— 只广播 auction.closed，不广播 auction.state(HAMMER_PENDING)。
func TestHammerSyncModeUnchanged(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5001)
	publisher := &recordingHammerPublisher{}
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:  auctionRepo,
		Bids:      bidRepo,
		Orders:    orderRepo,
		Realtime:  realtime,
		Publisher: publisher,
		// AsyncBidEnabled 默认 false：同步模式。
	})
	now := time.Now().UTC()
	result, _, err := svc.Hammer(ctx, domain.HammerInput{
		RequestID: "auto-sync-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "system",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if !result.Status.Terminal() {
		t.Fatalf("status = %s, want terminal", result.Status)
	}
	events := publisher.snapshot()
	for _, e := range events {
		if e.envType == "auction.state" {
			t.Fatalf("sync mode must not broadcast auction.state, got %v", events)
		}
	}
	hasClosed := false
	for _, e := range events {
		if e.envType == "auction.closed" {
			hasClosed = true
		}
	}
	if !hasClosed {
		t.Fatalf("expected auction.closed broadcast in sync mode, got %v", events)
	}
}

// TestHammerAsyncNoInFlightFinalizesImmediately 异步模式 + 无 in-flight：广播 HAMMER_PENDING → 立即 finalize → 广播 auction.closed。
func TestHammerAsyncNoInFlightFinalizesImmediately(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5002)
	publisher := &recordingHammerPublisher{}
	coord := &fakePendingCoordinator{} // pending=0
	gate := NewHammerPublisherGate(0)
	barrier := NewInFlightBarrier(coord, gate, 10)
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:        auctionRepo,
		Bids:            bidRepo,
		Orders:          orderRepo,
		Realtime:        realtime,
		Publisher:       publisher,
		AsyncBidEnabled: true,
		Barrier:         barrier,
		PublisherGate:   gate,
		DrainMaxWait:    time.Second,
	})
	now := time.Now().UTC()
	result, _, err := svc.Hammer(ctx, domain.HammerInput{
		RequestID: "auto-async-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "system",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	if !result.Status.Terminal() {
		t.Fatalf("status = %s, want terminal", result.Status)
	}
	events := publisher.snapshot()
	hasState, hasClosed := false, false
	stateBeforeClosed := false
	for i, e := range events {
		if e.envType == "auction.state" {
			hasState = true
			// 校验 status=HAMMER_PENDING。
			var s domain.AuctionState
			if err := json.Unmarshal(e.payload, &s); err == nil {
				if s.Status != domain.AuctionStatusHammerPending {
					t.Fatalf("auction.state.status = %s, want HAMMER_PENDING", s.Status)
				}
			}
			for j := i + 1; j < len(events); j++ {
				if events[j].envType == "auction.closed" {
					stateBeforeClosed = true
				}
			}
		}
		if e.envType == "auction.closed" {
			hasClosed = true
		}
	}
	if !hasState {
		t.Fatalf("expected auction.state(HAMMER_PENDING) broadcast, got %v", events)
	}
	if !hasClosed {
		t.Fatalf("expected auction.closed broadcast, got %v", events)
	}
	if !stateBeforeClosed {
		t.Fatalf("auction.state must precede auction.closed, got %v", events)
	}
	// 闸门在 finalize 完成后被 Open 清理。
	if gate.IsClosed(auction.AuctionID) {
		t.Fatalf("gate should be open after finalize")
	}
}

// TestHammerAsyncWaitsUntilDrained 异步模式 + 有 in-flight：barrier 等待 pending=0 才 finalize。
func TestHammerAsyncWaitsUntilDrained(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5003)
	publisher := &recordingHammerPublisher{}
	coord := &fakePendingCoordinator{}
	coord.set(2)
	gate := NewHammerPublisherGate(0)
	barrier := NewInFlightBarrier(coord, gate, 10)
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:        auctionRepo,
		Bids:            bidRepo,
		Orders:          orderRepo,
		Realtime:        realtime,
		Publisher:       publisher,
		AsyncBidEnabled: true,
		Barrier:         barrier,
		PublisherGate:   gate,
		DrainMaxWait:    time.Second,
	})
	// 起一个 goroutine 在 100ms 后清空 pending。
	go func() {
		time.Sleep(80 * time.Millisecond)
		coord.set(0)
	}()
	start := time.Now()
	now := time.Now().UTC()
	result, _, err := svc.Hammer(ctx, domain.HammerInput{
		RequestID: "auto-async-drain-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "system",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected barrier to wait at least 50ms, took %v", elapsed)
	}
	if !result.Status.Terminal() {
		t.Fatalf("status = %s, want terminal", result.Status)
	}
}

// TestHammerAsyncTimeoutFallbackForcesFinalize 异步模式 + 超时 fallback：pending 一直 >0 → maxWait 后强制 finalize。
func TestHammerAsyncTimeoutFallbackForcesFinalize(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5004)
	publisher := &recordingHammerPublisher{}
	coord := &fakePendingCoordinator{}
	coord.set(5) // 永远不归零。
	gate := NewHammerPublisherGate(0)
	barrier := NewInFlightBarrier(coord, gate, 10)
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:        auctionRepo,
		Bids:            bidRepo,
		Orders:          orderRepo,
		Realtime:        realtime,
		Publisher:       publisher,
		AsyncBidEnabled: true,
		Barrier:         barrier,
		PublisherGate:   gate,
		DrainMaxWait:    100 * time.Millisecond,
	})
	start := time.Now()
	now := time.Now().UTC()
	result, _, err := svc.Hammer(ctx, domain.HammerInput{
		RequestID: "auto-async-timeout-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "system",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected barrier to time out after >=100ms, took %v", elapsed)
	}
	if !result.Status.Terminal() {
		t.Fatalf("status = %s, want terminal", result.Status)
	}
}

// TestHammerForceTrueSkipsBarrier Force=true 走快路径，不进 BeginHammerPending、不等屏障。
func TestHammerForceTrueSkipsBarrier(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5005)
	publisher := &recordingHammerPublisher{}
	coord := &fakePendingCoordinator{}
	coord.set(99) // 即使有 pending，Force=true 也不应等待。
	gate := NewHammerPublisherGate(0)
	barrier := NewInFlightBarrier(coord, gate, 10)
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:        auctionRepo,
		Bids:            bidRepo,
		Orders:          orderRepo,
		Realtime:        realtime,
		Publisher:       publisher,
		AsyncBidEnabled: true,
		Barrier:         barrier,
		PublisherGate:   gate,
		DrainMaxWait:    10 * time.Second,
	})
	start := time.Now()
	now := time.Now().UTC()
	result, _, err := svc.Hammer(ctx, domain.HammerInput{
		RequestID: "cap-force-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "CAP_PRICE",
		Force:     true,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("hammer: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Force=true must skip barrier; took %v", elapsed)
	}
	if !result.Status.Terminal() {
		t.Fatalf("status = %s, want terminal", result.Status)
	}
	// Force 路径不广播 auction.state(HAMMER_PENDING)。
	for _, e := range publisher.snapshot() {
		if e.envType == "auction.state" {
			t.Fatalf("Force=true must not broadcast auction.state(HAMMER_PENDING), got %v", e)
		}
	}
	// 闸门也不应被关闭。
	if gate.IsClosed(auction.AuctionID) {
		t.Fatalf("Force=true must not close the publisher gate")
	}
}

// TestHammerPublisherGateBlocksNewCommands HAMMER_PENDING 后 gate.IsClosed 为 true，publisher 适配器据此返回 ErrHammerPending。
func TestHammerPublisherGateBlocksNewCommands(t *testing.T) {
	gate := NewHammerPublisherGate(0)
	auctionID := uint64(7001)
	if gate.IsClosed(auctionID) {
		t.Fatalf("gate should start open")
	}
	gate.Close(auctionID)
	if !gate.IsClosed(auctionID) {
		t.Fatalf("gate should be closed after Close()")
	}
	// 再 Close 一次幂等。
	gate.Close(auctionID)
	if !gate.IsClosed(auctionID) {
		t.Fatalf("Close idempotent")
	}
	gate.Open(auctionID)
	if gate.IsClosed(auctionID) {
		t.Fatalf("gate should be open after Open()")
	}
}

// TestInFlightBarrierRespectsGrace 屏障 B：闸门关闭后宽限期未过不算追上，即使 pending=0。
func TestInFlightBarrierRespectsGrace(t *testing.T) {
	ctx := context.Background()
	coord := &fakePendingCoordinator{}
	coord.set(0)
	gate := NewHammerPublisherGate(80 * time.Millisecond)
	barrier := NewInFlightBarrier(coord, gate, 10)
	auctionID := uint64(7002)
	start := time.Now()
	if !barrier.WaitDrain(ctx, auctionID, 500*time.Millisecond) {
		t.Fatalf("expected ok=true under grace once timer crosses grace")
	}
	elapsed := time.Since(start)
	if elapsed < 70*time.Millisecond {
		t.Fatalf("expected barrier to honor grace ~80ms, elapsed %v", elapsed)
	}
}

// TestBeginHammerPendingAbortsWhenEndTimeExtended 验证异步路径下 BeginHammerPending 在
// 进入过渡态前再读一次 Redis state；如果 endTime 被 anti-sniping 推后到 now 之后，
// 返回 ErrInvalidState、不切 HAMMER_PENDING、不广播 auction.state、不关闸门。
func TestBeginHammerPendingAbortsWhenEndTimeExtended(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5006)
	// 把 Redis state 的 endTime 推到 now 后面（模拟 anti-sniping 已延长）。
	now := time.Now().UTC()
	extendedAuction := auction
	extendedAuction.EndTime = now.Add(2 * time.Minute)
	if _, err := realtime.InitAuction(ctx, extendedAuction, 100); err != nil {
		t.Fatalf("re-init realtime: %v", err)
	}
	publisher := &recordingHammerPublisher{}
	gate := NewHammerPublisherGate(0)
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:      auctionRepo,
		Bids:          bidRepo,
		Orders:        orderRepo,
		Realtime:      realtime,
		Publisher:     publisher,
		PublisherGate: gate,
	})
	_, err := svc.BeginHammerPending(ctx, domain.HammerInput{
		RequestID: "auto-after-extend-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "system",
		Now:       now,
	})
	if err != domain.ErrInvalidState {
		t.Fatalf("expected ErrInvalidState when endTime was extended, got %v", err)
	}
	for _, e := range publisher.snapshot() {
		if e.envType == "auction.state" {
			t.Fatalf("BeginHammerPending must not broadcast auction.state when aborted, got %v", e)
		}
	}
	if gate.IsClosed(auction.AuctionID) {
		t.Fatalf("BeginHammerPending must not close gate when aborted")
	}
}

// TestBeginHammerPendingForceBypassesEndTimeCheck Force=true 路径仍然进入过渡态，
// 不被 anti-sniping 复核阻塞（CAP_PRICE / 管理员强制立即落锤）。
func TestBeginHammerPendingForceBypassesEndTimeCheck(t *testing.T) {
	ctx := context.Background()
	auctionRepo, bidRepo, orderRepo, realtime, auction := newRunningAuctionFixture(t, 5007)
	now := time.Now().UTC()
	extendedAuction := auction
	extendedAuction.EndTime = now.Add(2 * time.Minute)
	if _, err := realtime.InitAuction(ctx, extendedAuction, 100); err != nil {
		t.Fatalf("re-init realtime: %v", err)
	}
	publisher := &recordingHammerPublisher{}
	gate := NewHammerPublisherGate(0)
	svc := NewHammerServiceWithDeps(HammerServiceDeps{
		Auctions:      auctionRepo,
		Bids:          bidRepo,
		Orders:        orderRepo,
		Realtime:      realtime,
		Publisher:     publisher,
		PublisherGate: gate,
	})
	res, err := svc.BeginHammerPending(ctx, domain.HammerInput{
		RequestID: "cap-force-bypass-1",
		AuctionID: auction.AuctionID,
		ActorID:   "system",
		ActorRole: domain.RoleAdmin,
		ClosedBy:  "CAP_PRICE",
		Force:     true,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("Force=true should bypass anti-sniping double-check, got %v", err)
	}
	if res.AlreadyClosed || res.AlreadyPending {
		t.Fatalf("Force=true on RUNNING auction should transit to HAMMER_PENDING, got %+v", res)
	}
}
