package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	kafkainfra "aieas_backend/internal/infra/kafka"
	auctionapp "aieas_backend/internal/modules/auction/app"
	corews "aieas_backend/internal/transport/ws"
)

type fakeBidCommandConsumer struct {
	mu    sync.Mutex
	cmds  []kafkainfra.BidCommand
	idx   int
	doneC chan struct{}
}

func (c *fakeBidCommandConsumer) FetchBidCommand(ctx context.Context) (kafkainfra.BidCommand, func(context.Context) error, error) {
	c.mu.Lock()
	if c.idx < len(c.cmds) {
		cmd := c.cmds[c.idx]
		c.idx++
		c.mu.Unlock()
		return cmd, func(context.Context) error { return nil }, nil
	}
	c.mu.Unlock()
	// 命令耗尽：阻塞直到 ctx 取消，模拟 reader 在无新消息时等待。
	<-ctx.Done()
	return kafkainfra.BidCommand{}, nil, ctx.Err()
}

type fakeArbitrator struct {
	result domain.BidResult
	err    error
}

func (a fakeArbitrator) ArbitrateFromCommand(ctx context.Context, cmd auctionapp.BidCommandSnapshot) (domain.BidResult, error) {
	return a.result, a.err
}

type fakeDelivery struct {
	mu       sync.Mutex
	payloads []corews.BidResultPayload
	got      chan struct{}
}

func (d *fakeDelivery) DeliverBidResult(sessionID, auctionID uint64, userID string, p corews.BidResultPayload) {
	d.mu.Lock()
	d.payloads = append(d.payloads, p)
	d.mu.Unlock()
	select {
	case d.got <- struct{}{}:
	default:
	}
}

type fakeInFlightReleaser struct {
	mu       sync.Mutex
	released []string
}

func (r *fakeInFlightReleaser) ReleaseBidCommand(ctx context.Context, auctionID uint64, bidID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = append(r.released, bidID)
	return nil
}

func runWorkerOnce(t *testing.T, cmd kafkainfra.BidCommand, arb BidArbitrator) corews.BidResultPayload {
	t.Helper()
	consumer := &fakeBidCommandConsumer{cmds: []kafkainfra.BidCommand{cmd}}
	delivery := &fakeDelivery{got: make(chan struct{}, 1)}
	worker := NewBidDecisionWorker(consumer, arb, delivery)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	select {
	case <-delivery.got:
	case <-time.After(time.Second):
		t.Fatalf("worker did not deliver a result")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	return delivery.payloads[len(delivery.payloads)-1]
}

func TestBidDecisionWorkerReleasesInFlightAfterLuaDecision(t *testing.T) {
	cmd := kafkainfra.BidCommand{BidID: "release-1", AuctionID: 10, UserID: "u1", LiveSessionID: 900}
	consumer := &fakeBidCommandConsumer{cmds: []kafkainfra.BidCommand{cmd}}
	delivery := &fakeDelivery{got: make(chan struct{}, 1)}
	releaser := &fakeInFlightReleaser{}
	worker := NewBidDecisionWorker(consumer, fakeArbitrator{result: domain.BidResult{Accepted: true}}, delivery)
	worker.SetInFlightReleaser(releaser)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	select {
	case <-delivery.got:
	case <-time.After(time.Second):
		t.Fatalf("worker did not deliver a result")
	}
	deadline := time.After(time.Second)
	for {
		releaser.mu.Lock()
		got := append([]string(nil), releaser.released...)
		releaser.mu.Unlock()
		if len(got) == 1 && got[0] == "release-1" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("released = %v, want [release-1]", got)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestBidDecisionWorkerMapsAccepted(t *testing.T) {
	cmd := kafkainfra.BidCommand{BidID: "b1", AuctionID: 10, UserID: "u1", LiveSessionID: 900}
	p := runWorkerOnce(t, cmd, fakeArbitrator{result: domain.BidResult{Accepted: true, CurrentPrice: 1100}})
	if p.FinalStatus != "ACCEPTED" {
		t.Fatalf("expected ACCEPTED, got %s", p.FinalStatus)
	}
	if p.CurrentPrice != 1100 {
		t.Fatalf("expected currentPrice 1100, got %d", p.CurrentPrice)
	}
}

func TestBidDecisionWorkerMapsRejectedWithReason(t *testing.T) {
	cmd := kafkainfra.BidCommand{BidID: "b2", AuctionID: 10, UserID: "u1"}
	p := runWorkerOnce(t, cmd, fakeArbitrator{result: domain.BidResult{Accepted: false, Reason: domain.BidRejectBelowMinIncrement}})
	if p.FinalStatus != "REJECTED" {
		t.Fatalf("expected REJECTED, got %s", p.FinalStatus)
	}
	if p.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected reason %s, got %s", domain.BidRejectBelowMinIncrement, p.Reason)
	}
}

func TestBidDecisionWorkerMapsDuplicateToTerminal(t *testing.T) {
	cmd := kafkainfra.BidCommand{BidID: "b3", AuctionID: 10, UserID: "u1"}
	p := runWorkerOnce(t, cmd, fakeArbitrator{result: domain.BidResult{Accepted: false, Duplicate: true, Reason: "DUP"}})
	if p.FinalStatus != "REJECTED" {
		t.Fatalf("expected REJECTED for non-accepted duplicate, got %s", p.FinalStatus)
	}
}

func TestBidDecisionWorkerArbitrationErrorPushesRejected(t *testing.T) {
	cmd := kafkainfra.BidCommand{BidID: "b4", AuctionID: 10, UserID: "u1"}
	p := runWorkerOnce(t, cmd, fakeArbitrator{err: domain.ErrInvalidState})
	if p.FinalStatus != "REJECTED" || p.Reason != "ARBITRATION_ERROR" {
		t.Fatalf("expected ARBITRATION_ERROR rejected, got status=%s reason=%s", p.FinalStatus, p.Reason)
	}
}

// concurrentConsumer 用于验证 worker pool 并发：所有 cmd 一次性返回，commit 计数原子统计。
type concurrentConsumer struct {
	mu        sync.Mutex
	cmds      []kafkainfra.BidCommand
	commits   int32
	committed map[string]int
	idx       int
}

func (c *concurrentConsumer) FetchBidCommand(ctx context.Context) (kafkainfra.BidCommand, func(context.Context) error, error) {
	c.mu.Lock()
	if c.idx < len(c.cmds) {
		cmd := c.cmds[c.idx]
		c.idx++
		c.mu.Unlock()
		commitFn := func(context.Context) error {
			atomic.AddInt32(&c.commits, 1)
			c.mu.Lock()
			if c.committed == nil {
				c.committed = make(map[string]int)
			}
			c.committed[cmd.BidID]++
			c.mu.Unlock()
			return nil
		}
		return cmd, commitFn, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return kafkainfra.BidCommand{}, nil, ctx.Err()
}

type concurrentArbitrator struct {
	delay    time.Duration
	inflight int32
	maxSeen  int32
}

func (a *concurrentArbitrator) ArbitrateFromCommand(ctx context.Context, _ auctionapp.BidCommandSnapshot) (domain.BidResult, error) {
	cur := atomic.AddInt32(&a.inflight, 1)
	for {
		max := atomic.LoadInt32(&a.maxSeen)
		if cur <= max || atomic.CompareAndSwapInt32(&a.maxSeen, max, cur) {
			break
		}
	}
	time.Sleep(a.delay)
	atomic.AddInt32(&a.inflight, -1)
	return domain.BidResult{Accepted: true}, nil
}

// 验证 worker pool 真正并发：N 条命令同时被 N 个 goroutine 处理，maxSeen 应 > 1。
func TestBidDecisionWorkerPoolConcurrent(t *testing.T) {
	cmds := make([]kafkainfra.BidCommand, 16)
	for i := range cmds {
		cmds[i] = kafkainfra.BidCommand{
			BidID: "b-" + string(rune('a'+i)), AuctionID: uint64(i + 1), UserID: "u1", LiveSessionID: 900,
		}
	}
	consumer := &concurrentConsumer{cmds: cmds}
	arb := &concurrentArbitrator{delay: 30 * time.Millisecond}
	delivery := &fakeDelivery{got: make(chan struct{}, 64)}
	worker := NewBidDecisionWorkerWithOptions(consumer, arb, delivery, BidDecisionWorkerOptions{
		PoolSize: 8, CommitMode: "single",
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	worker.Start(ctx)
	deadline := time.After(time.Second)
	for got := 0; got < len(cmds); {
		select {
		case <-delivery.got:
			got++
		case <-deadline:
			t.Fatalf("timeout: got %d/%d deliveries", got, len(cmds))
		}
	}
	if max := atomic.LoadInt32(&arb.maxSeen); max < 2 {
		t.Fatalf("expected concurrent arbitration (>=2), max inflight = %d", max)
	}
	cancel()
	// 等待所有 commit 落地（worker 会在 ctx.Done 后等 grace 期）。
	deadline2 := time.After(2 * time.Second)
	for atomic.LoadInt32(&consumer.commits) < int32(len(cmds)) {
		select {
		case <-deadline2:
			t.Fatalf("commit count = %d, want %d", atomic.LoadInt32(&consumer.commits), len(cmds))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// 验证：重复的 bidId 在并发场景下，每条 commit 都会 fire（Lua idem_key 兜底重复
// 生效，worker 层不再去重）。这里仅验 commit 计数与去重不在 worker 层做，
// 这是路线 X 的关键不变量。
func TestBidDecisionWorkerCommitsAllOnDuplicateBidIDs(t *testing.T) {
	cmds := []kafkainfra.BidCommand{
		{BidID: "dup", AuctionID: 1, UserID: "u1"},
		{BidID: "dup", AuctionID: 1, UserID: "u1"},
		{BidID: "dup", AuctionID: 1, UserID: "u1"},
	}
	consumer := &concurrentConsumer{cmds: cmds}
	arb := fakeArbitrator{result: domain.BidResult{Accepted: false, Duplicate: true, Reason: "DUP"}}
	delivery := &fakeDelivery{got: make(chan struct{}, 16)}
	worker := NewBidDecisionWorkerWithOptions(consumer, arb, delivery, BidDecisionWorkerOptions{
		PoolSize: 4, CommitMode: "batch", CommitBatchSize: 2, CommitMaxLatencyMs: 50,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	worker.Start(ctx)
	for got := 0; got < len(cmds); {
		select {
		case <-delivery.got:
			got++
		case <-time.After(time.Second):
			t.Fatalf("timeout: got %d/%d deliveries", got, len(cmds))
		}
	}
	cancel()
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&consumer.commits) < int32(len(cmds)) {
		select {
		case <-deadline:
			t.Fatalf("commit count = %d, want %d (batch mode must commit every msg)", atomic.LoadInt32(&consumer.commits), len(cmds))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// 验证 graceful shutdown：ctx 取消后所有 in-flight goroutine 完成，commit 落地。
func TestBidDecisionWorkerGracefulShutdown(t *testing.T) {
	cmds := []kafkainfra.BidCommand{
		{BidID: "g1", AuctionID: 1},
		{BidID: "g2", AuctionID: 2},
	}
	consumer := &concurrentConsumer{cmds: cmds}
	arb := &concurrentArbitrator{delay: 50 * time.Millisecond}
	delivery := &fakeDelivery{got: make(chan struct{}, 8)}
	worker := NewBidDecisionWorkerWithOptions(consumer, arb, delivery, BidDecisionWorkerOptions{
		PoolSize: 8, CommitMode: "batch", CommitBatchSize: 2, CommitMaxLatencyMs: 20,
	})
	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)
	for got := 0; got < len(cmds); {
		select {
		case <-delivery.got:
			got++
		case <-time.After(time.Second):
			t.Fatalf("did not receive deliveries before shutdown")
		}
	}
	cancel()
	deadline := time.After(3 * time.Second)
	for atomic.LoadInt32(&consumer.commits) < int32(len(cmds)) {
		select {
		case <-deadline:
			t.Fatalf("graceful shutdown failed: commit count = %d, want %d",
				atomic.LoadInt32(&consumer.commits), len(cmds))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// 验证 commitMode 归一化：非法 commit mode 走默认 batch 路径。
func TestBidDecisionWorkerCommitModeNormalization(t *testing.T) {
	cmd := kafkainfra.BidCommand{BidID: "norm", AuctionID: 7}
	consumer := &concurrentConsumer{cmds: []kafkainfra.BidCommand{cmd}}
	delivery := &fakeDelivery{got: make(chan struct{}, 1)}
	w := NewBidDecisionWorkerWithOptions(consumer, fakeArbitrator{result: domain.BidResult{Accepted: true}}, delivery, BidDecisionWorkerOptions{
		PoolSize: 0, CommitMode: "garbage", CommitBatchSize: -1, CommitMaxLatencyMs: -1,
	})
	if w.poolSize != defaultBidDecisionPoolSize {
		t.Fatalf("expected pool size default, got %d", w.poolSize)
	}
	if w.commitMode != "batch" {
		t.Fatalf("expected commit mode batch, got %s", w.commitMode)
	}
	if w.commitBatchN != defaultBidDecisionCommitBatchSize {
		t.Fatalf("expected default commit batch size, got %d", w.commitBatchN)
	}
}
