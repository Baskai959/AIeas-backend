package ws

import (
	"sync"
	"testing"
	"time"
)

// fakeAsyncMetrics 记录协调器指标调用，用于断言。
type fakeAsyncMetrics struct {
	mu          sync.Mutex
	queueSize   int
	pushCount   int
	resultCount int
	lastOutcome string
	ackTimeoutN int
}

func (m *fakeAsyncMetrics) SetBidPendingQueueSize(size int) {
	m.mu.Lock()
	m.queueSize = size
	m.mu.Unlock()
}
func (m *fakeAsyncMetrics) ObserveBidResultPush(time.Duration) {
	m.mu.Lock()
	m.pushCount++
	m.mu.Unlock()
}
func (m *fakeAsyncMetrics) ObserveBidResultDuration(outcome string, _ time.Duration) {
	m.mu.Lock()
	m.resultCount++
	m.lastOutcome = outcome
	m.mu.Unlock()
}
func (m *fakeAsyncMetrics) IncBidResultAckTimeout() {
	m.mu.Lock()
	m.ackTimeoutN++
	m.mu.Unlock()
}

func TestDeliverToUserInSessionTargetsOnlyMatchingUser(t *testing.T) {
	hub := NewHub()
	target := NewClientWithSession("c1", "u1", 0, 900, 8)
	other := NewClientWithSession("c2", "u2", 0, 900, 8)
	if err := hub.SubscribeLiveSessionOnly(900, target); err != nil {
		t.Fatalf("subscribe target: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(900, other); err != nil {
		t.Fatalf("subscribe other: %v", err)
	}
	delivered := hub.DeliverToUserInSession(900, "u1", Envelope{Type: "bid.result"})
	if delivered != 1 {
		t.Fatalf("expected 1 delivery, got %d", delivered)
	}
	if !waitForEnvelopeType(target, "bid.result") {
		t.Fatalf("target did not receive bid.result frame")
	}
	if waitForEnvelopeType(other, "bid.result") {
		t.Fatalf("non-target user should not receive bid.result frame")
	}
}

// waitForEnvelopeType 从客户端 outbound 读取，跳过 presence 等非目标帧，
// 在短超时内寻找指定类型的帧。
func waitForEnvelopeType(c *Client, want string) bool {
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case env := <-c.Outbound():
			if env.Type == want {
				return true
			}
		case <-timeout:
			return false
		}
	}
}

func TestBidAsyncCoordinatorQueueProtection(t *testing.T) {
	coord := NewBidAsyncCoordinator(nil, 1, time.Hour, 3)
	// 第一条入队成功。
	if ok, reason := coord.TryEnqueue(10, 900, "u1", "b1"); !ok {
		t.Fatalf("first enqueue should succeed, got reason=%s", reason)
	}
	// 同用户同拍品再次入队 → USER_BID_ALREADY_PENDING。
	if ok, reason := coord.TryEnqueue(10, 900, "u1", "b2"); ok || reason != BidQueueRejectUserPending {
		t.Fatalf("expected USER_BID_ALREADY_PENDING, got ok=%v reason=%s", ok, reason)
	}
	// 其他用户入队 → 超 MaxPendingPerAuction(=1) → HOT_AUCTION_QUEUE_FULL。
	if ok, reason := coord.TryEnqueue(10, 900, "u2", "b3"); ok || reason != BidQueueRejectHotAuctionFull {
		t.Fatalf("expected HOT_AUCTION_QUEUE_FULL, got ok=%v reason=%s", ok, reason)
	}
	// 释放后可再次入队。
	coord.HandleAck("b1")
	if ok, _ := coord.TryEnqueue(10, 900, "u2", "b3"); !ok {
		t.Fatalf("enqueue should succeed after release")
	}
}

func TestBidAsyncCoordinatorAckReleasesPending(t *testing.T) {
	hub := NewHub()
	client := NewClientWithSession("c1", "u1", 0, 900, 8)
	if err := hub.SubscribeLiveSessionOnly(900, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	metrics := &fakeAsyncMetrics{}
	coord := NewBidAsyncCoordinator(hub, 10, time.Hour, 3)
	coord.SetMetrics(metrics)
	if ok, _ := coord.TryEnqueue(10, 900, "u1", "b1"); !ok {
		t.Fatalf("enqueue failed")
	}
	if coord.PendingQueueSize() != 1 {
		t.Fatalf("expected pending size 1, got %d", coord.PendingQueueSize())
	}
	coord.DeliverBidResult(900, 10, "u1", BidResultPayload{BidID: "b1", AuctionID: 10, FinalStatus: "ACCEPTED"})
	// 收到推送帧（跳过 presence）。
	if !waitForEnvelopeType(client, "bid.result") {
		t.Fatalf("client did not receive bid.result")
	}
	metrics.mu.Lock()
	resultCount := metrics.resultCount
	lastOutcome := metrics.lastOutcome
	metrics.mu.Unlock()
	if resultCount != 1 || lastOutcome != "accepted" {
		t.Fatalf("expected one accepted bid.result duration metric, got count=%d outcome=%q", resultCount, lastOutcome)
	}
	coord.HandleAck("b1")
	if coord.PendingQueueSize() != 0 {
		t.Fatalf("expected pending size 0 after ack, got %d", coord.PendingQueueSize())
	}
}

func TestBidAsyncCoordinatorResendOnAckTimeout(t *testing.T) {
	hub := NewHub()
	client := NewClientWithSession("c1", "u1", 0, 900, 16)
	if err := hub.SubscribeLiveSessionOnly(900, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	metrics := &fakeAsyncMetrics{}
	// 短超时 + 最多重发 2 次，便于快速断言。
	coord := NewBidAsyncCoordinator(hub, 10, 20*time.Millisecond, 2)
	coord.SetMetrics(metrics)
	coord.TryEnqueue(10, 900, "u1", "b1")
	coord.DeliverBidResult(900, 10, "u1", BidResultPayload{BidID: "b1", AuctionID: 10, FinalStatus: "ACCEPTED"})
	// 不发送 ack：等待若干超时周期后应触发重发并最终超限释放。
	deadline := time.After(time.Second)
	for {
		if coord.PendingQueueSize() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("pending not released after resend exhaustion")
		case <-time.After(10 * time.Millisecond):
		}
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.ackTimeoutN == 0 {
		t.Fatalf("expected ack timeout metric increments")
	}
}

func TestBidAsyncCoordinatorReleaseUserOnDisconnect(t *testing.T) {
	coord := NewBidAsyncCoordinator(nil, 10, time.Hour, 3)
	coord.TryEnqueue(10, 900, "u1", "b1")
	coord.TryEnqueue(11, 900, "u1", "b2")
	coord.TryEnqueue(10, 900, "u2", "b3")
	if coord.PendingQueueSize() != 3 {
		t.Fatalf("expected 3 pending, got %d", coord.PendingQueueSize())
	}
	coord.ReleaseUser("u1")
	if coord.PendingQueueSize() != 1 {
		t.Fatalf("expected 1 pending after release u1, got %d", coord.PendingQueueSize())
	}
}
