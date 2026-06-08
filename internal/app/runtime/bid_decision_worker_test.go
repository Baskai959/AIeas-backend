package runtime

import (
	"context"
	"sync"
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
