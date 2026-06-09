package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	wstransport "aieas_backend/internal/transport/ws"
)

type fakeBidResultEventPublisher struct {
	mu    sync.Mutex
	err   error
	calls []bidResultEventCall
	done  chan struct{}
}

type bidResultEventCall struct {
	LiveSessionID uint64
	UserID        string
	EventType     string
	RequestID     string
	Seq           int64
	Payload       json.RawMessage
}

func (p *fakeBidResultEventPublisher) PublishAuctionEvent(context.Context, uint64, string, string, int64, json.RawMessage) error {
	return nil
}

func (p *fakeBidResultEventPublisher) PublishLiveSessionEvent(context.Context, uint64, string, string, int64, json.RawMessage, bool) error {
	return nil
}

func (p *fakeBidResultEventPublisher) PublishLiveSessionUserEvent(_ context.Context, liveSessionID uint64, userID, eventType, requestID string, seq int64, payload json.RawMessage) error {
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		if p.done != nil {
			select {
			case <-p.done:
			default:
				close(p.done)
			}
		}
		return err
	}
	p.calls = append(p.calls, bidResultEventCall{
		LiveSessionID: liveSessionID,
		UserID:        userID,
		EventType:     eventType,
		RequestID:     requestID,
		Seq:           seq,
		Payload:       payload,
	})
	p.mu.Unlock()
	if p.done != nil {
		select {
		case <-p.done:
		default:
			close(p.done)
		}
	}
	return nil
}

func (p *fakeBidResultEventPublisher) callsSnapshot() []bidResultEventCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]bidResultEventCall, len(p.calls))
	copy(out, p.calls)
	return out
}

func TestBidResultDeliveryPublishesTargetedEvent(t *testing.T) {
	publisher := &fakeBidResultEventPublisher{done: make(chan struct{})}
	delivery := bidResultDelivery{eventPublisher: publisher}

	delivery.DeliverBidResult(90005, 10005, "u_1001", wstransport.BidResultPayload{
		BidID:        "bid-1",
		AuctionID:    10005,
		FinalStatus:  "ACCEPTED",
		CurrentPrice: 1200,
		ResultSeq:    18,
	})

	// 路线 X：publish 走 fire-and-forget goroutine，需要等回调执行完。
	select {
	case <-publisher.done:
	case <-time.After(time.Second):
		t.Fatalf("publish goroutine did not run within 1s")
	}

	calls := publisher.callsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("expected one targeted event, got %d", len(calls))
	}
	call := calls[0]
	if call.LiveSessionID != 90005 || call.UserID != "u_1001" || call.EventType != wstransport.TypeBidResult || call.RequestID != "bid-1" || call.Seq != 18 {
		t.Fatalf("unexpected targeted event call: %+v", call)
	}
	var payload wstransport.BidResultPayload
	if err := json.Unmarshal(call.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.BidID != "bid-1" || payload.AuctionID != 10005 || payload.FinalStatus != "ACCEPTED" || payload.CurrentPrice != 1200 {
		t.Fatalf("unexpected bid.result payload: %+v", payload)
	}
}

// 路线 X：publish 失败不再回退到本地 coordinator —— 失败仅 Warn 日志，
// 由 BidAsyncCoordinator 的 ack 重发态兜底。本地兜底只在 publisher 完全
// 缺席（nil/sessionID==0/userID 空）时使用。
func TestBidResultDeliveryFireAndForgetOnPublishError(t *testing.T) {
	hub := wstransport.NewHub()
	client := wstransport.NewClientWithSession("buyer-target", "u_1001", 0, 90005, 4)
	if err := hub.SubscribeLiveSessionOnly(90005, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainAppTestOutbound(client)

	coord := wstransport.NewBidAsyncCoordinator(hub, 10, 0, 0)
	if ok, reason := coord.TryEnqueue(10005, 90005, "u_1001", "bid-1"); !ok {
		t.Fatalf("enqueue: %s", reason)
	}
	publisher := &fakeBidResultEventPublisher{err: errors.New("pubsub unavailable"), done: make(chan struct{})}
	delivery := bidResultDelivery{
		coordinator:    coord,
		eventPublisher: publisher,
	}

	delivery.DeliverBidResult(90005, 10005, "u_1001", wstransport.BidResultPayload{BidID: "bid-1", AuctionID: 10005, FinalStatus: "ACCEPTED"})

	// 等 fire-and-forget goroutine 执行完。
	select {
	case <-publisher.done:
	case <-time.After(time.Second):
		t.Fatalf("publish goroutine did not run within 1s")
	}

	// 不应再有本地 fallback envelope 投递（路线 X 改为 fire-and-forget）。
	select {
	case env := <-client.Outbound():
		t.Fatalf("did not expect local fallback after publish error, got %+v", env)
	case <-time.After(50 * time.Millisecond):
	}
}

// 当 eventPublisher 完全缺席时仍走本地 coordinator 兜底。
func TestBidResultDeliveryFallsBackWhenPublisherAbsent(t *testing.T) {
	hub := wstransport.NewHub()
	client := wstransport.NewClientWithSession("buyer-target", "u_1001", 0, 90005, 4)
	if err := hub.SubscribeLiveSessionOnly(90005, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainAppTestOutbound(client)

	coord := wstransport.NewBidAsyncCoordinator(hub, 10, 0, 0)
	if ok, reason := coord.TryEnqueue(10005, 90005, "u_1001", "bid-1"); !ok {
		t.Fatalf("enqueue: %s", reason)
	}
	delivery := bidResultDelivery{coordinator: coord} // 不挂 publisher

	delivery.DeliverBidResult(90005, 10005, "u_1001", wstransport.BidResultPayload{BidID: "bid-1", AuctionID: 10005, FinalStatus: "ACCEPTED"})

	select {
	case env := <-client.Outbound():
		if env.Type != wstransport.TypeBidResult || env.RequestID != "bid-1" {
			t.Fatalf("unexpected fallback envelope: %+v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("expected local fallback bid.result when publisher absent")
	}
}

func drainAppTestOutbound(client *wstransport.Client) {
	for {
		select {
		case <-client.Outbound():
		default:
			return
		}
	}
}
