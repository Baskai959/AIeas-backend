package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	wstransport "aieas_backend/internal/transport/ws"
)

type fakeBidResultEventPublisher struct {
	err   error
	calls []bidResultEventCall
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
	if p.err != nil {
		return p.err
	}
	p.calls = append(p.calls, bidResultEventCall{
		LiveSessionID: liveSessionID,
		UserID:        userID,
		EventType:     eventType,
		RequestID:     requestID,
		Seq:           seq,
		Payload:       payload,
	})
	return nil
}

func TestBidResultDeliveryPublishesTargetedEvent(t *testing.T) {
	publisher := &fakeBidResultEventPublisher{}
	delivery := bidResultDelivery{eventPublisher: publisher}

	delivery.DeliverBidResult(90005, 10005, "u_1001", wstransport.BidResultPayload{
		BidID:        "bid-1",
		AuctionID:    10005,
		FinalStatus:  "ACCEPTED",
		CurrentPrice: 1200,
		ResultSeq:    18,
	})

	if len(publisher.calls) != 1 {
		t.Fatalf("expected one targeted event, got %d", len(publisher.calls))
	}
	call := publisher.calls[0]
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

func TestBidResultDeliveryFallsBackToLocalCoordinator(t *testing.T) {
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
	delivery := bidResultDelivery{
		coordinator:    coord,
		eventPublisher: &fakeBidResultEventPublisher{err: errors.New("pubsub unavailable")},
	}

	delivery.DeliverBidResult(90005, 10005, "u_1001", wstransport.BidResultPayload{BidID: "bid-1", AuctionID: 10005, FinalStatus: "ACCEPTED"})

	select {
	case env := <-client.Outbound():
		if env.Type != wstransport.TypeBidResult || env.RequestID != "bid-1" {
			t.Fatalf("unexpected fallback envelope: %+v", env)
		}
	default:
		t.Fatal("expected local fallback bid.result")
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
