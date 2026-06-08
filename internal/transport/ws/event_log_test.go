package ws

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	redisinfra "aieas_backend/internal/infra/redis"

	redisgo "github.com/redis/go-redis/v9"
)

func TestEventRelayBroadcastsStreamFactsAndSkipsDuplicates(t *testing.T) {
	ctx := context.Background()
	hub := NewHub()
	client := NewClient("c1", "u_1001", 10001, 4)
	if err := hub.Subscribe(10001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	<-client.Outbound()
	log := &fakeRelayLog{events: []redisinfra.BidEvent{
		{AuctionID: 10001, Seq: 7, StreamID: "7-0", EventType: "bid.accepted", RequestID: "r1", BidderID: "u_1001", BidPrice: 1100, Accepted: true},
		{AuctionID: 10001, Seq: 8, StreamID: "8-0", EventType: "bid.rejected", RequestID: "r2", BidderID: "u_1002", BidPrice: 1100, RejectReason: "BELOW_MIN_INCREMENT"},
	}}
	relay := NewEventRelay(log, hub, 0)

	relay.poll(ctx)
	select {
	case env := <-client.Outbound():
		if env.Type != "bid.accepted" || env.Seq != 7 {
			t.Fatalf("expected stream fact event seq=7, got %+v", env)
		}
	default:
		t.Fatal("expected first relay broadcast")
	}
	if len(client.Outbound()) != 0 {
		t.Fatalf("rejected stream event should not be broadcast, outbound events: %d", len(client.Outbound()))
	}

	relay.poll(ctx)
	select {
	case env := <-client.Outbound():
		t.Fatalf("duplicate stream event should not be rebroadcast: %+v", env)
	default:
	}
	if len(log.lastSeqs) != 2 || log.lastSeqs[0] != 0 || log.lastSeqs[1] != 8 {
		t.Fatalf("relay should replay from previous stream seq, got %+v", log.lastSeqs)
	}
}

func TestEventRelayForwardsStreamFactsToLiveSessionClients(t *testing.T) {
	ctx := context.Background()
	hub := NewHub()
	const auctionID uint64 = 10001
	const sessionID uint64 = 90004
	sessionOnly := NewClientWithSession("stream-session-only", "u_1001", 0, sessionID, 4)
	if err := hub.SubscribeLiveSessionOnly(sessionID, sessionOnly); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	drainPresence(t, sessionOnly)
	log := &fakeRelayLog{events: []redisinfra.BidEvent{
		{AuctionID: auctionID, LiveSessionID: sessionID, Seq: 9, StreamID: "9-0", EventType: "bid.accepted", RequestID: "r1", BidderID: "u_1001", BidPrice: 1100, Accepted: true},
	}}
	relay := NewEventRelay(log, hub, 0)

	relay.poll(ctx)
	select {
	case env := <-sessionOnly.Outbound():
		if env.Type != "bid.accepted" || env.Seq != 9 || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected stream live session envelope: %+v", env)
		}
	default:
		t.Fatal("expected stream event for live-session-only client")
	}
}

func TestPubSubBroadcasterSkipsRejectedBid(t *testing.T) {
	hub := NewHub()
	client := NewClient("c1", "u_1001", 10001, 4)
	if err := hub.Subscribe(10001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	<-client.Outbound()
	broadcaster := &PubSubBroadcaster{hub: hub}
	broadcaster.handleMessage(&redisgo.Message{
		Channel: "auction:10001:events",
		Payload: `{"auctionId":10001,"event":"bid.rejected","seq":11,"accepted":false,"reason":"BELOW_MIN_INCREMENT"}`,
	})
	select {
	case env := <-client.Outbound():
		t.Fatalf("rejected pubsub event should not be broadcast: %+v", env)
	default:
	}
}

func TestPubSubBroadcasterBroadcastsLiveSessionEvents(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 90001
	client := NewClientWithSession("buyer-live-session", "u_1001", 0, sessionID, 4)
	if err := hub.SubscribeLiveSessionOnly(sessionID, client); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	drainPresence(t, client)

	broadcaster := &PubSubBroadcaster{hub: hub}
	broadcaster.handleMessage(&redisgo.Message{
		Channel: "live_session:90001:events",
		Payload: `{"liveSessionId":90001,"event":"live_session.lot_changed","auctionId":10001,"action":"cancelled"}`,
	})
	select {
	case env := <-client.Outbound():
		if env.Type != TypeLiveSessionLotChanged || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected live session envelope: %+v", env)
		}
	default:
		t.Fatal("expected live session pubsub event")
	}
}

func TestPubSubBroadcasterDeliversBidResultToTargetUser(t *testing.T) {
	hub := NewHub()
	const (
		sessionID uint64 = 90005
		auctionID uint64 = 10005
	)
	target := NewClientWithSession("buyer-target", "u_1001", 0, sessionID, 4)
	other := NewClientWithSession("buyer-other", "u_1002", 0, sessionID, 4)
	if err := hub.SubscribeLiveSessionOnly(sessionID, target); err != nil {
		t.Fatalf("subscribe target: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(sessionID, other); err != nil {
		t.Fatalf("subscribe other: %v", err)
	}
	drainPresence(t, target, other)

	coord := NewBidAsyncCoordinator(hub, 10, time.Hour, 3)
	if ok, reason := coord.TryEnqueue(auctionID, sessionID, "u_1001", "bid-1"); !ok {
		t.Fatalf("enqueue pending: %s", reason)
	}
	broadcaster := &PubSubBroadcaster{hub: hub}
	broadcaster.SetBidAsyncCoordinator(coord)
	broadcaster.handleMessage(&redisgo.Message{
		Channel: "live_session:90005:user:u_1001:events",
		Payload: `{"liveSessionId":90005,"auctionId":10005,"event":"bid.result","requestId":"bid-1","bidId":"bid-1","finalStatus":"ACCEPTED","currentPrice":1200}`,
	})
	select {
	case env := <-target.Outbound():
		if env.Type != TypeBidResult || env.RequestID != "bid-1" || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected target bid result envelope: %+v", env)
		}
	default:
		t.Fatal("expected target bid.result")
	}
	select {
	case env := <-other.Outbound():
		t.Fatalf("non-target user should not receive bid.result: %+v", env)
	default:
	}
	coord.HandleAck("bid-1")
	if coord.PendingQueueSize() != 0 {
		t.Fatalf("expected pending released after ack, got %d", coord.PendingQueueSize())
	}
}

func TestPubSubBroadcasterBroadcastsLiveSessionOnlinePresence(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 90004
	client := NewClientWithSession("buyer-live-session-online", "u_1001", 0, sessionID, 4)
	if err := hub.SubscribeLiveSessionOnly(sessionID, client); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	drainPresence(t, client)

	broadcaster := &PubSubBroadcaster{hub: hub}
	broadcaster.handleMessage(&redisgo.Message{
		Channel: "live_session:90004:events",
		Payload: `{"liveSessionId":90004,"event":"room.online","online":2}`,
	})
	select {
	case env := <-client.Outbound():
		if env.Type != TypeRoomOnline || env.LiveSessionID != sessionID {
			t.Fatalf("unexpected online envelope: %+v", env)
		}
		var payload struct {
			LiveSessionID uint64 `json:"liveSessionId"`
			Online        int    `json:"online"`
		}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode online payload: %v", err)
		}
		if payload.LiveSessionID != sessionID || payload.Online != 2 {
			t.Fatalf("unexpected online payload: %+v", payload)
		}
	default:
		t.Fatal("expected live session online pubsub event")
	}
}

func TestPubSubBroadcasterForwardsAuctionEventsToLiveSessionClients(t *testing.T) {
	hub := NewHub()
	const auctionID uint64 = 10001
	const sessionID uint64 = 90003
	auctionOnly := NewClient("auction-only", "u_1001", auctionID, 4)
	sessionOnly := NewClientWithSession("session-only", "u_1002", 0, sessionID, 4)
	bothScopes := NewClientWithSession("both-scopes", "u_1003", auctionID, sessionID, 4)
	if err := hub.Subscribe(auctionID, auctionOnly); err != nil {
		t.Fatalf("subscribe auction only: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(sessionID, sessionOnly); err != nil {
		t.Fatalf("subscribe session only: %v", err)
	}
	if err := hub.Subscribe(auctionID, bothScopes); err != nil {
		t.Fatalf("subscribe both scopes: %v", err)
	}
	drainPresence(t, auctionOnly, sessionOnly, bothScopes)

	broadcaster := &PubSubBroadcaster{hub: hub}
	broadcaster.handleMessage(&redisgo.Message{
		Channel: "auction:10001:events",
		Payload: `{"auctionId":10001,"liveSessionId":90003,"event":"bid.accepted","seq":12,"accepted":true,"currentPrice":1200}`,
	})
	for name, client := range map[string]*Client{
		"auctionOnly": auctionOnly,
		"sessionOnly": sessionOnly,
		"bothScopes":  bothScopes,
	} {
		select {
		case env := <-client.Outbound():
			if env.Type != "bid.accepted" || env.Seq != 12 || env.LiveSessionID != sessionID {
				t.Fatalf("%s got unexpected auction/session envelope: %+v", name, env)
			}
			var payload struct {
				CurrentPrice int64 `json:"currentPrice"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				t.Fatalf("%s decode payload: %v", name, err)
			}
			if payload.CurrentPrice != 1200 {
				t.Fatalf("%s expected currentPrice=1200, got %+v", name, payload)
			}
		default:
			t.Fatalf("%s did not receive auction event", name)
		}
	}
	select {
	case env := <-bothScopes.Outbound():
		t.Fatalf("both-scopes client should receive merged event once, got extra: %+v", env)
	default:
	}
}

func TestPubSubBroadcasterLiveVoiceBroadcastsOnlyOnlineClients(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 90002
	buyer := NewClientWithSession("buyer-voice-pubsub", "u_1001", 0, sessionID, 4)
	merchant := NewClientWithSession("merchant-voice-pubsub", "u_2001", 0, sessionID, 4)
	merchant.CountOnline = false
	if err := hub.SubscribeLiveSessionOnly(sessionID, buyer); err != nil {
		t.Fatalf("subscribe buyer: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(sessionID, merchant); err != nil {
		t.Fatalf("subscribe merchant: %v", err)
	}
	drainPresence(t, buyer, merchant)

	broadcaster := &PubSubBroadcaster{hub: hub}
	broadcaster.handleMessage(&redisgo.Message{
		Channel: "live_session:90002:events",
		Payload: `{"liveSessionId":90002,"event":"live.voice_broadcast","requestId":"voice-1","onlineOnly":true,"audioBase64":"AQI="}`,
	})
	select {
	case env := <-buyer.Outbound():
		if env.Type != TypeLiveVoiceBroadcast || env.LiveSessionID != sessionID || env.RequestID != "voice-1" {
			t.Fatalf("unexpected buyer voice envelope: %+v", env)
		}
	default:
		t.Fatal("expected buyer voice pubsub event")
	}
	select {
	case env := <-merchant.Outbound():
		t.Fatalf("merchant console should not receive online-only voice: %+v", env)
	default:
	}
}

type fakeRelayLog struct {
	events   []redisinfra.BidEvent
	lastSeqs []int64
}

func (l *fakeRelayLog) Enabled() bool { return true }

func (l *fakeRelayLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	_ = ctx
	return []uint64{10001}, nil
}

func (l *fakeRelayLog) ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]redisinfra.BidEvent, bool, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	l.lastSeqs = append(l.lastSeqs, lastSeq)
	return l.events, true, nil
}
