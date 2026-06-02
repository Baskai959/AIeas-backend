package ws

import (
	"context"
	"testing"

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
