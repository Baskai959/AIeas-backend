package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

func TestRealtimeEventPublisherPublishesLiveSessionUserEvent(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	publisher := NewRealtimeEventPublisher(NewShardedRTClientFromShards([]*RedisRTClient{{Client: client}}))
	const channel = "live_session:90005:user:u_1001:events"
	pubsub := client.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	raw := json.RawMessage(`{"bidId":"bid-1","auctionId":10005,"finalStatus":"ACCEPTED","currentPrice":1200}`)
	if err := publisher.PublishLiveSessionUserEvent(ctx, 90005, "u_1001", "bid.result", "bid-1", 18, raw); err != nil {
		t.Fatalf("publish targeted event: %v", err)
	}

	select {
	case msg := <-ch:
		if msg == nil || msg.Channel != channel {
			t.Fatalf("unexpected pubsub message: %+v", msg)
		}
		var payload struct {
			Event         string `json:"event"`
			RequestID     string `json:"requestId"`
			Seq           int64  `json:"seq"`
			LiveSessionID uint64 `json:"liveSessionId"`
			BidID         string `json:"bidId"`
			AuctionID     uint64 `json:"auctionId"`
		}
		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.Event != "bid.result" || payload.RequestID != "bid-1" || payload.Seq != 18 {
			t.Fatalf("unexpected metadata: %+v", payload)
		}
		if payload.LiveSessionID != 90005 || payload.BidID != "bid-1" || payload.AuctionID != 10005 {
			t.Fatalf("unexpected bid payload: %+v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected targeted live session event")
	}
}
