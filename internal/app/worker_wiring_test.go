package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	wstransport "aieas_backend/internal/transport/ws"

	"github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

func TestWSGatewayStartsPubSubWithoutEventLog(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redisgo.NewClient(&redisgo.Options{Addr: redisServer.Addr()})
	defer redisClient.Close()

	cfg := appconfig.Default()
	cfg.App.Role = "ws-gateway"
	hub := wstransport.NewHub()
	const (
		auctionID     uint64 = 10001
		liveSessionID uint64 = 90003
	)
	client := wstransport.NewClientWithSession("buyer-live-session", "u_1001", 0, liveSessionID, 8)
	if err := hub.SubscribeLiveSessionOnly(liveSessionID, client); err != nil {
		t.Fatalf("subscribe live session: %v", err)
	}
	drainOutbound(client)

	shutdown := startAppWorkers(cfg, ServerDependencies{
		Hub:           hub,
		PubSubClients: []wstransport.PubSubClient{redisClient},
	}, appServices{})
	defer shutdown.stop(context.Background())

	payload := `{"auctionId":10001,"liveSessionId":90003,"event":"bid.accepted","requestId":"bid-1","seq":12,"accepted":true,"currentPrice":1200}`
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case env := <-client.Outbound():
			if env.Type != "bid.accepted" || env.Seq != 12 || env.LiveSessionID != liveSessionID || env.RequestID != "bid-1" {
				t.Fatalf("unexpected pubsub envelope: %+v", env)
			}
			var decoded struct {
				AuctionID    uint64 `json:"auctionId"`
				CurrentPrice int64  `json:"currentPrice"`
			}
			if err := json.Unmarshal(env.Payload, &decoded); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if decoded.AuctionID != auctionID || decoded.CurrentPrice != 1200 {
				t.Fatalf("unexpected payload: %+v", decoded)
			}
			return
		case <-ticker.C:
			if err := redisClient.Publish(context.Background(), "auction:10001:events", payload).Err(); err != nil {
				t.Fatalf("publish: %v", err)
			}
		case <-deadline:
			t.Fatal("expected bid.accepted from pubsub without EventLog")
		}
	}
}

func drainOutbound(client *wstransport.Client) {
	for {
		select {
		case <-client.Outbound():
		default:
			return
		}
	}
}
