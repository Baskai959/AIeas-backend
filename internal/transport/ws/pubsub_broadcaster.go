package ws

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

const AuctionEventPubSubPattern = "auction:*:events"

type PubSubClient interface {
	PSubscribe(ctx context.Context, patterns ...string) *redisgo.PubSub
}

type PubSubBroadcaster struct {
	client  PubSubClient
	hub     *Hub
	pattern string
}

func NewPubSubBroadcaster(client PubSubClient, hub *Hub) *PubSubBroadcaster {
	return &PubSubBroadcaster{client: client, hub: hub, pattern: AuctionEventPubSubPattern}
}

func (b *PubSubBroadcaster) Start(ctx context.Context) {
	if b == nil || b.client == nil || b.hub == nil {
		return
	}
	pattern := strings.TrimSpace(b.pattern)
	if pattern == "" {
		pattern = AuctionEventPubSubPattern
	}
	go b.run(ctx, pattern)
}

func (b *PubSubBroadcaster) run(ctx context.Context, pattern string) {
	pubsub := b.client.PSubscribe(ctx, pattern)
	defer pubsub.Close()
	ch := pubsub.Channel(redisgo.WithChannelSize(128), redisgo.WithChannelHealthCheckInterval(30*time.Second))
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			b.handleMessage(msg)
		}
	}
}

func (b *PubSubBroadcaster) handleMessage(msg *redisgo.Message) {
	if msg == nil || strings.TrimSpace(msg.Payload) == "" {
		return
	}
	auctionID := auctionIDFromEventChannel(msg.Channel)
	if auctionID == 0 {
		return
	}
	var payload struct {
		Event     string `json:"event"`
		Seq       int64  `json:"seq"`
		AuctionID uint64 `json:"auctionId"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		return
	}
	if payload.AuctionID != 0 && payload.AuctionID != auctionID {
		return
	}
	eventType := strings.TrimSpace(payload.Event)
	if eventType == "" {
		eventType = "bid.accepted"
	}
	if eventType == "bid.rejected" {
		return
	}
	b.hub.Broadcast(auctionID, Envelope{Type: eventType, Seq: payload.Seq, Payload: json.RawMessage(msg.Payload)})
}

func auctionIDFromEventChannel(channel string) uint64 {
	parts := strings.Split(channel, ":")
	if len(parts) != 3 || parts[0] != "auction" || parts[2] != "events" {
		return 0
	}
	id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}
