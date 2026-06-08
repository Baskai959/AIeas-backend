package ws

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

const (
	AuctionEventPubSubPattern         = "auction:*:events"
	LiveSessionEventPubSubPattern     = "live_session:*:events"
	LiveSessionUserEventPubSubPattern = "live_session:*:user:*:events"
)

type PubSubClient interface {
	PSubscribe(ctx context.Context, patterns ...string) *redisgo.PubSub
}

type PubSubBroadcaster struct {
	client     PubSubClient
	hub        *Hub
	asyncCoord *BidAsyncCoordinator
	patterns   []string
}

func NewPubSubBroadcaster(client PubSubClient, hub *Hub) *PubSubBroadcaster {
	return &PubSubBroadcaster{client: client, hub: hub, patterns: defaultPubSubPatterns()}
}

func (b *PubSubBroadcaster) SetBidAsyncCoordinator(coord *BidAsyncCoordinator) {
	if b == nil {
		return
	}
	b.asyncCoord = coord
}

func (b *PubSubBroadcaster) Start(ctx context.Context) {
	if b == nil || b.client == nil || b.hub == nil {
		return
	}
	patterns := b.patterns
	if len(patterns) == 0 {
		patterns = defaultPubSubPatterns()
	}
	go b.run(ctx, patterns)
}

func (b *PubSubBroadcaster) run(ctx context.Context, patterns []string) {
	pubsub := b.client.PSubscribe(ctx, patterns...)
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
	if liveSessionID, userID, ok := liveSessionUserTargetFromEventChannel(msg.Channel); ok {
		b.handleLiveSessionUserMessage(liveSessionID, userID, msg)
		return
	}
	auctionID := auctionIDFromEventChannel(msg.Channel)
	if auctionID != 0 {
		b.handleAuctionMessage(auctionID, msg)
		return
	}
	liveSessionID := liveSessionIDFromEventChannel(msg.Channel)
	if liveSessionID != 0 {
		b.handleLiveSessionMessage(liveSessionID, msg)
	}
}

func (b *PubSubBroadcaster) handleAuctionMessage(auctionID uint64, msg *redisgo.Message) {
	var payload struct {
		Event         string `json:"event"`
		RequestID     string `json:"requestId"`
		Seq           int64  `json:"seq"`
		AuctionID     uint64 `json:"auctionId"`
		LiveSessionID uint64 `json:"liveSessionId"`
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
	env := Envelope{Type: eventType, RequestID: payload.RequestID, Seq: payload.Seq, Payload: json.RawMessage(msg.Payload)}
	if payload.LiveSessionID != 0 {
		b.hub.BroadcastAuctionAndLiveSession(auctionID, payload.LiveSessionID, env)
		return
	}
	b.hub.Broadcast(auctionID, env)
}

func (b *PubSubBroadcaster) handleLiveSessionMessage(liveSessionID uint64, msg *redisgo.Message) {
	var payload struct {
		Event         string `json:"event"`
		RequestID     string `json:"requestId"`
		Seq           int64  `json:"seq"`
		LiveSessionID uint64 `json:"liveSessionId"`
		OnlineOnly    bool   `json:"onlineOnly"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		return
	}
	if payload.LiveSessionID != 0 && payload.LiveSessionID != liveSessionID {
		return
	}
	eventType := strings.TrimSpace(payload.Event)
	if eventType == "" {
		return
	}
	raw := json.RawMessage(msg.Payload)
	if eventType == "live_session.ended" {
		b.hub.BroadcastSessionEnd(liveSessionID, raw)
		return
	}
	env := Envelope{Type: eventType, RequestID: payload.RequestID, Seq: payload.Seq, Payload: raw}
	if payload.OnlineOnly {
		b.hub.BroadcastLiveSessionOnlineClients(liveSessionID, env)
		return
	}
	b.hub.BroadcastLiveSession(liveSessionID, env)
}

func (b *PubSubBroadcaster) handleLiveSessionUserMessage(liveSessionID uint64, userID string, msg *redisgo.Message) {
	if liveSessionID == 0 || userID == "" {
		return
	}
	var payload struct {
		Event         string `json:"event"`
		RequestID     string `json:"requestId"`
		Seq           int64  `json:"seq"`
		LiveSessionID uint64 `json:"liveSessionId"`
		AuctionID     uint64 `json:"auctionId"`
		BidID         string `json:"bidId"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		return
	}
	if payload.LiveSessionID != 0 && payload.LiveSessionID != liveSessionID {
		return
	}
	eventType := strings.TrimSpace(payload.Event)
	if eventType == "" {
		return
	}
	raw := json.RawMessage(msg.Payload)
	env := Envelope{Type: eventType, RequestID: payload.RequestID, Seq: payload.Seq, Payload: raw}
	if eventType == TypeBidResult && b.asyncCoord != nil {
		bidID := strings.TrimSpace(payload.BidID)
		if bidID == "" {
			bidID = strings.TrimSpace(payload.RequestID)
		}
		b.asyncCoord.OnDecision(liveSessionID, payload.AuctionID, userID, bidID, env)
		return
	}
	if b.hub != nil {
		b.hub.DeliverToUserInSession(liveSessionID, userID, env)
	}
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

func liveSessionIDFromEventChannel(channel string) uint64 {
	parts := strings.Split(channel, ":")
	if len(parts) != 3 || parts[0] != "live_session" || parts[2] != "events" {
		return 0
	}
	id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func liveSessionUserTargetFromEventChannel(channel string) (uint64, string, bool) {
	parts := strings.Split(channel, ":")
	if len(parts) != 5 || parts[0] != "live_session" || parts[2] != "user" || parts[4] != "events" {
		return 0, "", false
	}
	id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, "", false
	}
	userID, err := url.PathUnescape(parts[3])
	if err != nil {
		return 0, "", false
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return 0, "", false
	}
	return id, userID, true
}

func defaultPubSubPatterns() []string {
	return []string{AuctionEventPubSubPattern, LiveSessionEventPubSubPattern, LiveSessionUserEventPubSubPattern}
}
