package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const realtimeEventPublishTimeout = 200 * time.Millisecond

type RealtimeEventPublisher struct {
	sharded *ShardedRTClient
}

func NewRealtimeEventPublisher(sharded *ShardedRTClient) *RealtimeEventPublisher {
	if sharded == nil || sharded.Len() == 0 {
		return nil
	}
	return &RealtimeEventPublisher{sharded: sharded}
}

func (p *RealtimeEventPublisher) PublishAuctionEvent(ctx context.Context, auctionID uint64, eventType, requestID string, seq int64, payload json.RawMessage) error {
	if p == nil || p.sharded == nil || auctionID == 0 || strings.TrimSpace(eventType) == "" {
		return nil
	}
	client := p.sharded.ForAuction(auctionID)
	if client == nil {
		return nil
	}
	raw, err := mergeRealtimeEventPayload(payload, map[string]interface{}{
		"event":     strings.TrimSpace(eventType),
		"requestId": strings.TrimSpace(requestID),
		"seq":       seq,
		"auctionId": auctionID,
	})
	if err != nil {
		return err
	}
	pubCtx, cancel := realtimePublishContext(ctx)
	defer cancel()
	channel := "auction:" + strconv.FormatUint(auctionID, 10) + ":events"
	return client.Publish(pubCtx, channel, raw).Err()
}

func (p *RealtimeEventPublisher) PublishLiveSessionEvent(ctx context.Context, liveSessionID uint64, eventType, requestID string, seq int64, payload json.RawMessage, onlineOnly bool) error {
	if p == nil || p.sharded == nil || liveSessionID == 0 || strings.TrimSpace(eventType) == "" {
		return nil
	}
	client := p.sharded.ForSession(liveSessionID)
	if client == nil {
		return nil
	}
	raw, err := mergeRealtimeEventPayload(payload, map[string]interface{}{
		"event":         strings.TrimSpace(eventType),
		"requestId":     strings.TrimSpace(requestID),
		"seq":           seq,
		"liveSessionId": liveSessionID,
		"onlineOnly":    onlineOnly,
	})
	if err != nil {
		return err
	}
	pubCtx, cancel := realtimePublishContext(ctx)
	defer cancel()
	channel := "live_session:" + strconv.FormatUint(liveSessionID, 10) + ":events"
	return client.Publish(pubCtx, channel, raw).Err()
}

func realtimePublishContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(ctx, realtimeEventPublishTimeout)
}

func mergeRealtimeEventPayload(payload json.RawMessage, metadata map[string]interface{}) ([]byte, error) {
	obj := make(map[string]interface{}, len(metadata)+8)
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) {
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return nil, fmt.Errorf("merge realtime event payload: %w", err)
		}
	}
	for key, value := range metadata {
		switch v := value.(type) {
		case string:
			if v == "" {
				continue
			}
		case int64:
			if v == 0 {
				continue
			}
		}
		obj[key] = value
	}
	return json.Marshal(obj)
}
