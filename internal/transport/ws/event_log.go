package ws

import (
	"context"
	"sync"
	"time"

	redisinfra "aieas_backend/internal/infra/redis"
)

type RedisReplaySource struct {
	log   bidReplayLog
	limit int64
}

type bidReplayLog interface {
	ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]redisinfra.BidEvent, bool, error)
}

func NewRedisReplaySource(log bidReplayLog, limit int64) *RedisReplaySource {
	if limit <= 0 {
		limit = defaultEventWindow
	}
	return &RedisReplaySource{log: log, limit: limit}
}

func (s *RedisReplaySource) ReplaySince(ctx context.Context, auctionID uint64, lastSeq int64) ([]Envelope, bool, error) {
	events, complete, err := s.log.ReplayBidEvents(ctx, auctionID, lastSeq, s.limit)
	if err != nil || !complete {
		return nil, complete, err
	}
	envelopes := make([]Envelope, 0, len(events))
	for _, event := range events {
		envelopes = append(envelopes, Envelope{Type: event.EventType, Seq: event.Seq, Payload: event.PayloadJSON()})
	}
	return envelopes, true, nil
}

type EventRelay struct {
	log      bidRelayLog
	hub      *Hub
	interval time.Duration
	mu       sync.Mutex
	lastSeq  map[uint64]int64
}

type bidRelayLog interface {
	Enabled() bool
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]redisinfra.BidEvent, bool, error)
}

func NewEventRelay(log bidRelayLog, hub *Hub, interval time.Duration) *EventRelay {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &EventRelay{log: log, hub: hub, interval: interval, lastSeq: make(map[uint64]int64)}
}

func (r *EventRelay) Start(ctx context.Context) {
	if r == nil || r.log == nil || r.hub == nil || !r.log.Enabled() {
		return
	}
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.poll(ctx)
			}
		}
	}()
}

func (r *EventRelay) poll(ctx context.Context) {
	auctions, err := r.log.ActiveAuctions(ctx)
	if err != nil {
		return
	}
	for _, auctionID := range auctions {
		r.mu.Lock()
		lastSeq := r.lastSeq[auctionID]
		r.mu.Unlock()
		events, complete, err := r.log.ReplayBidEvents(ctx, auctionID, lastSeq, 512)
		if err != nil || !complete {
			continue
		}
		for _, event := range events {
			r.mu.Lock()
			if event.Seq <= r.lastSeq[auctionID] {
				r.mu.Unlock()
				continue
			}
			r.lastSeq[auctionID] = event.Seq
			r.mu.Unlock()
			if event.EventType == "bid.rejected" {
				continue
			}
			env := Envelope{Type: event.EventType, Seq: event.Seq, Payload: event.PayloadJSON()}
			if event.LiveSessionID != 0 {
				r.hub.BroadcastAuctionAndLiveSession(auctionID, event.LiveSessionID, env)
				continue
			}
			r.hub.Broadcast(auctionID, env)
		}
	}
}
