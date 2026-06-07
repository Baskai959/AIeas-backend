package runtime

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	realtimeinfra "aieas_backend/internal/infra/realtime"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	wstransport "aieas_backend/internal/transport/ws"
)

type eventPublisher interface {
	Broadcast(auctionID uint64, env wstransport.Envelope) int
}

type TimerScheduler struct {
	realtime  auctionports.AuctionRealtimeStore
	hammer    *auctionapp.HammerService
	publisher eventPublisher
	interval  time.Duration
	mu        sync.Mutex
	cancels   map[uint64]context.CancelFunc
}

func NewTimerScheduler(realtime auctionports.AuctionRealtimeStore, hammer *auctionapp.HammerService, publisher eventPublisher, interval time.Duration) *TimerScheduler {
	if realtime == nil {
		realtime = realtimeinfra.NoopRealtimeStore{}
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &TimerScheduler{realtime: realtime, hammer: hammer, publisher: publisher, interval: interval, cancels: make(map[uint64]context.CancelFunc)}
}

func (s *TimerScheduler) Schedule(auctionID uint64) {
	if auctionID == 0 {
		return
	}
	s.mu.Lock()
	if cancel := s.cancels[auctionID]; cancel != nil {
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancels[auctionID] = cancel
	s.mu.Unlock()
	go s.run(ctx, auctionID)
}

func (s *TimerScheduler) Stop(auctionID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel := s.cancels[auctionID]; cancel != nil {
		cancel()
		delete(s.cancels, auctionID)
	}
}

func strconvFormatTime(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

func (s *TimerScheduler) run(ctx context.Context, auctionID uint64) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			state, ok, err := s.realtime.GetAuctionState(ctx, auctionID)
			if err != nil || !ok {
				continue
			}
			if state.Status.Terminal() || state.Status == domain.AuctionStatusSettled {
				s.Stop(auctionID)
				return
			}
			if state.Status != domain.AuctionStatusRunning && state.Status != domain.AuctionStatusExtended {
				s.Stop(auctionID)
				return
			}
			remaining := state.EndTime.Sub(now)
			if remaining < 0 {
				remaining = 0
			}
			tickPayload := map[string]interface{}{
				"auctionId":   auctionID,
				"endTime":     state.EndTime,
				"remainingMs": remaining.Milliseconds(),
				"status":      state.Status,
				"serverTime":  now.UTC(),
			}
			if state.LiveSessionID != 0 {
				tickPayload["liveSessionId"] = state.LiveSessionID
			}
			broadcastJSON(s.publisher, auctionID, "timer.tick", tickPayload)
			if now.Before(state.EndTime) {
				continue
			}
			if s.hammer != nil {
				_, _, _ = s.hammer.Hammer(ctx, domain.HammerInput{
					RequestID:      "auto-" + strconvFormatTime(now.UTC()),
					AuctionID:      auctionID,
					ActorID:        "system",
					ActorRole:      domain.RoleAdmin,
					ClosedBy:       "system",
					Now:            now.UTC(),
					IdempotencyTTL: 24 * time.Hour,
				})
			}
			s.Stop(auctionID)
			return
		}
	}
}

func broadcastJSON(publisher eventPublisher, auctionID uint64, eventType string, payload interface{}) {
	broadcastJSONWithSeq(publisher, auctionID, eventType, 0, payload)
}

func broadcastJSONWithSeq(publisher eventPublisher, auctionID uint64, eventType string, seq int64, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, wstransport.Envelope{Type: eventType, Seq: seq, Payload: raw})
}
