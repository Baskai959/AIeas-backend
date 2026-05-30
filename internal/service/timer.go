package service

import (
	"context"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type TimerScheduler struct {
	realtime  repository.AuctionRealtimeStore
	hammer    *HammerService
	publisher EventPublisher
	interval  time.Duration
	mu        sync.Mutex
	cancels   map[uint64]context.CancelFunc
}

func NewTimerScheduler(realtime repository.AuctionRealtimeStore, hammer *HammerService, publisher EventPublisher, interval time.Duration) *TimerScheduler {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
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
			broadcastJSON(s.publisher, auctionID, "timer.tick", map[string]interface{}{
				"auctionId":   auctionID,
				"endTime":     state.EndTime,
				"remainingMs": remaining.Milliseconds(),
				"status":      state.Status,
			})
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
