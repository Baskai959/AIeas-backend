package runtime

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	realtimeinfra "aieas_backend/internal/infra/realtime"
)

type timerRealtimeStore struct {
	realtimeinfra.NoopRealtimeStore
	state domain.AuctionState
}

func (s timerRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	if s.state.AuctionID != auctionID {
		return domain.AuctionState{}, false, nil
	}
	return s.state, true, nil
}

func TestTimerSchedulerUsesConfiguredGraceWithoutAddingAntiSnipingWindow(t *testing.T) {
	auctionID := uint64(9001)
	now := time.Now().UTC()
	realtime := timerRealtimeStore{
		state: domain.AuctionState{
			AuctionID:     auctionID,
			Status:        domain.AuctionStatusRunning,
			EndTime:       now.Add(30 * time.Millisecond),
			AntiSnipingMS: int64(10 * time.Second / time.Millisecond),
		},
	}
	scheduler := NewTimerScheduler(realtime, nil, nil, 5*time.Millisecond)
	scheduler.SetHammerAntiSnipingGraceMs(20)
	scheduler.Schedule(auctionID)
	defer scheduler.Stop(auctionID)

	if !eventually(500*time.Millisecond, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return scheduler.cancels[auctionID] == nil
	}) {
		t.Fatalf("timer stayed scheduled; configured hammer grace should not be extended by AntiSnipingMS")
	}
}

func TestTimerSchedulerNextDelaySwitchesToFastTickNearEnd(t *testing.T) {
	now := time.Now().UTC()
	scheduler := NewTimerScheduler(nil, nil, nil, time.Second)

	if got := scheduler.nextDelay(now, now.Add(4*time.Second)); got != time.Second {
		t.Fatalf("delay above fast window = %s, want 1s", got)
	}
	if got := scheduler.nextDelay(now, now.Add(2*time.Second)); got != 200*time.Millisecond {
		t.Fatalf("delay inside fast window = %s, want 200ms", got)
	}
	if got := scheduler.nextDelay(now, now.Add(50*time.Millisecond)); got != 50*time.Millisecond {
		t.Fatalf("delay should not oversleep threshold = %s, want 50ms", got)
	}
}

func eventually(timeout time.Duration, condition func() bool) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return true
		}
		select {
		case <-deadline.C:
			return condition()
		case <-ticker.C:
		}
	}
}
