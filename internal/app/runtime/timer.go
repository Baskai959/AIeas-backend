package runtime

import (
	"context"
	"errors"
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
	fastTick  time.Duration
	fastBelow time.Duration
	mu        sync.Mutex
	cancels   map[uint64]context.CancelFunc
	// graceMs 是 timer 触发落锤前的短宽限：当 now 已过 endTime，但 graceMs
	// 时间还没到，仍保持 RUNNING/EXTENDED 不进 hammer 分支，给异步出价命令
	// 最后一刻把 endTime 推后的机会。0 等价于历史严格 now.Before(endTime) 行为。
	graceMs int64
}

func NewTimerScheduler(realtime auctionports.AuctionRealtimeStore, hammer *auctionapp.HammerService, publisher eventPublisher, interval time.Duration) *TimerScheduler {
	if realtime == nil {
		realtime = realtimeinfra.NoopRealtimeStore{}
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &TimerScheduler{
		realtime:  realtime,
		hammer:    hammer,
		publisher: publisher,
		interval:  interval,
		fastTick:  200 * time.Millisecond,
		fastBelow: 3 * time.Second,
		cancels:   make(map[uint64]context.CancelFunc),
	}
}

// SetHammerAntiSnipingGraceMs 注入 timer 到点后触发落锤前的短宽限期（毫秒）。
// <=0 表示禁用宽限（保持历史严格 now.Before(endTime) 行为）。装配期一次性写入。
func (s *TimerScheduler) SetHammerAntiSnipingGraceMs(grace int64) {
	if s == nil {
		return
	}
	if grace < 0 {
		grace = 0
	}
	s.graceMs = grace
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
	for {
		if ctx.Err() != nil {
			return
		}
		now := time.Now().UTC()
		state, ok, err := s.realtime.GetAuctionState(ctx, auctionID)
		if err != nil || !ok {
			if !sleepTimer(ctx, s.interval) {
				return
			}
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
		// 到点后只宽限配置的短 grace；防狙击窗口本身由出价命中尾段时
		// 写入的新 endTime 表达，不能再把整段 state.AntiSnipingMS 叠加到
		// endTime 之后，否则没有尾段出价也会平白延迟落锤。
		// CAP_PRICE / Force=true 走 BidService 直接调 Hammer，不经过 timer，因此
		// 不受本宽限影响。
		hammerThreshold := state.EndTime
		if s.graceMs > 0 {
			hammerThreshold = hammerThreshold.Add(time.Duration(s.graceMs) * time.Millisecond)
		}
		if now.Before(hammerThreshold) {
			if !sleepTimer(ctx, s.nextDelay(now, hammerThreshold)) {
				return
			}
			continue
		}
		if s.hammer != nil {
			_, _, hammerErr := s.hammer.Hammer(ctx, domain.HammerInput{
				RequestID:      "auto-" + strconvFormatTime(now.UTC()),
				AuctionID:      auctionID,
				ActorID:        "system",
				ActorRole:      domain.RoleAdmin,
				ClosedBy:       "system",
				Now:            now.UTC(),
				IdempotencyTTL: 24 * time.Hour,
			})
			// BeginHammerPending 二次确认发现 endTime 被推后时返回 ErrInvalidState；
			// 此时不结束 timer，下一轮 tick 会重新读 state 重新判断。
			if errors.Is(hammerErr, domain.ErrInvalidState) {
				if !sleepTimer(ctx, s.fastTick) {
					return
				}
				continue
			}
		}
		s.Stop(auctionID)
		return
	}
}

func (s *TimerScheduler) nextDelay(now, threshold time.Time) time.Duration {
	remaining := threshold.Sub(now)
	if remaining <= 0 {
		return 0
	}
	delay := s.interval
	if remaining <= s.fastBelow {
		delay = s.fastTick
	}
	if delay <= 0 || delay > remaining {
		return remaining
	}
	return delay
}

func sleepTimer(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
