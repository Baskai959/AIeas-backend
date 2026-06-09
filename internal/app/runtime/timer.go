package runtime

import (
	"context"
	"encoding/json"
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
	mu        sync.Mutex
	cancels   map[uint64]context.CancelFunc
	// graceMs 是 timer 触发落锤前的 anti-sniping 宽限：当 now 已过 endTime，但
	// max(state.AntiSnipingMS, graceMs) 时间还没到，仍保持 RUNNING/EXTENDED 不进
	// hammer 分支，给异步出价命令最后一刻把 endTime 推后的机会。0 等价于历史行为。
	graceMs int64
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

// SetHammerAntiSnipingGraceMs 注入 timer 的 anti-sniping 宽限期（毫秒）。
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
			// anti-sniping grace：到点后再宽限 max(state.AntiSnipingMS, graceMs)，
			// 才允许进入 hammer 分支；这一窗口让异步链路里"最后一刻"的出价命令
			// 仍有机会延长 endTime（通过 bid.lua 写入新 end_ts_ms）。
			// CAP_PRICE / Force=true 走 BidService 直接调 Hammer，不经过 timer，因此
			// 不受本宽限影响。
			grace := s.graceMs
			if state.AntiSnipingMS > grace {
				grace = state.AntiSnipingMS
			}
			hammerThreshold := state.EndTime
			if grace > 0 {
				hammerThreshold = hammerThreshold.Add(time.Duration(grace) * time.Millisecond)
			}
			if now.Before(hammerThreshold) {
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
					continue
				}
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
