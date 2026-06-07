package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	adminports "aieas_backend/internal/modules/admin/ports"
	adminrepo "aieas_backend/internal/modules/admin/repository"
	livesessionapp "aieas_backend/internal/modules/live_session/app"
	userports "aieas_backend/internal/modules/user/ports"
)

const (
	liveAgentHookDefaultConfigKey = "live.agent_hook.default"
	liveAgentHookDescription      = "商家直播拍卖 AI Agent hook 开关"
	defaultHighestBidHookQuiet    = 5 * time.Second
	defaultLiveAgentHookTimeout   = 30 * time.Second
)

type LiveAgentHookConfig = livesessionapp.LiveAgentHookConfig

type LiveAgentHookInvoker interface {
	InvokeLiveAgentHook(ctx context.Context, sessionID, question string) error
}

type DisabledLiveAgentHookInvoker struct{}

func (DisabledLiveAgentHookInvoker) InvokeLiveAgentHook(ctx context.Context, sessionID, question string) error {
	_ = ctx
	_ = sessionID
	_ = question
	return nil
}

type LiveAgentHookService struct {
	configs adminports.ConfigRepository
	users   userports.UserRepository
	invoker LiveAgentHookInvoker

	mu                     sync.RWMutex
	cache                  map[string]liveAgentHookCacheEntry
	highestBidQuietPeriod  time.Duration
	invokeTimeout          time.Duration
	pendingHighestBidHooks map[string]*liveAgentHighestBidHook
}

type liveAgentHookCacheEntry struct {
	enabled   bool
	expiresAt time.Time
}

type liveAgentHighestBidHook struct {
	merchantID string
	sessionID  uint64
	question   string
	generation uint64
	timer      *time.Timer
}

func NewLiveAgentHookService(configs adminports.ConfigRepository, users userports.UserRepository, invoker LiveAgentHookInvoker) *LiveAgentHookService {
	if configs == nil {
		configs = adminrepo.NewMemoryConfigRepository()
	}
	if invoker == nil {
		invoker = DisabledLiveAgentHookInvoker{}
	}
	return &LiveAgentHookService{
		configs:                configs,
		users:                  users,
		invoker:                invoker,
		cache:                  make(map[string]liveAgentHookCacheEntry),
		highestBidQuietPeriod:  defaultHighestBidHookQuiet,
		invokeTimeout:          defaultLiveAgentHookTimeout,
		pendingHighestBidHooks: make(map[string]*liveAgentHighestBidHook),
	}
}

func (s *LiveAgentHookService) SetHighestBidQuietPeriod(period time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.highestBidQuietPeriod = period
}

func (s *LiveAgentHookService) SetInvokeTimeout(timeout time.Duration) {
	if s == nil || timeout <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invokeTimeout = timeout
}

func (s *LiveAgentHookService) GetConfig(ctx context.Context, merchantID string) (LiveAgentHookConfig, error) {
	enabled, err := s.enabled(ctx, merchantID)
	if err != nil {
		return LiveAgentHookConfig{}, err
	}
	return LiveAgentHookConfig{Enabled: enabled}, nil
}

func (s *LiveAgentHookService) SetConfig(ctx context.Context, merchantID, updatedBy string, enabled bool) (LiveAgentHookConfig, error) {
	if s == nil || s.configs == nil || strings.TrimSpace(merchantID) == "" {
		return LiveAgentHookConfig{}, domain.ErrInvalidArgument
	}
	value, err := json.Marshal(LiveAgentHookConfig{Enabled: enabled})
	if err != nil {
		return LiveAgentHookConfig{}, err
	}
	key := liveAgentHookMerchantKey(merchantID)
	item := domain.ConfigItem{
		Key:         key,
		Value:       value,
		Description: liveAgentHookDescription,
		UpdatedBy:   strings.TrimSpace(updatedBy),
		UpdatedAt:   time.Now().UTC(),
	}
	if err := s.configs.Upsert(ctx, &item); err != nil {
		return LiveAgentHookConfig{}, err
	}
	s.storeCache(key, enabled)
	return LiveAgentHookConfig{Enabled: enabled}, nil
}

func (s *LiveAgentHookService) EmitLiveStarted(ctx context.Context, merchantID string, sessionID uint64) {
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d开播了", sessionID))
}

func (s *LiveAgentHookService) EmitLotMounted(ctx context.Context, merchantID string, sessionID, auctionID uint64) {
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d已上架", sessionID, auctionID))
}

func (s *LiveAgentHookService) EmitLotUnmounted(ctx context.Context, merchantID string, sessionID, auctionID uint64) {
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d已下架", sessionID, auctionID))
}

func (s *LiveAgentHookService) EmitLotStarted(ctx context.Context, merchantID string, sessionID, auctionID uint64, durationSec int) {
	if durationSec > 0 {
		s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d开始拍卖/讲解，拍卖时长%d秒", sessionID, auctionID, durationSec))
		return
	}
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d开始拍卖/讲解", sessionID, auctionID))
}

func (s *LiveAgentHookService) EmitLotScheduled(ctx context.Context, merchantID string, sessionID, auctionID uint64, startTime time.Time, durationSec int) {
	startAt := startTime.UTC().Format(time.RFC3339)
	if durationSec > 0 {
		s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d已预约开拍，开拍时间%s，拍卖时长%d秒", sessionID, auctionID, startAt, durationSec))
		return
	}
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d已预约开拍，开拍时间%s", sessionID, auctionID, startAt))
}

func (s *LiveAgentHookService) EmitLotCancelled(ctx context.Context, merchantID string, sessionID, auctionID uint64) {
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d已取消拍卖/讲解", sessionID, auctionID))
}

func (s *LiveAgentHookService) EmitAuctionCancelled(ctx context.Context, merchantID string, sessionID, auctionID uint64) {
	s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d已取消", sessionID, auctionID))
}

func (s *LiveAgentHookService) EmitHammerWon(ctx context.Context, merchantID string, sessionID, auctionID uint64, price int64) {
	s.EmitAuctionClosed(ctx, merchantID, sessionID, auctionID, domain.AuctionStatusClosedWon, price, false, "")
}

func (s *LiveAgentHookService) EmitAuctionClosed(ctx context.Context, merchantID string, sessionID, auctionID uint64, status domain.AuctionStatus, price int64, auto bool, reason string) {
	switch status {
	case domain.AuctionStatusClosedWon:
		if auto {
			s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d自动落锤成交，成交价%d分", sessionID, auctionID, price))
			return
		}
		s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d落锤成交，成交价%d分", sessionID, auctionID, price))
	case domain.AuctionStatusClosedFailed:
		if auto {
			s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d自动落锤流拍", sessionID, auctionID))
			return
		}
		if strings.TrimSpace(reason) != "" {
			s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d流拍，原因：%s", sessionID, auctionID, strings.TrimSpace(reason)))
			return
		}
		s.Emit(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%d的拍品%d流拍", sessionID, auctionID))
	}
}

func (s *LiveAgentHookService) EmitHighestBid(ctx context.Context, merchantID string, sessionID uint64, bidderID string, price int64) {
	name := strings.TrimSpace(bidderID)
	if s != nil && s.users != nil {
		if user, err := s.users.FindByID(bidderID); err == nil && strings.TrimSpace(user.Nickname) != "" {
			name = strings.TrimSpace(user.Nickname)
		}
	}
	s.EmitHighestBidWithBidderName(ctx, merchantID, sessionID, bidderID, name, price)
}

func (s *LiveAgentHookService) EmitHighestBidWithBidderName(ctx context.Context, merchantID string, sessionID uint64, bidderID string, bidderName string, price int64) {
	s.emitHighestBidDebounced(ctx, merchantID, sessionID, highestBidQuestion(sessionID, bidderID, bidderName, price))
}

func (s *LiveAgentHookService) EmitHighestBidWithBidderNameNow(ctx context.Context, merchantID string, sessionID uint64, bidderID string, bidderName string, price int64) {
	s.Emit(ctx, merchantID, sessionID, highestBidQuestion(sessionID, bidderID, bidderName, price))
}

func highestBidQuestion(sessionID uint64, bidderID string, bidderName string, price int64) string {
	name := strings.TrimSpace(bidderName)
	if name == "" {
		name = strings.TrimSpace(bidderID)
	}
	return fmt.Sprintf("直播场次%d用户%s目前最高价格%d分", sessionID, name, price)
}

func (s *LiveAgentHookService) EmitConfigChanged(ctx context.Context, merchantID string, sessionID uint64, enabled bool) {
	if !enabled {
		return
	}
	s.emitDirect(ctx, merchantID, sessionID, fmt.Sprintf("直播场次%dAI直播助手已开启", sessionID))
}

func (s *LiveAgentHookService) Emit(ctx context.Context, merchantID string, sessionID uint64, question string) {
	if s == nil || s.invoker == nil || strings.TrimSpace(merchantID) == "" || sessionID == 0 || strings.TrimSpace(question) == "" {
		return
	}
	enabled, err := s.enabled(ctx, merchantID)
	if err != nil {
		slog.Default().Warn("live agent hook config check failed", "merchant_id", merchantID, "error", err)
		return
	}
	if !enabled {
		return
	}
	s.emitDirect(ctx, merchantID, sessionID, question)
}

func (s *LiveAgentHookService) emitHighestBidDebounced(ctx context.Context, merchantID string, sessionID uint64, question string) {
	if s == nil || strings.TrimSpace(merchantID) == "" || sessionID == 0 || strings.TrimSpace(question) == "" {
		return
	}
	period := s.highestBidPeriod()
	if period <= 0 {
		s.Emit(ctx, merchantID, sessionID, question)
		return
	}
	key := fmt.Sprintf("%s:%d", strings.TrimSpace(merchantID), sessionID)
	s.mu.Lock()
	if s.pendingHighestBidHooks == nil {
		s.pendingHighestBidHooks = make(map[string]*liveAgentHighestBidHook)
	}
	pending := s.pendingHighestBidHooks[key]
	if pending == nil {
		pending = &liveAgentHighestBidHook{}
		s.pendingHighestBidHooks[key] = pending
	}
	pending.merchantID = strings.TrimSpace(merchantID)
	pending.sessionID = sessionID
	pending.question = strings.TrimSpace(question)
	pending.generation++
	generation := pending.generation
	if pending.timer != nil {
		pending.timer.Stop()
	}
	pending.timer = time.AfterFunc(period, func() {
		s.fireHighestBidHook(key, generation)
	})
	s.mu.Unlock()
}

func (s *LiveAgentHookService) highestBidPeriod() time.Duration {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.highestBidQuietPeriod
}

func (s *LiveAgentHookService) fireHighestBidHook(key string, generation uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	pending := s.pendingHighestBidHooks[key]
	if pending == nil || pending.generation != generation {
		s.mu.Unlock()
		return
	}
	merchantID := pending.merchantID
	sessionID := pending.sessionID
	question := pending.question
	delete(s.pendingHighestBidHooks, key)
	s.mu.Unlock()
	s.Emit(context.Background(), merchantID, sessionID, question)
}

func (s *LiveAgentHookService) emitDirect(ctx context.Context, merchantID string, sessionID uint64, question string) {
	if s == nil || s.invoker == nil || strings.TrimSpace(merchantID) == "" || sessionID == 0 || strings.TrimSpace(question) == "" {
		return
	}
	session := strconv.FormatUint(sessionID, 10)
	q := strings.TrimSpace(question)
	invoker := s.invoker
	timeout := s.hookInvokeTimeout()
	go func() {
		defer func() { _ = recover() }()
		hookCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := invoker.InvokeLiveAgentHook(hookCtx, session, q); err != nil {
			slog.Default().Warn("live agent hook invoke failed", "merchant_id", merchantID, "error", err)
		}
	}()
}

func (s *LiveAgentHookService) hookInvokeTimeout() time.Duration {
	if s == nil {
		return defaultLiveAgentHookTimeout
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.invokeTimeout <= 0 {
		return defaultLiveAgentHookTimeout
	}
	return s.invokeTimeout
}

func (s *LiveAgentHookService) enabled(ctx context.Context, merchantID string) (bool, error) {
	if s == nil || s.configs == nil || strings.TrimSpace(merchantID) == "" {
		return false, nil
	}
	key := liveAgentHookMerchantKey(merchantID)
	if enabled, ok := s.cachedEnabled(key); ok {
		return enabled, nil
	}
	enabled, err := s.readConfigEnabled(ctx, key)
	if err != nil {
		return false, err
	}
	s.storeCache(key, enabled)
	return enabled, nil
}

func (s *LiveAgentHookService) readConfigEnabled(ctx context.Context, key string) (bool, error) {
	item, err := s.configs.FindByKey(ctx, key)
	if err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			return false, err
		}
		item, err = s.configs.FindByKey(ctx, liveAgentHookDefaultConfigKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
	}
	var cfg LiveAgentHookConfig
	if err := json.Unmarshal(item.Value, &cfg); err != nil {
		return false, err
	}
	return cfg.Enabled, nil
}

func (s *LiveAgentHookService) cachedEnabled(key string) (bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.cache[key]
	if !ok || time.Now().After(entry.expiresAt) {
		return false, false
	}
	return entry.enabled, true
}

func (s *LiveAgentHookService) storeCache(key string, enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[key] = liveAgentHookCacheEntry{enabled: enabled, expiresAt: time.Now().Add(5 * time.Second)}
}

func liveAgentHookMerchantKey(merchantID string) string {
	return fmt.Sprintf("merchant.%s.live_agent_hook", strings.TrimSpace(merchantID))
}
