package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

const (
	liveAgentHookDefaultConfigKey = "live.agent_hook.default"
	liveAgentHookDescription      = "商家直播拍卖 AI Agent hook 开关"
)

type LiveAgentHookConfig struct {
	Enabled bool `json:"enabled"`
}

type LiveAgentHookInvoker interface {
	InvokeLiveAgentHook(ctx context.Context, message string) error
}

type DisabledLiveAgentHookInvoker struct{}

func (DisabledLiveAgentHookInvoker) InvokeLiveAgentHook(ctx context.Context, message string) error {
	_ = ctx
	_ = message
	return nil
}

type LiveAgentHookService struct {
	configs repository.ConfigRepository
	users   repository.UserRepository
	invoker LiveAgentHookInvoker

	mu    sync.RWMutex
	cache map[string]liveAgentHookCacheEntry
}

type liveAgentHookCacheEntry struct {
	enabled   bool
	expiresAt time.Time
}

func NewLiveAgentHookService(configs repository.ConfigRepository, users repository.UserRepository, invoker LiveAgentHookInvoker) *LiveAgentHookService {
	if configs == nil {
		configs = repository.NewMemoryConfigRepository()
	}
	if invoker == nil {
		invoker = DisabledLiveAgentHookInvoker{}
	}
	return &LiveAgentHookService{
		configs: configs,
		users:   users,
		invoker: invoker,
		cache:   make(map[string]liveAgentHookCacheEntry),
	}
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
	s.Emit(ctx, merchantID, fmt.Sprintf("直播场次%d开播了", sessionID))
}

func (s *LiveAgentHookService) EmitItemListed(ctx context.Context, merchantID string, itemID uint64) {
	s.Emit(ctx, merchantID, fmt.Sprintf("商品%d上架了", itemID))
}

func (s *LiveAgentHookService) EmitItemOffline(ctx context.Context, merchantID string, itemID uint64) {
	s.Emit(ctx, merchantID, fmt.Sprintf("商品%d下架了", itemID))
}

func (s *LiveAgentHookService) EmitHammerWon(ctx context.Context, merchantID string, sessionID, auctionID uint64, price int64) {
	s.Emit(ctx, merchantID, fmt.Sprintf("直播场次%d的拍品%d落锤成交，成交价%d分", sessionID, auctionID, price))
}

func (s *LiveAgentHookService) EmitHighestBid(ctx context.Context, merchantID string, sessionID uint64, bidderID string, price int64) {
	name := strings.TrimSpace(bidderID)
	if s != nil && s.users != nil {
		if user, err := s.users.FindByID(bidderID); err == nil && strings.TrimSpace(user.Nickname) != "" {
			name = strings.TrimSpace(user.Nickname)
		}
	}
	s.Emit(ctx, merchantID, fmt.Sprintf("直播场次%d用户%s目前最高价格%d分", sessionID, name, price))
}

func (s *LiveAgentHookService) EmitConfigChanged(ctx context.Context, merchantID string, sessionID uint64, enabled bool) {
	if !enabled {
		return
	}
	s.emitDirect(ctx, merchantID, fmt.Sprintf("直播场次%dAI直播助手已开启", sessionID))
}

func (s *LiveAgentHookService) Emit(ctx context.Context, merchantID, message string) {
	if s == nil || s.invoker == nil || strings.TrimSpace(merchantID) == "" || strings.TrimSpace(message) == "" {
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
	s.emitDirect(ctx, merchantID, message)
}

func (s *LiveAgentHookService) emitDirect(ctx context.Context, merchantID, message string) {
	if s == nil || s.invoker == nil || strings.TrimSpace(merchantID) == "" || strings.TrimSpace(message) == "" {
		return
	}
	msg := strings.TrimSpace(message)
	invoker := s.invoker
	go func() {
		defer func() { _ = recover() }()
		hookCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := invoker.InvokeLiveAgentHook(hookCtx, msg); err != nil {
			slog.Default().Warn("live agent hook invoke failed", "merchant_id", merchantID, "error", err)
		}
	}()
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
