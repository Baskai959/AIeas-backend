package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"

	redisgo "github.com/redis/go-redis/v9"
)

const ConfigInvalidateChannel = "config.invalidate"

type ConfigInvalidationBus interface {
	Publish(ctx context.Context, channel string, message interface{}) *redisgo.IntCmd
	PSubscribe(ctx context.Context, patterns ...string) *redisgo.PubSub
}

type FeatureFlagService struct {
	repo     repository.ConfigRepository
	bus      ConfigInvalidationBus
	cacheTTL time.Duration
	now      func() time.Time

	mu    sync.RWMutex
	cache map[string]featureFlagCacheEntry
}

type featureFlagCacheEntry struct {
	flag      domain.FeatureFlag
	expiresAt time.Time
}

func NewFeatureFlagService(repo repository.ConfigRepository, bus ConfigInvalidationBus) *FeatureFlagService {
	if repo == nil {
		repo = repository.NewMemoryConfigRepository()
	}
	return &FeatureFlagService{repo: repo, bus: bus, cacheTTL: 30 * time.Second, now: time.Now, cache: make(map[string]featureFlagCacheEntry)}
}

func (s *FeatureFlagService) Decide(ctx context.Context, flag, userID string) bool {
	cfg, err := s.Get(ctx, flag)
	if err != nil || !cfg.Enabled {
		return false
	}
	userID = strings.TrimSpace(userID)
	for _, allowed := range cfg.Allowlist {
		if strings.TrimSpace(allowed) == userID && userID != "" {
			return true
		}
	}
	if cfg.RolloutPercentage >= 100 {
		return true
	}
	if cfg.RolloutPercentage <= 0 || userID == "" {
		return false
	}
	return rolloutBucket(cfg.Key, userID) < cfg.RolloutPercentage
}

func (s *FeatureFlagService) Get(ctx context.Context, key string) (domain.FeatureFlag, error) {
	key = normalizeFeatureFlagKey(key)
	if key == "" {
		return domain.FeatureFlag{}, domain.ErrInvalidArgument
	}
	now := s.now().UTC()
	s.mu.RLock()
	entry, ok := s.cache[key]
	s.mu.RUnlock()
	if ok && now.Before(entry.expiresAt) {
		return cloneFeatureFlag(entry.flag), nil
	}
	flag, err := s.load(ctx, key)
	if err != nil {
		return domain.FeatureFlag{}, err
	}
	s.mu.Lock()
	s.cache[key] = featureFlagCacheEntry{flag: cloneFeatureFlag(flag), expiresAt: now.Add(s.cacheTTL)}
	s.mu.Unlock()
	return cloneFeatureFlag(flag), nil
}

func (s *FeatureFlagService) Update(ctx context.Context, flag domain.FeatureFlag, actorID string) (domain.FeatureFlag, error) {
	flag.Key = normalizeFeatureFlagKey(flag.Key)
	if flag.Key == "" || flag.RolloutPercentage < 0 || flag.RolloutPercentage > 100 {
		return domain.FeatureFlag{}, domain.ErrInvalidArgument
	}
	flag.Allowlist = normalizeAllowlist(flag.Allowlist)
	flag.UpdatedBy = strings.TrimSpace(actorID)
	flag.UpdatedAt = s.now().UTC()
	value, err := json.Marshal(flag)
	if err != nil {
		return domain.FeatureFlag{}, err
	}
	item := domain.ConfigItem{Key: flag.Key, Value: value, Description: flag.Description, UpdatedBy: flag.UpdatedBy, UpdatedAt: flag.UpdatedAt}
	if err := s.repo.Upsert(ctx, &item); err != nil {
		return domain.FeatureFlag{}, err
	}
	s.Invalidate(flag.Key)
	if s.bus != nil {
		_ = s.bus.Publish(ctx, ConfigInvalidateChannel, flag.Key).Err()
	}
	return cloneFeatureFlag(flag), nil
}

func (s *FeatureFlagService) Invalidate(key string) {
	key = normalizeFeatureFlagKey(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" {
		s.cache = make(map[string]featureFlagCacheEntry)
		return
	}
	delete(s.cache, key)
}

func (s *FeatureFlagService) StartInvalidationSubscriber(ctx context.Context) {
	if s == nil || s.bus == nil {
		return
	}
	go func() {
		pubsub := s.bus.PSubscribe(ctx, ConfigInvalidateChannel)
		defer pubsub.Close()
		ch := pubsub.Channel(redisgo.WithChannelSize(32), redisgo.WithChannelHealthCheckInterval(30*time.Second))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				s.Invalidate(strings.TrimSpace(msg.Payload))
			}
		}
	}()
}

func (s *FeatureFlagService) load(ctx context.Context, key string) (domain.FeatureFlag, error) {
	item, err := s.repo.FindByKey(ctx, key)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			if flag, ok := domain.DefaultFeatureFlags()[key]; ok {
				return cloneFeatureFlag(flag), nil
			}
			return domain.FeatureFlag{Key: key, Enabled: false}, nil
		}
		return domain.FeatureFlag{}, err
	}
	var flag domain.FeatureFlag
	if err := json.Unmarshal(item.Value, &flag); err != nil {
		return domain.FeatureFlag{}, fmt.Errorf("decode feature flag %s: %w", key, err)
	}
	flag.Key = key
	if flag.UpdatedBy == "" {
		flag.UpdatedBy = item.UpdatedBy
	}
	if flag.UpdatedAt.IsZero() {
		flag.UpdatedAt = item.UpdatedAt
	}
	flag.Allowlist = normalizeAllowlist(flag.Allowlist)
	return flag, nil
}

func normalizeFeatureFlagKey(key string) string {
	key = strings.TrimSpace(key)
	if !strings.HasPrefix(key, "feature.") {
		return ""
	}
	parts := strings.Split(key, ".")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return ""
	}
	return key
}

func normalizeAllowlist(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func rolloutBucket(flag, userID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(flag))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(userID))
	return int(h.Sum32() % 100)
}

func cloneFeatureFlag(flag domain.FeatureFlag) domain.FeatureFlag {
	flag.Allowlist = append([]string(nil), flag.Allowlist...)
	return flag
}
