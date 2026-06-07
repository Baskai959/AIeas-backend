package app

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
	adminports "aieas_backend/internal/modules/admin/ports"
)

const (
	ConfigInvalidateChannel = "config.invalidate"
)

type ConfigInvalidationBus interface {
	Publish(ctx context.Context, channel string, message string) error
	Subscribe(ctx context.Context, patterns ...string) (ConfigInvalidationSubscription, error)
}

type ConfigInvalidationMessage struct {
	Payload string
}

type ConfigInvalidationSubscription interface {
	Channel() <-chan ConfigInvalidationMessage
	Close() error
}

type AdminService struct {
	users     adminports.UserRepository
	auctions  adminports.AuctionAdminUseCase
	hammers   adminports.HammerUseCase
	orders    adminports.OrderQueryUseCase
	risk      adminports.RiskUseCase
	audits    adminports.AuditRepository
	dashboard adminports.DashboardRepository
	sessions  adminports.LiveSessionRepository
	configs   adminports.ConfigRepository
	flags     adminports.FeatureFlagManager
}

func NewAdminService(users adminports.UserRepository, auctions adminports.AuctionAdminUseCase, hammers adminports.HammerUseCase, orders adminports.OrderQueryUseCase, risk adminports.RiskUseCase, audits adminports.AuditRepository) *AdminService {
	return &AdminService{users: users, auctions: auctions, hammers: hammers, orders: orders, risk: risk, audits: audits}
}

func (s *AdminService) SetDashboardRepository(repo adminports.DashboardRepository) {
	s.dashboard = repo
}

func (s *AdminService) SetLookupRepositories(sessions adminports.LiveSessionRepository) {
	s.sessions = sessions
}

func (s *AdminService) SetConfigRepository(repo adminports.ConfigRepository) {
	s.configs = repo
}

func (s *AdminService) SetFeatureFlagService(flags adminports.FeatureFlagManager) {
	s.flags = flags
}

func (s *AdminService) ListAuctions(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error) {
	return s.auctions.List(ctx, filter, "admin", domain.RoleAdmin)
}

func (s *AdminService) AuditAuction(ctx context.Context, auctionID uint64, approved bool, actorID string) (domain.AuctionLot, error) {
	status := domain.AuctionStatusAuditRejected
	if approved {
		status = domain.AuctionStatusReady
	}
	return s.auctions.AdminUpdateStatus(ctx, auctionID, actorID, status)
}

func (s *AdminService) CancelAuction(ctx context.Context, auctionID uint64, actorID string) (domain.AuctionLot, error) {
	return s.auctions.Cancel(ctx, auctionID, actorID, domain.RoleAdmin)
}

func (s *AdminService) CloseAuction(ctx context.Context, auctionID uint64, actorID, requestID string) (domain.HammerResult, *domain.OrderDeal, error) {
	return s.hammers.Hammer(ctx, domain.HammerInput{RequestID: requestID, AuctionID: auctionID, ActorID: actorID, ActorRole: domain.RoleAdmin, ClosedBy: actorID, Force: true, Now: time.Now().UTC()})
}

func (s *AdminService) ListUsers(filter domain.UserFilter) ([]domain.SafeUser, error) {
	users, err := s.users.List(filter)
	if err != nil {
		return nil, err
	}
	safe := make([]domain.SafeUser, 0, len(users))
	for _, user := range users {
		safe = append(safe, user.Safe())
	}
	return safe, nil
}

func (s *AdminService) AuctionByID(ctx context.Context, auctionID uint64) (domain.AuctionLot, error) {
	if auctionID == 0 || s.auctions == nil {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	return s.auctions.Get(ctx, auctionID, "admin", domain.RoleAdmin)
}

func (s *AdminService) LiveSessionByID(ctx context.Context, sessionID uint64) (domain.LiveSession, error) {
	if sessionID == 0 || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	return s.sessions.Get(ctx, sessionID)
}

func (s *AdminService) UserByID(userID string) (domain.SafeUser, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" || s.users == nil {
		return domain.SafeUser{}, domain.ErrInvalidArgument
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, err
	}
	return user.Safe(), nil
}

func (s *AdminService) UpdateUserStatus(userID string, status domain.UserStatus) (domain.SafeUser, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" || (status != domain.UserStatusActive && status != domain.UserStatusDisabled) {
		return domain.SafeUser{}, domain.ErrInvalidArgument
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, err
	}
	user.Status = status
	if err := s.users.Update(&user); err != nil {
		return domain.SafeUser{}, err
	}
	return user.Safe(), nil
}

func (s *AdminService) AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error {
	return s.risk.AddBlacklist(ctx, userID, reason, actorID, expiresAt)
}

func (s *AdminService) RemoveBlacklist(ctx context.Context, userID string) error {
	return s.risk.RemoveBlacklist(ctx, userID)
}

func (s *AdminService) ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error) {
	return s.risk.ListBlacklist(ctx, limit, offset)
}

func (s *AdminService) IsBlacklisted(ctx context.Context, userID string) (bool, error) {
	if s.risk == nil {
		return false, nil
	}
	return s.risk.IsBlacklisted(ctx, userID)
}

func (s *AdminService) BlacklistStrategyConfig(ctx context.Context) (domain.BlacklistStrategyConfig, error) {
	return ReadBlacklistStrategyConfig(ctx, s.configs)
}

func (s *AdminService) UpdateBlacklistStrategyConfig(ctx context.Context, cfg domain.BlacklistStrategyConfig, actorID string) (domain.BlacklistStrategyConfig, error) {
	return UpsertBlacklistStrategyConfig(ctx, s.configs, cfg, actorID)
}

func (s *AdminService) FeatureFlag(ctx context.Context, key string) (domain.FeatureFlag, error) {
	if s.flags == nil {
		return domain.FeatureFlag{}, domain.ErrInvalidState
	}
	return s.flags.Get(ctx, key)
}

func (s *AdminService) UpdateFeatureFlag(ctx context.Context, flag domain.FeatureFlag, actorID string) (domain.FeatureFlag, error) {
	if s.flags == nil {
		return domain.FeatureFlag{}, domain.ErrInvalidState
	}
	return s.flags.Update(ctx, flag, actorID)
}

func (s *AdminService) ListOrders(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error) {
	return s.orders.List(ctx, filter, "admin", domain.RoleAdmin)
}

func (s *AdminService) ListAuditLogs(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error) {
	return s.audits.List(ctx, filter)
}

func (s *AdminService) ListRiskEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error) {
	return s.risk.ListEvents(ctx, filter)
}

func (s *AdminService) HandleRiskEvent(ctx context.Context, eventID uint64, status domain.RiskEventStatus, actorID string) (domain.RiskEvent, error) {
	return s.risk.HandleEvent(ctx, eventID, status, actorID)
}

func (s *AdminService) DashboardMetrics(ctx context.Context, startTime, endTime *time.Time, bucket string) (domain.AdminDashboardMetrics, error) {
	if s.dashboard == nil {
		return domain.AdminDashboardMetrics{}, domain.ErrInvalidState
	}
	filter, err := normalizeDashboardMetricsFilter(startTime, endTime, bucket)
	if err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	return s.dashboard.DashboardMetrics(ctx, filter)
}

type FeatureFlagService struct {
	repo     adminports.ConfigRepository
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

func NewFeatureFlagService(repo adminports.ConfigRepository, bus ConfigInvalidationBus) *FeatureFlagService {
	return &FeatureFlagService{repo: repo, bus: bus, cacheTTL: 30 * time.Second, now: time.Now, cache: make(map[string]featureFlagCacheEntry)}
}

func (s *FeatureFlagService) SetNowFunc(now func() time.Time) {
	if s == nil {
		return
	}
	if now == nil {
		now = time.Now
	}
	s.mu.Lock()
	s.now = now
	s.mu.Unlock()
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
	if s == nil || s.repo == nil {
		return domain.FeatureFlag{}, domain.ErrInvalidState
	}
	key = normalizeFeatureFlagKey(key)
	if key == "" {
		return domain.FeatureFlag{}, domain.ErrInvalidArgument
	}
	now := s.nowFunc()().UTC()
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
	if s == nil || s.repo == nil {
		return domain.FeatureFlag{}, domain.ErrInvalidState
	}
	flag.Key = normalizeFeatureFlagKey(flag.Key)
	if flag.Key == "" || flag.RolloutPercentage < 0 || flag.RolloutPercentage > 100 {
		return domain.FeatureFlag{}, domain.ErrInvalidArgument
	}
	flag.Allowlist = normalizeAllowlist(flag.Allowlist)
	flag.UpdatedBy = strings.TrimSpace(actorID)
	flag.UpdatedAt = s.nowFunc()().UTC()
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
		_ = s.bus.Publish(ctx, ConfigInvalidateChannel, flag.Key)
	}
	return cloneFeatureFlag(flag), nil
}

func (s *FeatureFlagService) Invalidate(key string) {
	if s == nil {
		return
	}
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
		sub, err := s.bus.Subscribe(ctx, ConfigInvalidateChannel)
		if err != nil {
			return
		}
		defer sub.Close()
		ch := sub.Channel()
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

func (s *FeatureFlagService) nowFunc() func() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.now == nil {
		return time.Now
	}
	return s.now
}

func ReadBlacklistStrategyConfig(ctx context.Context, configs adminports.ConfigRepository) (domain.BlacklistStrategyConfig, error) {
	return adminports.ReadBlacklistStrategyConfig(ctx, configs)
}

func UpsertBlacklistStrategyConfig(ctx context.Context, configs adminports.ConfigRepository, cfg domain.BlacklistStrategyConfig, actorID string) (domain.BlacklistStrategyConfig, error) {
	return adminports.UpsertBlacklistStrategyConfig(ctx, configs, cfg, actorID)
}

func BlacklistExpiresAt(cfg domain.BlacklistStrategyConfig, now time.Time) *time.Time {
	return adminports.BlacklistExpiresAt(cfg, now)
}

func normalizeDashboardMetricsFilter(startTime, endTime *time.Time, bucket string) (domain.AdminDashboardMetricsFilter, error) {
	const (
		defaultRange = 24 * time.Hour
		maxRange     = 90 * 24 * time.Hour
	)
	now := time.Now().UTC()
	end := now
	if endTime != nil {
		end = endTime.UTC()
	}
	start := end.Add(-defaultRange)
	if startTime != nil {
		start = startTime.UTC()
	}
	if !start.Before(end) {
		return domain.AdminDashboardMetricsFilter{}, domain.ErrInvalidArgument
	}
	if end.Sub(start) > maxRange {
		return domain.AdminDashboardMetricsFilter{}, domain.ErrInvalidArgument
	}
	bucket = strings.ToLower(strings.TrimSpace(bucket))
	switch bucket {
	case "":
		if end.Sub(start) <= 72*time.Hour {
			bucket = "hour"
		} else {
			bucket = "day"
		}
	case "hour", "day":
	default:
		return domain.AdminDashboardMetricsFilter{}, domain.ErrInvalidArgument
	}
	return domain.AdminDashboardMetricsFilter{StartTime: start, EndTime: end, Bucket: bucket}, nil
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

var _ adminports.FeatureFlagManager = (*FeatureFlagService)(nil)
var _ AdminUseCase = (*AdminService)(nil)
