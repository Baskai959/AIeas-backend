package ports

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"aieas_backend/internal/domain"
)

const (
	BlacklistStrategyDescription        = "自动黑名单策略：频控、异常高价、保证金未满足"
	SystemBlacklistActorID       string = "0"
)

// UserRepository 是 admin 模块管理用户所需端口。
type UserRepository interface {
	List(filter domain.UserFilter) ([]domain.User, error)
	FindByID(id string) (domain.User, error)
	Update(user *domain.User) error
}

// AuctionAdminUseCase 是 admin 模块调用拍卖边界的端口。
type AuctionAdminUseCase interface {
	List(ctx context.Context, filter domain.AuctionFilter, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error)
	Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
	Cancel(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
	AdminUpdateStatus(ctx context.Context, id uint64, actorID string, status domain.AuctionStatus) (domain.AuctionLot, error)
}

// HammerUseCase 是 admin 模块调用落槌能力的端口。
type HammerUseCase interface {
	Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error)
}

// OrderQueryUseCase 是 admin 模块查询订单所需端口。
type OrderQueryUseCase interface {
	List(ctx context.Context, filter domain.OrderFilter, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error)
}

// RiskUseCase 是 admin 模块管理风险/黑名单所需端口。
type RiskUseCase interface {
	AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error
	RemoveBlacklist(ctx context.Context, userID string) error
	ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error)
	IsBlacklisted(ctx context.Context, userID string) (bool, error)
	ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error)
	HandleEvent(ctx context.Context, eventID uint64, status domain.RiskEventStatus, actorID string) (domain.RiskEvent, error)
}

// AuditRepository 是 admin 模块审计查询端口。
type AuditRepository interface {
	Create(ctx context.Context, log *domain.AuditLog) error
	List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error)
}

// DashboardRepository 是 admin/dashboard 统计查询端口。
type DashboardRepository interface {
	DashboardMetrics(ctx context.Context, filter domain.AdminDashboardMetricsFilter) (domain.AdminDashboardMetrics, error)
}

// LiveSessionRepository 是 admin 查询直播场次详情所需端口。
type LiveSessionRepository interface {
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
}

// ConfigRepository 是 admin/config 支撑配置读写的端口。
type ConfigRepository interface {
	FindByKey(ctx context.Context, key string) (domain.ConfigItem, error)
	Upsert(ctx context.Context, item *domain.ConfigItem) error
}

// FeatureFlagManager 是 admin/feature_flag 读写能力端口。
type FeatureFlagManager interface {
	Get(ctx context.Context, key string) (domain.FeatureFlag, error)
	Update(ctx context.Context, flag domain.FeatureFlag, actorID string) (domain.FeatureFlag, error)
}

func ReadBlacklistStrategyConfig(ctx context.Context, configs ConfigRepository) (domain.BlacklistStrategyConfig, error) {
	cfg := domain.DefaultBlacklistStrategyConfig()
	if configs == nil {
		return cfg, nil
	}
	item, err := configs.FindByKey(ctx, domain.ConfigKeyBlacklistStrategy)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return cfg, nil
		}
		return domain.BlacklistStrategyConfig{}, err
	}
	if len(item.Value) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(item.Value, &cfg); err != nil {
		return domain.BlacklistStrategyConfig{}, domain.ErrInvalidArgument
	}
	return domain.NormalizeBlacklistStrategyConfig(cfg)
}

func UpsertBlacklistStrategyConfig(ctx context.Context, configs ConfigRepository, cfg domain.BlacklistStrategyConfig, actorID string) (domain.BlacklistStrategyConfig, error) {
	if configs == nil {
		return domain.BlacklistStrategyConfig{}, domain.ErrInvalidState
	}
	normalized, err := domain.NormalizeBlacklistStrategyConfig(cfg)
	if err != nil {
		return domain.BlacklistStrategyConfig{}, err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return domain.BlacklistStrategyConfig{}, err
	}
	item := domain.ConfigItem{
		Key:         domain.ConfigKeyBlacklistStrategy,
		Value:       raw,
		Description: BlacklistStrategyDescription,
		UpdatedBy:   actorID,
		UpdatedAt:   time.Now().UTC(),
	}
	if err := configs.Upsert(ctx, &item); err != nil {
		return domain.BlacklistStrategyConfig{}, err
	}
	return normalized, nil
}

func BlacklistExpiresAt(cfg domain.BlacklistStrategyConfig, now time.Time) *time.Time {
	if cfg.BlacklistDurationSeconds <= 0 {
		return nil
	}
	expiresAt := now.Add(time.Duration(cfg.BlacklistDurationSeconds) * time.Second)
	return &expiresAt
}
