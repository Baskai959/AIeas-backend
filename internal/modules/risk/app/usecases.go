package app

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
)

// RiskUseCase 暴露风控/黑名单管理边界。
type RiskUseCase interface {
	IsBlacklisted(ctx context.Context, userID string) (bool, error)
	AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error
	RemoveBlacklist(ctx context.Context, userID string) error
	ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error)
	ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error)
	HandleEvent(ctx context.Context, id uint64, status domain.RiskEventStatus, actorID string) (domain.RiskEvent, error)
}

// RiskControlUseCase 暴露风控开关读取边界。
type RiskControlUseCase interface {
	Enabled(ctx context.Context) bool
	Config(ctx context.Context) domain.RiskControlConfig
}
