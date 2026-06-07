package app

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
)

// AdminUseCase 暴露 admin/support 边界：admin、config、feature_flag、audit、dashboard。
type AdminUseCase interface {
	ListAuctions(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error)
	AuditAuction(ctx context.Context, auctionID uint64, approved bool, actorID string) (domain.AuctionLot, error)
	CancelAuction(ctx context.Context, auctionID uint64, actorID string) (domain.AuctionLot, error)
	CloseAuction(ctx context.Context, auctionID uint64, actorID, requestID string) (domain.HammerResult, *domain.OrderDeal, error)
	ListUsers(filter domain.UserFilter) ([]domain.SafeUser, error)
	AuctionByID(ctx context.Context, auctionID uint64) (domain.AuctionLot, error)
	LiveSessionByID(ctx context.Context, sessionID uint64) (domain.LiveSession, error)
	UserByID(userID string) (domain.SafeUser, error)
	UpdateUserStatus(userID string, status domain.UserStatus) (domain.SafeUser, error)
	AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error
	RemoveBlacklist(ctx context.Context, userID string) error
	ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error)
	IsBlacklisted(ctx context.Context, userID string) (bool, error)
	BlacklistStrategyConfig(ctx context.Context) (domain.BlacklistStrategyConfig, error)
	UpdateBlacklistStrategyConfig(ctx context.Context, cfg domain.BlacklistStrategyConfig, actorID string) (domain.BlacklistStrategyConfig, error)
	FeatureFlag(ctx context.Context, key string) (domain.FeatureFlag, error)
	UpdateFeatureFlag(ctx context.Context, flag domain.FeatureFlag, actorID string) (domain.FeatureFlag, error)
	ListOrders(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error)
	ListAuditLogs(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error)
	ListRiskEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error)
	HandleRiskEvent(ctx context.Context, eventID uint64, status domain.RiskEventStatus, actorID string) (domain.RiskEvent, error)
	DashboardMetrics(ctx context.Context, startTime, endTime *time.Time, bucket string) (domain.AdminDashboardMetrics, error)
}
