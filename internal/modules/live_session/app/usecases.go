package app

import (
	"context"

	"aieas_backend/internal/domain"
)

// LiveSessionCommandUseCase 暴露直播场次写用例边界。
type LiveSessionCommandUseCase interface {
	Create(ctx context.Context, in CreateLiveSessionInput) (domain.LiveSession, error)
	Update(ctx context.Context, id uint64, in UpdateLiveSessionInput) (domain.LiveSession, error)
	Start(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error)
	End(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error)
	MountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
	UnmountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) error
	UnmountAuctionWithOptions(ctx context.Context, in UnmountLiveSessionAuctionInput) error
	ActivateAuctionWithOptions(ctx context.Context, in ActivateLiveSessionAuctionInput) (domain.AuctionLot, error)
	DeactivateAuction(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error)
	DeactivateAuctionWithOptions(ctx context.Context, in DeactivateLiveSessionAuctionInput) (domain.LiveSession, error)
	UpdateAgentHookConfig(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role, enabled bool) (LiveAgentHookConfig, error)
}

// LiveSessionQueryUseCase 暴露直播场次读用例边界。
type LiveSessionQueryUseCase interface {
	ListVisibleFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error)
	ListByMerchantFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error)
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
	ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error)
	ListAuctionBids(ctx context.Context, sessionID, auctionID uint64, limit int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error)
	ListBids(ctx context.Context, sessionID uint64, limit int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error)
	ListOrders(ctx context.Context, sessionID uint64, limit, offset int, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error)
	Stats(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (LiveSessionStats, error)
	AgentHookConfig(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (LiveAgentHookConfig, error)
	AIAssistantSwitchSnapshot(ctx context.Context, sessionID uint64) (AIAssistantSwitchSnapshot, error)
}

// LiveSessionUseCase 汇总直播场次读写用例边界。
type LiveSessionUseCase interface {
	LiveSessionCommandUseCase
	LiveSessionQueryUseCase
}

// WSLiveSessionLookupUseCase 是 WS 场次入口查找当前活跃拍品的 app 边界。
type WSLiveSessionLookupUseCase interface {
	ActiveAuctionAndSession(ctx context.Context, sessionID uint64) (uint64, uint64, error)
}
