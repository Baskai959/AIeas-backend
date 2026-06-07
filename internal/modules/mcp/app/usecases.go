package app

import (
	"context"

	"aieas_backend/internal/domain"
)

type LiveControlContext = MCPLiveControlContext
type LiveLotOperationInput = MCPLiveLotOperationInput
type LiveLotOperationResult = MCPLiveLotOperationResult
type LiveVoiceBroadcastInput = MCPLiveVoiceBroadcastInput
type LiveVoiceBroadcastResult = MCPLiveVoiceBroadcastResult

// MCPReadUseCase 暴露 MCP read transport 依赖的应用边界。
type MCPReadUseCase interface {
	CurrentTime(ctx context.Context, actor MCPActor) (CurrentTimeResult, error)
	ReadUser(ctx context.Context, userID string, actor MCPActor) (domain.SafeUser, error)
	ListUsers(ctx context.Context, filter domain.UserFilter, actor MCPActor) ([]domain.SafeUser, error)
	ReadMerchant(ctx context.Context, merchantID string, actor MCPActor) (MerchantProfile, error)
	ReadAuctionLot(ctx context.Context, auctionID uint64, actor MCPActor) (domain.AuctionLot, error)
	ListAuctionLots(ctx context.Context, filter domain.AuctionFilter, actor MCPActor) ([]domain.AuctionLot, error)
	ReadAuctionState(ctx context.Context, auctionID uint64, actor MCPActor) (domain.AuctionState, error)
	ListLiveSessions(ctx context.Context, filter domain.LiveSessionFilter, actor MCPActor) ([]domain.LiveSession, error)
	ReadLiveSession(ctx context.Context, sessionID uint64, actor MCPActor) (domain.LiveSession, error)
	ListLiveSessionLots(ctx context.Context, sessionID uint64, actor MCPActor) ([]domain.AuctionLot, error)
	ListLiveSessionBids(ctx context.Context, sessionID uint64, sortBy string, limit, offset int, actor MCPActor) ([]domain.BidRecord, error)
	ListLiveSessionOrders(ctx context.Context, sessionID uint64, status domain.OrderStatus, payStatus domain.PayStatus, limit, offset int, actor MCPActor) ([]domain.OrderDeal, error)
	ReadLiveSessionSettlement(ctx context.Context, sessionID uint64, actor MCPActor) (LiveSessionSettlement, error)
	ReadOrder(ctx context.Context, orderID uint64, actor MCPActor) (domain.OrderDeal, error)
	ListOrders(ctx context.Context, filter domain.OrderFilter, actor MCPActor) ([]domain.OrderDeal, error)
	ListRiskEvents(ctx context.Context, filter domain.RiskEventFilter, actor MCPActor) ([]domain.RiskEvent, error)
	ListAuditLogs(ctx context.Context, filter domain.AuditFilter, actor MCPActor) ([]domain.AuditLog, error)
}

// MCPControlUseCase 暴露 MCP control transport 依赖的应用边界。
type MCPControlUseCase interface {
	CurrentTime(ctx context.Context, actor MCPActor) (CurrentTimeResult, error)
	ReadMerchantLiveControlContext(ctx context.Context, merchantID string, actor MCPActor) (MCPLiveControlContext, error)
	OperateLiveSessionLot(ctx context.Context, in MCPLiveLotOperationInput, actor MCPActor) (MCPLiveLotOperationResult, error)
	CreateLiveVoiceBroadcast(ctx context.Context, in MCPLiveVoiceBroadcastInput, actor MCPActor) (MCPLiveVoiceBroadcastResult, error)
}
