package ports

import (
	"context"

	"aieas_backend/internal/domain"
)

// AuctionRepository 是商城模块搜索/读取拍品所需的持久化端口。
type AuctionRepository interface {
	Search(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error)
	FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error)
}

// LiveSessionRepository 是商城模块读取商家直播场次所需端口。
type LiveSessionRepository interface {
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
	GetActiveByMerchantID(ctx context.Context, merchantID string) (domain.LiveSession, error)
	List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error)
}

// DepositRepository 是商城模块读取参与记录所需端口。
type DepositRepository interface {
	ListByAuction(ctx context.Context, auctionID uint64) ([]domain.DepositLedger, error)
	ListByUser(ctx context.Context, userID string, limit, offset int) ([]domain.DepositLedger, error)
}

// OrderRepository 是商城模块读取参与订单所需端口。
type OrderRepository interface {
	FindByID(ctx context.Context, id uint64) (domain.OrderDeal, error)
	FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error)
}

// UserRepository 是商城模块读取商家资料所需端口。
type UserRepository interface {
	List(filter domain.UserFilter) ([]domain.User, error)
	FindByID(id string) (domain.User, error)
}

// AuctionRealtimeStore 是商城模块读取实时拍卖状态的端口。
type AuctionRealtimeStore interface {
	GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error)
}

// OnlineCounter 是商城模块读取在线人数的端口。
type OnlineCounter interface {
	OnlineCount(auctionID uint64) int
}

// LiveSessionOnlineCounter 是支持直播场次维度在线人数的可选端口。
type LiveSessionOnlineCounter interface {
	LiveSessionOnlineCount(liveSessionID uint64) int
}
