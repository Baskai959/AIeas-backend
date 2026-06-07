package ports

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
)

// LiveSessionRepository 是直播场次用例所需的最小持久化端口。
type LiveSessionRepository interface {
	Create(ctx context.Context, session *domain.LiveSession) error
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
	GetActiveByMerchantID(ctx context.Context, merchantID string) (domain.LiveSession, error)
	Update(ctx context.Context, session *domain.LiveSession) error
	List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error)
}

// LiveSessionRealtimeReader 是直播场次实时只读端口，供 WS 网关等只读路径使用。
type LiveSessionRealtimeReader interface {
	ActiveAuction(ctx context.Context, sessionID uint64) (uint64, bool, error)
}

// LiveSessionRealtimeWriter 是直播场次实时状态写入端口。
type LiveSessionRealtimeWriter interface {
	IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error
	BumpViewerPeak(ctx context.Context, sessionID uint64, value int) (int, error)
	SetActiveAuction(ctx context.Context, sessionID uint64, auctionID uint64) error
	ClearActiveAuction(ctx context.Context, sessionID uint64) error
	Reset(ctx context.Context, sessionID uint64) error
}

// LiveSessionRealtimeStore 汇总直播场次实时读写端口。
type LiveSessionRealtimeStore interface {
	LiveSessionRealtimeReader
	LiveSessionRealtimeWriter
	LoadCounters(ctx context.Context, sessionID uint64) (domain.LiveSessionCounters, int, error)
}

// LiveSessionLock 是直播场次活跃拍品互斥端口。
type LiveSessionLock interface {
	Acquire(ctx context.Context, sessionID uint64, auctionID uint64, ttl time.Duration) (acquired bool, currentAuctionID uint64, err error)
	Release(ctx context.Context, sessionID uint64, auctionID uint64) error
	Current(ctx context.Context, sessionID uint64) (uint64, error)
}

// AuctionLotRepository 是直播场次挂载/下架拍品所需的拍品读写端口。
type AuctionLotRepository interface {
	FindByID(ctx context.Context, auctionID uint64) (domain.AuctionLot, error)
	List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error)
	Update(ctx context.Context, auction *domain.AuctionLot) error
}

// AuctionUseCase 表达直播场次激活拍品时调用 auction 模块开拍能力的边界。
type AuctionUseCase interface {
	StartWithTiming(ctx context.Context, auctionID uint64, actorID string, actorRole domain.Role, startTime, endTime time.Time) (domain.AuctionLot, error)
}

// AuctionRealtimeReader 是直播场次统计读取当前拍品状态所需的拍卖实时读端口。
type AuctionRealtimeReader interface {
	GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error)
}

// BidReader 是直播场次查询出价列表、统计出价数量所需端口。
type BidReader interface {
	ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error)
	ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error)
	CountByAuction(ctx context.Context, auctionID uint64) (int, error)
}

// BidRoundReader 是支持按开拍轮次过滤出价的可选端口。
type BidRoundReader interface {
	ListByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64, limit int) ([]domain.BidRecord, error)
	CountByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64) (int, error)
}

// OrderReader 是直播场次查询订单列表所需端口。
type OrderReader interface {
	List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error)
}

// UserReader 是直播场次补充买家/赢家昵称所需端口。
type UserReader interface {
	FindByID(id string) (domain.User, error)
}

// OnlineCounter 是直播场次统计在线人数所需端口。
type OnlineCounter interface {
	OnlineCount(auctionID uint64) int
	LiveSessionOnlineCount(liveSessionID uint64) int
}

// 过渡期别名：旧 repository/transport 代码仍使用短名；后续 adapter 完全迁入模块后可删除。
type RealtimeReader = LiveSessionRealtimeReader
type RealtimeWriter = LiveSessionRealtimeWriter
type RealtimeStore = LiveSessionRealtimeStore
type SessionLock = LiveSessionLock
