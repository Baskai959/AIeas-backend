package ports

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
)

const (
	DefaultPayTimeout           = 20 * time.Minute
	DefaultTimeoutScanInterval  = 30 * time.Second
	DefaultTimeoutScanBatchSize = 100
)

// OrderRepository 是 order 用例所需的订单持久化端口。
type OrderRepository interface {
	CreateIfAbsentByAuction(ctx context.Context, order *domain.OrderDeal) (domain.OrderDeal, bool, error)
	FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error)
	FindByID(ctx context.Context, id uint64) (domain.OrderDeal, error)
	List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error)
	ListPayTimeoutCandidates(ctx context.Context, now time.Time, limit int) ([]domain.OrderDeal, error)
	Update(ctx context.Context, order *domain.OrderDeal) error
	UpdateStatusWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.OrderStatus) error
	UpdateFulfillmentWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.FulfillmentStatus) error
}

// UserReader 是 order 用例补充赢家昵称所需的用户只读端口。
type UserReader interface {
	FindByID(id string) (domain.User, error)
}

// TxManager 是 order 模块事务边界端口。
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
