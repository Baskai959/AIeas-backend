package ports

import (
	"context"

	"aieas_backend/internal/domain"
)

// DepositRepository 是保证金模块所需的保证金持久化端口。
type DepositRepository interface {
	Create(ctx context.Context, deposit *domain.DepositLedger) error
	FindByAuctionUser(ctx context.Context, auctionID uint64, userID string) (domain.DepositLedger, error)
	Update(ctx context.Context, deposit *domain.DepositLedger) error
}

// AuctionRepository 是保证金模块校验拍品状态所需端口。
type AuctionRepository interface {
	FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error)
}

// AuctionRealtimeStore 是保证金模块同步报名态到实时层的端口。
type AuctionRealtimeStore interface {
	MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error
}

// BlacklistReader 是保证金模块进行黑名单校验所需风险读端口。
type BlacklistReader interface {
	IsBlacklisted(ctx context.Context, userID string) (bool, error)
}

// RiskControlUseCase 是保证金模块决定是否开启黑名单校验的控制端口。
type RiskControlUseCase interface {
	Enabled(ctx context.Context) bool
}

// TxManager 是保证金模块事务边界端口。
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
