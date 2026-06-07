package app

import (
	"context"

	"aieas_backend/internal/domain"
)

// OrderCommandUseCase 暴露订单写用例边界。
type OrderCommandUseCase interface {
	Pay(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error)
	Ship(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error)
	Receive(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error)
}

// OrderQueryUseCase 暴露订单读用例边界。
type OrderQueryUseCase interface {
	List(ctx context.Context, filter domain.OrderFilter, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error)
	Mine(ctx context.Context, actorID string, actorRole domain.Role, filter domain.OrderFilter) ([]domain.OrderDeal, error)
	Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error)
}

// OrderUseCase 汇总订单读写用例边界。
type OrderUseCase interface {
	OrderCommandUseCase
	OrderQueryUseCase
}
