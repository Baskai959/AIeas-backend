package repository

import (
	"context"

	orderports "aieas_backend/internal/modules/order/ports"
	orderrepo "aieas_backend/internal/modules/order/repository"

	"gorm.io/gorm"
)

type OrderRepository = orderports.OrderRepository

type MySQLOrderRepository = orderrepo.MySQLOrderRepository
type MemoryOrderRepository = orderrepo.MemoryOrderRepository

func NewMySQLOrderRepository(db *gorm.DB) *MySQLOrderRepository {
	return orderrepo.NewMySQLOrderRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryOrderRepository() *MemoryOrderRepository {
	return orderrepo.NewMemoryOrderRepository()
}
