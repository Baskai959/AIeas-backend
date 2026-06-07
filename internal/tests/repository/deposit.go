package repository

import (
	"context"

	"aieas_backend/internal/domain"
	depositrepo "aieas_backend/internal/modules/deposit/repository"

	"gorm.io/gorm"
)

type DepositRepository interface {
	Create(ctx context.Context, deposit *domain.DepositLedger) error
	FindByAuctionUser(ctx context.Context, auctionID uint64, userID string) (domain.DepositLedger, error)
	ListByAuction(ctx context.Context, auctionID uint64) ([]domain.DepositLedger, error)
	ListByUser(ctx context.Context, userID string, limit, offset int) ([]domain.DepositLedger, error)
	Update(ctx context.Context, deposit *domain.DepositLedger) error
}

type MySQLDepositRepository = depositrepo.MySQLDepositRepository
type MemoryDepositRepository = depositrepo.MemoryDepositRepository

func NewMySQLDepositRepository(db *gorm.DB) *MySQLDepositRepository {
	return depositrepo.NewMySQLDepositRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryDepositRepository() *MemoryDepositRepository {
	return depositrepo.NewMemoryDepositRepository()
}
