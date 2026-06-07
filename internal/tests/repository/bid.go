package repository

import (
	"context"

	auctionports "aieas_backend/internal/modules/auction/ports"
	auctionrepo "aieas_backend/internal/modules/auction/repository"

	"gorm.io/gorm"
)

type BidRepository = auctionports.BidRepository
type BidRoundRepository = auctionports.BidRoundRepository

type MySQLBidRepository = auctionrepo.MySQLBidRepository
type MemoryBidRepository = auctionrepo.MemoryBidRepository

func NewMySQLBidRepository(db *gorm.DB) *MySQLBidRepository {
	return auctionrepo.NewMySQLBidRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryBidRepository() *MemoryBidRepository {
	return auctionrepo.NewMemoryBidRepository()
}
