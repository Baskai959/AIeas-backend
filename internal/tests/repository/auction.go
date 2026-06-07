package repository

import (
	"context"

	auctionports "aieas_backend/internal/modules/auction/ports"
	auctionrepo "aieas_backend/internal/modules/auction/repository"

	"gorm.io/gorm"
)

type AuctionRepository = auctionports.AuctionRepository

type MySQLAuctionRepository = auctionrepo.MySQLAuctionRepository

func NewMySQLAuctionRepository(db *gorm.DB) *MySQLAuctionRepository {
	return auctionrepo.NewMySQLAuctionRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}
