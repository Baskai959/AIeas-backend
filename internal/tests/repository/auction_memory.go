package repository

import auctionrepo "aieas_backend/internal/modules/auction/repository"

type MemoryAuctionRepository = auctionrepo.MemoryAuctionRepository

func NewMemoryAuctionRepository() *MemoryAuctionRepository {
	return auctionrepo.NewMemoryAuctionRepository()
}
