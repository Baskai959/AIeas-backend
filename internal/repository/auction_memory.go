package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

type MemoryAuctionRepository struct {
	mu       sync.RWMutex
	nextID   uint64
	auctions map[uint64]domain.AuctionLot
}

func NewMemoryAuctionRepository() *MemoryAuctionRepository {
	return &MemoryAuctionRepository{nextID: 10001, auctions: make(map[uint64]domain.AuctionLot)}
}

func (r *MemoryAuctionRepository) Create(ctx context.Context, auction *domain.AuctionLot) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if auction.AuctionID == 0 {
		auction.AuctionID = r.nextID
		r.nextID++
	} else if auction.AuctionID >= r.nextID {
		r.nextID = auction.AuctionID + 1
	}
	now := time.Now().UTC()
	if auction.CreatedAt.IsZero() {
		auction.CreatedAt = now
	}
	auction.UpdatedAt = now
	r.auctions[auction.AuctionID] = cloneAuction(*auction)
	return nil
}

func (r *MemoryAuctionRepository) FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	auction, ok := r.auctions[id]
	if !ok {
		return domain.AuctionLot{}, domain.ErrNotFound
	}
	return cloneAuction(auction), nil
}

func (r *MemoryAuctionRepository) List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint64, 0, len(r.auctions))
	for id := range r.auctions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	auctions := make([]domain.AuctionLot, 0, len(ids))
	for _, id := range ids {
		auction := r.auctions[id]
		if filter.SellerID != "" && auction.SellerID != filter.SellerID {
			continue
		}
		if filter.Status.Valid() && auction.Status != filter.Status {
			continue
		}
		if filter.ItemID != 0 && auction.ItemID != filter.ItemID {
			continue
		}
		if filter.LiveRoomID != 0 && auction.LiveRoomID != filter.LiveRoomID {
			continue
		}
		auctions = append(auctions, cloneAuction(auction))
	}
	return paginateAuctions(auctions, filter.Limit, filter.Offset), nil
}

func (r *MemoryAuctionRepository) Update(ctx context.Context, auction *domain.AuctionLot) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.auctions[auction.AuctionID]; !ok {
		return domain.ErrNotFound
	}
	auction.UpdatedAt = time.Now().UTC()
	r.auctions[auction.AuctionID] = cloneAuction(*auction)
	return nil
}

func (r *MemoryAuctionRepository) Delete(ctx context.Context, id uint64) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.auctions[id]; !ok {
		return domain.ErrNotFound
	}
	delete(r.auctions, id)
	return nil
}

func cloneAuction(auction domain.AuctionLot) domain.AuctionLot {
	auction.IncrementRule = append([]byte(nil), auction.IncrementRule...)
	auction.RuleSnapshot = append([]byte(nil), auction.RuleSnapshot...)
	auction.LiveSessionID = cloneUint64Ptr(auction.LiveSessionID)
	return auction
}

func paginateAuctions(auctions []domain.AuctionLot, limit, offset int) []domain.AuctionLot {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset >= len(auctions) {
		return []domain.AuctionLot{}
	}
	end := offset + limit
	if end > len(auctions) {
		end = len(auctions)
	}
	return auctions[offset:end]
}
