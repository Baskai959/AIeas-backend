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
		if filter.LiveSessionID != 0 {
			if auction.LiveSessionID == nil || *auction.LiveSessionID != filter.LiveSessionID {
				continue
			}
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

// CloseWithVersion 内存层 CAS 实现：在 mu 内对比 version 与状态白名单，
// 等价 MySQL CAS 语义；冲突返回 domain.ErrOptimisticConflict，已是终态返回
// domain.ErrInvalidState；成功时回写 expectedVersion+1 到入参 auction.Version。
func (r *MemoryAuctionRepository) CloseWithVersion(ctx context.Context, auction *domain.AuctionLot, expectedVersion int64, allowedFromStatuses []domain.AuctionStatus) error {
	_ = ctx
	if auction == nil || auction.AuctionID == 0 {
		return domain.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.auctions[auction.AuctionID]
	if !ok {
		return domain.ErrNotFound
	}
	if current.Version != expectedVersion || !statusInList(current.Status, allowedFromStatuses) {
		if current.Status.Terminal() {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	now := time.Now().UTC()
	current.Status = auction.Status
	current.WinnerID = auction.WinnerID
	current.DealPrice = auction.DealPrice
	current.ClosedAt = auction.ClosedAt
	current.ClosedBy = auction.ClosedBy
	current.Version = expectedVersion + 1
	current.UpdatedAt = now
	r.auctions[auction.AuctionID] = cloneAuction(current)
	auction.Version = current.Version
	auction.UpdatedAt = now
	return nil
}

func statusInList(s domain.AuctionStatus, list []domain.AuctionStatus) bool {
	if len(list) == 0 {
		return true
	}
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
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
