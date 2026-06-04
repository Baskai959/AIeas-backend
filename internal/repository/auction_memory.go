package repository

import (
	"context"
	"sort"
	"strings"
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
		if filter.Category != "" && auction.Category != filter.Category {
			continue
		}
		if filter.Keyword != "" {
			keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
			haystack := strings.ToLower(auction.Title + " " + auction.Description + " " + auction.Brand)
			if !strings.Contains(haystack, keyword) {
				continue
			}
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

func (r *MemoryAuctionRepository) Search(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	statusSet := make(map[domain.AuctionStatus]struct{}, len(filter.VisibleStatuses))
	for _, status := range filter.VisibleStatuses {
		if status.Valid() {
			statusSet[status] = struct{}{}
		}
	}
	categorySet := make(map[string]struct{})
	for _, category := range normalizedCategoryValues(filter.CategoryID, filter.CategoryValues) {
		categorySet[category] = struct{}{}
	}
	keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
	auctions := make([]domain.AuctionLot, 0, len(r.auctions))
	for _, auction := range r.auctions {
		if len(statusSet) > 0 {
			if _, ok := statusSet[auction.Status]; !ok {
				continue
			}
		}
		if filter.Status.Valid() && auction.Status != filter.Status {
			continue
		}
		if filter.MerchantID != "" && auction.SellerID != filter.MerchantID {
			continue
		}
		if len(categorySet) > 0 {
			if _, ok := categorySet[auction.Category]; !ok {
				continue
			}
		}
		if keyword != "" {
			haystack := strings.ToLower(auction.Title + " " + auction.Description + " " + auction.Brand)
			if !strings.Contains(haystack, keyword) {
				continue
			}
		}
		auctions = append(auctions, cloneAuction(auction))
	}
	sortAuctionsForSearch(auctions, filter.Sort)
	total := int64(len(auctions))
	return paginateAuctions(auctions, filter.Limit, filter.Offset), total, nil
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
	auction.ImageURLs = append([]string(nil), auction.ImageURLs...)
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

func sortAuctionsForSearch(auctions []domain.AuctionLot, sortBy string) {
	switch strings.TrimSpace(sortBy) {
	case "priceAsc":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartPrice == auctions[j].StartPrice {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			return auctions[i].StartPrice < auctions[j].StartPrice
		})
	case "priceDesc":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartPrice == auctions[j].StartPrice {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			return auctions[i].StartPrice > auctions[j].StartPrice
		})
	case "endingSoon":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].EndTime.Equal(auctions[j].EndTime) {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			if auctions[i].EndTime.IsZero() {
				return false
			}
			if auctions[j].EndTime.IsZero() {
				return true
			}
			return auctions[i].EndTime.Before(auctions[j].EndTime)
		})
	case "startTimeAsc":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartTime.Equal(auctions[j].StartTime) {
				return auctions[i].AuctionID < auctions[j].AuctionID
			}
			if auctions[i].StartTime.IsZero() {
				return false
			}
			if auctions[j].StartTime.IsZero() {
				return true
			}
			return auctions[i].StartTime.Before(auctions[j].StartTime)
		})
	case "startTimeDesc", "latest", "newest":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartTime.Equal(auctions[j].StartTime) {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			return auctions[i].StartTime.After(auctions[j].StartTime)
		})
	default:
		sort.Slice(auctions, func(i, j int) bool { return auctions[i].AuctionID > auctions[j].AuctionID })
	}
}
