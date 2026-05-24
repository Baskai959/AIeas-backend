package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

type MemoryItemRepository struct {
	mu     sync.RWMutex
	nextID uint64
	items  map[uint64]domain.Item
}

func NewMemoryItemRepository() *MemoryItemRepository {
	return &MemoryItemRepository{nextID: 1, items: make(map[uint64]domain.Item)}
}

func (r *MemoryItemRepository) Create(ctx context.Context, item *domain.Item) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if item.ID == 0 {
		item.ID = r.nextID
		r.nextID++
	} else if item.ID >= r.nextID {
		r.nextID = item.ID + 1
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	r.items[item.ID] = cloneItem(*item)
	return nil
}

func (r *MemoryItemRepository) FindByID(ctx context.Context, id uint64) (domain.Item, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.items[id]
	if !ok {
		return domain.Item{}, domain.ErrNotFound
	}
	return cloneItem(item), nil
}

func (r *MemoryItemRepository) List(ctx context.Context, filter domain.ItemFilter) ([]domain.Item, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint64, 0, len(r.items))
	for id := range r.items {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	items := make([]domain.Item, 0, len(ids))
	for _, id := range ids {
		item := r.items[id]
		if filter.SellerID != "" && item.SellerID != filter.SellerID {
			continue
		}
		if filter.Status.Valid() && item.Status != filter.Status {
			continue
		}
		if filter.Category != "" && item.Category != filter.Category {
			continue
		}
		items = append(items, cloneItem(item))
	}
	return paginateItems(items, filter.Limit, filter.Offset), nil
}

func (r *MemoryItemRepository) Update(ctx context.Context, item *domain.Item) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[item.ID]; !ok {
		return domain.ErrNotFound
	}
	item.UpdatedAt = time.Now().UTC()
	r.items[item.ID] = cloneItem(*item)
	return nil
}

func (r *MemoryItemRepository) Delete(ctx context.Context, id uint64) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[id]; !ok {
		return domain.ErrNotFound
	}
	delete(r.items, id)
	return nil
}

func cloneItem(item domain.Item) domain.Item {
	item.Images = append([]byte(nil), item.Images...)
	return item
}

func paginateItems(items []domain.Item, limit, offset int) []domain.Item {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset >= len(items) {
		return []domain.Item{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}
