package repository

import (
	"context"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

type MemoryConfigRepository struct {
	mu    sync.RWMutex
	items map[string]domain.ConfigItem
}

func NewMemoryConfigRepository() *MemoryConfigRepository {
	return &MemoryConfigRepository{items: make(map[string]domain.ConfigItem)}
}

func (r *MemoryConfigRepository) FindByKey(ctx context.Context, key string) (domain.ConfigItem, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.items[key]
	if !ok {
		return domain.ConfigItem{}, domain.ErrNotFound
	}
	item.Value = append([]byte(nil), item.Value...)
	return item, nil
}

func (r *MemoryConfigRepository) Upsert(ctx context.Context, item *domain.ConfigItem) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = time.Now().UTC()
	}
	clone := *item
	clone.Value = append([]byte(nil), item.Value...)
	r.items[item.Key] = clone
	return nil
}

var _ ConfigRepository = (*MemoryConfigRepository)(nil)
