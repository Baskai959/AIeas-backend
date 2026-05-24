package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

// MemoryLiveRoomRepository 提供内存实现，便于单元测试与 NewServerWithDependencies 兜底。
type MemoryLiveRoomRepository struct {
	mu     sync.RWMutex
	nextID uint64
	rooms  map[uint64]domain.LiveRoom
}

func NewMemoryLiveRoomRepository() *MemoryLiveRoomRepository {
	return &MemoryLiveRoomRepository{nextID: 90001, rooms: make(map[uint64]domain.LiveRoom)}
}

func (r *MemoryLiveRoomRepository) Create(ctx context.Context, room *domain.LiveRoom) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if room.ID == 0 {
		room.ID = r.nextID
		r.nextID++
	} else if room.ID >= r.nextID {
		r.nextID = room.ID + 1
	}
	now := time.Now().UTC()
	if room.CreatedAt.IsZero() {
		room.CreatedAt = now
	}
	room.UpdatedAt = now
	r.rooms[room.ID] = *room
	return nil
}

func (r *MemoryLiveRoomRepository) FindByID(ctx context.Context, id uint64) (domain.LiveRoom, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	room, ok := r.rooms[id]
	if !ok {
		return domain.LiveRoom{}, domain.ErrNotFound
	}
	return room, nil
}

func (r *MemoryLiveRoomRepository) FindByMerchantID(ctx context.Context, merchantID string) (domain.LiveRoom, error) {
	_ = ctx
	if merchantID == "" {
		return domain.LiveRoom{}, domain.ErrNotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, room := range r.rooms {
		if room.MerchantID == merchantID {
			return room, nil
		}
	}
	return domain.LiveRoom{}, domain.ErrNotFound
}

func (r *MemoryLiveRoomRepository) List(ctx context.Context, filter domain.LiveRoomFilter) ([]domain.LiveRoom, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint64, 0, len(r.rooms))
	for id := range r.rooms {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	rooms := make([]domain.LiveRoom, 0, len(ids))
	for _, id := range ids {
		room := r.rooms[id]
		if filter.MerchantID != "" && room.MerchantID != filter.MerchantID {
			continue
		}
		if filter.Status.Valid() && room.Status != filter.Status {
			continue
		}
		rooms = append(rooms, room)
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset >= len(rooms) {
		return []domain.LiveRoom{}, nil
	}
	end := filter.Offset + filter.Limit
	if end > len(rooms) {
		end = len(rooms)
	}
	return rooms[filter.Offset:end], nil
}

func (r *MemoryLiveRoomRepository) Update(ctx context.Context, room *domain.LiveRoom) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rooms[room.ID]; !ok {
		return domain.ErrNotFound
	}
	room.UpdatedAt = time.Now().UTC()
	r.rooms[room.ID] = *room
	return nil
}

func (r *MemoryLiveRoomRepository) Delete(ctx context.Context, id uint64) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rooms[id]; !ok {
		return domain.ErrNotFound
	}
	delete(r.rooms, id)
	return nil
}
