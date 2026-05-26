package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

// MemoryLiveSessionRepository 是 live_session 的内存实现，用于单测与 NewServerWithDependencies 兜底。
type MemoryLiveSessionRepository struct {
	mu       sync.RWMutex
	nextID   uint64
	sessions map[uint64]domain.LiveSession
}

func NewMemoryLiveSessionRepository() *MemoryLiveSessionRepository {
	return &MemoryLiveSessionRepository{nextID: 70001, sessions: make(map[uint64]domain.LiveSession)}
}

func (r *MemoryLiveSessionRepository) Create(ctx context.Context, session *domain.LiveSession) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if session.ID == 0 {
		session.ID = r.nextID
		r.nextID++
	} else if session.ID >= r.nextID {
		r.nextID = session.ID + 1
	}
	now := time.Now().UTC()
	if session.CreatedAt.IsZero() {
		session.CreatedAt = now
	}
	session.UpdatedAt = now
	r.sessions[session.ID] = *session
	return nil
}

func (r *MemoryLiveSessionRepository) Get(ctx context.Context, id uint64) (domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[id]
	if !ok {
		return domain.LiveSession{}, domain.ErrNotFound
	}
	return session, nil
}

func (r *MemoryLiveSessionRepository) GetActiveByRoomID(ctx context.Context, roomID uint64) (domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	var (
		latest    domain.LiveSession
		latestSet bool
	)
	for _, session := range r.sessions {
		if session.LiveRoomID != roomID || session.Status != domain.LiveSessionStatusLive {
			continue
		}
		if !latestSet || session.ID > latest.ID {
			latest = session
			latestSet = true
		}
	}
	if !latestSet {
		return domain.LiveSession{}, domain.ErrNotFound
	}
	return latest, nil
}

func (r *MemoryLiveSessionRepository) Update(ctx context.Context, session *domain.LiveSession) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[session.ID]; !ok {
		return domain.ErrNotFound
	}
	session.UpdatedAt = time.Now().UTC()
	r.sessions[session.ID] = *session
	return nil
}

func (r *MemoryLiveSessionRepository) List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint64, 0, len(r.sessions))
	for id := range r.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	sessions := make([]domain.LiveSession, 0, len(ids))
	for _, id := range ids {
		session := r.sessions[id]
		if filter.LiveRoomID != 0 && session.LiveRoomID != filter.LiveRoomID {
			continue
		}
		if filter.MerchantID != "" && session.MerchantID != filter.MerchantID {
			continue
		}
		if filter.Status.Valid() && session.Status != filter.Status {
			continue
		}
		if filter.OpenedFrom != nil && session.OpenedAt.Before(*filter.OpenedFrom) {
			continue
		}
		if filter.OpenedTo != nil && session.OpenedAt.After(*filter.OpenedTo) {
			continue
		}
		sessions = append(sessions, session)
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset >= len(sessions) {
		return []domain.LiveSession{}, nil
	}
	end := filter.Offset + filter.Limit
	if end > len(sessions) {
		end = len(sessions)
	}
	return sessions[filter.Offset:end], nil
}
