package repository

import (
	"context"
	"sort"
	"strings"
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

func (r *MemoryLiveSessionRepository) GetActiveByMerchantID(ctx context.Context, merchantID string) (domain.LiveSession, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	var (
		latest    domain.LiveSession
		latestSet bool
	)
	for _, session := range r.sessions {
		if session.MerchantID != merchantID || session.Status != domain.LiveSessionStatusLive {
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
	sessions := make([]domain.LiveSession, 0, len(r.sessions))
	keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
	for _, session := range r.sessions {
		if filter.MerchantID != "" && session.MerchantID != filter.MerchantID {
			continue
		}
		if filter.Status.Valid() && session.Status != filter.Status {
			continue
		}
		if keyword != "" && !liveSessionMatchesKeyword(session, keyword) {
			continue
		}
		if filter.OpenedFrom != nil && (session.OpenedAt == nil || session.OpenedAt.Before(*filter.OpenedFrom)) {
			continue
		}
		if filter.OpenedTo != nil && (session.OpenedAt == nil || session.OpenedAt.After(*filter.OpenedTo)) {
			continue
		}
		sessions = append(sessions, session)
	}
	sortLiveSessions(sessions, filter.Sort)
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

func liveSessionMatchesKeyword(session domain.LiveSession, keyword string) bool {
	return strings.Contains(strings.ToLower(session.Title), keyword) ||
		strings.Contains(strings.ToLower(session.Description), keyword) ||
		strings.Contains(strings.ToLower(session.MerchantID), keyword)
}

func sortLiveSessions(sessions []domain.LiveSession, sortBy string) {
	sort.SliceStable(sessions, func(i, j int) bool {
		left, right := sessions[i], sessions[j]
		switch strings.TrimSpace(sortBy) {
		case "oldest", "createdAtAsc":
			return left.ID < right.ID
		case "startTimeAsc", "scheduledStartAsc":
			return timeBeforePtr(left.ScheduledStartTime, right.ScheduledStartTime, true, left.ID, right.ID)
		case "startTimeDesc", "scheduledStartDesc":
			return timeAfterPtr(left.ScheduledStartTime, right.ScheduledStartTime, true, left.ID, right.ID)
		case "openedAtAsc":
			return timeBeforePtr(left.OpenedAt, right.OpenedAt, true, left.ID, right.ID)
		case "openedAtDesc":
			return timeAfterPtr(left.OpenedAt, right.OpenedAt, true, left.ID, right.ID)
		case "gmvDesc":
			if left.GMVCent != right.GMVCent {
				return left.GMVCent > right.GMVCent
			}
			return left.ID > right.ID
		case "viewerDesc", "viewerPeakDesc":
			if left.ViewerPeak != right.ViewerPeak {
				return left.ViewerPeak > right.ViewerPeak
			}
			return left.ID > right.ID
		case "latest", "newest", "createdAtDesc":
			fallthrough
		default:
			return left.ID > right.ID
		}
	})
}

func timeBeforePtr(left, right *time.Time, nullLast bool, leftID, rightID uint64) bool {
	if left == nil || right == nil {
		if left == nil && right == nil {
			return leftID < rightID
		}
		if nullLast {
			return left != nil
		}
		return left == nil
	}
	if !left.Equal(*right) {
		return left.Before(*right)
	}
	return leftID < rightID
}

func timeAfterPtr(left, right *time.Time, nullLast bool, leftID, rightID uint64) bool {
	if left == nil || right == nil {
		if left == nil && right == nil {
			return leftID > rightID
		}
		if nullLast {
			return right == nil
		}
		return left != nil
	}
	if !left.Equal(*right) {
		return left.After(*right)
	}
	return leftID > rightID
}
