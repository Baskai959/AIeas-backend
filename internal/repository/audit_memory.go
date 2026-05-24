package repository

import (
	"context"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"
)

type MemoryAuditRepository struct {
	mu     sync.RWMutex
	nextID uint64
	logs   []domain.AuditLog
}

func NewMemoryAuditRepository() *MemoryAuditRepository {
	return &MemoryAuditRepository{nextID: 1}
}

func (r *MemoryAuditRepository) Create(ctx context.Context, log *domain.AuditLog) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if log.ID == 0 {
		log.ID = r.nextID
		r.nextID++
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = time.Now().UTC()
	}
	clone := *log
	clone.Payload = append([]byte(nil), log.Payload...)
	r.logs = append(r.logs, clone)
	return nil
}

func (r *MemoryAuditRepository) Logs() []domain.AuditLog {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.AuditLog, 0, len(r.logs))
	for _, log := range r.logs {
		log.Payload = append([]byte(nil), log.Payload...)
		out = append(out, log)
	}
	return out
}

func (r *MemoryAuditRepository) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error) {
	_ = ctx
	logs := r.Logs()
	filtered := make([]domain.AuditLog, 0, len(logs))
	for _, log := range logs {
		if filter.OperatorID != "" && log.OperatorID != filter.OperatorID {
			continue
		}
		if filter.Action != "" && log.Action != filter.Action {
			continue
		}
		if filter.StartTime != nil && log.CreatedAt.Before(*filter.StartTime) {
			continue
		}
		if filter.EndTime != nil && log.CreatedAt.After(*filter.EndTime) {
			continue
		}
		filtered = append(filtered, log)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID > filtered[j].ID })
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset >= len(filtered) {
		return []domain.AuditLog{}, nil
	}
	end := filter.Offset + filter.Limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[filter.Offset:end], nil
}
