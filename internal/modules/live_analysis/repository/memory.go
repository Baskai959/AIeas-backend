package repository

import (
	"context"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
)

type MemoryLiveAnalysisReportRepository struct {
	mu      sync.RWMutex
	reports map[string]domain.LiveAnalysisReport
}

func NewMemoryLiveAnalysisReportRepository() *MemoryLiveAnalysisReportRepository {
	return &MemoryLiveAnalysisReportRepository{reports: make(map[string]domain.LiveAnalysisReport)}
}

func (r *MemoryLiveAnalysisReportRepository) Create(ctx context.Context, report *domain.LiveAnalysisReport) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if report.TaskID == "" {
		return domain.ErrInvalidArgument
	}
	for _, existing := range r.reports {
		if report.LiveSessionID != 0 && existing.LiveSessionID == report.LiveSessionID && existing.TaskID != report.TaskID {
			return domain.ErrConflict
		}
	}
	now := time.Now().UTC()
	if report.CreatedAt.IsZero() {
		report.CreatedAt = now
	}
	report.UpdatedAt = now
	r.reports[report.TaskID] = *report
	return nil
}

func (r *MemoryLiveAnalysisReportRepository) FindByTaskID(ctx context.Context, taskID string) (domain.LiveAnalysisReport, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	report, ok := r.reports[taskID]
	if !ok {
		return domain.LiveAnalysisReport{}, domain.ErrNotFound
	}
	return report, nil
}

func (r *MemoryLiveAnalysisReportRepository) FindByLiveSessionID(ctx context.Context, liveSessionID uint64) (domain.LiveAnalysisReport, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	if liveSessionID == 0 {
		return domain.LiveAnalysisReport{}, domain.ErrNotFound
	}
	for _, report := range r.reports {
		if report.LiveSessionID == liveSessionID {
			return report, nil
		}
	}
	return domain.LiveAnalysisReport{}, domain.ErrNotFound
}

func (r *MemoryLiveAnalysisReportRepository) FindByAgentRequestID(ctx context.Context, requestID string) (domain.LiveAnalysisReport, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	if requestID == "" {
		return domain.LiveAnalysisReport{}, domain.ErrNotFound
	}
	for _, report := range r.reports {
		if report.AgentRequestID == requestID {
			return report, nil
		}
	}
	return domain.LiveAnalysisReport{}, domain.ErrNotFound
}

func (r *MemoryLiveAnalysisReportRepository) Update(ctx context.Context, report *domain.LiveAnalysisReport) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.reports[report.TaskID]; !ok {
		return domain.ErrNotFound
	}
	for _, existing := range r.reports {
		if report.LiveSessionID != 0 && existing.LiveSessionID == report.LiveSessionID && existing.TaskID != report.TaskID {
			return domain.ErrConflict
		}
	}
	report.UpdatedAt = time.Now().UTC()
	r.reports[report.TaskID] = *report
	return nil
}

var _ liveanalysisports.LiveAnalysisReportRepository = (*MemoryLiveAnalysisReportRepository)(nil)
