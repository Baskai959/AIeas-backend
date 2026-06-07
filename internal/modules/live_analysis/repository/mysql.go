package repository

import (
	"context"
	"errors"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"

	"gorm.io/gorm"
)

type ContextDBResolver func(ctx context.Context, db *gorm.DB) *gorm.DB

type MySQLLiveAnalysisReportRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLLiveAnalysisReportRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLLiveAnalysisReportRepository {
	return &MySQLLiveAnalysisReportRepository{db: db, resolveDB: resolver}
}

func (r *MySQLLiveAnalysisReportRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLLiveAnalysisReportRepository) Create(ctx context.Context, report *domain.LiveAnalysisReport) error {
	row := liveAnalysisReportRowFromDomain(*report)
	if err := r.dbFor(ctx).Table("live_analysis_report").Create(&row).Error; err != nil {
		return err
	}
	*report = row.toDomain()
	return nil
}

func (r *MySQLLiveAnalysisReportRepository) FindByTaskID(ctx context.Context, taskID string) (domain.LiveAnalysisReport, error) {
	var row liveAnalysisReportRow
	err := r.dbFor(ctx).Table("live_analysis_report").Where("task_id = ?", taskID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveAnalysisReport{}, domain.ErrNotFound
		}
		return domain.LiveAnalysisReport{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveAnalysisReportRepository) FindByLiveSessionID(ctx context.Context, liveSessionID uint64) (domain.LiveAnalysisReport, error) {
	var row liveAnalysisReportRow
	err := r.dbFor(ctx).Table("live_analysis_report").Where("live_session_id = ?", liveSessionID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveAnalysisReport{}, domain.ErrNotFound
		}
		return domain.LiveAnalysisReport{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveAnalysisReportRepository) FindByAgentRequestID(ctx context.Context, requestID string) (domain.LiveAnalysisReport, error) {
	var row liveAnalysisReportRow
	err := r.dbFor(ctx).Table("live_analysis_report").Where("agent_request_id = ?", requestID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveAnalysisReport{}, domain.ErrNotFound
		}
		return domain.LiveAnalysisReport{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveAnalysisReportRepository) Update(ctx context.Context, report *domain.LiveAnalysisReport) error {
	now := time.Now().UTC()
	updates := map[string]interface{}{
		"agent_request_id": report.AgentRequestID,
		"live_session_id":  report.LiveSessionID,
		"merchant_id":      normalizeUserIDForDB(report.MerchantID),
		"status":           report.Status,
		"attempt_count":    report.AttemptCount,
		"prompt":           report.Prompt,
		"report":           report.Report,
		"error_message":    report.ErrorMessage,
		"updated_at":       now,
	}
	res := r.dbFor(ctx).Table("live_analysis_report").Where("task_id = ?", report.TaskID).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindByTaskID(ctx, report.TaskID)
	if err != nil {
		return err
	}
	*report = updated
	return nil
}

type liveAnalysisReportRow struct {
	TaskID         string                          `gorm:"column:task_id;primaryKey"`
	AgentRequestID string                          `gorm:"column:agent_request_id"`
	LiveSessionID  uint64                          `gorm:"column:live_session_id"`
	MerchantID     string                          `gorm:"column:merchant_id"`
	Status         domain.LiveAnalysisReportStatus `gorm:"column:status"`
	AttemptCount   int                             `gorm:"column:attempt_count"`
	Prompt         string                          `gorm:"column:prompt"`
	Report         string                          `gorm:"column:report"`
	ErrorMessage   string                          `gorm:"column:error_message"`
	CreatedAt      time.Time                       `gorm:"column:created_at"`
	UpdatedAt      time.Time                       `gorm:"column:updated_at"`
}

func liveAnalysisReportRowFromDomain(report domain.LiveAnalysisReport) liveAnalysisReportRow {
	return liveAnalysisReportRow{
		TaskID:         report.TaskID,
		AgentRequestID: report.AgentRequestID,
		LiveSessionID:  report.LiveSessionID,
		MerchantID:     normalizeUserIDForDB(report.MerchantID),
		Status:         report.Status,
		AttemptCount:   report.AttemptCount,
		Prompt:         report.Prompt,
		Report:         report.Report,
		ErrorMessage:   report.ErrorMessage,
		CreatedAt:      report.CreatedAt,
		UpdatedAt:      report.UpdatedAt,
	}
}

func (r liveAnalysisReportRow) toDomain() domain.LiveAnalysisReport {
	return domain.LiveAnalysisReport{
		TaskID:         r.TaskID,
		AgentRequestID: r.AgentRequestID,
		LiveSessionID:  r.LiveSessionID,
		MerchantID:     r.MerchantID,
		Status:         r.Status,
		AttemptCount:   r.AttemptCount,
		Prompt:         r.Prompt,
		Report:         r.Report,
		ErrorMessage:   r.ErrorMessage,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
	}
}

func normalizeUserIDForDB(id string) string {
	id = strings.TrimSpace(id)
	for _, prefix := range []string{"u_", "U_"} {
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix)
		}
	}
	return id
}

var _ liveanalysisports.LiveAnalysisReportRepository = (*MySQLLiveAnalysisReportRepository)(nil)
