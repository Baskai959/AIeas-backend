package repository

import (
	"context"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type AuditRepository interface {
	Create(ctx context.Context, log *domain.AuditLog) error
	List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error)
}

type MySQLAuditRepository struct {
	db *gorm.DB
}

func NewMySQLAuditRepository(db *gorm.DB) *MySQLAuditRepository {
	return &MySQLAuditRepository{db: db}
}

func (r *MySQLAuditRepository) Create(ctx context.Context, log *domain.AuditLog) error {
	row := auditRow{
		ID:           log.ID,
		OperatorID:   normalizeUserIDForDB(log.OperatorID),
		OperatorRole: log.OperatorRole,
		Action:       log.Action,
		TargetType:   log.TargetType,
		TargetID:     log.TargetID,
		Payload:      []byte(log.Payload),
		IP:           log.IP,
		UserAgent:    log.UserAgent,
		CreatedAt:    log.CreatedAt,
	}
	if row.CreatedAt.IsZero() {
		row.CreatedAt = time.Now()
	}
	if tx := DBFromContext(ctx); tx != nil {
		return tx.Table("audit_log").Create(&row).Error
	}
	if err := r.db.WithContext(ctx).Table("audit_log").Create(&row).Error; err != nil {
		return err
	}
	log.ID = row.ID
	log.CreatedAt = row.CreatedAt
	return nil
}

func (r *MySQLAuditRepository) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error) {
	query := r.db.WithContext(ctx).Table("audit_log").Order("id DESC")
	if filter.OperatorID != "" {
		query = query.Where("operator_id = ?", normalizeUserIDForDB(filter.OperatorID))
	}
	if filter.Action != "" {
		query = query.Where("action = ?", filter.Action)
	}
	if filter.StartTime != nil {
		query = query.Where("created_at >= ?", *filter.StartTime)
	}
	if filter.EndTime != nil {
		query = query.Where("created_at <= ?", *filter.EndTime)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []auditRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	logs := make([]domain.AuditLog, 0, len(rows))
	for _, row := range rows {
		logs = append(logs, row.toDomain())
	}
	return logs, nil
}

type auditRow struct {
	ID           uint64      `gorm:"column:id;primaryKey"`
	OperatorID   string      `gorm:"column:operator_id"`
	OperatorRole domain.Role `gorm:"column:operator_role"`
	Action       string      `gorm:"column:action"`
	TargetType   string      `gorm:"column:target_type"`
	TargetID     string      `gorm:"column:target_id"`
	Payload      []byte      `gorm:"column:payload"`
	IP           string      `gorm:"column:ip"`
	UserAgent    string      `gorm:"column:ua"`
	CreatedAt    time.Time   `gorm:"column:created_at"`
}

func (r auditRow) toDomain() domain.AuditLog {
	return domain.AuditLog{
		ID:           r.ID,
		OperatorID:   r.OperatorID,
		OperatorRole: r.OperatorRole,
		Action:       r.Action,
		TargetType:   r.TargetType,
		TargetID:     r.TargetID,
		Payload:      append([]byte(nil), r.Payload...),
		IP:           r.IP,
		UserAgent:    r.UserAgent,
		CreatedAt:    r.CreatedAt,
	}
}
