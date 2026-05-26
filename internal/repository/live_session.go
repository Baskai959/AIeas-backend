package repository

import (
	"context"
	"errors"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

// LiveSessionRepository 负责直播场次（live_session）的持久化能力。
type LiveSessionRepository interface {
	Create(ctx context.Context, session *domain.LiveSession) error
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
	GetActiveByRoomID(ctx context.Context, roomID uint64) (domain.LiveSession, error)
	Update(ctx context.Context, session *domain.LiveSession) error
	List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error)
}

// MySQLLiveSessionRepository 是基于 GORM 的 live_session 表实现。
type MySQLLiveSessionRepository struct {
	db *gorm.DB
}

func NewMySQLLiveSessionRepository(db *gorm.DB) *MySQLLiveSessionRepository {
	return &MySQLLiveSessionRepository{db: db}
}

func (r *MySQLLiveSessionRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLLiveSessionRepository) Create(ctx context.Context, session *domain.LiveSession) error {
	row := liveSessionRowFromDomain(*session)
	if err := r.dbFor(ctx).Table("live_session").Create(&row).Error; err != nil {
		return err
	}
	*session = row.toDomain()
	return nil
}

func (r *MySQLLiveSessionRepository) Get(ctx context.Context, id uint64) (domain.LiveSession, error) {
	var row liveSessionRow
	err := r.dbFor(ctx).Table("live_session").Where("id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveSession{}, domain.ErrNotFound
		}
		return domain.LiveSession{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveSessionRepository) GetActiveByRoomID(ctx context.Context, roomID uint64) (domain.LiveSession, error) {
	var row liveSessionRow
	err := r.dbFor(ctx).Table("live_session").
		Where("live_room_id = ? AND status = ?", roomID, domain.LiveSessionStatusLive).
		Order("id DESC").
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveSession{}, domain.ErrNotFound
		}
		return domain.LiveSession{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveSessionRepository) Update(ctx context.Context, session *domain.LiveSession) error {
	row := liveSessionRowFromDomain(*session)
	updates := map[string]interface{}{
		"live_room_id": row.LiveRoomID,
		"merchant_id":  row.MerchantID,
		"title":        row.Title,
		"status":       row.Status,
		"opened_at":    row.OpenedAt,
		"closed_at":    row.ClosedAt,
		"lots_total":   row.LotsTotal,
		"lots_sold":    row.LotsSold,
		"lots_unsold":  row.LotsUnsold,
		"bid_count":    row.BidCount,
		"gmv_cent":     row.GMVCent,
		"viewer_peak":  row.ViewerPeak,
		"viewer_total": row.ViewerTotal,
		"updated_at":   time.Now().UTC(),
	}
	res := r.dbFor(ctx).Table("live_session").Where("id = ?", session.ID).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.Get(ctx, session.ID)
	if err != nil {
		return err
	}
	*session = updated
	return nil
}

func (r *MySQLLiveSessionRepository) List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error) {
	query := r.dbFor(ctx).Table("live_session").Order("id DESC")
	if filter.LiveRoomID != 0 {
		query = query.Where("live_room_id = ?", filter.LiveRoomID)
	}
	if filter.MerchantID != "" {
		query = query.Where("merchant_id = ?", normalizeUserIDForDB(filter.MerchantID))
	}
	if filter.Status.Valid() {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.OpenedFrom != nil {
		query = query.Where("opened_at >= ?", *filter.OpenedFrom)
	}
	if filter.OpenedTo != nil {
		query = query.Where("opened_at <= ?", *filter.OpenedTo)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []liveSessionRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	sessions := make([]domain.LiveSession, 0, len(rows))
	for _, row := range rows {
		sessions = append(sessions, row.toDomain())
	}
	return sessions, nil
}

type liveSessionRow struct {
	ID          uint64                   `gorm:"column:id;primaryKey"`
	LiveRoomID  uint64                   `gorm:"column:live_room_id"`
	MerchantID  string                   `gorm:"column:merchant_id"`
	Title       string                   `gorm:"column:title"`
	Status      domain.LiveSessionStatus `gorm:"column:status"`
	OpenedAt    time.Time                `gorm:"column:opened_at"`
	ClosedAt    *time.Time               `gorm:"column:closed_at"`
	LotsTotal   int                      `gorm:"column:lots_total"`
	LotsSold    int                      `gorm:"column:lots_sold"`
	LotsUnsold  int                      `gorm:"column:lots_unsold"`
	BidCount    int                      `gorm:"column:bid_count"`
	GMVCent     int64                    `gorm:"column:gmv_cent"`
	ViewerPeak  int                      `gorm:"column:viewer_peak"`
	ViewerTotal int                      `gorm:"column:viewer_total"`
	CreatedAt   time.Time                `gorm:"column:created_at"`
	UpdatedAt   time.Time                `gorm:"column:updated_at"`
}

func liveSessionRowFromDomain(s domain.LiveSession) liveSessionRow {
	return liveSessionRow{
		ID:          s.ID,
		LiveRoomID:  s.LiveRoomID,
		MerchantID:  normalizeUserIDForDB(s.MerchantID),
		Title:       s.Title,
		Status:      s.Status,
		OpenedAt:    s.OpenedAt,
		ClosedAt:    s.ClosedAt,
		LotsTotal:   s.LotsTotal,
		LotsSold:    s.LotsSold,
		LotsUnsold:  s.LotsUnsold,
		BidCount:    s.BidCount,
		GMVCent:     s.GMVCent,
		ViewerPeak:  s.ViewerPeak,
		ViewerTotal: s.ViewerTotal,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
}

func (r liveSessionRow) toDomain() domain.LiveSession {
	return domain.LiveSession{
		ID:          r.ID,
		LiveRoomID:  r.LiveRoomID,
		MerchantID:  r.MerchantID,
		Title:       r.Title,
		Status:      r.Status,
		OpenedAt:    r.OpenedAt,
		ClosedAt:    r.ClosedAt,
		LotsTotal:   r.LotsTotal,
		LotsSold:    r.LotsSold,
		LotsUnsold:  r.LotsUnsold,
		BidCount:    r.BidCount,
		GMVCent:     r.GMVCent,
		ViewerPeak:  r.ViewerPeak,
		ViewerTotal: r.ViewerTotal,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}
