package repository

import (
	"context"
	"errors"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

// LiveRoomRepository 提供直播间持久化能力。
type LiveRoomRepository interface {
	Create(ctx context.Context, room *domain.LiveRoom) error
	FindByID(ctx context.Context, id uint64) (domain.LiveRoom, error)
	FindByMerchantID(ctx context.Context, merchantID string) (domain.LiveRoom, error)
	List(ctx context.Context, filter domain.LiveRoomFilter) ([]domain.LiveRoom, error)
	Update(ctx context.Context, room *domain.LiveRoom) error
	Delete(ctx context.Context, id uint64) error
}

// MySQLLiveRoomRepository 是基于 GORM 的 live_room 表实现。
type MySQLLiveRoomRepository struct {
	db *gorm.DB
}

func NewMySQLLiveRoomRepository(db *gorm.DB) *MySQLLiveRoomRepository {
	return &MySQLLiveRoomRepository{db: db}
}

func (r *MySQLLiveRoomRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLLiveRoomRepository) Create(ctx context.Context, room *domain.LiveRoom) error {
	row := liveRoomRowFromDomain(*room)
	if err := r.dbFor(ctx).Table("live_room").Create(&row).Error; err != nil {
		return err
	}
	*room = row.toDomain()
	return nil
}

func (r *MySQLLiveRoomRepository) FindByID(ctx context.Context, id uint64) (domain.LiveRoom, error) {
	var row liveRoomRow
	err := r.dbFor(ctx).Table("live_room").Where("id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveRoom{}, domain.ErrNotFound
		}
		return domain.LiveRoom{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveRoomRepository) FindByMerchantID(ctx context.Context, merchantID string) (domain.LiveRoom, error) {
	normalized := normalizeUserIDForDB(merchantID)
	if normalized == "" {
		return domain.LiveRoom{}, domain.ErrNotFound
	}
	var row liveRoomRow
	err := r.dbFor(ctx).Table("live_room").Where("merchant_id = ?", normalized).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.LiveRoom{}, domain.ErrNotFound
		}
		return domain.LiveRoom{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLLiveRoomRepository) List(ctx context.Context, filter domain.LiveRoomFilter) ([]domain.LiveRoom, error) {
	query := r.dbFor(ctx).Table("live_room").Order("id DESC")
	if filter.MerchantID != "" {
		query = query.Where("merchant_id = ?", normalizeUserIDForDB(filter.MerchantID))
	}
	if filter.Status.Valid() {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []liveRoomRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	rooms := make([]domain.LiveRoom, 0, len(rows))
	for _, row := range rows {
		rooms = append(rooms, row.toDomain())
	}
	return rooms, nil
}

func (r *MySQLLiveRoomRepository) Update(ctx context.Context, room *domain.LiveRoom) error {
	row := liveRoomRowFromDomain(*room)
	updates := map[string]interface{}{
		"merchant_id":       row.MerchantID,
		"title":             row.Title,
		"description":       row.Description,
		"cover_url":         row.CoverURL,
		"status":            row.Status,
		"active_auction_id": row.ActiveAuctionID,
		"updated_at":        time.Now(),
	}
	res := r.dbFor(ctx).Table("live_room").Where("id = ?", room.ID).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindByID(ctx, room.ID)
	if err != nil {
		return err
	}
	*room = updated
	return nil
}

func (r *MySQLLiveRoomRepository) Delete(ctx context.Context, id uint64) error {
	res := r.dbFor(ctx).Table("live_room").Where("id = ?", id).Delete(&liveRoomRow{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	return nil
}

type liveRoomRow struct {
	ID              uint64                `gorm:"column:id;primaryKey"`
	MerchantID      string                `gorm:"column:merchant_id"`
	Title           string                `gorm:"column:title"`
	Description     string                `gorm:"column:description"`
	CoverURL        string                `gorm:"column:cover_url"`
	Status          domain.LiveRoomStatus `gorm:"column:status"`
	ActiveAuctionID *uint64               `gorm:"column:active_auction_id"`
	CreatedAt       time.Time             `gorm:"column:created_at"`
	UpdatedAt       time.Time             `gorm:"column:updated_at"`
}

func liveRoomRowFromDomain(room domain.LiveRoom) liveRoomRow {
	var active *uint64
	if room.ActiveAuctionID != 0 {
		v := room.ActiveAuctionID
		active = &v
	}
	return liveRoomRow{
		ID:              room.ID,
		MerchantID:      normalizeUserIDForDB(room.MerchantID),
		Title:           room.Title,
		Description:     room.Description,
		CoverURL:        room.CoverURL,
		Status:          room.Status,
		ActiveAuctionID: active,
		CreatedAt:       room.CreatedAt,
		UpdatedAt:       room.UpdatedAt,
	}
}

func (r liveRoomRow) toDomain() domain.LiveRoom {
	var active uint64
	if r.ActiveAuctionID != nil {
		active = *r.ActiveAuctionID
	}
	return domain.LiveRoom{
		ID:              r.ID,
		MerchantID:      r.MerchantID,
		Title:           r.Title,
		Description:     r.Description,
		CoverURL:        r.CoverURL,
		Status:          r.Status,
		ActiveAuctionID: active,
		CreatedAt:       r.CreatedAt,
		UpdatedAt:       r.UpdatedAt,
	}
}
