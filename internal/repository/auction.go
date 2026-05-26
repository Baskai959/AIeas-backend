package repository

import (
	"context"
	"errors"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type AuctionRepository interface {
	Create(ctx context.Context, auction *domain.AuctionLot) error
	FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error)
	List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error)
	Update(ctx context.Context, auction *domain.AuctionLot) error
	Delete(ctx context.Context, id uint64) error
}

type MySQLAuctionRepository struct {
	db *gorm.DB
}

func NewMySQLAuctionRepository(db *gorm.DB) *MySQLAuctionRepository {
	return &MySQLAuctionRepository{db: db}
}

func (r *MySQLAuctionRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLAuctionRepository) Create(ctx context.Context, auction *domain.AuctionLot) error {
	row := auctionRowFromDomain(*auction)
	if err := r.dbFor(ctx).Table("auction_lot").Create(&row).Error; err != nil {
		return err
	}
	*auction = row.toDomain()
	return nil
}

func (r *MySQLAuctionRepository) FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error) {
	var row auctionRow
	err := r.dbFor(ctx).Table("auction_lot").Where("auction_id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.AuctionLot{}, domain.ErrNotFound
		}
		return domain.AuctionLot{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLAuctionRepository) List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error) {
	query := r.dbFor(ctx).Table("auction_lot").Order("auction_id DESC")
	if filter.SellerID != "" {
		query = query.Where("seller_id = ?", normalizeUserIDForDB(filter.SellerID))
	}
	if filter.Status.Valid() {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.ItemID != 0 {
		query = query.Where("item_id = ?", filter.ItemID)
	}
	if filter.LiveRoomID != 0 {
		query = query.Where("live_room_id = ?", filter.LiveRoomID)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []auctionRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	auctions := make([]domain.AuctionLot, 0, len(rows))
	for _, row := range rows {
		auctions = append(auctions, row.toDomain())
	}
	return auctions, nil
}

func (r *MySQLAuctionRepository) Update(ctx context.Context, auction *domain.AuctionLot) error {
	row := auctionRowFromDomain(*auction)
	updates := map[string]interface{}{
		"item_id":          row.ItemID,
		"seller_id":        row.SellerID,
		"live_room_id":     row.LiveRoomID,
		"live_session_id":  row.LiveSessionID,
		"auction_type":     row.AuctionType,
		"start_price":      row.StartPrice,
		"reserve_price":    row.ReservePrice,
		"increment_rule":   row.IncrementRule,
		"anti_sniping_sec": row.AntiSnipingSec,
		"anti_extend_sec":  row.AntiExtendSec,
		"deposit_amount":   row.DepositAmount,
		"status":           row.Status,
		"rule_snapshot":    row.RuleSnapshot,
		"start_time":       row.StartTime,
		"end_time":         row.EndTime,
		"winner_id":        row.WinnerID,
		"deal_price":       row.DealPrice,
		"closed_at":        row.ClosedAt,
		"closed_by":        row.ClosedBy,
		"updated_at":       time.Now(),
	}
	res := r.dbFor(ctx).Table("auction_lot").Where("auction_id = ?", auction.AuctionID).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindByID(ctx, auction.AuctionID)
	if err != nil {
		return err
	}
	*auction = updated
	return nil
}

func (r *MySQLAuctionRepository) Delete(ctx context.Context, id uint64) error {
	res := r.dbFor(ctx).Table("auction_lot").Where("auction_id = ?", id).Delete(&auctionRow{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	return nil
}

type auctionRow struct {
	AuctionID      uint64               `gorm:"column:auction_id;primaryKey"`
	ItemID         uint64               `gorm:"column:item_id"`
	SellerID       string               `gorm:"column:seller_id"`
	LiveRoomID     uint64               `gorm:"column:live_room_id"`
	LiveSessionID  *uint64              `gorm:"column:live_session_id"`
	AuctionType    domain.AuctionType   `gorm:"column:auction_type"`
	StartPrice     int64                `gorm:"column:start_price"`
	ReservePrice   int64                `gorm:"column:reserve_price"`
	IncrementRule  []byte               `gorm:"column:increment_rule"`
	AntiSnipingSec int                  `gorm:"column:anti_sniping_sec"`
	AntiExtendSec  int                  `gorm:"column:anti_extend_sec"`
	DepositAmount  int64                `gorm:"column:deposit_amount"`
	Status         domain.AuctionStatus `gorm:"column:status"`
	RuleSnapshot   []byte               `gorm:"column:rule_snapshot"`
	StartTime      time.Time            `gorm:"column:start_time"`
	EndTime        time.Time            `gorm:"column:end_time"`
	WinnerID       *string              `gorm:"column:winner_id"`
	DealPrice      *int64               `gorm:"column:deal_price"`
	ClosedAt       *time.Time           `gorm:"column:closed_at"`
	ClosedBy       string               `gorm:"column:closed_by"`
	CreatedAt      time.Time            `gorm:"column:created_at"`
	UpdatedAt      time.Time            `gorm:"column:updated_at"`
}

func auctionRowFromDomain(auction domain.AuctionLot) auctionRow {
	return auctionRow{
		AuctionID:      auction.AuctionID,
		ItemID:         auction.ItemID,
		SellerID:       normalizeUserIDForDB(auction.SellerID),
		LiveRoomID:     auction.LiveRoomID,
		LiveSessionID:  cloneUint64Ptr(auction.LiveSessionID),
		AuctionType:    auction.AuctionType,
		StartPrice:     auction.StartPrice,
		ReservePrice:   auction.ReservePrice,
		IncrementRule:  []byte(auction.IncrementRule),
		AntiSnipingSec: auction.AntiSnipingSec,
		AntiExtendSec:  auction.AntiExtendSec,
		DepositAmount:  auction.DepositAmount,
		Status:         auction.Status,
		RuleSnapshot:   []byte(auction.RuleSnapshot),
		StartTime:      auction.StartTime,
		EndTime:        auction.EndTime,
		WinnerID:       normalizeOptionalUserIDForDB(auction.WinnerID),
		DealPrice:      auction.DealPrice,
		ClosedAt:       auction.ClosedAt,
		ClosedBy:       auction.ClosedBy,
		CreatedAt:      auction.CreatedAt,
		UpdatedAt:      auction.UpdatedAt,
	}
}

func normalizeOptionalUserIDForDB(id *string) *string {
	if id == nil {
		return nil
	}
	normalized := normalizeUserIDForDB(*id)
	return &normalized
}

func (r auctionRow) toDomain() domain.AuctionLot {
	return domain.AuctionLot{
		AuctionID:      r.AuctionID,
		ItemID:         r.ItemID,
		SellerID:       r.SellerID,
		LiveRoomID:     r.LiveRoomID,
		LiveSessionID:  cloneUint64Ptr(r.LiveSessionID),
		AuctionType:    r.AuctionType,
		StartPrice:     r.StartPrice,
		ReservePrice:   r.ReservePrice,
		IncrementRule:  append([]byte(nil), r.IncrementRule...),
		AntiSnipingSec: r.AntiSnipingSec,
		AntiExtendSec:  r.AntiExtendSec,
		DepositAmount:  r.DepositAmount,
		Status:         r.Status,
		RuleSnapshot:   append([]byte(nil), r.RuleSnapshot...),
		StartTime:      r.StartTime,
		EndTime:        r.EndTime,
		WinnerID:       r.WinnerID,
		DealPrice:      r.DealPrice,
		ClosedAt:       r.ClosedAt,
		ClosedBy:       r.ClosedBy,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
	}
}
