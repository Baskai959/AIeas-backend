package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	// CloseWithVersion 使用乐观锁 CAS 完成落槌：仅当 version=expectedVersion 且
	// status ∈ allowedFromStatuses 时把 status/hammer_price_cent/winner_user_id/
	// hammer_at/version+1/updated_at 持久化。冲突或状态不允许时返回
	// domain.ErrOptimisticConflict 或 domain.ErrInvalidState；成功时把
	// expectedVersion+1 回写到入参 auction.Version。
	CloseWithVersion(ctx context.Context, auction *domain.AuctionLot, expectedVersion int64, allowedFromStatuses []domain.AuctionStatus) error
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
	if filter.Category != "" {
		query = query.Where("category = ?", strings.TrimSpace(filter.Category))
	}
	if filter.Keyword != "" {
		keyword := "%" + strings.TrimSpace(filter.Keyword) + "%"
		query = query.Where("title LIKE ? OR description LIKE ? OR brand LIKE ?", keyword, keyword, keyword)
	}
	if filter.LiveSessionID != 0 {
		query = query.Where("live_session_id = ?", filter.LiveSessionID)
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
		"seller_id":        row.SellerID,
		"live_session_id":  row.LiveSessionID,
		"title":            row.Title,
		"description":      row.Description,
		"category":         row.Category,
		"brand":            row.Brand,
		"condition_grade":  row.ConditionGrade,
		"image_urls":       row.ImageURLs,
		"cover_url":        row.CoverURL,
		"auction_type":     row.AuctionType,
		"start_price":      row.StartPrice,
		"reserve_price":    row.ReservePrice,
		"cap_price":        row.CapPrice,
		"increment_rule":   row.IncrementRule,
		"anti_sniping_sec": row.AntiSnipingSec,
		"anti_extend_sec":  row.AntiExtendSec,
		"anti_extend_mode": row.AntiExtendMode,
		"deposit_amount":   row.DepositAmount,
		"status":           row.Status,
		"rule_snapshot":    row.RuleSnapshot,
		"audit_task_id":    row.AuditTaskID,
		"start_time":       row.StartTime,
		"end_time":         row.EndTime,
		"duration_sec":     row.DurationSec,
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

// CloseWithVersion 见接口注释。SQL 形如：
//
//	UPDATE auction_lot SET status=?, deal_price=?, winner_id=?, closed_at=?,
//	       closed_by=?, version=version+1, updated_at=?
//	WHERE  auction_id=? AND version=? AND status IN (...)
//
// RowsAffected==0 时回查一次：行已是终态返回 ErrInvalidState；否则返回
// ErrOptimisticConflict（version 不匹配或 status 不在白名单中）。
func (r *MySQLAuctionRepository) CloseWithVersion(ctx context.Context, auction *domain.AuctionLot, expectedVersion int64, allowedFromStatuses []domain.AuctionStatus) error {
	if auction == nil || auction.AuctionID == 0 {
		return domain.ErrInvalidArgument
	}
	row := auctionRowFromDomain(*auction)
	now := time.Now().UTC()
	updates := map[string]interface{}{
		"status":     row.Status,
		"deal_price": row.DealPrice,
		"winner_id":  row.WinnerID,
		"closed_at":  row.ClosedAt,
		"closed_by":  row.ClosedBy,
		"version":    gorm.Expr("version + 1"),
		"updated_at": now,
	}
	query := r.dbFor(ctx).Table("auction_lot").
		Where("auction_id = ? AND version = ?", auction.AuctionID, expectedVersion)
	if len(allowedFromStatuses) > 0 {
		query = query.Where("status IN ?", allowedFromStatuses)
	}
	res := query.Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		current, err := r.FindByID(ctx, auction.AuctionID)
		if err != nil {
			return err
		}
		if current.Status.Terminal() {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	auction.Version = expectedVersion + 1
	auction.UpdatedAt = now
	return nil
}

type auctionRow struct {
	AuctionID      uint64                   `gorm:"column:auction_id;primaryKey"`
	SellerID       string                   `gorm:"column:seller_id"`
	LiveSessionID  *uint64                  `gorm:"column:live_session_id"`
	Title          string                   `gorm:"column:title"`
	Description    string                   `gorm:"column:description"`
	Category       string                   `gorm:"column:category"`
	Brand          string                   `gorm:"column:brand"`
	ConditionGrade domain.ConditionGrade    `gorm:"column:condition_grade"`
	ImageURLs      []byte                   `gorm:"column:image_urls"`
	CoverURL       string                   `gorm:"column:cover_url"`
	AuctionType    domain.AuctionType       `gorm:"column:auction_type"`
	StartPrice     int64                    `gorm:"column:start_price"`
	ReservePrice   int64                    `gorm:"column:reserve_price"`
	CapPrice       int64                    `gorm:"column:cap_price"`
	IncrementRule  []byte                   `gorm:"column:increment_rule"`
	AntiSnipingSec int                      `gorm:"column:anti_sniping_sec"`
	AntiExtendSec  int                      `gorm:"column:anti_extend_sec"`
	AntiExtendMode domain.AuctionExtendMode `gorm:"column:anti_extend_mode"`
	DepositAmount  int64                    `gorm:"column:deposit_amount"`
	Status         domain.AuctionStatus     `gorm:"column:status"`
	RuleSnapshot   []byte                   `gorm:"column:rule_snapshot"`
	AuditTaskID    string                   `gorm:"column:audit_task_id"`
	StartTime      *time.Time               `gorm:"column:start_time"`
	EndTime        *time.Time               `gorm:"column:end_time"`
	DurationSec    int                      `gorm:"column:duration_sec"`
	WinnerID       *string                  `gorm:"column:winner_id"`
	DealPrice      *int64                   `gorm:"column:deal_price"`
	ClosedAt       *time.Time               `gorm:"column:closed_at"`
	ClosedBy       string                   `gorm:"column:closed_by"`
	Version        int64                    `gorm:"column:version;not null;default:0"`
	CreatedAt      time.Time                `gorm:"column:created_at"`
	UpdatedAt      time.Time                `gorm:"column:updated_at"`
}

func auctionRowFromDomain(auction domain.AuctionLot) auctionRow {
	imageURLs, _ := json.Marshal(auction.ImageURLs)
	return auctionRow{
		AuctionID:      auction.AuctionID,
		SellerID:       normalizeUserIDForDB(auction.SellerID),
		LiveSessionID:  cloneUint64Ptr(auction.LiveSessionID),
		Title:          auction.Title,
		Description:    auction.Description,
		Category:       auction.Category,
		Brand:          auction.Brand,
		ConditionGrade: auction.ConditionGrade,
		ImageURLs:      imageURLs,
		CoverURL:       auction.CoverURL,
		AuctionType:    auction.AuctionType,
		StartPrice:     auction.StartPrice,
		ReservePrice:   auction.ReservePrice,
		CapPrice:       auction.CapPrice,
		IncrementRule:  []byte(auction.IncrementRule),
		AntiSnipingSec: auction.AntiSnipingSec,
		AntiExtendSec:  auction.AntiExtendSec,
		AntiExtendMode: domain.NormalizeAuctionExtendMode(auction.AntiExtendMode),
		DepositAmount:  auction.DepositAmount,
		Status:         auction.Status,
		RuleSnapshot:   []byte(auction.RuleSnapshot),
		AuditTaskID:    auction.AuditTaskID,
		StartTime:      timePtrOrNil(auction.StartTime),
		EndTime:        timePtrOrNil(auction.EndTime),
		DurationSec:    auction.DurationSec,
		WinnerID:       normalizeOptionalUserIDForDB(auction.WinnerID),
		DealPrice:      auction.DealPrice,
		ClosedAt:       auction.ClosedAt,
		ClosedBy:       auction.ClosedBy,
		Version:        auction.Version,
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

func timePtrOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	t = t.UTC()
	return &t
}

func timeValueOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}

func (r auctionRow) toDomain() domain.AuctionLot {
	var imageURLs []string
	_ = json.Unmarshal(r.ImageURLs, &imageURLs)
	return domain.AuctionLot{
		AuctionID:      r.AuctionID,
		SellerID:       r.SellerID,
		LiveSessionID:  cloneUint64Ptr(r.LiveSessionID),
		Title:          r.Title,
		Description:    r.Description,
		Category:       r.Category,
		Brand:          r.Brand,
		ConditionGrade: r.ConditionGrade,
		ImageURLs:      append([]string(nil), imageURLs...),
		CoverURL:       r.CoverURL,
		AuctionType:    r.AuctionType,
		StartPrice:     r.StartPrice,
		ReservePrice:   r.ReservePrice,
		CapPrice:       r.CapPrice,
		IncrementRule:  append([]byte(nil), r.IncrementRule...),
		AntiSnipingSec: r.AntiSnipingSec,
		AntiExtendSec:  r.AntiExtendSec,
		AntiExtendMode: domain.NormalizeAuctionExtendMode(r.AntiExtendMode),
		DepositAmount:  r.DepositAmount,
		Status:         r.Status,
		RuleSnapshot:   append([]byte(nil), r.RuleSnapshot...),
		AuditTaskID:    r.AuditTaskID,
		StartTime:      timeValueOrZero(r.StartTime),
		EndTime:        timeValueOrZero(r.EndTime),
		DurationSec:    r.DurationSec,
		WinnerID:       r.WinnerID,
		DealPrice:      r.DealPrice,
		ClosedAt:       r.ClosedAt,
		ClosedBy:       r.ClosedBy,
		Version:        r.Version,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
	}
}
