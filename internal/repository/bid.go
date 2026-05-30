package repository

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type BidRepository interface {
	Create(ctx context.Context, bid *domain.BidRecord) error
	FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error)
	ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error)
	CountByAuction(ctx context.Context, auctionID uint64) (int, error)
	ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error)
}

type MySQLBidRepository struct {
	db *gorm.DB
}

func NewMySQLBidRepository(db *gorm.DB) *MySQLBidRepository {
	return &MySQLBidRepository{db: db}
}

func (r *MySQLBidRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLBidRepository) Create(ctx context.Context, bid *domain.BidRecord) error {
	row := bidRecordRowFromDomain(*bid)
	if err := r.dbFor(ctx).Table("bid_record").Create(&row).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") || strings.Contains(err.Error(), "1062") {
			return domain.ErrConflict
		}
		return err
	}
	*bid = row.toDomain()
	return nil
}

func (r *MySQLBidRepository) FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error) {
	var row bidRecordRow
	err := r.dbFor(ctx).Table("bid_record").Where("request_id = ?", requestID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.BidRecord{}, domain.ErrNotFound
		}
		return domain.BidRecord{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLBidRepository) ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var rows []bidRecordRow
	if err := r.dbFor(ctx).Table("bid_record").Where("auction_id = ?", auctionID).Order("bid_price DESC, bid_ts_ms ASC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	records := make([]domain.BidRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.toDomain())
	}
	return records, nil
}

func (r *MySQLBidRepository) CountByAuction(ctx context.Context, auctionID uint64) (int, error) {
	var count int64
	if err := r.dbFor(ctx).Table("bid_record").Where("auction_id = ?", auctionID).Count(&count).Error; err != nil {
		return 0, err
	}
	return int(count), nil
}

func (r *MySQLBidRepository) ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	orderBy := "bid_ts_ms DESC, id DESC"
	switch sortBy {
	case "timeAsc":
		orderBy = "bid_ts_ms ASC, id ASC"
	case "priceDesc":
		orderBy = "bid_price DESC, bid_ts_ms ASC, id ASC"
	}
	var rows []bidRecordRow
	if err := r.dbFor(ctx).
		Table("bid_record").
		Where("live_session_id = ?", sessionID).
		Order(orderBy).
		Limit(limit).
		Offset(offset).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	records := make([]domain.BidRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.toDomain())
	}
	return records, nil
}

type bidRecordRow struct {
	ID            uint64               `gorm:"column:id;primaryKey"`
	RequestID     string               `gorm:"column:request_id"`
	AuctionID     uint64               `gorm:"column:auction_id"`
	LiveSessionID *uint64              `gorm:"column:live_session_id"`
	BidderID      string               `gorm:"column:bidder_id"`
	BidPrice      int64                `gorm:"column:bid_price"`
	BidTSMS       int64                `gorm:"column:bid_ts_ms"`
	Source        string               `gorm:"column:source"`
	RiskResult    domain.BidRiskResult `gorm:"column:risk_result"`
	RejectReason  string               `gorm:"column:reject_reason"`
	CreatedAt     time.Time            `gorm:"column:created_at"`
}

func bidRecordRowFromDomain(bid domain.BidRecord) bidRecordRow {
	return bidRecordRow{
		ID:            bid.ID,
		RequestID:     bid.RequestID,
		AuctionID:     bid.AuctionID,
		LiveSessionID: cloneUint64Ptr(bid.LiveSessionID),
		BidderID:      normalizeUserIDForDB(bid.BidderID),
		BidPrice:      bid.BidPrice,
		BidTSMS:       bid.BidTSMS,
		Source:        bid.Source,
		RiskResult:    bid.RiskResult,
		RejectReason:  bid.RejectReason,
		CreatedAt:     bid.CreatedAt,
	}
}

func (r bidRecordRow) toDomain() domain.BidRecord {
	return domain.BidRecord{
		ID:            r.ID,
		RequestID:     r.RequestID,
		AuctionID:     r.AuctionID,
		LiveSessionID: cloneUint64Ptr(r.LiveSessionID),
		BidderID:      r.BidderID,
		BidPrice:      r.BidPrice,
		BidTSMS:       r.BidTSMS,
		Source:        r.Source,
		RiskResult:    r.RiskResult,
		RejectReason:  r.RejectReason,
		CreatedAt:     r.CreatedAt,
	}
}

type MemoryBidRepository struct {
	mu        sync.RWMutex
	nextID    uint64
	byID      map[uint64]domain.BidRecord
	byRequest map[string]uint64
}

func NewMemoryBidRepository() *MemoryBidRepository {
	return &MemoryBidRepository{nextID: 1, byID: make(map[uint64]domain.BidRecord), byRequest: make(map[string]uint64)}
}

func (r *MemoryBidRepository) Create(ctx context.Context, bid *domain.BidRecord) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if bid.RequestID != "" {
		if id, ok := r.byRequest[bid.RequestID]; ok {
			existing := r.byID[id]
			*bid = existing
			return domain.ErrConflict
		}
	}
	if bid.ID == 0 {
		bid.ID = r.nextID
		r.nextID++
	}
	if bid.CreatedAt.IsZero() {
		bid.CreatedAt = time.Now().UTC()
	}
	r.byID[bid.ID] = cloneBidRecord(*bid)
	if bid.RequestID != "" {
		r.byRequest[bid.RequestID] = bid.ID
	}
	return nil
}

func (r *MemoryBidRepository) FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byRequest[requestID]
	if !ok {
		return domain.BidRecord{}, domain.ErrNotFound
	}
	return cloneBidRecord(r.byID[id]), nil
}

func (r *MemoryBidRepository) ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error) {
	_ = ctx
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	records := make([]domain.BidRecord, 0)
	for _, bid := range r.byID {
		if bid.AuctionID == auctionID {
			records = append(records, cloneBidRecord(bid))
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].BidPrice == records[j].BidPrice {
			return records[i].BidTSMS < records[j].BidTSMS
		}
		return records[i].BidPrice > records[j].BidPrice
	})
	if len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func (r *MemoryBidRepository) CountByAuction(ctx context.Context, auctionID uint64) (int, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, bid := range r.byID {
		if bid.AuctionID == auctionID {
			count++
		}
	}
	return count, nil
}

func (r *MemoryBidRepository) ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error) {
	_ = ctx
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	records := make([]domain.BidRecord, 0)
	for _, bid := range r.byID {
		if bid.LiveSessionID != nil && *bid.LiveSessionID == sessionID {
			records = append(records, cloneBidRecord(bid))
		}
	}
	sort.Slice(records, func(i, j int) bool {
		switch sortBy {
		case "timeAsc":
			if records[i].BidTSMS == records[j].BidTSMS {
				return records[i].ID < records[j].ID
			}
			return records[i].BidTSMS < records[j].BidTSMS
		case "priceDesc":
			if records[i].BidPrice == records[j].BidPrice {
				if records[i].BidTSMS == records[j].BidTSMS {
					return records[i].ID < records[j].ID
				}
				return records[i].BidTSMS < records[j].BidTSMS
			}
			return records[i].BidPrice > records[j].BidPrice
		default:
			if records[i].BidTSMS == records[j].BidTSMS {
				return records[i].ID > records[j].ID
			}
			return records[i].BidTSMS > records[j].BidTSMS
		}
	})
	if offset >= len(records) {
		return []domain.BidRecord{}, nil
	}
	end := offset + limit
	if end > len(records) {
		end = len(records)
	}
	return records[offset:end], nil
}

func cloneBidRecord(bid domain.BidRecord) domain.BidRecord {
	bid.LiveSessionID = cloneUint64Ptr(bid.LiveSessionID)
	return bid
}
