package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	auctionports "aieas_backend/internal/modules/auction/ports"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ContextDBResolver func(ctx context.Context, db *gorm.DB) *gorm.DB

type MySQLAuctionRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLAuctionRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLAuctionRepository {
	return &MySQLAuctionRepository{db: db, resolveDB: resolver}
}

func (r *MySQLAuctionRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
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

func (r *MySQLAuctionRepository) ListExpiredActive(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	statuses := []domain.AuctionStatus{
		domain.AuctionStatusRunning,
		domain.AuctionStatusExtended,
		domain.AuctionStatusHammerPending,
	}
	var rows []auctionRow
	if err := r.dbFor(ctx).Table("auction_lot").
		Where("status IN ? AND end_time <= ?", statuses, now.UTC()).
		Order("end_time ASC, auction_id ASC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	auctions := make([]domain.AuctionLot, 0, len(rows))
	for _, row := range rows {
		auctions = append(auctions, row.toDomain())
	}
	return auctions, nil
}

func (r *MySQLAuctionRepository) ListDueScheduled(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []auctionRow
	if err := r.dbFor(ctx).Table("auction_lot").
		Where("status = ? AND live_session_id IS NOT NULL AND start_time <= ?", domain.AuctionStatusWarmingUp, now.UTC()).
		Order("start_time ASC, auction_id ASC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	auctions := make([]domain.AuctionLot, 0, len(rows))
	for _, row := range rows {
		auctions = append(auctions, row.toDomain())
	}
	return auctions, nil
}

func (r *MySQLAuctionRepository) Search(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error) {
	query := r.dbFor(ctx).Table("auction_lot")
	if len(filter.VisibleStatuses) > 0 {
		query = query.Where("status IN ?", filter.VisibleStatuses)
	}
	if filter.Status.Valid() {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.MerchantID != "" {
		query = query.Where("seller_id = ?", normalizeUserIDForDB(filter.MerchantID))
	}
	if values := normalizedCategoryValues(filter.CategoryID, filter.CategoryValues); len(values) > 0 {
		query = query.Where("category IN ?", values)
	}
	if filter.Keyword != "" {
		keyword := "%" + strings.TrimSpace(filter.Keyword) + "%"
		query = query.Where("title LIKE ? OR description LIKE ? OR brand LIKE ?", keyword, keyword, keyword)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []auctionRow
	if err := query.Order(auctionSearchOrder(filter.Sort)).Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, 0, err
	}
	auctions := make([]domain.AuctionLot, 0, len(rows))
	for _, row := range rows {
		auctions = append(auctions, row.toDomain())
	}
	return auctions, total, nil
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

type MemoryAuctionRepository struct {
	mu       sync.RWMutex
	nextID   uint64
	auctions map[uint64]domain.AuctionLot
}

func NewMemoryAuctionRepository() *MemoryAuctionRepository {
	return &MemoryAuctionRepository{nextID: 10001, auctions: make(map[uint64]domain.AuctionLot)}
}

func (r *MemoryAuctionRepository) Create(ctx context.Context, auction *domain.AuctionLot) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if auction.AuctionID == 0 {
		auction.AuctionID = r.nextID
		r.nextID++
	} else if auction.AuctionID >= r.nextID {
		r.nextID = auction.AuctionID + 1
	}
	now := time.Now().UTC()
	if auction.CreatedAt.IsZero() {
		auction.CreatedAt = now
	}
	auction.UpdatedAt = now
	r.auctions[auction.AuctionID] = cloneAuction(*auction)
	return nil
}

func (r *MemoryAuctionRepository) FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	auction, ok := r.auctions[id]
	if !ok {
		return domain.AuctionLot{}, domain.ErrNotFound
	}
	return cloneAuction(auction), nil
}

func (r *MemoryAuctionRepository) List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint64, 0, len(r.auctions))
	for id := range r.auctions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	auctions := make([]domain.AuctionLot, 0, len(ids))
	for _, id := range ids {
		auction := r.auctions[id]
		if filter.SellerID != "" && auction.SellerID != filter.SellerID {
			continue
		}
		if filter.Status.Valid() && auction.Status != filter.Status {
			continue
		}
		if filter.Category != "" && auction.Category != filter.Category {
			continue
		}
		if filter.Keyword != "" {
			keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
			haystack := strings.ToLower(auction.Title + " " + auction.Description + " " + auction.Brand)
			if !strings.Contains(haystack, keyword) {
				continue
			}
		}
		if filter.LiveSessionID != 0 {
			if auction.LiveSessionID == nil || *auction.LiveSessionID != filter.LiveSessionID {
				continue
			}
		}
		auctions = append(auctions, cloneAuction(auction))
	}
	return paginateAuctions(auctions, filter.Limit, filter.Offset), nil
}

func (r *MemoryAuctionRepository) ListExpiredActive(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error) {
	_ = ctx
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	active := map[domain.AuctionStatus]bool{
		domain.AuctionStatusRunning:       true,
		domain.AuctionStatusExtended:      true,
		domain.AuctionStatusHammerPending: true,
	}
	auctions := make([]domain.AuctionLot, 0)
	for _, auction := range r.auctions {
		if !active[auction.Status] {
			continue
		}
		if auction.EndTime.IsZero() || auction.EndTime.After(now) {
			continue
		}
		auctions = append(auctions, cloneAuction(auction))
	}
	sort.Slice(auctions, func(i, j int) bool {
		if auctions[i].EndTime.Equal(auctions[j].EndTime) {
			return auctions[i].AuctionID < auctions[j].AuctionID
		}
		return auctions[i].EndTime.Before(auctions[j].EndTime)
	})
	if len(auctions) > limit {
		auctions = auctions[:limit]
	}
	return auctions, nil
}

func (r *MemoryAuctionRepository) ListDueScheduled(ctx context.Context, now time.Time, limit int) ([]domain.AuctionLot, error) {
	_ = ctx
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	now = now.UTC()
	r.mu.RLock()
	defer r.mu.RUnlock()
	auctions := make([]domain.AuctionLot, 0)
	for _, auction := range r.auctions {
		if auction.Status != domain.AuctionStatusWarmingUp {
			continue
		}
		if auction.LiveSessionID == nil || *auction.LiveSessionID == 0 {
			continue
		}
		if auction.StartTime.IsZero() || auction.StartTime.After(now) {
			continue
		}
		auctions = append(auctions, cloneAuction(auction))
	}
	sort.Slice(auctions, func(i, j int) bool {
		if auctions[i].StartTime.Equal(auctions[j].StartTime) {
			return auctions[i].AuctionID < auctions[j].AuctionID
		}
		return auctions[i].StartTime.Before(auctions[j].StartTime)
	})
	if len(auctions) > limit {
		auctions = auctions[:limit]
	}
	return auctions, nil
}

func (r *MemoryAuctionRepository) Search(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	statusSet := make(map[domain.AuctionStatus]struct{}, len(filter.VisibleStatuses))
	for _, status := range filter.VisibleStatuses {
		if status.Valid() {
			statusSet[status] = struct{}{}
		}
	}
	categorySet := make(map[string]struct{})
	for _, category := range normalizedCategoryValues(filter.CategoryID, filter.CategoryValues) {
		categorySet[category] = struct{}{}
	}
	keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
	auctions := make([]domain.AuctionLot, 0, len(r.auctions))
	for _, auction := range r.auctions {
		if len(statusSet) > 0 {
			if _, ok := statusSet[auction.Status]; !ok {
				continue
			}
		}
		if filter.Status.Valid() && auction.Status != filter.Status {
			continue
		}
		if filter.MerchantID != "" && auction.SellerID != filter.MerchantID {
			continue
		}
		if len(categorySet) > 0 {
			if _, ok := categorySet[auction.Category]; !ok {
				continue
			}
		}
		if keyword != "" {
			haystack := strings.ToLower(auction.Title + " " + auction.Description + " " + auction.Brand)
			if !strings.Contains(haystack, keyword) {
				continue
			}
		}
		auctions = append(auctions, cloneAuction(auction))
	}
	sortAuctionsForSearch(auctions, filter.Sort)
	total := int64(len(auctions))
	return paginateAuctions(auctions, filter.Limit, filter.Offset), total, nil
}

func (r *MemoryAuctionRepository) Update(ctx context.Context, auction *domain.AuctionLot) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.auctions[auction.AuctionID]; !ok {
		return domain.ErrNotFound
	}
	auction.UpdatedAt = time.Now().UTC()
	r.auctions[auction.AuctionID] = cloneAuction(*auction)
	return nil
}

func (r *MemoryAuctionRepository) SnapshotAuctions() []domain.AuctionLot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.AuctionLot, 0, len(r.auctions))
	for _, auction := range r.auctions {
		out = append(out, cloneAuction(auction))
	}
	return out
}

func (r *MemoryAuctionRepository) Delete(ctx context.Context, id uint64) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.auctions[id]; !ok {
		return domain.ErrNotFound
	}
	delete(r.auctions, id)
	return nil
}

func (r *MemoryAuctionRepository) CloseWithVersion(ctx context.Context, auction *domain.AuctionLot, expectedVersion int64, allowedFromStatuses []domain.AuctionStatus) error {
	_ = ctx
	if auction == nil || auction.AuctionID == 0 {
		return domain.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.auctions[auction.AuctionID]
	if !ok {
		return domain.ErrNotFound
	}
	if current.Version != expectedVersion || !statusInList(current.Status, allowedFromStatuses) {
		if current.Status.Terminal() {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	now := time.Now().UTC()
	current.Status = auction.Status
	current.WinnerID = auction.WinnerID
	current.DealPrice = auction.DealPrice
	current.ClosedAt = auction.ClosedAt
	current.ClosedBy = auction.ClosedBy
	current.Version = expectedVersion + 1
	current.UpdatedAt = now
	r.auctions[auction.AuctionID] = cloneAuction(current)
	auction.Version = current.Version
	auction.UpdatedAt = now
	return nil
}

type MySQLBidRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLBidRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLBidRepository {
	return &MySQLBidRepository{db: db, resolveDB: resolver}
}

func (r *MySQLBidRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
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

func (r *MySQLBidRepository) CreateIgnoreBatch(ctx context.Context, records []domain.BidRecord) error {
	if len(records) == 0 {
		return nil
	}
	rows := make([]bidRecordRow, 0, len(records))
	for _, rec := range records {
		rows = append(rows, bidRecordRowFromDomain(rec))
	}
	return r.dbFor(ctx).Table("bid_record").
		Clauses(clause.Insert{Modifier: "IGNORE"}).
		CreateInBatches(&rows, bidRecordBatchInsertSize).Error
}

const bidRecordBatchInsertSize = 256

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
	if err := r.dbFor(ctx).
		Table("bid_record").
		Where("auction_id = ? AND risk_result = ? AND reject_reason = ''", auctionID, domain.BidRiskAllow).
		Order("bid_price DESC, bid_ts_ms ASC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	records := make([]domain.BidRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.toDomain())
	}
	return records, nil
}

func (r *MySQLBidRepository) ListByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64, limit int) ([]domain.BidRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var rows []bidRecordRow
	query := r.dbFor(ctx).
		Table("bid_record").
		Where("auction_id = ? AND risk_result = ? AND reject_reason = ''", auctionID, domain.BidRiskAllow)
	if sinceTSMS > 0 {
		query = query.Where("bid_ts_ms >= ?", sinceTSMS)
	}
	if err := query.
		Order("bid_price DESC, bid_ts_ms ASC").
		Limit(limit).
		Find(&rows).Error; err != nil {
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
	if err := r.dbFor(ctx).
		Table("bid_record").
		Where("auction_id = ? AND risk_result = ? AND reject_reason = ''", auctionID, domain.BidRiskAllow).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return int(count), nil
}

func (r *MySQLBidRepository) CountByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64) (int, error) {
	var count int64
	query := r.dbFor(ctx).
		Table("bid_record").
		Where("auction_id = ? AND risk_result = ? AND reject_reason = ''", auctionID, domain.BidRiskAllow)
	if sinceTSMS > 0 {
		query = query.Where("bid_ts_ms >= ?", sinceTSMS)
	}
	if err := query.Count(&count).Error; err != nil {
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
		Where("live_session_id = ? AND risk_result = ? AND reject_reason = ''", sessionID, domain.BidRiskAllow).
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

func (r *MemoryBidRepository) CreateIgnoreBatch(ctx context.Context, records []domain.BidRecord) error {
	_ = ctx
	if len(records) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(records))
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range records {
		if rec.RequestID != "" {
			if _, dup := seen[rec.RequestID]; dup {
				continue
			}
			seen[rec.RequestID] = struct{}{}
			if _, ok := r.byRequest[rec.RequestID]; ok {
				continue
			}
		}
		stored := rec
		if stored.ID == 0 {
			stored.ID = r.nextID
			r.nextID++
		}
		if stored.CreatedAt.IsZero() {
			stored.CreatedAt = time.Now().UTC()
		}
		r.byID[stored.ID] = cloneBidRecord(stored)
		if stored.RequestID != "" {
			r.byRequest[stored.RequestID] = stored.ID
		}
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
		if bid.AuctionID == auctionID && bid.RiskResult == domain.BidRiskAllow && bid.RejectReason == "" {
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

func (r *MemoryBidRepository) ListByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64, limit int) ([]domain.BidRecord, error) {
	_ = ctx
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	records := make([]domain.BidRecord, 0)
	for _, bid := range r.byID {
		if bid.AuctionID == auctionID && bid.BidTSMS >= sinceTSMS && bid.RiskResult == domain.BidRiskAllow && bid.RejectReason == "" {
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
		if bid.AuctionID == auctionID && bid.RiskResult == domain.BidRiskAllow && bid.RejectReason == "" {
			count++
		}
	}
	return count, nil
}

func (r *MemoryBidRepository) CountByAuctionSince(ctx context.Context, auctionID uint64, sinceTSMS int64) (int, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, bid := range r.byID {
		if bid.AuctionID == auctionID && bid.BidTSMS >= sinceTSMS && bid.RiskResult == domain.BidRiskAllow && bid.RejectReason == "" {
			count++
		}
	}
	return count, nil
}

func (r *MemoryBidRepository) SnapshotBids() []domain.BidRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.BidRecord, 0, len(r.byID))
	for _, bid := range r.byID {
		out = append(out, cloneBidRecord(bid))
	}
	return out
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
		if bid.LiveSessionID != nil && *bid.LiveSessionID == sessionID && bid.RiskResult == domain.BidRiskAllow && bid.RejectReason == "" {
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

func auctionSearchOrder(sortBy string) string {
	switch strings.TrimSpace(sortBy) {
	case "priceAsc":
		return "start_price ASC, auction_id DESC"
	case "priceDesc":
		return "start_price DESC, auction_id DESC"
	case "endingSoon":
		return "end_time ASC, auction_id DESC"
	case "startTimeAsc":
		return "start_time ASC, auction_id ASC"
	case "startTimeDesc", "latest", "newest":
		return "start_time DESC, auction_id DESC"
	default:
		return "auction_id DESC"
	}
}

func normalizedCategoryValues(categoryID string, values []string) []string {
	seen := make(map[string]struct{}, len(values)+1)
	out := make([]string, 0, len(values)+1)
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	add(categoryID)
	for _, value := range values {
		add(value)
	}
	return out
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

func statusInList(s domain.AuctionStatus, list []domain.AuctionStatus) bool {
	if len(list) == 0 {
		return true
	}
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func cloneAuction(auction domain.AuctionLot) domain.AuctionLot {
	auction.IncrementRule = append([]byte(nil), auction.IncrementRule...)
	auction.RuleSnapshot = append([]byte(nil), auction.RuleSnapshot...)
	auction.ImageURLs = append([]string(nil), auction.ImageURLs...)
	auction.LiveSessionID = cloneUint64Ptr(auction.LiveSessionID)
	return auction
}

func paginateAuctions(auctions []domain.AuctionLot, limit, offset int) []domain.AuctionLot {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset >= len(auctions) {
		return []domain.AuctionLot{}
	}
	end := offset + limit
	if end > len(auctions) {
		end = len(auctions)
	}
	return auctions[offset:end]
}

func sortAuctionsForSearch(auctions []domain.AuctionLot, sortBy string) {
	switch strings.TrimSpace(sortBy) {
	case "priceAsc":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartPrice == auctions[j].StartPrice {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			return auctions[i].StartPrice < auctions[j].StartPrice
		})
	case "priceDesc":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartPrice == auctions[j].StartPrice {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			return auctions[i].StartPrice > auctions[j].StartPrice
		})
	case "endingSoon":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].EndTime.Equal(auctions[j].EndTime) {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			if auctions[i].EndTime.IsZero() {
				return false
			}
			if auctions[j].EndTime.IsZero() {
				return true
			}
			return auctions[i].EndTime.Before(auctions[j].EndTime)
		})
	case "startTimeAsc":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartTime.Equal(auctions[j].StartTime) {
				return auctions[i].AuctionID < auctions[j].AuctionID
			}
			if auctions[i].StartTime.IsZero() {
				return false
			}
			if auctions[j].StartTime.IsZero() {
				return true
			}
			return auctions[i].StartTime.Before(auctions[j].StartTime)
		})
	case "startTimeDesc", "latest", "newest":
		sort.Slice(auctions, func(i, j int) bool {
			if auctions[i].StartTime.Equal(auctions[j].StartTime) {
				return auctions[i].AuctionID > auctions[j].AuctionID
			}
			return auctions[i].StartTime.After(auctions[j].StartTime)
		})
	default:
		sort.Slice(auctions, func(i, j int) bool { return auctions[i].AuctionID > auctions[j].AuctionID })
	}
}

func cloneBidRecord(bid domain.BidRecord) domain.BidRecord {
	bid.LiveSessionID = cloneUint64Ptr(bid.LiveSessionID)
	return bid
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

func cloneUint64Ptr(p *uint64) *uint64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneTimePtrUTC(p *time.Time) *time.Time {
	if p == nil {
		return nil
	}
	v := p.UTC()
	return &v
}

var _ auctionports.AuctionRepository = (*MySQLAuctionRepository)(nil)
var _ auctionports.AuctionRepository = (*MemoryAuctionRepository)(nil)
var _ auctionports.BidRepository = (*MySQLBidRepository)(nil)
var _ auctionports.BidRoundRepository = (*MySQLBidRepository)(nil)
var _ auctionports.BidRepository = (*MemoryBidRepository)(nil)
var _ auctionports.BidRoundRepository = (*MemoryBidRepository)(nil)
