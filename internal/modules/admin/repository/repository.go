package repository

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	adminports "aieas_backend/internal/modules/admin/ports"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ContextDBResolver func(ctx context.Context, db *gorm.DB) *gorm.DB

type MySQLConfigRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLConfigRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLConfigRepository {
	return &MySQLConfigRepository{db: db, resolveDB: resolver}
}

func (r *MySQLConfigRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLConfigRepository) FindByKey(ctx context.Context, key string) (domain.ConfigItem, error) {
	var row configItemRow
	err := r.dbFor(ctx).Table("config_item").Where("config_key = ?", key).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.ConfigItem{}, domain.ErrNotFound
		}
		return domain.ConfigItem{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLConfigRepository) Upsert(ctx context.Context, item *domain.ConfigItem) error {
	row := configItemRowFromDomain(*item)
	now := time.Now().UTC()
	row.UpdatedAt = now
	if err := r.dbFor(ctx).Table("config_item").Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "config_key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"config_value": row.Value,
			"description":  row.Description,
			"updated_by":   row.UpdatedBy,
			"updated_at":   now,
		}),
	}).Create(&row).Error; err != nil {
		return err
	}
	*item = row.toDomain()
	return nil
}

type MemoryConfigRepository struct {
	mu    sync.RWMutex
	items map[string]domain.ConfigItem
}

func NewMemoryConfigRepository() *MemoryConfigRepository {
	return &MemoryConfigRepository{items: make(map[string]domain.ConfigItem)}
}

func (r *MemoryConfigRepository) FindByKey(ctx context.Context, key string) (domain.ConfigItem, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.items[key]
	if !ok {
		return domain.ConfigItem{}, domain.ErrNotFound
	}
	item.Value = append([]byte(nil), item.Value...)
	return item, nil
}

func (r *MemoryConfigRepository) Upsert(ctx context.Context, item *domain.ConfigItem) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = time.Now().UTC()
	}
	clone := *item
	clone.Value = append([]byte(nil), item.Value...)
	r.items[item.Key] = clone
	return nil
}

type MySQLAuditRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLAuditRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLAuditRepository {
	return &MySQLAuditRepository{db: db, resolveDB: resolver}
}

func (r *MySQLAuditRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
	}
	return r.db.WithContext(ctx)
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
	if err := r.dbFor(ctx).Table("audit_log").Create(&row).Error; err != nil {
		return err
	}
	log.ID = row.ID
	log.CreatedAt = row.CreatedAt
	return nil
}

func (r *MySQLAuditRepository) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error) {
	query := r.dbFor(ctx).Table("audit_log").Order("id DESC")
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

type MySQLAdminDashboardRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLAdminDashboardRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLAdminDashboardRepository {
	return &MySQLAdminDashboardRepository{db: db, resolveDB: resolver}
}

func (r *MySQLAdminDashboardRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLAdminDashboardRepository) DashboardMetrics(ctx context.Context, filter domain.AdminDashboardMetricsFilter) (domain.AdminDashboardMetrics, error) {
	db := r.dbFor(ctx)
	result := domain.AdminDashboardMetrics{
		StartTime:   filter.StartTime,
		EndTime:     filter.EndTime,
		Bucket:      filter.Bucket,
		GeneratedAt: time.Now().UTC(),
	}
	if err := r.loadCurrent(db, &result.Current); err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	if err := r.loadSummary(db, filter, &result.Summary); err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	breakdowns, err := r.loadBreakdowns(db, filter)
	if err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	result.Breakdowns = breakdowns
	trend, err := r.loadTrend(db, filter)
	if err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	result.Trend = trend
	return result, nil
}

func (r *MySQLAdminDashboardRepository) loadCurrent(db *gorm.DB, current *domain.AdminDashboardCurrent) error {
	if err := db.Table("live_session").Where("status = ?", domain.LiveSessionStatusLive).Count(&current.ActiveLiveSessionCount).Error; err != nil {
		return err
	}
	runningStatuses := []domain.AuctionStatus{
		domain.AuctionStatusRunning,
		domain.AuctionStatusExtended,
		domain.AuctionStatusHammerPending,
	}
	if err := db.Table("auction_lot").Where("status IN ?", runningStatuses).Count(&current.RunningAuctionCount).Error; err != nil {
		return err
	}
	return db.Table("risk_event").Where("status = ?", domain.RiskEventPending).Count(&current.PendingRiskEventCount).Error
}

func (r *MySQLAdminDashboardRepository) loadSummary(db *gorm.DB, filter domain.AdminDashboardMetricsFilter, summary *domain.AdminDashboardSummary) error {
	start, end := filter.StartTime, filter.EndTime
	var orders struct {
		OrderCreatedCount   int64 `gorm:"column:order_created_count"`
		UnpaidOrderCount    int64 `gorm:"column:unpaid_order_count"`
		TimeoutOrderCount   int64 `gorm:"column:timeout_order_count"`
		CancelledOrderCount int64 `gorm:"column:cancelled_order_count"`
	}
	if err := db.Table("order_deal").
		Select(`COUNT(*) AS order_created_count,
			COALESCE(SUM(CASE WHEN pay_status = ? THEN 1 ELSE 0 END), 0) AS unpaid_order_count,
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS timeout_order_count,
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS cancelled_order_count`,
			domain.PayStatusUnpaid,
			domain.OrderStatusTimeout,
			domain.OrderStatusCancelled,
		).
		Where("created_at >= ? AND created_at < ?", start, end).
		Scan(&orders).Error; err != nil {
		return err
	}
	summary.OrderCreatedCount = orders.OrderCreatedCount
	summary.UnpaidOrderCount = orders.UnpaidOrderCount
	summary.TimeoutOrderCount = orders.TimeoutOrderCount
	summary.CancelledOrderCount = orders.CancelledOrderCount

	var paid struct {
		PaidOrderCount int64 `gorm:"column:paid_order_count"`
		PaidGMVCent    int64 `gorm:"column:paid_gmv_cent"`
	}
	if err := db.Table("order_deal").
		Select("COUNT(*) AS paid_order_count, COALESCE(SUM(deal_price), 0) AS paid_gmv_cent").
		Where("pay_status = ? AND paid_at >= ? AND paid_at < ?", domain.PayStatusPaid, start, end).
		Scan(&paid).Error; err != nil {
		return err
	}
	summary.PaidOrderCount = paid.PaidOrderCount
	summary.PaidGMVCent = paid.PaidGMVCent

	if err := db.Table("auction_lot").
		Where("created_at >= ? AND created_at < ?", start, end).
		Count(&summary.AuctionCreatedCount).Error; err != nil {
		return err
	}
	var closed struct {
		ClosedWonAuctionCount    int64 `gorm:"column:closed_won_auction_count"`
		ClosedFailedAuctionCount int64 `gorm:"column:closed_failed_auction_count"`
		DealGMVCent              int64 `gorm:"column:deal_gmv_cent"`
	}
	if err := db.Table("auction_lot").
		Select(`COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS closed_won_auction_count,
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS closed_failed_auction_count,
			COALESCE(SUM(CASE WHEN status = ? THEN COALESCE(deal_price, 0) ELSE 0 END), 0) AS deal_gmv_cent`,
			domain.AuctionStatusClosedWon,
			domain.AuctionStatusClosedFailed,
			domain.AuctionStatusClosedWon,
		).
		Where("closed_at >= ? AND closed_at < ?", start, end).
		Scan(&closed).Error; err != nil {
		return err
	}
	summary.ClosedWonAuctionCount = closed.ClosedWonAuctionCount
	summary.ClosedFailedAuctionCount = closed.ClosedFailedAuctionCount
	summary.DealGMVCent = closed.DealGMVCent

	var bids struct {
		BidCount          int64 `gorm:"column:bid_count"`
		ActiveBidderCount int64 `gorm:"column:active_bidder_count"`
	}
	if err := db.Table("bid_record").
		Select("COUNT(*) AS bid_count, COUNT(DISTINCT bidder_id) AS active_bidder_count").
		Where("created_at >= ? AND created_at < ?", start, end).
		Scan(&bids).Error; err != nil {
		return err
	}
	summary.BidCount = bids.BidCount
	summary.ActiveBidderCount = bids.ActiveBidderCount

	if err := db.Table("risk_event").
		Where("created_at >= ? AND created_at < ?", start, end).
		Count(&summary.RiskEventCount).Error; err != nil {
		return err
	}

	var sessions struct {
		LiveSessionCount int64 `gorm:"column:live_session_count"`
		LotsTotal        int64 `gorm:"column:lots_total"`
		LotsSold         int64 `gorm:"column:lots_sold"`
		LotsUnsold       int64 `gorm:"column:lots_unsold"`
		ViewerPeak       int64 `gorm:"column:viewer_peak"`
		ViewerTotal      int64 `gorm:"column:viewer_total"`
	}
	if err := db.Table("live_session").
		Select(`COUNT(*) AS live_session_count,
			COALESCE(SUM(lots_total), 0) AS lots_total,
			COALESCE(SUM(lots_sold), 0) AS lots_sold,
			COALESCE(SUM(lots_unsold), 0) AS lots_unsold,
			COALESCE(MAX(viewer_peak), 0) AS viewer_peak,
			COALESCE(SUM(viewer_total), 0) AS viewer_total`).
		Where("opened_at >= ? AND opened_at < ?", start, end).
		Scan(&sessions).Error; err != nil {
		return err
	}
	summary.LiveSessionCount = sessions.LiveSessionCount
	summary.LotsTotal = sessions.LotsTotal
	summary.LotsSold = sessions.LotsSold
	summary.LotsUnsold = sessions.LotsUnsold
	summary.ViewerPeak = sessions.ViewerPeak
	summary.ViewerTotal = sessions.ViewerTotal
	return nil
}

func (r *MySQLAdminDashboardRepository) loadBreakdowns(db *gorm.DB, filter domain.AdminDashboardMetricsFilter) (domain.AdminDashboardBreakdowns, error) {
	start, end := filter.StartTime, filter.EndTime
	var out domain.AdminDashboardBreakdowns
	var rows []statusCountRow
	if err := db.Table("auction_lot").
		Select("status, COUNT(*) AS count").
		Where("created_at >= ? AND created_at < ?", start, end).
		Group("status").
		Order("status ASC").
		Scan(&rows).Error; err != nil {
		return out, err
	}
	out.AuctionStatus = statusCountRows(rows)

	rows = nil
	if err := db.Table("order_deal").
		Select("status, COUNT(*) AS count").
		Where("created_at >= ? AND created_at < ?", start, end).
		Group("status").
		Order("status ASC").
		Scan(&rows).Error; err != nil {
		return out, err
	}
	out.OrderStatus = statusCountRows(rows)

	rows = nil
	if err := db.Table("order_deal").
		Select("pay_status AS status, COUNT(*) AS count").
		Where("created_at >= ? AND created_at < ?", start, end).
		Group("pay_status").
		Order("pay_status ASC").
		Scan(&rows).Error; err != nil {
		return out, err
	}
	out.PayStatus = statusCountRows(rows)

	rows = nil
	if err := db.Table("risk_event").
		Select("status, COUNT(*) AS count").
		Where("created_at >= ? AND created_at < ?", start, end).
		Group("status").
		Order("status ASC").
		Scan(&rows).Error; err != nil {
		return out, err
	}
	out.RiskStatus = statusCountRows(rows)
	return out, nil
}

func (r *MySQLAdminDashboardRepository) loadTrend(db *gorm.DB, filter domain.AdminDashboardMetricsFilter) ([]domain.AdminDashboardTrendPoint, error) {
	trend, keys := newDashboardTrend(filter)
	bucketSec := int64(dashboardBucketDuration(filter.Bucket).Seconds())
	start, end := filter.StartTime, filter.EndTime

	var moneyRows []bucketValueRow
	if err := db.Table("auction_lot").
		Select("CAST(FLOOR(UNIX_TIMESTAMP(closed_at) / ?) * ? AS SIGNED) AS bucket_unix, COALESCE(SUM(COALESCE(deal_price, 0)), 0) AS value", bucketSec, bucketSec).
		Where("status = ? AND closed_at >= ? AND closed_at < ?", domain.AuctionStatusClosedWon, start, end).
		Group("bucket_unix").
		Scan(&moneyRows).Error; err != nil {
		return nil, err
	}
	for _, row := range moneyRows {
		if point := trend[row.BucketUnix]; point != nil {
			point.DealGMVCent = row.Value
		}
	}

	moneyRows = nil
	if err := db.Table("order_deal").
		Select("CAST(FLOOR(UNIX_TIMESTAMP(paid_at) / ?) * ? AS SIGNED) AS bucket_unix, COALESCE(SUM(deal_price), 0) AS value", bucketSec, bucketSec).
		Where("pay_status = ? AND paid_at >= ? AND paid_at < ?", domain.PayStatusPaid, start, end).
		Group("bucket_unix").
		Scan(&moneyRows).Error; err != nil {
		return nil, err
	}
	for _, row := range moneyRows {
		if point := trend[row.BucketUnix]; point != nil {
			point.PaidGMVCent = row.Value
		}
	}

	var countRows []bucketValueRow
	if err := db.Table("order_deal").
		Select("CAST(FLOOR(UNIX_TIMESTAMP(paid_at) / ?) * ? AS SIGNED) AS bucket_unix, COUNT(*) AS value", bucketSec, bucketSec).
		Where("pay_status = ? AND paid_at >= ? AND paid_at < ?", domain.PayStatusPaid, start, end).
		Group("bucket_unix").
		Scan(&countRows).Error; err != nil {
		return nil, err
	}
	for _, row := range countRows {
		if point := trend[row.BucketUnix]; point != nil {
			point.PaidOrderCount = row.Value
		}
	}

	countRows = nil
	if err := db.Table("bid_record").
		Select("CAST(FLOOR(UNIX_TIMESTAMP(created_at) / ?) * ? AS SIGNED) AS bucket_unix, COUNT(*) AS value", bucketSec, bucketSec).
		Where("created_at >= ? AND created_at < ?", start, end).
		Group("bucket_unix").
		Scan(&countRows).Error; err != nil {
		return nil, err
	}
	for _, row := range countRows {
		if point := trend[row.BucketUnix]; point != nil {
			point.BidCount = row.Value
		}
	}

	countRows = nil
	if err := db.Table("risk_event").
		Select("CAST(FLOOR(UNIX_TIMESTAMP(created_at) / ?) * ? AS SIGNED) AS bucket_unix, COUNT(*) AS value", bucketSec, bucketSec).
		Where("created_at >= ? AND created_at < ?", start, end).
		Group("bucket_unix").
		Scan(&countRows).Error; err != nil {
		return nil, err
	}
	for _, row := range countRows {
		if point := trend[row.BucketUnix]; point != nil {
			point.RiskEventCount = row.Value
		}
	}
	return dashboardTrendSlice(trend, keys), nil
}

type AuctionSnapshotSource interface {
	SnapshotAuctions() []domain.AuctionLot
}

type LiveSessionSnapshotSource interface {
	SnapshotLiveSessions() []domain.LiveSession
}

type BidSnapshotSource interface {
	SnapshotBids() []domain.BidRecord
}

type OrderSnapshotSource interface {
	SnapshotOrders() []domain.OrderDeal
}

type RiskEventSnapshotSource interface {
	SnapshotRiskEvents() []domain.RiskEvent
}

type MemoryAdminDashboardRepository struct {
	auctions AuctionSnapshotSource
	sessions LiveSessionSnapshotSource
	bids     BidSnapshotSource
	orders   OrderSnapshotSource
	risk     RiskEventSnapshotSource
}

func NewMemoryAdminDashboardRepository(auctions AuctionSnapshotSource, sessions LiveSessionSnapshotSource, bids BidSnapshotSource, orders OrderSnapshotSource, risk RiskEventSnapshotSource) *MemoryAdminDashboardRepository {
	return &MemoryAdminDashboardRepository{
		auctions: auctions,
		sessions: sessions,
		bids:     bids,
		orders:   orders,
		risk:     risk,
	}
}

func (r *MemoryAdminDashboardRepository) DashboardMetrics(ctx context.Context, filter domain.AdminDashboardMetricsFilter) (domain.AdminDashboardMetrics, error) {
	_ = ctx
	result := domain.AdminDashboardMetrics{
		StartTime:   filter.StartTime,
		EndTime:     filter.EndTime,
		Bucket:      filter.Bucket,
		GeneratedAt: time.Now().UTC(),
	}
	trend, keys := newDashboardTrend(filter)
	auctionStatus := make(map[string]int64)
	orderStatus := make(map[string]int64)
	payStatus := make(map[string]int64)
	riskStatus := make(map[string]int64)

	for _, session := range r.snapshotSessions() {
		if session.Status == domain.LiveSessionStatusLive {
			result.Current.ActiveLiveSessionCount++
		}
		if session.OpenedAt == nil || !inDashboardRange(*session.OpenedAt, filter.StartTime, filter.EndTime) {
			continue
		}
		result.Summary.LiveSessionCount++
		result.Summary.LotsTotal += int64(session.LotsTotal)
		result.Summary.LotsSold += int64(session.LotsSold)
		result.Summary.LotsUnsold += int64(session.LotsUnsold)
		if int64(session.ViewerPeak) > result.Summary.ViewerPeak {
			result.Summary.ViewerPeak = int64(session.ViewerPeak)
		}
		result.Summary.ViewerTotal += int64(session.ViewerTotal)
	}
	for _, auction := range r.snapshotAuctions() {
		if dashboardRunningAuction(auction.Status) {
			result.Current.RunningAuctionCount++
		}
		if inDashboardRange(auction.CreatedAt, filter.StartTime, filter.EndTime) {
			result.Summary.AuctionCreatedCount++
			auctionStatus[string(auction.Status)]++
		}
		if auction.ClosedAt != nil && inDashboardRange(*auction.ClosedAt, filter.StartTime, filter.EndTime) {
			switch auction.Status {
			case domain.AuctionStatusClosedWon:
				result.Summary.ClosedWonAuctionCount++
				if auction.DealPrice != nil {
					result.Summary.DealGMVCent += *auction.DealPrice
					if point := trend[dashboardBucketUnix(*auction.ClosedAt, filter.Bucket)]; point != nil {
						point.DealGMVCent += *auction.DealPrice
					}
				}
			case domain.AuctionStatusClosedFailed:
				result.Summary.ClosedFailedAuctionCount++
			}
		}
	}
	for _, order := range r.snapshotOrders() {
		if inDashboardRange(order.CreatedAt, filter.StartTime, filter.EndTime) {
			result.Summary.OrderCreatedCount++
			orderStatus[string(order.Status)]++
			payStatus[string(order.PayStatus)]++
			if order.PayStatus == domain.PayStatusUnpaid {
				result.Summary.UnpaidOrderCount++
			}
			if order.Status == domain.OrderStatusTimeout {
				result.Summary.TimeoutOrderCount++
			}
			if order.Status == domain.OrderStatusCancelled {
				result.Summary.CancelledOrderCount++
			}
		}
		if order.PayStatus == domain.PayStatusPaid && order.PaidAt != nil && inDashboardRange(*order.PaidAt, filter.StartTime, filter.EndTime) {
			result.Summary.PaidOrderCount++
			result.Summary.PaidGMVCent += order.DealPrice
			if point := trend[dashboardBucketUnix(*order.PaidAt, filter.Bucket)]; point != nil {
				point.PaidGMVCent += order.DealPrice
				point.PaidOrderCount++
			}
		}
	}
	bidders := make(map[string]struct{})
	for _, bid := range r.snapshotBids() {
		if !inDashboardRange(bid.CreatedAt, filter.StartTime, filter.EndTime) {
			continue
		}
		result.Summary.BidCount++
		if bid.BidderID != "" {
			bidders[bid.BidderID] = struct{}{}
		}
		if point := trend[dashboardBucketUnix(bid.CreatedAt, filter.Bucket)]; point != nil {
			point.BidCount++
		}
	}
	result.Summary.ActiveBidderCount = int64(len(bidders))
	for _, event := range r.snapshotRiskEvents() {
		if event.Status == domain.RiskEventPending {
			result.Current.PendingRiskEventCount++
		}
		if !inDashboardRange(event.CreatedAt, filter.StartTime, filter.EndTime) {
			continue
		}
		result.Summary.RiskEventCount++
		riskStatus[string(event.Status)]++
		if point := trend[dashboardBucketUnix(event.CreatedAt, filter.Bucket)]; point != nil {
			point.RiskEventCount++
		}
	}
	result.Breakdowns = domain.AdminDashboardBreakdowns{
		AuctionStatus: dashboardStatusCounts(auctionStatus),
		OrderStatus:   dashboardStatusCounts(orderStatus),
		PayStatus:     dashboardStatusCounts(payStatus),
		RiskStatus:    dashboardStatusCounts(riskStatus),
	}
	result.Trend = dashboardTrendSlice(trend, keys)
	return result, nil
}

func (r *MemoryAdminDashboardRepository) snapshotAuctions() []domain.AuctionLot {
	if r == nil || r.auctions == nil {
		return nil
	}
	return r.auctions.SnapshotAuctions()
}

func (r *MemoryAdminDashboardRepository) snapshotSessions() []domain.LiveSession {
	if r == nil || r.sessions == nil {
		return nil
	}
	return r.sessions.SnapshotLiveSessions()
}

func (r *MemoryAdminDashboardRepository) snapshotBids() []domain.BidRecord {
	if r == nil || r.bids == nil {
		return nil
	}
	return r.bids.SnapshotBids()
}

func (r *MemoryAdminDashboardRepository) snapshotOrders() []domain.OrderDeal {
	if r == nil || r.orders == nil {
		return nil
	}
	return r.orders.SnapshotOrders()
}

func (r *MemoryAdminDashboardRepository) snapshotRiskEvents() []domain.RiskEvent {
	if r == nil || r.risk == nil {
		return nil
	}
	return r.risk.SnapshotRiskEvents()
}

type configItemRow struct {
	Key         string    `gorm:"column:config_key;primaryKey"`
	Value       []byte    `gorm:"column:config_value"`
	Description string    `gorm:"column:description"`
	UpdatedBy   *string   `gorm:"column:updated_by"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

func configItemRowFromDomain(item domain.ConfigItem) configItemRow {
	var updatedBy *string
	if normalized := normalizeUserIDForDB(item.UpdatedBy); normalized != "" {
		updatedBy = &normalized
	}
	return configItemRow{
		Key:         item.Key,
		Value:       append([]byte(nil), item.Value...),
		Description: item.Description,
		UpdatedBy:   updatedBy,
		UpdatedAt:   item.UpdatedAt,
	}
}

func (r configItemRow) toDomain() domain.ConfigItem {
	updatedBy := ""
	if r.UpdatedBy != nil {
		updatedBy = *r.UpdatedBy
	}
	return domain.ConfigItem{
		Key:         r.Key,
		Value:       append([]byte(nil), r.Value...),
		Description: r.Description,
		UpdatedBy:   updatedBy,
		UpdatedAt:   r.UpdatedAt,
	}
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

type statusCountRow struct {
	Status string `gorm:"column:status"`
	Count  int64  `gorm:"column:count"`
}

func statusCountRows(rows []statusCountRow) []domain.AdminStatusCount {
	out := make([]domain.AdminStatusCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.AdminStatusCount{Status: row.Status, Count: row.Count})
	}
	return out
}

type bucketValueRow struct {
	BucketUnix int64 `gorm:"column:bucket_unix"`
	Value      int64 `gorm:"column:value"`
}

func newDashboardTrend(filter domain.AdminDashboardMetricsFilter) (map[int64]*domain.AdminDashboardTrendPoint, []int64) {
	trend := make(map[int64]*domain.AdminDashboardTrendPoint)
	start := dashboardBucketStart(filter.StartTime, filter.Bucket)
	step := dashboardBucketDuration(filter.Bucket)
	keys := make([]int64, 0)
	for t := start; t.Before(filter.EndTime); t = t.Add(step) {
		key := t.Unix()
		keys = append(keys, key)
		point := domain.AdminDashboardTrendPoint{BucketStart: t}
		trend[key] = &point
	}
	return trend, keys
}

func dashboardTrendSlice(trend map[int64]*domain.AdminDashboardTrendPoint, keys []int64) []domain.AdminDashboardTrendPoint {
	out := make([]domain.AdminDashboardTrendPoint, 0, len(keys))
	for _, key := range keys {
		if point := trend[key]; point != nil {
			out = append(out, *point)
		}
	}
	return out
}

func dashboardStatusCounts(counts map[string]int64) []domain.AdminStatusCount {
	statuses := make([]string, 0, len(counts))
	for status := range counts {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	out := make([]domain.AdminStatusCount, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, domain.AdminStatusCount{Status: status, Count: counts[status]})
	}
	return out
}

func inDashboardRange(t, start, end time.Time) bool {
	return !t.IsZero() && !t.Before(start) && t.Before(end)
}

func dashboardRunningAuction(status domain.AuctionStatus) bool {
	switch status {
	case domain.AuctionStatusRunning, domain.AuctionStatusExtended, domain.AuctionStatusHammerPending:
		return true
	default:
		return false
	}
}

func dashboardBucketUnix(t time.Time, bucket string) int64 {
	return dashboardBucketStart(t, bucket).Unix()
}

func dashboardBucketStart(t time.Time, bucket string) time.Time {
	t = t.UTC()
	if bucket == "day" {
		y, m, d := t.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	}
	return t.Truncate(time.Hour)
}

func dashboardBucketDuration(bucket string) time.Duration {
	if bucket == "day" {
		return 24 * time.Hour
	}
	return time.Hour
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

var _ adminports.ConfigRepository = (*MySQLConfigRepository)(nil)
var _ adminports.ConfigRepository = (*MemoryConfigRepository)(nil)
var _ adminports.AuditRepository = (*MySQLAuditRepository)(nil)
var _ adminports.AuditRepository = (*MemoryAuditRepository)(nil)
var _ adminports.DashboardRepository = (*MySQLAdminDashboardRepository)(nil)
var _ adminports.DashboardRepository = (*MemoryAdminDashboardRepository)(nil)
