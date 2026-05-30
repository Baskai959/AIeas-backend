package repository

import (
	"context"
	"sort"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type AdminDashboardRepository interface {
	DashboardMetrics(ctx context.Context, filter domain.AdminDashboardMetricsFilter) (domain.AdminDashboardMetrics, error)
}

type MySQLAdminDashboardRepository struct {
	db *gorm.DB
}

func NewMySQLAdminDashboardRepository(db *gorm.DB) *MySQLAdminDashboardRepository {
	return &MySQLAdminDashboardRepository{db: db}
}

func (r *MySQLAdminDashboardRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
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
	if err := r.loadCurrent(ctx, db, &result.Current); err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	if err := r.loadSummary(ctx, db, filter, &result.Summary); err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	breakdowns, err := r.loadBreakdowns(ctx, db, filter)
	if err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	result.Breakdowns = breakdowns
	trend, err := r.loadTrend(ctx, db, filter)
	if err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	result.Trend = trend
	return result, nil
}

func (r *MySQLAdminDashboardRepository) loadCurrent(ctx context.Context, db *gorm.DB, current *domain.AdminDashboardCurrent) error {
	_ = ctx
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

func (r *MySQLAdminDashboardRepository) loadSummary(ctx context.Context, db *gorm.DB, filter domain.AdminDashboardMetricsFilter, summary *domain.AdminDashboardSummary) error {
	_ = ctx
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

func (r *MySQLAdminDashboardRepository) loadBreakdowns(ctx context.Context, db *gorm.DB, filter domain.AdminDashboardMetricsFilter) (domain.AdminDashboardBreakdowns, error) {
	_ = ctx
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

func (r *MySQLAdminDashboardRepository) loadTrend(ctx context.Context, db *gorm.DB, filter domain.AdminDashboardMetricsFilter) ([]domain.AdminDashboardTrendPoint, error) {
	_ = ctx
	trend, keys := newDashboardTrend(filter)
	bucketSeconds := dashboardBucketDuration(filter.Bucket).Seconds()
	bucketSec := int64(bucketSeconds)
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

type MemoryAdminDashboardRepository struct {
	auctions AuctionRepository
	sessions LiveSessionRepository
	bids     BidRepository
	orders   OrderRepository
	risk     RiskRepository
}

func NewMemoryAdminDashboardRepository(auctions AuctionRepository, sessions LiveSessionRepository, bids BidRepository, orders OrderRepository, risk RiskRepository) *MemoryAdminDashboardRepository {
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
	repo, ok := r.auctions.(*MemoryAuctionRepository)
	if !ok || repo == nil {
		return nil
	}
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	out := make([]domain.AuctionLot, 0, len(repo.auctions))
	for _, auction := range repo.auctions {
		out = append(out, cloneAuction(auction))
	}
	return out
}

func (r *MemoryAdminDashboardRepository) snapshotSessions() []domain.LiveSession {
	repo, ok := r.sessions.(*MemoryLiveSessionRepository)
	if !ok || repo == nil {
		return nil
	}
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	out := make([]domain.LiveSession, 0, len(repo.sessions))
	for _, session := range repo.sessions {
		out = append(out, session)
	}
	return out
}

func (r *MemoryAdminDashboardRepository) snapshotBids() []domain.BidRecord {
	repo, ok := r.bids.(*MemoryBidRepository)
	if !ok || repo == nil {
		return nil
	}
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	out := make([]domain.BidRecord, 0, len(repo.byID))
	for _, bid := range repo.byID {
		out = append(out, cloneBidRecord(bid))
	}
	return out
}

func (r *MemoryAdminDashboardRepository) snapshotOrders() []domain.OrderDeal {
	repo, ok := r.orders.(*MemoryOrderRepository)
	if !ok || repo == nil {
		return nil
	}
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	out := make([]domain.OrderDeal, 0, len(repo.orders))
	for _, order := range repo.orders {
		out = append(out, cloneOrder(order))
	}
	return out
}

func (r *MemoryAdminDashboardRepository) snapshotRiskEvents() []domain.RiskEvent {
	repo, ok := r.risk.(*MemoryRiskRepository)
	if !ok || repo == nil {
		return nil
	}
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	out := make([]domain.RiskEvent, 0, len(repo.riskEvents))
	for _, event := range repo.riskEvents {
		out = append(out, cloneRiskEvent(event))
	}
	return out
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
