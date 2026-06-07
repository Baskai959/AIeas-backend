package repository

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	orderports "aieas_backend/internal/modules/order/ports"

	"gorm.io/gorm"
)

type ContextDBResolver func(ctx context.Context, db *gorm.DB) *gorm.DB

type MySQLOrderRepository struct {
	db        *gorm.DB
	resolveDB ContextDBResolver
}

func NewMySQLOrderRepository(db *gorm.DB, resolver ContextDBResolver) *MySQLOrderRepository {
	return &MySQLOrderRepository{db: db, resolveDB: resolver}
}

func (r *MySQLOrderRepository) dbFor(ctx context.Context) *gorm.DB {
	if r.resolveDB != nil {
		return r.resolveDB(ctx, r.db)
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLOrderRepository) CreateIfAbsentByAuction(ctx context.Context, order *domain.OrderDeal) (domain.OrderDeal, bool, error) {
	if existing, err := r.FindByAuctionID(ctx, order.AuctionID); err == nil {
		return existing, false, nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.OrderDeal{}, false, err
	}
	row := orderRowFromDomain(*order)
	if err := r.dbFor(ctx).Table("order_deal").Create(&row).Error; err != nil {
		if existing, findErr := r.FindByAuctionID(ctx, order.AuctionID); findErr == nil {
			return existing, false, nil
		}
		return domain.OrderDeal{}, false, err
	}
	return row.toDomain(), true, nil
}

func (r *MySQLOrderRepository) FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error) {
	var row orderRow
	err := r.dbFor(ctx).Table("order_deal").Where("auction_id = ?", auctionID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.OrderDeal{}, domain.ErrNotFound
		}
		return domain.OrderDeal{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLOrderRepository) FindByID(ctx context.Context, id uint64) (domain.OrderDeal, error) {
	var row orderRow
	err := r.dbFor(ctx).Table("order_deal").Where("id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.OrderDeal{}, domain.ErrNotFound
		}
		return domain.OrderDeal{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLOrderRepository) List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error) {
	query := r.dbFor(ctx).Table("order_deal").Order("id DESC")
	if filter.WinnerID != "" {
		query = query.Where("winner_id = ?", normalizeUserIDForDB(filter.WinnerID))
	}
	if filter.SellerID != "" {
		query = query.Where("seller_id = ?", normalizeUserIDForDB(filter.SellerID))
	}
	if filter.AuctionID != 0 {
		query = query.Where("auction_id = ?", filter.AuctionID)
	}
	if filter.LiveSessionID != 0 {
		query = query.Where("live_session_id = ?", filter.LiveSessionID)
	}
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.PayStatus != "" {
		query = query.Where("pay_status = ?", filter.PayStatus)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []orderRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	orders := make([]domain.OrderDeal, 0, len(rows))
	for _, row := range rows {
		orders = append(orders, row.toDomain())
	}
	return orders, nil
}

func (r *MySQLOrderRepository) ListPayTimeoutCandidates(ctx context.Context, now time.Time, limit int) ([]domain.OrderDeal, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []orderRow
	err := r.dbFor(ctx).Table("order_deal").
		Where("status = ? AND pay_status = ? AND pay_deadline IS NOT NULL AND pay_deadline <= ?", domain.OrderStatusCreated, domain.PayStatusUnpaid, now.UTC()).
		Order("pay_deadline ASC, id ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	orders := make([]domain.OrderDeal, 0, len(rows))
	for _, row := range rows {
		orders = append(orders, row.toDomain())
	}
	return orders, nil
}

func (r *MySQLOrderRepository) Update(ctx context.Context, order *domain.OrderDeal) error {
	row := orderRowFromDomain(*order)
	row.UpdatedAt = time.Now().UTC()
	res := r.dbFor(ctx).Table("order_deal").Where("id = ?", order.ID).Updates(&row)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindByID(ctx, order.ID)
	if err != nil {
		return err
	}
	*order = updated
	return nil
}

func (r *MySQLOrderRepository) UpdateStatusWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.OrderStatus) error {
	if order == nil || order.ID == 0 {
		return domain.ErrInvalidArgument
	}
	now := time.Now().UTC()
	updates := map[string]interface{}{
		"status":     order.Status,
		"pay_status": order.PayStatus,
		"paid_at":    order.PaidAt,
		"closed_at":  order.ClosedAt,
		"version":    gorm.Expr("version + 1"),
		"updated_at": now,
	}
	query := r.dbFor(ctx).Table("order_deal").Where("id = ? AND version = ?", order.ID, expectedVersion)
	if len(allowedFromStatuses) > 0 {
		query = query.Where("status IN ?", allowedFromStatuses)
	}
	res := query.Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		current, err := r.FindByID(ctx, order.ID)
		if err != nil {
			return err
		}
		if current.Status.Terminal() {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	order.Version = expectedVersion + 1
	order.UpdatedAt = now
	return nil
}

func (r *MySQLOrderRepository) UpdateFulfillmentWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.FulfillmentStatus) error {
	if order == nil || order.ID == 0 {
		return domain.ErrInvalidArgument
	}
	now := time.Now().UTC()
	updates := map[string]interface{}{
		"fulfillment_status": domain.NormalizeFulfillmentStatus(order.FulfillmentStatus),
		"shipped_at":         order.ShippedAt,
		"received_at":        order.ReceivedAt,
		"version":            gorm.Expr("version + 1"),
		"updated_at":         now,
	}
	query := r.dbFor(ctx).Table("order_deal").Where("id = ? AND version = ?", order.ID, expectedVersion)
	if len(allowedFromStatuses) > 0 {
		query = query.Where("fulfillment_status IN ?", allowedFromStatuses)
	}
	res := query.Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		current, err := r.FindByID(ctx, order.ID)
		if err != nil {
			return err
		}
		if current.FulfillmentStatus == order.FulfillmentStatus {
			*order = current
			return nil
		}
		if current.FulfillmentStatus == domain.FulfillmentStatusReceived {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	order.Version = expectedVersion + 1
	order.UpdatedAt = now
	return nil
}

type orderRow struct {
	ID                uint64                   `gorm:"column:id;primaryKey"`
	AuctionID         uint64                   `gorm:"column:auction_id"`
	LiveSessionID     *uint64                  `gorm:"column:live_session_id"`
	LotSnapshot       []byte                   `gorm:"column:lot_snapshot"`
	WinnerID          string                   `gorm:"column:winner_id"`
	SellerID          string                   `gorm:"column:seller_id"`
	DealPrice         int64                    `gorm:"column:deal_price"`
	DepositAmount     int64                    `gorm:"column:deposit_amount"`
	Status            domain.OrderStatus       `gorm:"column:status"`
	PayStatus         domain.PayStatus         `gorm:"column:pay_status"`
	FulfillmentStatus domain.FulfillmentStatus `gorm:"column:fulfillment_status"`
	PayDeadline       *time.Time               `gorm:"column:pay_deadline"`
	PaidAt            *time.Time               `gorm:"column:paid_at"`
	ShippedAt         *time.Time               `gorm:"column:shipped_at"`
	ReceivedAt        *time.Time               `gorm:"column:received_at"`
	ClosedAt          *time.Time               `gorm:"column:closed_at"`
	Version           int64                    `gorm:"column:version;not null;default:0"`
	CreatedAt         time.Time                `gorm:"column:created_at"`
	UpdatedAt         time.Time                `gorm:"column:updated_at"`
}

func orderRowFromDomain(order domain.OrderDeal) orderRow {
	return orderRow{
		ID:                order.ID,
		AuctionID:         order.AuctionID,
		LiveSessionID:     cloneUint64Ptr(order.LiveSessionID),
		LotSnapshot:       []byte(order.LotSnapshot),
		WinnerID:          normalizeUserIDForDB(order.WinnerID),
		SellerID:          normalizeUserIDForDB(order.SellerID),
		DealPrice:         order.DealPrice,
		DepositAmount:     order.DepositAmount,
		Status:            order.Status,
		PayStatus:         order.PayStatus,
		FulfillmentStatus: domain.NormalizeFulfillmentStatus(order.FulfillmentStatus),
		PayDeadline:       order.PayDeadline,
		PaidAt:            order.PaidAt,
		ShippedAt:         order.ShippedAt,
		ReceivedAt:        order.ReceivedAt,
		ClosedAt:          order.ClosedAt,
		Version:           order.Version,
		CreatedAt:         order.CreatedAt,
		UpdatedAt:         order.UpdatedAt,
	}
}

func (r orderRow) toDomain() domain.OrderDeal {
	return domain.OrderDeal{
		ID:                r.ID,
		AuctionID:         r.AuctionID,
		LiveSessionID:     cloneUint64Ptr(r.LiveSessionID),
		LotSnapshot:       append([]byte(nil), r.LotSnapshot...),
		WinnerID:          r.WinnerID,
		SellerID:          r.SellerID,
		DealPrice:         r.DealPrice,
		DepositAmount:     r.DepositAmount,
		Status:            r.Status,
		PayStatus:         r.PayStatus,
		FulfillmentStatus: domain.NormalizeFulfillmentStatus(r.FulfillmentStatus),
		PayDeadline:       cloneTimePtrUTC(r.PayDeadline),
		PaidAt:            cloneTimePtrUTC(r.PaidAt),
		ShippedAt:         cloneTimePtrUTC(r.ShippedAt),
		ReceivedAt:        cloneTimePtrUTC(r.ReceivedAt),
		ClosedAt:          cloneTimePtrUTC(r.ClosedAt),
		Version:           r.Version,
		CreatedAt:         r.CreatedAt,
		UpdatedAt:         r.UpdatedAt,
	}
}

type MemoryOrderRepository struct {
	mu        sync.RWMutex
	next      uint64
	orders    map[uint64]domain.OrderDeal
	byAuction map[uint64]uint64
}

func NewMemoryOrderRepository() *MemoryOrderRepository {
	return &MemoryOrderRepository{next: 1, orders: make(map[uint64]domain.OrderDeal), byAuction: make(map[uint64]uint64)}
}

func (r *MemoryOrderRepository) CreateIfAbsentByAuction(ctx context.Context, order *domain.OrderDeal) (domain.OrderDeal, bool, error) {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.byAuction[order.AuctionID]; ok {
		return cloneOrder(r.orders[id]), false, nil
	}
	if order.ID == 0 {
		order.ID = r.next
		r.next++
	}
	now := time.Now().UTC()
	if order.CreatedAt.IsZero() {
		order.CreatedAt = now
	}
	order.UpdatedAt = now
	r.orders[order.ID] = cloneOrder(*order)
	r.byAuction[order.AuctionID] = order.ID
	return cloneOrder(*order), true, nil
}

func (r *MemoryOrderRepository) FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byAuction[auctionID]
	if !ok {
		return domain.OrderDeal{}, domain.ErrNotFound
	}
	return cloneOrder(r.orders[id]), nil
}

func (r *MemoryOrderRepository) FindByID(ctx context.Context, id uint64) (domain.OrderDeal, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	order, ok := r.orders[id]
	if !ok {
		return domain.OrderDeal{}, domain.ErrNotFound
	}
	return cloneOrder(order), nil
}

func (r *MemoryOrderRepository) List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error) {
	_ = ctx
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	orders := make([]domain.OrderDeal, 0, len(r.orders))
	for _, order := range r.orders {
		if filter.WinnerID != "" && normalizeUserIDForDB(order.WinnerID) != normalizeUserIDForDB(filter.WinnerID) {
			continue
		}
		if filter.SellerID != "" && normalizeUserIDForDB(order.SellerID) != normalizeUserIDForDB(filter.SellerID) {
			continue
		}
		if filter.AuctionID != 0 && order.AuctionID != filter.AuctionID {
			continue
		}
		if filter.LiveSessionID != 0 && (order.LiveSessionID == nil || *order.LiveSessionID != filter.LiveSessionID) {
			continue
		}
		if filter.Status != "" && order.Status != filter.Status {
			continue
		}
		if filter.PayStatus != "" && order.PayStatus != filter.PayStatus {
			continue
		}
		orders = append(orders, cloneOrder(order))
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].ID > orders[j].ID })
	if filter.Offset >= len(orders) {
		return []domain.OrderDeal{}, nil
	}
	end := filter.Offset + filter.Limit
	if end > len(orders) {
		end = len(orders)
	}
	return orders[filter.Offset:end], nil
}

func (r *MemoryOrderRepository) ListPayTimeoutCandidates(ctx context.Context, now time.Time, limit int) ([]domain.OrderDeal, error) {
	_ = ctx
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	now = now.UTC()
	r.mu.RLock()
	defer r.mu.RUnlock()
	orders := make([]domain.OrderDeal, 0, len(r.orders))
	for _, order := range r.orders {
		if order.Status != domain.OrderStatusCreated || order.PayStatus != domain.PayStatusUnpaid || !order.PaymentExpired(now) {
			continue
		}
		orders = append(orders, cloneOrder(order))
	}
	sort.Slice(orders, func(i, j int) bool {
		if orders[i].PayDeadline != nil && orders[j].PayDeadline != nil && !orders[i].PayDeadline.Equal(*orders[j].PayDeadline) {
			return orders[i].PayDeadline.Before(*orders[j].PayDeadline)
		}
		return orders[i].ID < orders[j].ID
	})
	if len(orders) > limit {
		orders = orders[:limit]
	}
	return orders, nil
}

func (r *MemoryOrderRepository) Update(ctx context.Context, order *domain.OrderDeal) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.orders[order.ID]; !ok {
		return domain.ErrNotFound
	}
	order.UpdatedAt = time.Now().UTC()
	r.orders[order.ID] = cloneOrder(*order)
	r.byAuction[order.AuctionID] = order.ID
	return nil
}

func (r *MemoryOrderRepository) SnapshotOrders() []domain.OrderDeal {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.OrderDeal, 0, len(r.orders))
	for _, order := range r.orders {
		out = append(out, cloneOrder(order))
	}
	return out
}

func (r *MemoryOrderRepository) UpdateStatusWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.OrderStatus) error {
	_ = ctx
	if order == nil || order.ID == 0 {
		return domain.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.orders[order.ID]
	if !ok {
		return domain.ErrNotFound
	}
	if current.Version != expectedVersion || !orderStatusInList(current.Status, allowedFromStatuses) {
		if current.Status.Terminal() {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	now := time.Now().UTC()
	current.Status = order.Status
	current.PayStatus = order.PayStatus
	current.PaidAt = cloneTimePtrUTC(order.PaidAt)
	current.ClosedAt = cloneTimePtrUTC(order.ClosedAt)
	current.Version = expectedVersion + 1
	current.UpdatedAt = now
	r.orders[order.ID] = cloneOrder(current)
	order.Version = current.Version
	order.UpdatedAt = now
	return nil
}

func (r *MemoryOrderRepository) UpdateFulfillmentWithVersion(ctx context.Context, order *domain.OrderDeal, expectedVersion int64, allowedFromStatuses []domain.FulfillmentStatus) error {
	_ = ctx
	if order == nil || order.ID == 0 {
		return domain.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.orders[order.ID]
	if !ok {
		return domain.ErrNotFound
	}
	if current.Version != expectedVersion || !fulfillmentStatusInList(current.FulfillmentStatus, allowedFromStatuses) {
		if current.FulfillmentStatus == order.FulfillmentStatus {
			*order = cloneOrder(current)
			return nil
		}
		if current.FulfillmentStatus == domain.FulfillmentStatusReceived {
			return domain.ErrInvalidState
		}
		return domain.ErrOptimisticConflict
	}
	now := time.Now().UTC()
	current.FulfillmentStatus = domain.NormalizeFulfillmentStatus(order.FulfillmentStatus)
	current.ShippedAt = cloneTimePtrUTC(order.ShippedAt)
	current.ReceivedAt = cloneTimePtrUTC(order.ReceivedAt)
	current.Version = expectedVersion + 1
	current.UpdatedAt = now
	r.orders[order.ID] = cloneOrder(current)
	*order = cloneOrder(current)
	return nil
}

func cloneOrder(order domain.OrderDeal) domain.OrderDeal {
	order.LiveSessionID = cloneUint64Ptr(order.LiveSessionID)
	order.LotSnapshot = append([]byte(nil), order.LotSnapshot...)
	order.FulfillmentStatus = domain.NormalizeFulfillmentStatus(order.FulfillmentStatus)
	order.PayDeadline = cloneTimePtrUTC(order.PayDeadline)
	order.PaidAt = cloneTimePtrUTC(order.PaidAt)
	order.ShippedAt = cloneTimePtrUTC(order.ShippedAt)
	order.ReceivedAt = cloneTimePtrUTC(order.ReceivedAt)
	order.ClosedAt = cloneTimePtrUTC(order.ClosedAt)
	return order
}

func orderStatusInList(s domain.OrderStatus, list []domain.OrderStatus) bool {
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

func fulfillmentStatusInList(s domain.FulfillmentStatus, list []domain.FulfillmentStatus) bool {
	if len(list) == 0 {
		return true
	}
	s = domain.NormalizeFulfillmentStatus(s)
	for _, v := range list {
		if s == domain.NormalizeFulfillmentStatus(v) {
			return true
		}
	}
	return false
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

var _ orderports.OrderRepository = (*MySQLOrderRepository)(nil)
