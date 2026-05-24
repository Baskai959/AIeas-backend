package repository

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type OrderRepository interface {
	CreateIfAbsentByAuction(ctx context.Context, order *domain.OrderDeal) (domain.OrderDeal, bool, error)
	FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error)
	FindByID(ctx context.Context, id uint64) (domain.OrderDeal, error)
	List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error)
	Update(ctx context.Context, order *domain.OrderDeal) error
}

type MySQLOrderRepository struct {
	db *gorm.DB
}

func NewMySQLOrderRepository(db *gorm.DB) *MySQLOrderRepository {
	return &MySQLOrderRepository{db: db}
}

func (r *MySQLOrderRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
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

type orderRow struct {
	ID            uint64             `gorm:"column:id;primaryKey"`
	AuctionID     uint64             `gorm:"column:auction_id"`
	WinnerID      string             `gorm:"column:winner_id"`
	SellerID      string             `gorm:"column:seller_id"`
	DealPrice     int64              `gorm:"column:deal_price"`
	DepositAmount int64              `gorm:"column:deposit_amount"`
	Status        domain.OrderStatus `gorm:"column:status"`
	PayStatus     domain.PayStatus   `gorm:"column:pay_status"`
	PayDeadline   *time.Time         `gorm:"column:pay_deadline"`
	PaidAt        *time.Time         `gorm:"column:paid_at"`
	ClosedAt      *time.Time         `gorm:"column:closed_at"`
	CreatedAt     time.Time          `gorm:"column:created_at"`
	UpdatedAt     time.Time          `gorm:"column:updated_at"`
}

func orderRowFromDomain(order domain.OrderDeal) orderRow {
	return orderRow{
		ID:            order.ID,
		AuctionID:     order.AuctionID,
		WinnerID:      normalizeUserIDForDB(order.WinnerID),
		SellerID:      normalizeUserIDForDB(order.SellerID),
		DealPrice:     order.DealPrice,
		DepositAmount: order.DepositAmount,
		Status:        order.Status,
		PayStatus:     order.PayStatus,
		PayDeadline:   order.PayDeadline,
		PaidAt:        order.PaidAt,
		ClosedAt:      order.ClosedAt,
		CreatedAt:     order.CreatedAt,
		UpdatedAt:     order.UpdatedAt,
	}
}

func (r orderRow) toDomain() domain.OrderDeal {
	return domain.OrderDeal{
		ID:            r.ID,
		AuctionID:     r.AuctionID,
		WinnerID:      r.WinnerID,
		SellerID:      r.SellerID,
		DealPrice:     r.DealPrice,
		DepositAmount: r.DepositAmount,
		Status:        r.Status,
		PayStatus:     r.PayStatus,
		PayDeadline:   r.PayDeadline,
		PaidAt:        r.PaidAt,
		ClosedAt:      r.ClosedAt,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
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
		if filter.WinnerID != "" && order.WinnerID != filter.WinnerID {
			continue
		}
		if filter.SellerID != "" && order.SellerID != filter.SellerID {
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

func cloneOrder(order domain.OrderDeal) domain.OrderDeal {
	return order
}
