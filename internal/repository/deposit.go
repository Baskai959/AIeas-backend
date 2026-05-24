package repository

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type DepositRepository interface {
	Create(ctx context.Context, deposit *domain.DepositLedger) error
	FindByAuctionUser(ctx context.Context, auctionID uint64, userID string) (domain.DepositLedger, error)
	ListByAuction(ctx context.Context, auctionID uint64) ([]domain.DepositLedger, error)
	Update(ctx context.Context, deposit *domain.DepositLedger) error
}

type MySQLDepositRepository struct {
	db *gorm.DB
}

func NewMySQLDepositRepository(db *gorm.DB) *MySQLDepositRepository {
	return &MySQLDepositRepository{db: db}
}

func (r *MySQLDepositRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLDepositRepository) Create(ctx context.Context, deposit *domain.DepositLedger) error {
	row := depositRowFromDomain(*deposit)
	if err := r.dbFor(ctx).Table("deposit_ledger").Create(&row).Error; err != nil {
		return err
	}
	*deposit = row.toDomain()
	return nil
}

func (r *MySQLDepositRepository) FindByAuctionUser(ctx context.Context, auctionID uint64, userID string) (domain.DepositLedger, error) {
	var row depositRow
	err := r.dbFor(ctx).Table("deposit_ledger").Where("auction_id = ? AND user_id = ?", auctionID, normalizeUserIDForDB(userID)).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.DepositLedger{}, domain.ErrNotFound
		}
		return domain.DepositLedger{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLDepositRepository) ListByAuction(ctx context.Context, auctionID uint64) ([]domain.DepositLedger, error) {
	var rows []depositRow
	if err := r.dbFor(ctx).Table("deposit_ledger").Where("auction_id = ?", auctionID).Find(&rows).Error; err != nil {
		return nil, err
	}
	items := make([]domain.DepositLedger, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.toDomain())
	}
	return items, nil
}

func (r *MySQLDepositRepository) Update(ctx context.Context, deposit *domain.DepositLedger) error {
	row := depositRowFromDomain(*deposit)
	row.UpdatedAt = time.Now().UTC()
	res := r.dbFor(ctx).Table("deposit_ledger").Where("id = ?", deposit.ID).Updates(&row)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindByAuctionUser(ctx, deposit.AuctionID, deposit.UserID)
	if err != nil {
		return err
	}
	*deposit = updated
	return nil
}

type depositRow struct {
	ID             uint64               `gorm:"column:id;primaryKey"`
	AuctionID      uint64               `gorm:"column:auction_id"`
	UserID         string               `gorm:"column:user_id"`
	Amount         int64                `gorm:"column:amount"`
	Status         domain.DepositStatus `gorm:"column:status"`
	RelatedOrderID *uint64              `gorm:"column:related_order_id"`
	Remark         string               `gorm:"column:remark"`
	CreatedAt      time.Time            `gorm:"column:created_at"`
	UpdatedAt      time.Time            `gorm:"column:updated_at"`
}

func depositRowFromDomain(deposit domain.DepositLedger) depositRow {
	return depositRow{
		ID:             deposit.ID,
		AuctionID:      deposit.AuctionID,
		UserID:         normalizeUserIDForDB(deposit.UserID),
		Amount:         deposit.Amount,
		Status:         deposit.Status,
		RelatedOrderID: deposit.RelatedOrderID,
		Remark:         deposit.Remark,
		CreatedAt:      deposit.CreatedAt,
		UpdatedAt:      deposit.UpdatedAt,
	}
}

func (r depositRow) toDomain() domain.DepositLedger {
	return domain.DepositLedger{
		ID:             r.ID,
		AuctionID:      r.AuctionID,
		UserID:         r.UserID,
		Amount:         r.Amount,
		Status:         r.Status,
		RelatedOrderID: r.RelatedOrderID,
		Remark:         r.Remark,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
	}
}

type MemoryDepositRepository struct {
	mu    sync.RWMutex
	next  uint64
	items map[uint64]domain.DepositLedger
	index map[string]uint64
}

func NewMemoryDepositRepository() *MemoryDepositRepository {
	return &MemoryDepositRepository{next: 1, items: make(map[uint64]domain.DepositLedger), index: make(map[string]uint64)}
}

func (r *MemoryDepositRepository) Create(ctx context.Context, deposit *domain.DepositLedger) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	key := depositKey(deposit.AuctionID, deposit.UserID)
	if id, ok := r.index[key]; ok {
		*deposit = r.items[id]
		return domain.ErrConflict
	}
	if deposit.ID == 0 {
		deposit.ID = r.next
		r.next++
	}
	now := time.Now().UTC()
	if deposit.CreatedAt.IsZero() {
		deposit.CreatedAt = now
	}
	deposit.UpdatedAt = now
	r.items[deposit.ID] = cloneDeposit(*deposit)
	r.index[key] = deposit.ID
	return nil
}

func (r *MemoryDepositRepository) FindByAuctionUser(ctx context.Context, auctionID uint64, userID string) (domain.DepositLedger, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.index[depositKey(auctionID, userID)]
	if !ok {
		return domain.DepositLedger{}, domain.ErrNotFound
	}
	return cloneDeposit(r.items[id]), nil
}

func (r *MemoryDepositRepository) ListByAuction(ctx context.Context, auctionID uint64) ([]domain.DepositLedger, error) {
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]domain.DepositLedger, 0)
	for _, deposit := range r.items {
		if deposit.AuctionID == auctionID {
			items = append(items, cloneDeposit(deposit))
		}
	}
	return items, nil
}

func (r *MemoryDepositRepository) Update(ctx context.Context, deposit *domain.DepositLedger) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[deposit.ID]; !ok {
		return domain.ErrNotFound
	}
	deposit.UpdatedAt = time.Now().UTC()
	r.items[deposit.ID] = cloneDeposit(*deposit)
	r.index[depositKey(deposit.AuctionID, deposit.UserID)] = deposit.ID
	return nil
}

func depositKey(auctionID uint64, userID string) string {
	return strconv.FormatUint(auctionID, 10) + ":" + userID
}

func cloneDeposit(deposit domain.DepositLedger) domain.DepositLedger {
	return deposit
}
