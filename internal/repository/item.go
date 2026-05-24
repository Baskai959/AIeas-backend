package repository

import (
	"context"
	"errors"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
)

type ItemRepository interface {
	Create(ctx context.Context, item *domain.Item) error
	FindByID(ctx context.Context, id uint64) (domain.Item, error)
	List(ctx context.Context, filter domain.ItemFilter) ([]domain.Item, error)
	Update(ctx context.Context, item *domain.Item) error
	Delete(ctx context.Context, id uint64) error
}

type MySQLItemRepository struct {
	db *gorm.DB
}

func NewMySQLItemRepository(db *gorm.DB) *MySQLItemRepository {
	return &MySQLItemRepository{db: db}
}

func (r *MySQLItemRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return r.db.WithContext(ctx)
}

func (r *MySQLItemRepository) Create(ctx context.Context, item *domain.Item) error {
	row := itemRowFromDomain(*item)
	if err := r.dbFor(ctx).Table("item").Create(&row).Error; err != nil {
		return err
	}
	*item = row.toDomain()
	return nil
}

func (r *MySQLItemRepository) FindByID(ctx context.Context, id uint64) (domain.Item, error) {
	var row itemRow
	err := r.dbFor(ctx).Table("item").Where("id = ?", id).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.Item{}, domain.ErrNotFound
		}
		return domain.Item{}, err
	}
	return row.toDomain(), nil
}

func (r *MySQLItemRepository) List(ctx context.Context, filter domain.ItemFilter) ([]domain.Item, error) {
	query := r.dbFor(ctx).Table("item").Order("id DESC")
	if filter.SellerID != "" {
		query = query.Where("seller_id = ?", normalizeUserIDForDB(filter.SellerID))
	}
	if filter.Status.Valid() {
		query = query.Where("status = ?", filter.Status)
	}
	if filter.Category != "" {
		query = query.Where("category = ?", filter.Category)
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var rows []itemRow
	if err := query.Limit(filter.Limit).Offset(filter.Offset).Find(&rows).Error; err != nil {
		return nil, err
	}
	items := make([]domain.Item, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.toDomain())
	}
	return items, nil
}

func (r *MySQLItemRepository) Update(ctx context.Context, item *domain.Item) error {
	row := itemRowFromDomain(*item)
	row.UpdatedAt = time.Now()
	res := r.dbFor(ctx).Table("item").Where("id = ?", item.ID).Updates(&row)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	updated, err := r.FindByID(ctx, item.ID)
	if err != nil {
		return err
	}
	*item = updated
	return nil
}

func (r *MySQLItemRepository) Delete(ctx context.Context, id uint64) error {
	res := r.dbFor(ctx).Table("item").Where("id = ?", id).Delete(&itemRow{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return domain.ErrNotFound
	}
	return nil
}

type itemRow struct {
	ID             uint64                `gorm:"column:id;primaryKey"`
	SellerID       string                `gorm:"column:seller_id"`
	Title          string                `gorm:"column:title"`
	Category       string                `gorm:"column:category"`
	Brand          string                `gorm:"column:brand"`
	ConditionGrade domain.ConditionGrade `gorm:"column:condition_grade"`
	Images         []byte                `gorm:"column:images"`
	Description    string                `gorm:"column:description"`
	Status         domain.ItemStatus     `gorm:"column:status"`
	CreatedAt      time.Time             `gorm:"column:created_at"`
	UpdatedAt      time.Time             `gorm:"column:updated_at"`
}

func itemRowFromDomain(item domain.Item) itemRow {
	return itemRow{
		ID:             item.ID,
		SellerID:       normalizeUserIDForDB(item.SellerID),
		Title:          item.Title,
		Category:       item.Category,
		Brand:          item.Brand,
		ConditionGrade: item.ConditionGrade,
		Images:         []byte(item.Images),
		Description:    item.Description,
		Status:         item.Status,
		CreatedAt:      item.CreatedAt,
		UpdatedAt:      item.UpdatedAt,
	}
}

func (r itemRow) toDomain() domain.Item {
	return domain.Item{
		ID:             r.ID,
		SellerID:       r.SellerID,
		Title:          r.Title,
		Category:       r.Category,
		Brand:          r.Brand,
		ConditionGrade: r.ConditionGrade,
		Images:         append([]byte(nil), r.Images...),
		Description:    r.Description,
		Status:         r.Status,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
	}
}
