package repository

import (
	"context"
	"errors"
	"time"

	"aieas_backend/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ConfigRepository interface {
	FindByKey(ctx context.Context, key string) (domain.ConfigItem, error)
	Upsert(ctx context.Context, item *domain.ConfigItem) error
}

type MySQLConfigRepository struct {
	db *gorm.DB
}

func NewMySQLConfigRepository(db *gorm.DB) *MySQLConfigRepository {
	return &MySQLConfigRepository{db: db}
}

func (r *MySQLConfigRepository) dbFor(ctx context.Context) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
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
