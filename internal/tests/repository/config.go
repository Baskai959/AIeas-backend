package repository

import (
	"context"

	adminports "aieas_backend/internal/modules/admin/ports"
	adminrepo "aieas_backend/internal/modules/admin/repository"

	"gorm.io/gorm"
)

type ConfigRepository = adminports.ConfigRepository
type MySQLConfigRepository = adminrepo.MySQLConfigRepository
type MemoryConfigRepository = adminrepo.MemoryConfigRepository

func NewMySQLConfigRepository(db *gorm.DB) *MySQLConfigRepository {
	return adminrepo.NewMySQLConfigRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryConfigRepository() *MemoryConfigRepository {
	return adminrepo.NewMemoryConfigRepository()
}
