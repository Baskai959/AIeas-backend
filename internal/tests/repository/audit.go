package repository

import (
	"context"

	adminports "aieas_backend/internal/modules/admin/ports"
	adminrepo "aieas_backend/internal/modules/admin/repository"

	"gorm.io/gorm"
)

type AuditRepository = adminports.AuditRepository
type MySQLAuditRepository = adminrepo.MySQLAuditRepository
type MemoryAuditRepository = adminrepo.MemoryAuditRepository

func NewMySQLAuditRepository(db *gorm.DB) *MySQLAuditRepository {
	return adminrepo.NewMySQLAuditRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryAuditRepository() *MemoryAuditRepository {
	return adminrepo.NewMemoryAuditRepository()
}
