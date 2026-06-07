package repository

import (
	"context"

	riskports "aieas_backend/internal/modules/risk/ports"
	riskrepo "aieas_backend/internal/modules/risk/repository"

	"gorm.io/gorm"
)

type RiskRepository = riskports.RiskRepository
type MySQLRiskRepository = riskrepo.MySQLRiskRepository
type MemoryRiskRepository = riskrepo.MemoryRiskRepository

func NewMySQLRiskRepository(db *gorm.DB) *MySQLRiskRepository {
	return riskrepo.NewMySQLRiskRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryRiskRepository() *MemoryRiskRepository {
	return riskrepo.NewMemoryRiskRepository()
}
