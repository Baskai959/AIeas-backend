package repository

import (
	"context"

	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
	liveanalysisrepo "aieas_backend/internal/modules/live_analysis/repository"

	"gorm.io/gorm"
)

type LiveAnalysisReportRepository = liveanalysisports.LiveAnalysisReportRepository
type MySQLLiveAnalysisReportRepository = liveanalysisrepo.MySQLLiveAnalysisReportRepository

func NewMySQLLiveAnalysisReportRepository(db *gorm.DB) *MySQLLiveAnalysisReportRepository {
	return liveanalysisrepo.NewMySQLLiveAnalysisReportRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}
