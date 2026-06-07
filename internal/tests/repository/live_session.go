package repository

import (
	"context"

	livesessionports "aieas_backend/internal/modules/live_session/ports"
	livesessionrepo "aieas_backend/internal/modules/live_session/repository"

	"gorm.io/gorm"
)

// LiveSessionRepository 负责直播场次（live_session）的持久化能力。
type LiveSessionRepository = livesessionports.LiveSessionRepository

type MySQLLiveSessionRepository = livesessionrepo.MySQLLiveSessionRepository

func NewMySQLLiveSessionRepository(db *gorm.DB) *MySQLLiveSessionRepository {
	return livesessionrepo.NewMySQLLiveSessionRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}
