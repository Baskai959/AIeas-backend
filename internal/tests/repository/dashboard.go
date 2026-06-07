package repository

import (
	"context"

	"aieas_backend/internal/domain"
	adminports "aieas_backend/internal/modules/admin/ports"
	adminrepo "aieas_backend/internal/modules/admin/repository"
	riskrepo "aieas_backend/internal/modules/risk/repository"

	"gorm.io/gorm"
)

type AdminDashboardRepository = adminports.DashboardRepository
type MySQLAdminDashboardRepository = adminrepo.MySQLAdminDashboardRepository
type MemoryAdminDashboardRepository = adminrepo.MemoryAdminDashboardRepository

func NewMySQLAdminDashboardRepository(db *gorm.DB) *MySQLAdminDashboardRepository {
	return adminrepo.NewMySQLAdminDashboardRepository(db, func(ctx context.Context, base *gorm.DB) *gorm.DB {
		if tx := DBFromContext(ctx); tx != nil {
			return tx
		}
		return base.WithContext(ctx)
	})
}

func NewMemoryAdminDashboardRepository(auctions AuctionRepository, sessions LiveSessionRepository, bids BidRepository, orders OrderRepository, risk RiskRepository) *MemoryAdminDashboardRepository {
	return adminrepo.NewMemoryAdminDashboardRepository(
		auctionSnapshotAdapter{repo: auctions},
		liveSessionSnapshotAdapter{repo: sessions},
		bidSnapshotAdapter{repo: bids},
		orderSnapshotAdapter{repo: orders},
		riskEventSnapshotAdapter{repo: risk},
	)
}

type auctionSnapshotAdapter struct{ repo AuctionRepository }

func (a auctionSnapshotAdapter) SnapshotAuctions() []domain.AuctionLot {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotAuctions() []domain.AuctionLot }); ok {
		return snapshotter.SnapshotAuctions()
	}
	return nil
}

type liveSessionSnapshotAdapter struct{ repo LiveSessionRepository }

func (a liveSessionSnapshotAdapter) SnapshotLiveSessions() []domain.LiveSession {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotLiveSessions() []domain.LiveSession }); ok {
		return snapshotter.SnapshotLiveSessions()
	}
	return nil
}

type bidSnapshotAdapter struct{ repo BidRepository }

func (a bidSnapshotAdapter) SnapshotBids() []domain.BidRecord {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotBids() []domain.BidRecord }); ok {
		return snapshotter.SnapshotBids()
	}
	return nil
}

type orderSnapshotAdapter struct{ repo OrderRepository }

func (a orderSnapshotAdapter) SnapshotOrders() []domain.OrderDeal {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotOrders() []domain.OrderDeal }); ok {
		return snapshotter.SnapshotOrders()
	}
	return nil
}

type riskEventSnapshotAdapter struct{ repo RiskRepository }

func (a riskEventSnapshotAdapter) SnapshotRiskEvents() []domain.RiskEvent {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotRiskEvents() []domain.RiskEvent }); ok {
		return snapshotter.SnapshotRiskEvents()
	}
	repo, ok := a.repo.(*riskrepo.MemoryRiskRepository)
	if !ok || repo == nil {
		return nil
	}
	return repo.SnapshotEvents()
}
