package service

import (
	"context"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type AdminService struct {
	users     repository.UserRepository
	auctions  *AuctionService
	hammers   *HammerService
	orders    *OrderService
	risk      *RiskService
	audits    repository.AuditRepository
	dashboard repository.AdminDashboardRepository
}

func NewAdminService(users repository.UserRepository, auctions *AuctionService, hammers *HammerService, orders *OrderService, risk *RiskService, audits repository.AuditRepository) *AdminService {
	return &AdminService{users: users, auctions: auctions, hammers: hammers, orders: orders, risk: risk, audits: audits}
}

func (s *AdminService) SetDashboardRepository(repo repository.AdminDashboardRepository) {
	s.dashboard = repo
}

func (s *AdminService) ListAuctions(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error) {
	return s.auctions.List(ctx, filter, "admin", domain.RoleAdmin)
}

func (s *AdminService) AuditAuction(ctx context.Context, auctionID uint64, approved bool, actorID string) (domain.AuctionLot, error) {
	status := domain.AuctionStatusClosedFailed
	if approved {
		status = domain.AuctionStatusReady
	}
	return s.auctions.Update(ctx, auctionID, UpdateAuctionInput{ActorID: actorID, ActorRole: domain.RoleAdmin, Status: &status, allowSystemStatus: true})
}

func (s *AdminService) CancelAuction(ctx context.Context, auctionID uint64, actorID string) (domain.AuctionLot, error) {
	return s.auctions.Cancel(ctx, auctionID, actorID, domain.RoleAdmin)
}

func (s *AdminService) CloseAuction(ctx context.Context, auctionID uint64, actorID, requestID string) (domain.HammerResult, *domain.OrderDeal, error) {
	return s.hammers.Hammer(ctx, domain.HammerInput{RequestID: requestID, AuctionID: auctionID, ActorID: actorID, ActorRole: domain.RoleAdmin, ClosedBy: actorID, Force: true, Now: time.Now().UTC()})
}

func (s *AdminService) ListUsers(filter domain.UserFilter) ([]domain.SafeUser, error) {
	users, err := s.users.List(filter)
	if err != nil {
		return nil, err
	}
	safe := make([]domain.SafeUser, 0, len(users))
	for _, user := range users {
		safe = append(safe, user.Safe())
	}
	return safe, nil
}

func (s *AdminService) UpdateUserStatus(userID string, status domain.UserStatus) (domain.SafeUser, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" || (status != domain.UserStatusActive && status != domain.UserStatusDisabled) {
		return domain.SafeUser{}, domain.ErrInvalidArgument
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, err
	}
	user.Status = status
	if err := s.users.Update(&user); err != nil {
		return domain.SafeUser{}, err
	}
	return user.Safe(), nil
}

func (s *AdminService) AddBlacklist(ctx context.Context, userID, reason, actorID string, expiresAt *time.Time) error {
	return s.risk.AddBlacklist(ctx, userID, reason, actorID, expiresAt)
}

func (s *AdminService) RemoveBlacklist(ctx context.Context, userID string) error {
	return s.risk.RemoveBlacklist(ctx, userID)
}

func (s *AdminService) ListBlacklist(ctx context.Context, limit, offset int) ([]domain.Blacklist, error) {
	return s.risk.ListBlacklist(ctx, limit, offset)
}

func (s *AdminService) ListOrders(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error) {
	return s.orders.List(ctx, filter, "admin", domain.RoleAdmin)
}

func (s *AdminService) ListAuditLogs(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error) {
	return s.audits.List(ctx, filter)
}

func (s *AdminService) ListRiskEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error) {
	return s.risk.ListEvents(ctx, filter)
}

func (s *AdminService) HandleRiskEvent(ctx context.Context, eventID uint64, status domain.RiskEventStatus, actorID string) (domain.RiskEvent, error) {
	return s.risk.HandleEvent(ctx, eventID, status, actorID)
}

func (s *AdminService) DashboardMetrics(ctx context.Context, startTime, endTime *time.Time, bucket string) (domain.AdminDashboardMetrics, error) {
	if s.dashboard == nil {
		return domain.AdminDashboardMetrics{}, domain.ErrInvalidState
	}
	filter, err := normalizeDashboardMetricsFilter(startTime, endTime, bucket)
	if err != nil {
		return domain.AdminDashboardMetrics{}, err
	}
	return s.dashboard.DashboardMetrics(ctx, filter)
}

func normalizeDashboardMetricsFilter(startTime, endTime *time.Time, bucket string) (domain.AdminDashboardMetricsFilter, error) {
	const (
		defaultRange = 24 * time.Hour
		maxRange     = 90 * 24 * time.Hour
	)
	now := time.Now().UTC()
	end := now
	if endTime != nil {
		end = endTime.UTC()
	}
	start := end.Add(-defaultRange)
	if startTime != nil {
		start = startTime.UTC()
	}
	if !start.Before(end) {
		return domain.AdminDashboardMetricsFilter{}, domain.ErrInvalidArgument
	}
	if end.Sub(start) > maxRange {
		return domain.AdminDashboardMetricsFilter{}, domain.ErrInvalidArgument
	}
	bucket = strings.ToLower(strings.TrimSpace(bucket))
	switch bucket {
	case "":
		if end.Sub(start) <= 72*time.Hour {
			bucket = "hour"
		} else {
			bucket = "day"
		}
	case "hour", "day":
	default:
		return domain.AdminDashboardMetricsFilter{}, domain.ErrInvalidArgument
	}
	return domain.AdminDashboardMetricsFilter{StartTime: start, EndTime: end, Bucket: bucket}, nil
}
