package service

import (
	"context"
	"errors"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

// MCPActor 是 MCP 调用方在 service 层使用的身份上下文。
type MCPActor struct {
	ID   string
	Role domain.Role
}

// MCPReadDependencies 汇总只读 MCP facade 需要的服务和仓储。
type MCPReadDependencies struct {
	Users       repository.UserRepository
	Auctions    repository.AuctionRepository
	Sessions    repository.LiveSessionRepository
	Bids        repository.BidRepository
	Orders      repository.OrderRepository
	Risk        *RiskService
	AuditLogs   repository.AuditRepository
	AuctionSvc  *AuctionService
	LiveSession *LiveSessionService
	OrderSvc    *OrderService
}

// MCPReadService 提供 MCP 只读入口需要的聚合查询能力。
//
// 该类型不包含 MCP/JSON-RPC 协议细节；协议解析和返回格式由 transport/mcp 负责。
type MCPReadService struct {
	users      repository.UserRepository
	auctions   repository.AuctionRepository
	sessions   repository.LiveSessionRepository
	bids       repository.BidRepository
	orders     repository.OrderRepository
	risk       *RiskService
	audits     repository.AuditRepository
	auctionSvc *AuctionService
	sessionSvc *LiveSessionService
	orderSvc   *OrderService
}

func NewMCPReadService(deps MCPReadDependencies) *MCPReadService {
	return &MCPReadService{
		users:      deps.Users,
		auctions:   deps.Auctions,
		sessions:   deps.Sessions,
		bids:       deps.Bids,
		orders:     deps.Orders,
		risk:       deps.Risk,
		audits:     deps.AuditLogs,
		auctionSvc: deps.AuctionSvc,
		sessionSvc: deps.LiveSession,
		orderSvc:   deps.OrderSvc,
	}
}

type MerchantProfile struct {
	Merchant domain.SafeUser `json:"merchant"`
	Summary  MerchantSummary `json:"summary"`
}

type MerchantSummary struct {
	LiveSessionCount int   `json:"liveSessionCount"`
	SoldLotCount     int   `json:"soldLotCount"`
	GMVCent          int64 `json:"gmvCent"`
}

type LiveSessionSettlement struct {
	SessionID           uint64   `json:"sessionId"`
	SoldCount           int      `json:"soldCount"`
	UnsoldCount         int      `json:"unsoldCount"`
	TotalDealCent       int64    `json:"totalDealCent"`
	PaidOrderCount      int      `json:"paidOrderCount"`
	UnpaidOrderCount    int      `json:"unpaidOrderCount"`
	TimeoutOrderCount   int      `json:"timeoutOrderCount"`
	CancelledOrderCount int      `json:"cancelledOrderCount"`
	TopDeal             *TopDeal `json:"topDeal,omitempty"`
}

type TopDeal struct {
	AuctionID uint64           `json:"auctionId"`
	OrderID   uint64           `json:"orderId"`
	WinnerID  string           `json:"winnerId"`
	DealPrice int64            `json:"dealPrice"`
	PayStatus domain.PayStatus `json:"payStatus"`
}

func (s *MCPReadService) ReadUser(ctx context.Context, userID string, actor MCPActor) (domain.SafeUser, error) {
	if err := requireMCPActor(actor); err != nil {
		return domain.SafeUser{}, err
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		userID = actor.ID
	}
	if actor.Role != domain.RoleAdmin && userID != actor.ID {
		return domain.SafeUser{}, domain.ErrForbidden
	}
	if s.users == nil {
		return domain.SafeUser{}, domain.ErrNotFound
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, err
	}
	return user.Safe(), nil
}

func (s *MCPReadService) ListUsers(ctx context.Context, filter domain.UserFilter, actor MCPActor) ([]domain.SafeUser, error) {
	_ = ctx
	if err := requireMCPActor(actor); err != nil {
		return nil, err
	}
	if actor.Role != domain.RoleAdmin {
		return nil, domain.ErrForbidden
	}
	if s.users == nil {
		return nil, domain.ErrNotFound
	}
	users, err := s.users.List(filter)
	if err != nil {
		return nil, err
	}
	out := make([]domain.SafeUser, 0, len(users))
	for _, user := range users {
		out = append(out, user.Safe())
	}
	return out, nil
}

func (s *MCPReadService) ReadMerchant(ctx context.Context, merchantID string, actor MCPActor) (MerchantProfile, error) {
	if err := requireMCPActor(actor); err != nil {
		return MerchantProfile{}, err
	}
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" && actor.Role == domain.RoleMerchant {
		merchantID = actor.ID
	}
	if merchantID == "" || s.users == nil {
		return MerchantProfile{}, domain.ErrInvalidArgument
	}
	if actor.Role == domain.RoleMerchant && actor.ID != merchantID {
		return MerchantProfile{}, domain.ErrForbidden
	}
	user, err := s.users.FindByID(merchantID)
	if err != nil {
		return MerchantProfile{}, err
	}
	if user.Role != domain.RoleMerchant {
		return MerchantProfile{}, domain.ErrNotFound
	}
	profile := MerchantProfile{Merchant: user.Safe()}
	if actor.Role == domain.RoleBuyer {
		return profile, nil
	}
	if s.sessions != nil {
		sessions, err := s.sessions.List(ctx, domain.LiveSessionFilter{MerchantID: merchantID, Limit: 100})
		if err != nil {
			return MerchantProfile{}, err
		}
		profile.Summary.LiveSessionCount = len(sessions)
		for _, session := range sessions {
			profile.Summary.SoldLotCount += session.LotsSold
			profile.Summary.GMVCent += session.GMVCent
		}
	}
	return profile, nil
}

func (s *MCPReadService) ReadAuctionLot(ctx context.Context, auctionID uint64, actor MCPActor) (domain.AuctionLot, error) {
	if err := requireMCPActor(actor); err != nil {
		return domain.AuctionLot{}, err
	}
	if auctionID == 0 || s.auctions == nil {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	lot, err := s.auctions.FindByID(ctx, auctionID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if err := s.requireAuctionReadable(ctx, lot, actor); err != nil {
		return domain.AuctionLot{}, err
	}
	return lot, nil
}

func (s *MCPReadService) ListAuctionLots(ctx context.Context, filter domain.AuctionFilter, actor MCPActor) ([]domain.AuctionLot, error) {
	if err := requireMCPActor(actor); err != nil {
		return nil, err
	}
	if s.auctions == nil {
		return nil, domain.ErrNotFound
	}
	switch actor.Role {
	case domain.RoleAdmin:
	case domain.RoleMerchant:
		filter.SellerID = actor.ID
	case domain.RoleBuyer:
		if filter.LiveSessionID == 0 {
			return nil, domain.ErrForbidden
		}
		session, err := s.readAuthorizedLiveSession(ctx, filter.LiveSessionID, actor)
		if err != nil {
			return nil, err
		}
		if session.Status != domain.LiveSessionStatusLive {
			return nil, domain.ErrForbidden
		}
		filter.SellerID = ""
	default:
		return nil, domain.ErrForbidden
	}
	return s.auctions.List(ctx, filter)
}

func (s *MCPReadService) ReadAuctionState(ctx context.Context, auctionID uint64, actor MCPActor) (domain.AuctionState, error) {
	if err := requireMCPActor(actor); err != nil {
		return domain.AuctionState{}, err
	}
	if auctionID == 0 || s.auctionSvc == nil {
		return domain.AuctionState{}, domain.ErrInvalidArgument
	}
	return s.auctionSvc.State(ctx, auctionID, actor.ID, actor.Role)
}

func (s *MCPReadService) ListLiveSessions(ctx context.Context, filter domain.LiveSessionFilter, actor MCPActor) ([]domain.LiveSession, error) {
	if err := requireMCPActor(actor); err != nil {
		return nil, err
	}
	if actor.Role == domain.RoleBuyer {
		return nil, domain.ErrForbidden
	}
	if s.sessionSvc == nil {
		return nil, domain.ErrNotFound
	}
	if actor.Role == domain.RoleMerchant || filter.MerchantID != "" {
		return s.sessionSvc.ListByMerchantFiltered(ctx, filter, actor.ID, actor.Role)
	}
	if s.sessions == nil {
		return nil, domain.ErrNotFound
	}
	return s.sessions.List(ctx, filter)
}

func (s *MCPReadService) ReadLiveSession(ctx context.Context, sessionID uint64, actor MCPActor) (domain.LiveSession, error) {
	if err := requireMCPActor(actor); err != nil {
		return domain.LiveSession{}, err
	}
	session, err := s.readAuthorizedLiveSession(ctx, sessionID, actor)
	if err != nil {
		return domain.LiveSession{}, err
	}
	return session, nil
}

func (s *MCPReadService) ListLiveSessionLots(ctx context.Context, sessionID uint64, actor MCPActor) ([]domain.AuctionLot, error) {
	if s.sessionSvc == nil {
		return nil, domain.ErrNotFound
	}
	return s.sessionSvc.ListLots(ctx, sessionID, actor.ID, actor.Role)
}

func (s *MCPReadService) ListLiveSessionBids(ctx context.Context, sessionID uint64, sortBy string, limit, offset int, actor MCPActor) ([]domain.BidRecord, error) {
	if s.sessionSvc == nil {
		return nil, domain.ErrNotFound
	}
	return s.sessionSvc.ListBidsPaged(ctx, sessionID, sortBy, limit, offset, actor.ID, actor.Role)
}

func (s *MCPReadService) ListLiveSessionOrders(ctx context.Context, sessionID uint64, status domain.OrderStatus, payStatus domain.PayStatus, limit, offset int, actor MCPActor) ([]domain.OrderDeal, error) {
	session, err := s.readAuthorizedLiveSession(ctx, sessionID, actor)
	if err != nil {
		return nil, err
	}
	if s.orders == nil {
		return []domain.OrderDeal{}, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	filter := domain.OrderFilter{SellerID: session.MerchantID, LiveSessionID: sessionID, Status: status, PayStatus: payStatus, Limit: limit, Offset: offset}
	if actor.Role == domain.RoleAdmin {
		filter.SellerID = ""
	}
	return s.orders.List(ctx, filter)
}

func (s *MCPReadService) ReadLiveSessionSettlement(ctx context.Context, sessionID uint64, actor MCPActor) (LiveSessionSettlement, error) {
	session, err := s.readAuthorizedLiveSession(ctx, sessionID, actor)
	if err != nil {
		return LiveSessionSettlement{}, err
	}
	settlement := LiveSessionSettlement{
		SessionID:     session.ID,
		SoldCount:     session.LotsSold,
		UnsoldCount:   session.LotsUnsold,
		TotalDealCent: session.GMVCent,
	}
	if s.orders == nil {
		return settlement, nil
	}
	orders, err := s.orders.List(ctx, domain.OrderFilter{LiveSessionID: sessionID, Limit: 100})
	if err != nil {
		return LiveSessionSettlement{}, err
	}
	for _, order := range orders {
		switch order.Status {
		case domain.OrderStatusPaid:
			settlement.PaidOrderCount++
		case domain.OrderStatusTimeout:
			settlement.TimeoutOrderCount++
		case domain.OrderStatusCancelled:
			settlement.CancelledOrderCount++
		default:
			if order.PayStatus == domain.PayStatusPaid {
				settlement.PaidOrderCount++
			} else {
				settlement.UnpaidOrderCount++
			}
		}
		if settlement.TopDeal == nil || order.DealPrice > settlement.TopDeal.DealPrice {
			settlement.TopDeal = &TopDeal{
				AuctionID: order.AuctionID,
				OrderID:   order.ID,
				WinnerID:  order.WinnerID,
				DealPrice: order.DealPrice,
				PayStatus: order.PayStatus,
			}
		}
	}
	return settlement, nil
}

func (s *MCPReadService) ReadOrder(ctx context.Context, orderID uint64, actor MCPActor) (domain.OrderDeal, error) {
	if err := requireMCPActor(actor); err != nil {
		return domain.OrderDeal{}, err
	}
	if s.orderSvc == nil {
		return domain.OrderDeal{}, domain.ErrNotFound
	}
	return s.orderSvc.Get(ctx, orderID, actor.ID, actor.Role)
}

func (s *MCPReadService) ListOrders(ctx context.Context, filter domain.OrderFilter, actor MCPActor) ([]domain.OrderDeal, error) {
	if err := requireMCPActor(actor); err != nil {
		return nil, err
	}
	if s.orderSvc == nil {
		return nil, domain.ErrNotFound
	}
	return s.orderSvc.List(ctx, filter, actor.ID, actor.Role)
}

func (s *MCPReadService) ListRiskEvents(ctx context.Context, filter domain.RiskEventFilter, actor MCPActor) ([]domain.RiskEvent, error) {
	if err := requireMCPActor(actor); err != nil {
		return nil, err
	}
	if actor.Role != domain.RoleAdmin {
		return nil, domain.ErrForbidden
	}
	if s.risk == nil {
		return nil, domain.ErrNotFound
	}
	return s.risk.ListEvents(ctx, filter)
}

func (s *MCPReadService) ListAuditLogs(ctx context.Context, filter domain.AuditFilter, actor MCPActor) ([]domain.AuditLog, error) {
	if err := requireMCPActor(actor); err != nil {
		return nil, err
	}
	switch actor.Role {
	case domain.RoleAdmin:
	case domain.RoleMerchant:
		if filter.OperatorID != "" && filter.OperatorID != actor.ID {
			return nil, domain.ErrForbidden
		}
		filter.OperatorID = actor.ID
	default:
		return nil, domain.ErrForbidden
	}
	if s.audits == nil {
		return nil, domain.ErrNotFound
	}
	return s.audits.List(ctx, filter)
}

func (s *MCPReadService) readAuthorizedLiveSession(ctx context.Context, sessionID uint64, actor MCPActor) (domain.LiveSession, error) {
	if err := requireMCPActor(actor); err != nil {
		return domain.LiveSession{}, err
	}
	if sessionID == 0 || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if canAccessSellerOwned(actor.ID, actor.Role, session.MerchantID) {
		return session, nil
	}
	if actor.Role == domain.RoleBuyer && session.Status == domain.LiveSessionStatusLive {
		return session, nil
	}
	if !canAccessSellerOwned(actor.ID, actor.Role, session.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	return session, nil
}

func (s *MCPReadService) requireAuctionReadable(ctx context.Context, lot domain.AuctionLot, actor MCPActor) error {
	if canAccessSellerOwned(actor.ID, actor.Role, lot.SellerID) {
		return nil
	}
	if actor.Role != domain.RoleBuyer {
		return domain.ErrForbidden
	}
	if lot.LiveSessionID != nil && s.sessions != nil {
		session, err := s.sessions.Get(ctx, *lot.LiveSessionID)
		if err == nil && session.Status == domain.LiveSessionStatusLive {
			return nil
		}
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
	}
	if s.orders != nil {
		order, err := s.orders.FindByAuctionID(ctx, lot.AuctionID)
		if err == nil && order.WinnerID == actor.ID {
			return nil
		}
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
	}
	return domain.ErrForbidden
}

func requireMCPActor(actor MCPActor) error {
	if strings.TrimSpace(actor.ID) == "" || !actor.Role.Valid() {
		return domain.ErrForbidden
	}
	return nil
}
