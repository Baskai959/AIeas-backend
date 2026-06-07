package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/modules/mcp/ports"
)

const maxMCPLiveVoiceBroadcastTextRunes = 1000

// MCPActor 是 MCP 调用方在 service 层使用的身份上下文。
type MCPActor struct {
	ID   string
	Role domain.Role
}

// MCPReadDependencies 汇总只读 MCP facade 需要的依赖。
type MCPReadDependencies struct {
	Users       ports.UserRepository
	Auctions    ports.AuctionRepository
	Sessions    ports.LiveSessionRepository
	Bids        ports.BidRepository
	Orders      ports.OrderRepository
	Risk        ports.RiskUseCase
	AuditLogs   ports.AuditRepository
	AuctionSvc  ports.AuctionStateUseCase
	LiveSession ports.LiveSessionUseCase
	OrderSvc    ports.OrderUseCase
}

// MCPReadService 提供 MCP 只读入口需要的聚合查询能力。
type MCPReadService struct {
	users      ports.UserRepository
	auctions   ports.AuctionRepository
	sessions   ports.LiveSessionRepository
	bids       ports.BidRepository
	orders     ports.OrderRepository
	risk       ports.RiskUseCase
	audits     ports.AuditRepository
	auctionSvc ports.AuctionStateUseCase
	sessionSvc ports.LiveSessionUseCase
	orderSvc   ports.OrderUseCase
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

type CurrentTimeResult struct {
	CurrentTime      string `json:"currentTime"`
	UnixSeconds      int64  `json:"unixSeconds"`
	UnixMilliseconds int64  `json:"unixMilliseconds"`
	Timezone         string `json:"timezone"`
}

func currentTimeResult(now time.Time) CurrentTimeResult {
	now = now.UTC()
	return CurrentTimeResult{
		CurrentTime:      now.Format(time.RFC3339Nano),
		UnixSeconds:      now.Unix(),
		UnixMilliseconds: now.UnixMilli(),
		Timezone:         "UTC",
	}
}

func (s *MCPReadService) CurrentTime(ctx context.Context, actor MCPActor) (CurrentTimeResult, error) {
	_ = ctx
	if err := requireMCPActor(actor); err != nil {
		return CurrentTimeResult{}, err
	}
	return currentTimeResult(time.Now().UTC()), nil
}

func (s *MCPReadService) ReadUser(ctx context.Context, userID string, actor MCPActor) (domain.SafeUser, error) {
	_ = ctx
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

// MCPLiveControlDependencies 汇总 MCP live façade 控制能力所需依赖。
type MCPLiveControlDependencies struct {
	Auctions             ports.AuctionRepository
	Sessions             ports.LiveSessionRepository
	LiveSessionSvc       ports.LiveSessionUseCase
	AuctionSvc           ports.AuctionStateUseCase
	HammerSvc            ports.HammerUseCase
	LiveVoiceSynthesizer LiveVoiceSynthesizer
	LiveVoiceBroadcaster LiveVoiceBroadcaster
	AIAssistant          ports.AIAssistantFacade
}

type MCPControlService struct {
	auctions             ports.AuctionRepository
	sessions             ports.LiveSessionRepository
	sessionSvc           ports.LiveSessionUseCase
	auctionSvc           ports.AuctionStateUseCase
	hammerSvc            ports.HammerUseCase
	voiceSynthesizer     LiveVoiceSynthesizer
	liveVoiceBroadcaster LiveVoiceBroadcaster
	aiAssistant          ports.AIAssistantFacade
}

func NewMCPControlService(deps MCPLiveControlDependencies) *MCPControlService {
	return &MCPControlService{
		auctions:             deps.Auctions,
		sessions:             deps.Sessions,
		sessionSvc:           deps.LiveSessionSvc,
		auctionSvc:           deps.AuctionSvc,
		hammerSvc:            deps.HammerSvc,
		voiceSynthesizer:     deps.LiveVoiceSynthesizer,
		liveVoiceBroadcaster: deps.LiveVoiceBroadcaster,
		aiAssistant:          deps.AIAssistant,
	}
}

func (s *MCPControlService) CurrentTime(ctx context.Context, actor MCPActor) (CurrentTimeResult, error) {
	_ = ctx
	if err := requireMCPActor(actor); err != nil {
		return CurrentTimeResult{}, err
	}
	return currentTimeResult(time.Now().UTC()), nil
}

type LiveSessionStats = ports.LiveSessionStats
type LiveVoiceSynthesizer = ports.LiveVoiceSynthesizer
type LiveVoiceBroadcaster = ports.LiveVoiceBroadcaster
type LiveVoiceSynthesisInput = ports.LiveVoiceSynthesisInput
type LiveVoiceSynthesisResult = ports.LiveVoiceSynthesisResult
type LiveVoiceBroadcastPayload = ports.LiveVoiceBroadcastPayload

type MCPLiveControlContext struct {
	MerchantID          string                      `json:"merchantId"`
	Session             *domain.LiveSession         `json:"session,omitempty"`
	Stats               LiveSessionStats            `json:"stats"`
	CurrentAuctionState *MCPLiveCurrentAuctionState `json:"currentAuctionState,omitempty"`
	Lots                MCPLiveControlLotState      `json:"lots"`
}

type MCPLiveControlLotState struct {
	ExplainingLot *domain.AuctionLot  `json:"explainingLot,omitempty"`
	SessionLots   []domain.AuctionLot `json:"sessionLots"`
	SoldLots      []domain.AuctionLot `json:"soldLots"`
	UnsoldLots    []domain.AuctionLot `json:"unsoldLots"`
	UpcomingLots  []domain.AuctionLot `json:"upcomingLots"`
	CandidateLots []domain.AuctionLot `json:"candidateLots"`
}

type MCPLiveCurrentAuctionState struct {
	AuctionID      uint64               `json:"auctionId"`
	Status         domain.AuctionStatus `json:"status"`
	CurrentPrice   int64                `json:"currentPrice"`
	LeaderBidderID string               `json:"leaderBidderId,omitempty"`
	StartTime      time.Time            `json:"startTime"`
	EndTime        time.Time            `json:"endTime"`
	RemainSeconds  int64                `json:"remainSeconds"`
	LastBidTSMS    int64                `json:"lastBidTsMs,omitempty"`
	ExtendCount    int                  `json:"extendCount,omitempty"`
	Version        int64                `json:"version,omitempty"`
	Source         string               `json:"source,omitempty"`
}

type MCPLiveLotOperationInput struct {
	LiveSessionID uint64
	AuctionID     uint64
	Action        string
	DurationSec   int
	StartTime     *time.Time
	Force         bool
	RequestID     string
}

type MCPLiveLotOperationResult struct {
	Action        string                 `json:"action"`
	LiveSessionID uint64                 `json:"liveSessionId"`
	AuctionID     uint64                 `json:"auctionId"`
	Lot           *domain.AuctionLot     `json:"lot,omitempty"`
	Session       *domain.LiveSession    `json:"session,omitempty"`
	HammerResult  *domain.HammerResult   `json:"hammerResult,omitempty"`
	Order         *domain.OrderDeal      `json:"order,omitempty"`
	Removed       bool                   `json:"removed,omitempty"`
	Context       *MCPLiveControlContext `json:"context,omitempty"`
}

type MCPLiveVoiceBroadcastInput struct {
	LiveSessionID uint64
	Text          string
	RequestID     string
}

type MCPLiveVoiceBroadcastResult struct {
	LiveSessionID uint64    `json:"liveSessionId"`
	Text          string    `json:"text"`
	RequestID     string    `json:"requestId,omitempty"`
	Status        string    `json:"status"`
	Message       string    `json:"message,omitempty"`
	AudioFormat   string    `json:"audioFormat,omitempty"`
	Encoding      string    `json:"encoding,omitempty"`
	SampleRate    int       `json:"sampleRate,omitempty"`
	Channels      int       `json:"channels,omitempty"`
	Voice         string    `json:"voice,omitempty"`
	Provider      string    `json:"provider,omitempty"`
	AudioBytes    int       `json:"audioBytes,omitempty"`
	Delivered     int       `json:"delivered"`
	CreatedAt     time.Time `json:"createdAt"`
}

func (s *MCPControlService) ReadMerchantLiveControlContext(ctx context.Context, merchantID string, actor MCPActor) (MCPLiveControlContext, error) {
	if err := requireMCPActor(actor); err != nil {
		return MCPLiveControlContext{}, err
	}
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" && actor.Role == domain.RoleMerchant {
		merchantID = actor.ID
	}
	if merchantID == "" || s == nil || s.sessions == nil || s.sessionSvc == nil {
		return MCPLiveControlContext{}, domain.ErrInvalidArgument
	}
	if !canAccessSellerOwned(actor.ID, actor.Role, merchantID) {
		return MCPLiveControlContext{}, domain.ErrForbidden
	}
	session, ok, err := s.currentLiveControlSession(ctx, merchantID)
	if err != nil {
		return MCPLiveControlContext{}, err
	}
	if !ok {
		return MCPLiveControlContext{MerchantID: merchantID}, nil
	}
	s.notifyAIStatus(ctx, session.ID, merchantID, "get_merchant_live_control_context", "running", "正在查询直播场次信息", "")
	contextPayload, err := s.buildLiveControlContext(ctx, session, actor)
	if err != nil {
		s.notifyAIStatus(ctx, session.ID, merchantID, "get_merchant_live_control_context", "failed", "直播场次信息查询失败", "")
		return MCPLiveControlContext{}, err
	}
	s.notifyAIStatus(ctx, session.ID, merchantID, "get_merchant_live_control_context", "completed", "直播场次信息查询完成", "")
	return contextPayload, nil
}

func (s *MCPControlService) OperateLiveSessionLot(ctx context.Context, in MCPLiveLotOperationInput, actor MCPActor) (MCPLiveLotOperationResult, error) {
	if err := requireMCPActor(actor); err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	if s == nil || s.sessions == nil || s.sessionSvc == nil || in.LiveSessionID == 0 || in.AuctionID == 0 {
		return MCPLiveLotOperationResult{}, domain.ErrInvalidArgument
	}
	session, err := s.requireLiveControlSession(ctx, in.LiveSessionID, actor)
	if err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	action := normalizeMCPLiveLotAction(in.Action)
	if action == "" {
		return MCPLiveLotOperationResult{}, domain.ErrInvalidArgument
	}
	if in.DurationSec < 0 || (action == "startExplain" && in.DurationSec == 0) || (action != "startExplain" && in.DurationSec != 0) || (action != "startExplain" && in.StartTime != nil) {
		return MCPLiveLotOperationResult{}, domain.ErrInvalidArgument
	}
	lotName := s.mcpLotDisplayName(ctx, in.AuctionID)
	if mcpLiveLotActionRequiresApproval(action) {
		if err := s.requestAIControlPermission(ctx, session, "operate_live_session_lot", action, lotName, in.RequestID); err != nil {
			return MCPLiveLotOperationResult{}, err
		}
	}
	ctx = context.WithoutCancel(ctx)
	s.notifyAIStatus(ctx, session.ID, session.MerchantID, "operate_live_session_lot", "running", MCPLiveLotActionRunningMessage(action, lotName), in.RequestID)
	result := MCPLiveLotOperationResult{Action: action, LiveSessionID: session.ID, AuctionID: in.AuctionID}
	fail := func(err error) (MCPLiveLotOperationResult, error) {
		s.notifyAIStatus(ctx, session.ID, session.MerchantID, "operate_live_session_lot", "failed", "AI 控制操作执行失败", in.RequestID)
		return MCPLiveLotOperationResult{}, err
	}

	switch action {
	case "onShelf":
		lot, err := s.sessionSvc.MountAuction(ctx, session.ID, in.AuctionID, actor.ID, actor.Role)
		if err != nil {
			return fail(err)
		}
		result.Lot = &lot
	case "offShelf":
		if err := s.sessionSvc.UnmountAuction(ctx, session.ID, in.AuctionID, actor.ID, actor.Role); err != nil {
			return fail(err)
		}
		result.Removed = true
		if s.auctions != nil {
			if lot, err := s.auctions.FindByID(ctx, in.AuctionID); err == nil {
				result.Lot = &lot
			}
		}
	case "startExplain":
		lot, err := s.sessionSvc.ActivateAuctionWithOptions(ctx, ports.ActivateLiveSessionAuctionInput{
			SessionID:   session.ID,
			AuctionID:   in.AuctionID,
			ActorID:     actor.ID,
			ActorRole:   actor.Role,
			DurationSec: in.DurationSec,
			StartTime:   in.StartTime,
		})
		if err != nil {
			return fail(err)
		}
		result.Lot = &lot
	case "hammer":
		if session.ActiveAuctionID != in.AuctionID {
			return fail(domain.ErrInvalidState)
		}
		if s.hammerSvc == nil {
			return fail(domain.ErrNotFound)
		}
		requestID := strings.TrimSpace(in.RequestID)
		if requestID == "" {
			requestID = fmt.Sprintf("mcp-hammer-%d-%d-%d", session.ID, in.AuctionID, time.Now().UTC().UnixNano())
		}
		hammerResult, order, err := s.hammerSvc.Hammer(ctx, domain.HammerInput{
			RequestID: requestID,
			AuctionID: in.AuctionID,
			ActorID:   actor.ID,
			ActorRole: actor.Role,
			ClosedBy:  actor.ID,
			Force:     in.Force,
			Now:       time.Now().UTC(),
		})
		if err != nil {
			return fail(err)
		}
		result.HammerResult = &hammerResult
		result.Order = order
		if s.auctions != nil {
			if lot, err := s.auctions.FindByID(ctx, in.AuctionID); err == nil {
				result.Lot = &lot
			}
		}
	case "endLive":
		if session.ActiveAuctionID != 0 && session.ActiveAuctionID != in.AuctionID {
			return fail(domain.ErrInvalidState)
		}
		updated, err := s.sessionSvc.DeactivateAuction(ctx, session.ID, actor.ID, actor.Role)
		if err != nil {
			return fail(err)
		}
		result.Session = &updated
	default:
		return fail(domain.ErrInvalidArgument)
	}

	latestSession, err := s.sessions.Get(ctx, session.ID)
	if err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	contextPayload, err := s.buildLiveControlContext(ctx, latestSession, actor)
	if err != nil {
		return fail(err)
	}
	result.Context = &contextPayload
	s.notifyAIStatus(ctx, session.ID, session.MerchantID, "operate_live_session_lot", "completed", MCPLiveLotActionCompletedMessage(action, lotName), in.RequestID)
	return result, nil
}

func (s *MCPControlService) CreateLiveVoiceBroadcast(ctx context.Context, in MCPLiveVoiceBroadcastInput, actor MCPActor) (MCPLiveVoiceBroadcastResult, error) {
	if err := requireMCPActor(actor); err != nil {
		return MCPLiveVoiceBroadcastResult{}, err
	}
	if actor.Role != domain.RoleMerchant && actor.Role != domain.RoleAdmin {
		return MCPLiveVoiceBroadcastResult{}, domain.ErrForbidden
	}
	text := strings.TrimSpace(in.Text)
	if s == nil || s.sessions == nil || in.LiveSessionID == 0 || text == "" {
		return MCPLiveVoiceBroadcastResult{}, domain.ErrInvalidArgument
	}
	if len([]rune(text)) > maxMCPLiveVoiceBroadcastTextRunes {
		return MCPLiveVoiceBroadcastResult{}, domain.ErrInvalidArgument
	}
	session, err := s.requireLiveControlSession(ctx, in.LiveSessionID, actor)
	if err != nil {
		return MCPLiveVoiceBroadcastResult{}, err
	}
	requestID := strings.TrimSpace(in.RequestID)
	if s.aiAssistant != nil {
		s.aiAssistant.NotifyBroadcast(ctx, session.ID, session.MerchantID, text, requestID)
	}
	if s.voiceSynthesizer == nil {
		return MCPLiveVoiceBroadcastResult{}, domain.ErrInvalidState
	}
	synthesized, err := s.voiceSynthesizer.SynthesizeLiveVoice(ctx, LiveVoiceSynthesisInput{
		LiveSessionID: session.ID,
		Text:          text,
		RequestID:     requestID,
	})
	if err != nil {
		s.notifyAIStatus(ctx, session.ID, session.MerchantID, "live_voice_broadcast", "failed", "AI 直播播报生成失败", requestID)
		return MCPLiveVoiceBroadcastResult{}, err
	}
	if len(synthesized.Audio) == 0 {
		s.notifyAIStatus(ctx, session.ID, session.MerchantID, "live_voice_broadcast", "failed", "AI 直播播报生成失败", requestID)
		return MCPLiveVoiceBroadcastResult{}, domain.ErrInvalidState
	}
	createdAt := time.Now().UTC()
	payload := LiveVoiceBroadcastPayload{
		LiveSessionID: session.ID,
		Text:          text,
		RequestID:     requestID,
		AudioBase64:   base64.StdEncoding.EncodeToString(synthesized.Audio),
		AudioFormat:   synthesized.AudioFormat,
		Encoding:      synthesized.Encoding,
		SampleRate:    synthesized.SampleRate,
		Channels:      synthesized.Channels,
		Voice:         synthesized.Voice,
		Provider:      synthesized.Provider,
		AudioBytes:    len(synthesized.Audio),
		CreatedAt:     createdAt,
	}
	status := "GENERATED"
	message := "语音已生成，但未配置 WebSocket 推送器。"
	delivered := 0
	if s.liveVoiceBroadcaster != nil {
		delivered, err = s.liveVoiceBroadcaster.BroadcastLiveVoice(ctx, session.ID, payload)
		if err != nil {
			s.notifyAIStatus(ctx, session.ID, session.MerchantID, "live_voice_broadcast", "failed", "AI 直播播报推送失败", requestID)
			return MCPLiveVoiceBroadcastResult{}, err
		}
		if delivered > 0 {
			status = "BROADCASTED"
			message = "语音已生成并通过 WebSocket 推送。"
		} else {
			message = "语音已生成，但当前没有订阅该直播场次的 WebSocket 客户端。"
		}
	}
	s.notifyAIStatus(ctx, session.ID, session.MerchantID, "live_voice_broadcast", "completed", "AI 直播播报已生成", requestID)
	return MCPLiveVoiceBroadcastResult{
		LiveSessionID: session.ID,
		Text:          text,
		RequestID:     requestID,
		Status:        status,
		Message:       message,
		AudioFormat:   synthesized.AudioFormat,
		Encoding:      synthesized.Encoding,
		SampleRate:    synthesized.SampleRate,
		Channels:      synthesized.Channels,
		Voice:         synthesized.Voice,
		Provider:      synthesized.Provider,
		AudioBytes:    len(synthesized.Audio),
		Delivered:     delivered,
		CreatedAt:     createdAt,
	}, nil
}

func (s *MCPControlService) requireLiveControlSession(ctx context.Context, sessionID uint64, actor MCPActor) (domain.LiveSession, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if session.Status != domain.LiveSessionStatusLive {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if !canAccessSellerOwned(actor.ID, actor.Role, session.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	return session, nil
}

func (s *MCPControlService) buildLiveControlContext(ctx context.Context, session domain.LiveSession, actor MCPActor) (MCPLiveControlContext, error) {
	stats, err := s.sessionSvc.Stats(ctx, session.ID, actor.ID, actor.Role)
	if err != nil {
		return MCPLiveControlContext{}, err
	}
	out := MCPLiveControlContext{MerchantID: session.MerchantID, Session: &session, Stats: stats}
	sessionLots, err := s.sessionSvc.ListLots(ctx, session.ID, actor.ID, actor.Role)
	if err != nil {
		return MCPLiveControlContext{}, err
	}
	var sellerLots []domain.AuctionLot
	if s.auctions != nil {
		sellerLots, err = s.auctions.List(ctx, domain.AuctionFilter{SellerID: session.MerchantID, Limit: 100})
		if err != nil {
			return MCPLiveControlContext{}, err
		}
	}
	if session.ActiveAuctionID != 0 && s.auctions != nil {
		if lot, err := s.auctions.FindByID(ctx, session.ActiveAuctionID); err == nil {
			out.Lots.ExplainingLot = &lot
		} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return MCPLiveControlContext{}, err
		}
	}
	if session.ActiveAuctionID != 0 {
		currentState, err := s.readCurrentAuctionState(ctx, session.ActiveAuctionID, actor)
		if err != nil {
			return MCPLiveControlContext{}, err
		}
		out.CurrentAuctionState = currentState
	}

	for _, lot := range sellerLots {
		if lot.LiveSessionID != nil && *lot.LiveSessionID == session.ID {
			out.Lots.SessionLots = append(out.Lots.SessionLots, lot)
			switch lot.Status {
			case domain.AuctionStatusClosedWon:
				out.Lots.SoldLots = append(out.Lots.SoldLots, lot)
			case domain.AuctionStatusClosedFailed:
				out.Lots.UnsoldLots = append(out.Lots.UnsoldLots, lot)
			}
		}
		if lot.LiveSessionID == nil && isMCPMountCandidate(lot.Status) {
			out.Lots.CandidateLots = append(out.Lots.CandidateLots, lot)
		}
	}
	for _, lot := range sessionLots {
		if lot.AuctionID == session.ActiveAuctionID || lot.Status.Terminal() {
			continue
		}
		out.Lots.UpcomingLots = append(out.Lots.UpcomingLots, lot)
	}
	return out, nil
}

func (s *MCPControlService) readCurrentAuctionState(ctx context.Context, auctionID uint64, actor MCPActor) (*MCPLiveCurrentAuctionState, error) {
	var state domain.AuctionState
	var err error
	if s.auctionSvc != nil {
		state, err = s.auctionSvc.State(ctx, auctionID, actor.ID, actor.Role)
	} else if s.auctions != nil {
		lot, findErr := s.auctions.FindByID(ctx, auctionID)
		if findErr != nil {
			err = findErr
		} else {
			currentPrice := lot.StartPrice
			if lot.DealPrice != nil {
				currentPrice = *lot.DealPrice
			}
			leaderBidderID := ""
			if lot.WinnerID != nil {
				leaderBidderID = *lot.WinnerID
			}
			state = domain.AuctionState{
				AuctionID:      lot.AuctionID,
				Status:         lot.Status,
				CurrentPrice:   currentPrice,
				LeaderBidderID: leaderBidderID,
				StartTime:      lot.StartTime,
				EndTime:        lot.EndTime,
				Version:        lot.UpdatedAt.UnixMilli(),
				Source:         "db",
			}
		}
	} else {
		return nil, nil
	}
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return mcpCurrentAuctionStateFromDomain(state), nil
}

func mcpCurrentAuctionStateFromDomain(state domain.AuctionState) *MCPLiveCurrentAuctionState {
	remain := int64(0)
	if !state.EndTime.IsZero() {
		remain = int64(time.Until(state.EndTime).Seconds())
		if remain < 0 {
			remain = 0
		}
	}
	return &MCPLiveCurrentAuctionState{
		AuctionID:      state.AuctionID,
		Status:         state.Status,
		CurrentPrice:   state.CurrentPrice,
		LeaderBidderID: state.LeaderBidderID,
		StartTime:      state.StartTime,
		EndTime:        state.EndTime,
		RemainSeconds:  remain,
		LastBidTSMS:    state.LastBidTSMS,
		ExtendCount:    state.ExtendCount,
		Version:        state.Version,
		Source:         state.Source,
	}
}

func (s *MCPControlService) currentLiveControlSession(ctx context.Context, merchantID string) (domain.LiveSession, bool, error) {
	if s.sessions == nil {
		return domain.LiveSession{}, false, nil
	}
	if session, err := s.sessions.GetActiveByMerchantID(ctx, merchantID); err == nil {
		return session, true, nil
	} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return domain.LiveSession{}, false, err
	}
	sessions, err := s.sessions.List(ctx, domain.LiveSessionFilter{MerchantID: merchantID, Limit: 1})
	if err != nil {
		return domain.LiveSession{}, false, err
	}
	if len(sessions) == 0 {
		return domain.LiveSession{}, false, nil
	}
	return sessions[0], true, nil
}

func normalizeMCPLiveLotAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "onshelf", "on_shelf", "mount", "上架":
		return "onShelf"
	case "offshelf", "off_shelf", "unmount", "下架":
		return "offShelf"
	case "startexplain", "start_explain", "activate", "讲解", "开始讲解":
		return "startExplain"
	case "hammer", "close", "落槌", "成交":
		return "hammer"
	case "endlive", "end_live", "下播":
		return "endLive"
	default:
		return ""
	}
}

func mcpLiveLotActionRequiresApproval(action string) bool {
	return action != "startExplain"
}

func (s *MCPControlService) requestAIControlPermission(ctx context.Context, session domain.LiveSession, toolName, action, lotName, requestID string) error {
	if s == nil || s.aiAssistant == nil {
		return nil
	}
	_, err := s.aiAssistant.RequestApproval(context.WithoutCancel(ctx), ports.ApprovalInput{
		MerchantID:    session.MerchantID,
		LiveSessionID: session.ID,
		ToolName:      toolName,
		RequestID:     requestID,
		Message:       MCPLiveLotActionApprovalMessage(action, lotName),
	})
	return err
}

func (s *MCPControlService) notifyAIStatus(ctx context.Context, liveSessionID uint64, merchantID, toolName, status, message, requestID string) {
	if s == nil || s.aiAssistant == nil {
		return
	}
	s.aiAssistant.NotifyStatus(ctx, liveSessionID, merchantID, toolName, status, message, requestID)
}

func (s *MCPControlService) mcpLotDisplayName(ctx context.Context, auctionID uint64) string {
	if s != nil && s.auctions != nil && auctionID != 0 {
		if lot, err := s.auctions.FindByID(ctx, auctionID); err == nil {
			if title := strings.TrimSpace(lot.Title); title != "" {
				return title
			}
		}
	}
	return "未命名拍品"
}

func MCPLiveLotActionApprovalMessage(action, lotName string) string {
	lotName = mcpQuoteLotName(lotName)
	switch action {
	case "onShelf":
		return fmt.Sprintf("AI 请求上架%s，是否允许执行？", lotName)
	case "offShelf":
		return fmt.Sprintf("AI 请求下架%s，是否允许执行？", lotName)
	case "startExplain":
		return fmt.Sprintf("AI 请求开始讲解%s，是否允许执行？", lotName)
	case "hammer":
		return fmt.Sprintf("AI 请求对%s落槌成交，是否允许执行？", lotName)
	case "endLive":
		return "AI 请求结束当前直播，是否允许执行？"
	default:
		return "AI 请求执行直播控制操作，是否允许执行？"
	}
}

func MCPLiveLotActionRunningMessage(action, lotName string) string {
	lotName = mcpQuoteLotName(lotName)
	switch action {
	case "onShelf":
		return fmt.Sprintf("正在上架%s", lotName)
	case "offShelf":
		return fmt.Sprintf("正在下架%s", lotName)
	case "startExplain":
		return fmt.Sprintf("正在开始讲解%s", lotName)
	case "hammer":
		return fmt.Sprintf("正在对%s落槌", lotName)
	case "endLive":
		return "正在结束当前直播"
	default:
		return "正在执行直播控制操作"
	}
}

func MCPLiveLotActionCompletedMessage(action, lotName string) string {
	lotName = mcpQuoteLotName(lotName)
	switch action {
	case "onShelf":
		return fmt.Sprintf("%s已上架", lotName)
	case "offShelf":
		return fmt.Sprintf("%s已下架", lotName)
	case "startExplain":
		return fmt.Sprintf("%s已开始讲解", lotName)
	case "hammer":
		return fmt.Sprintf("%s已落槌", lotName)
	case "endLive":
		return "当前直播已结束"
	default:
		return "直播控制操作已完成"
	}
}

func mcpQuoteLotName(lotName string) string {
	lotName = strings.TrimSpace(lotName)
	if lotName == "" {
		lotName = "未命名拍品"
	}
	return fmt.Sprintf("「%s」", lotName)
}

func isMCPMountCandidate(status domain.AuctionStatus) bool {
	switch status {
	case domain.AuctionStatusDraft, domain.AuctionStatusPendingAudit, domain.AuctionStatusReady:
		return true
	default:
		return false
	}
}

func requireMCPActor(actor MCPActor) error {
	if strings.TrimSpace(actor.ID) == "" || !actor.Role.Valid() {
		return domain.ErrForbidden
	}
	return nil
}

func canAccessSellerOwned(actorID string, actorRole domain.Role, sellerID string) bool {
	sellerID = strings.TrimSpace(sellerID)
	if sellerID == "" {
		return false
	}
	if actorRole == domain.RoleAdmin {
		return true
	}
	return actorRole == domain.RoleMerchant && strings.TrimSpace(actorID) == sellerID
}
