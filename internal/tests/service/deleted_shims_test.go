package service

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
	adminports "aieas_backend/internal/modules/admin/ports"
	aiapp "aieas_backend/internal/modules/ai/app"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	authapp "aieas_backend/internal/modules/auth/app"
	mcpapp "aieas_backend/internal/modules/mcp/app"
	mcpports "aieas_backend/internal/modules/mcp/ports"
	orderapp "aieas_backend/internal/modules/order/app"
	orderports "aieas_backend/internal/modules/order/ports"
	riskapp "aieas_backend/internal/modules/risk/app"
	"aieas_backend/internal/tests/repository"
	jwtpkg "aieas_backend/pkg/jwt"
)

type UpdateProfileInput = authapp.UpdateProfileInput

func NewAuthService(users repository.UserRepository, jwt *jwtpkg.Manager) *authapp.AuthService {
	return authapp.NewAuthService(users, jwt, repositoryPasswordHasher{})
}

type repositoryPasswordHasher struct{}

func (repositoryPasswordHasher) Matches(password, passwordHash string) bool {
	return repository.HashPassword(password) == passwordHash
}

type HammerService = auctionapp.HammerService

type HammerServiceDeps struct {
	Auctions        repository.AuctionRepository
	Orders          repository.OrderRepository
	Deposits        repository.DepositRepository
	Realtime        repository.AuctionRealtimeStore
	Tx              repository.TxManager
	Publisher       EventPublisher
	OrderID         auctionports.OrderIDGenerator
	Sessions        auctionapp.LiveSessionCounterWriter
	LiveAgentHook   auctionapp.HammerLiveAgentHook
	Events          auctionports.SettlementEventPublisher
	OnClose         func(ctx context.Context, auctionID uint64)
	OrderPayTimeout time.Duration
}

type OrderIDGenerator = auctionports.OrderIDGenerator

func NewHammerService(auctions repository.AuctionRepository, orders repository.OrderRepository, deposits repository.DepositRepository, realtime repository.AuctionRealtimeStore, tx repository.TxManager, publisher EventPublisher) *HammerService {
	return auctionapp.NewHammerService(auctions, orders, deposits, realtime, tx, auctionEventPublisherAdapter{publisher: publisher})
}

func NewHammerServiceWithDeps(deps HammerServiceDeps) *HammerService {
	if deps.Realtime == nil {
		deps.Realtime = repository.NoopRealtimeStore{}
	}
	if deps.Tx == nil {
		deps.Tx = repository.NoopTxManager{}
	}
	return auctionapp.NewHammerServiceWithDeps(auctionapp.HammerServiceDeps{
		Auctions:        deps.Auctions,
		Orders:          deps.Orders,
		Deposits:        deps.Deposits,
		Realtime:        deps.Realtime,
		Tx:              deps.Tx,
		Publisher:       auctionEventPublisherAdapter{publisher: deps.Publisher},
		OrderID:         deps.OrderID,
		Sessions:        deps.Sessions,
		LiveAgentHook:   deps.LiveAgentHook,
		Events:          deps.Events,
		OnClose:         deps.OnClose,
		OrderPayTimeout: deps.OrderPayTimeout,
	})
}

type OrderService = orderapp.OrderService

const (
	DefaultOrderPayTimeout           = orderports.DefaultPayTimeout
	DefaultOrderTimeoutScanInterval  = orderports.DefaultTimeoutScanInterval
	DefaultOrderTimeoutScanBatchSize = orderports.DefaultTimeoutScanBatchSize
)

func NewOrderService(orders repository.OrderRepository, tx repository.TxManager) *OrderService {
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	return orderapp.NewOrderService(orders, tx)
}

type RiskControlService struct {
	inner *riskapp.RiskControlService
	cfg   domain.RiskControlConfig
}

func NewRiskControlService(cfg domain.RiskControlConfig) *RiskControlService {
	return &RiskControlService{inner: riskapp.NewRiskControlService(cfg), cfg: cfg}
}

func (s *RiskControlService) Config(ctx context.Context) domain.RiskControlConfig {
	_ = ctx
	if s == nil {
		return domain.DefaultRiskControlConfig()
	}
	if s.inner == nil {
		s.inner = riskapp.NewRiskControlService(s.cfg)
	}
	return s.cfg
}

func (s *RiskControlService) Enabled(ctx context.Context) bool {
	if s == nil {
		return false
	}
	if s.inner == nil || s.inner.Config(ctx) != s.cfg {
		s.inner = riskapp.NewRiskControlService(s.cfg)
	}
	return s.inner.Enabled(ctx)
}

type MCPActor = mcpapp.MCPActor
type MCPControlService = mcpapp.MCPControlService
type LiveVoiceSynthesizer = mcpapp.LiveVoiceSynthesizer
type LiveVoiceBroadcaster = mcpapp.LiveVoiceBroadcaster
type MCPLiveControlContext = mcpapp.MCPLiveControlContext
type MCPLiveControlLotState = mcpapp.MCPLiveControlLotState
type MCPLiveCurrentAuctionState = mcpapp.MCPLiveCurrentAuctionState
type MCPLiveLotOperationInput = mcpapp.MCPLiveLotOperationInput
type MCPLiveLotOperationResult = mcpapp.MCPLiveLotOperationResult
type MCPLiveVoiceBroadcastInput = mcpapp.MCPLiveVoiceBroadcastInput
type MCPLiveVoiceBroadcastResult = mcpapp.MCPLiveVoiceBroadcastResult

type MCPLiveControlDependencies struct {
	Auctions             repository.AuctionRepository
	Sessions             repository.LiveSessionRepository
	LiveSessionSvc       *LiveSessionService
	AuctionSvc           *AuctionService
	HammerSvc            *HammerService
	LiveVoiceSynthesizer LiveVoiceSynthesizer
	LiveVoiceBroadcaster LiveVoiceBroadcaster
	AIAssistant          *aiapp.AIAssistantService
}

func NewMCPControlService(deps MCPLiveControlDependencies) *MCPControlService {
	var aiAssistant mcpports.AIAssistantFacade
	if deps.AIAssistant != nil {
		aiAssistant = deps.AIAssistant
	}
	return mcpapp.NewMCPControlService(mcpapp.MCPLiveControlDependencies{
		Auctions:             deps.Auctions,
		Sessions:             deps.Sessions,
		LiveSessionSvc:       newTestMCPLiveSessionUseCaseAdapter(deps.LiveSessionSvc),
		AuctionSvc:           deps.AuctionSvc,
		HammerSvc:            deps.HammerSvc,
		LiveVoiceSynthesizer: deps.LiveVoiceSynthesizer,
		LiveVoiceBroadcaster: deps.LiveVoiceBroadcaster,
		AIAssistant:          aiAssistant,
	})
}

func mcpLiveLotActionApprovalMessage(action, lotName string) string {
	return mcpapp.MCPLiveLotActionApprovalMessage(action, lotName)
}

func mcpLiveLotActionRunningMessage(action, lotName string) string {
	return mcpapp.MCPLiveLotActionRunningMessage(action, lotName)
}

func mcpLiveLotActionCompletedMessage(action, lotName string) string {
	return mcpapp.MCPLiveLotActionCompletedMessage(action, lotName)
}

const (
	blacklistStrategyDescription = adminports.BlacklistStrategyDescription
	systemBlacklistActorID       = adminports.SystemBlacklistActorID
)

var ErrUserRejected = aiapp.ErrUserRejected

func readBlacklistStrategyConfig(ctx context.Context, configs repository.ConfigRepository) (domain.BlacklistStrategyConfig, error) {
	return adminports.ReadBlacklistStrategyConfig(ctx, configs)
}

func upsertBlacklistStrategyConfig(ctx context.Context, configs repository.ConfigRepository, cfg domain.BlacklistStrategyConfig, actorID string) (domain.BlacklistStrategyConfig, error) {
	return adminports.UpsertBlacklistStrategyConfig(ctx, configs, cfg, actorID)
}

func blacklistExpiresAt(cfg domain.BlacklistStrategyConfig, now time.Time) *time.Time {
	return adminports.BlacklistExpiresAt(cfg, now)
}

type testMCPLiveSessionUseCaseAdapter struct {
	svc *LiveSessionService
}

func newTestMCPLiveSessionUseCaseAdapter(svc *LiveSessionService) mcpports.LiveSessionUseCase {
	if svc == nil {
		return nil
	}
	return testMCPLiveSessionUseCaseAdapter{svc: svc}
}

func (a testMCPLiveSessionUseCaseAdapter) ListByMerchantFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error) {
	return a.svc.ListByMerchantFiltered(ctx, filter, actorID, actorRole)
}

func (a testMCPLiveSessionUseCaseAdapter) ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	return a.svc.ListLots(ctx, sessionID, actorID, actorRole)
}

func (a testMCPLiveSessionUseCaseAdapter) ListBidsPaged(ctx context.Context, sessionID uint64, sortBy string, limit, offset int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	return a.svc.ListBidsPaged(ctx, sessionID, sortBy, limit, offset, actorID, actorRole)
}

func (a testMCPLiveSessionUseCaseAdapter) Stats(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (mcpports.LiveSessionStats, error) {
	stats, err := a.svc.Stats(ctx, sessionID, actorID, actorRole)
	if err != nil {
		return mcpports.LiveSessionStats{}, err
	}
	return mcpports.LiveSessionStats(stats), nil
}

func (a testMCPLiveSessionUseCaseAdapter) AgentHookConfig(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (mcpports.LiveAgentHookConfig, error) {
	cfg, err := a.svc.AgentHookConfig(ctx, sessionID, actorID, actorRole)
	if err != nil {
		return mcpports.LiveAgentHookConfig{}, err
	}
	return mcpports.LiveAgentHookConfig(cfg), nil
}

func (a testMCPLiveSessionUseCaseAdapter) MountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	return a.svc.MountAuction(ctx, sessionID, auctionID, actorID, actorRole)
}

func (a testMCPLiveSessionUseCaseAdapter) UnmountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) error {
	return a.svc.UnmountAuction(ctx, sessionID, auctionID, actorID, actorRole)
}

func (a testMCPLiveSessionUseCaseAdapter) UnmountAuctionWithOptions(ctx context.Context, in mcpports.UnmountLiveSessionAuctionInput) error {
	return a.svc.UnmountAuctionWithOptions(ctx, UnmountLiveSessionAuctionInput(in))
}

func (a testMCPLiveSessionUseCaseAdapter) ActivateAuctionWithOptions(ctx context.Context, in mcpports.ActivateLiveSessionAuctionInput) (domain.AuctionLot, error) {
	return a.svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput(in))
}

func (a testMCPLiveSessionUseCaseAdapter) DeactivateAuction(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error) {
	return a.svc.DeactivateAuction(ctx, sessionID, actorID, actorRole)
}

func (a testMCPLiveSessionUseCaseAdapter) DeactivateAuctionWithOptions(ctx context.Context, in mcpports.DeactivateLiveSessionAuctionInput) (domain.LiveSession, error) {
	return a.svc.DeactivateAuctionWithOptions(ctx, DeactivateLiveSessionAuctionInput(in))
}
