package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	appruntime "aieas_backend/internal/app/runtime"
	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/agent"
	"aieas_backend/internal/infra/idgen"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	realtimeinfra "aieas_backend/internal/infra/realtime"
	ttsinfr "aieas_backend/internal/infra/tts"
	adminapp "aieas_backend/internal/modules/admin/app"
	adminrepo "aieas_backend/internal/modules/admin/repository"
	aiapp "aieas_backend/internal/modules/ai/app"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
	authapp "aieas_backend/internal/modules/auth/app"
	authports "aieas_backend/internal/modules/auth/ports"
	depositapp "aieas_backend/internal/modules/deposit/app"
	depositrepo "aieas_backend/internal/modules/deposit/repository"
	liveanalysisapp "aieas_backend/internal/modules/live_analysis/app"
	liveanalysisrepo "aieas_backend/internal/modules/live_analysis/repository"
	livesessionapp "aieas_backend/internal/modules/live_session/app"
	livesessionports "aieas_backend/internal/modules/live_session/ports"
	livesessionrepo "aieas_backend/internal/modules/live_session/repository"
	marketplaceapp "aieas_backend/internal/modules/marketplace/app"
	mcpapp "aieas_backend/internal/modules/mcp/app"
	mcpports "aieas_backend/internal/modules/mcp/ports"
	orderapp "aieas_backend/internal/modules/order/app"
	orderports "aieas_backend/internal/modules/order/ports"
	orderrepo "aieas_backend/internal/modules/order/repository"
	riskapp "aieas_backend/internal/modules/risk/app"
	riskports "aieas_backend/internal/modules/risk/ports"
	riskrepo "aieas_backend/internal/modules/risk/repository"
	userrepo "aieas_backend/internal/modules/user/repository"
	wstransport "aieas_backend/internal/transport/ws"
	jwtpkg "aieas_backend/pkg/jwt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	tracecodes "go.opentelemetry.io/otel/codes"
	traceapi "go.opentelemetry.io/otel/trace"
)

type appServices struct {
	auth          *authapp.AuthService
	auction       *auctionapp.AuctionService
	bid           *auctionapp.BidService
	deposit       *depositapp.DepositService
	hammer        *auctionapp.HammerService
	order         *orderapp.OrderService
	orderTimeout  orderTimeoutWorker
	admin         *adminapp.AdminService
	liveSession   *livesessionapp.LiveSessionService
	marketplace   *marketplaceapp.MarketplaceService
	liveAnalysis  *liveanalysisapp.LiveAnalysisService
	aiAssistant   *aiapp.AIAssistantService
	mcpRead       *mcpapp.MCPReadService
	mcpControl    *mcpapp.MCPControlService
	riskControl   *riskapp.RiskControlService
	depositWorker *appruntime.DepositReconciler
}

type depositTelemetryAdapter struct {
	metrics *metrics.Registry
}

// hammerBarrierMetricsAdapter 把 *metrics.Registry 适配为 auctionapp.HammerBarrierMetrics。
// nil 安全：内部方法都 nil-check。
type hammerBarrierMetricsAdapter struct {
	registry *metrics.Registry
}

func (a hammerBarrierMetricsAdapter) ObserveHammerDrain(elapsed time.Duration) {
	if a.registry != nil {
		a.registry.ObserveHammerDrain(elapsed)
	}
}

func (a hammerBarrierMetricsAdapter) IncHammerDrainTimeout() {
	if a.registry != nil {
		a.registry.IncHammerDrainTimeout()
	}
}

func (a depositTelemetryAdapter) StartEnroll(ctx context.Context, auctionID uint64, userID string) (context.Context, depositapp.DepositSpan) {
	ctx, span := tracing.StartSpan(ctx, "deposit.enroll",
		attribute.Int64("auction.id", int64(auctionID)),
		attribute.String("user.id", userID),
	)
	return ctx, depositTraceSpanAdapter{inner: span}
}

func (a depositTelemetryAdapter) ObserveEnroll(result string, elapsed time.Duration) {
	if a.metrics != nil {
		a.metrics.ObserveEnroll(result, elapsed)
	}
}

func (a depositTelemetryAdapter) IncDepositReady() {
	if a.metrics != nil {
		a.metrics.IncDepositReady()
	}
}

func (a depositTelemetryAdapter) IncDepositSyncRedisFail() {
	if a.metrics != nil {
		a.metrics.IncDepositSyncRedisFail()
	}
}

type depositTraceSpanAdapter struct {
	inner traceapi.Span
}

func (a depositTraceSpanAdapter) End() {
	if a.inner != nil {
		a.inner.End()
	}
}

func (a depositTraceSpanAdapter) SetDepositStatus(status string) {
	if a.inner != nil {
		a.inner.SetAttributes(attribute.String("deposit.status", status))
	}
}

func (a depositTraceSpanAdapter) RecordError(err error) {
	if a.inner != nil && err != nil {
		a.inner.RecordError(err)
		a.inner.SetStatus(codes.Error, err.Error())
	}
}

func withDefaultServerDependencies(cfg appconfig.Config, deps ServerDependencies) ServerDependencies {
	if deps.UserRepo == nil {
		deps.UserRepo = userrepo.NewSeedUserRepository()
	}
	if deps.AuctionRepo == nil {
		deps.AuctionRepo = auctionrepo.NewMemoryAuctionRepository()
	}
	if deps.LiveSessionRepo == nil {
		deps.LiveSessionRepo = livesessionrepo.NewMemoryLiveSessionRepository()
	}
	if deps.LiveAnalysisReportRepo == nil {
		deps.LiveAnalysisReportRepo = liveanalysisrepo.NewMemoryLiveAnalysisReportRepository()
	}
	if deps.ConfigRepo == nil {
		deps.ConfigRepo = adminrepo.NewMemoryConfigRepository()
	}
	if deps.BidRepo == nil {
		deps.BidRepo = auctionrepo.NewMemoryBidRepository()
	}
	if deps.DepositRepo == nil {
		deps.DepositRepo = depositrepo.NewMemoryDepositRepository()
	}
	if deps.OrderRepo == nil {
		deps.OrderRepo = orderrepo.NewMemoryOrderRepository()
	}
	if deps.RiskRepo == nil {
		deps.RiskRepo = riskrepo.NewMemoryRiskRepository()
	}
	if deps.FeatureFlags == nil {
		deps.FeatureFlags = adminapp.NewFeatureFlagService(deps.ConfigRepo, nil)
	}
	if deps.AuditRepo == nil {
		deps.AuditRepo = adminrepo.NewMemoryAuditRepository()
	}
	if deps.AdminDashboardRepo == nil {
		deps.AdminDashboardRepo = newMemoryAdminDashboardRepository(deps.AuctionRepo, deps.LiveSessionRepo, deps.BidRepo, deps.OrderRepo, deps.RiskRepo)
	}
	if deps.RealtimeStore == nil {
		deps.RealtimeStore = realtimeinfra.NewMemoryRealtimeStore()
	}
	if deps.LiveSessionRealtimeStore == nil {
		deps.LiveSessionRealtimeStore = livesessionrepo.NewMemoryLiveSessionRealtimeStore()
	}
	if deps.LiveSessionLock == nil {
		deps.LiveSessionLock = livesessionrepo.NewMemoryLiveSessionLock()
	}
	if deps.TxManager == nil {
		deps.TxManager = mysqlinfra.NoopTxManager{}
	}
	if deps.Hub == nil {
		deps.Hub = wstransport.NewHub()
	}
	if deps.RealtimeEventPublisher != nil {
		deps.Hub.SetLiveSessionEventPublisher(deps.RealtimeEventPublisher)
	}
	if deps.ObjectUploader == nil {
		uploader, err := objectstorage.NewUploader(cfg.ObjectStorage)
		if err != nil {
			panic(fmt.Errorf("init object storage uploader: %w", err))
		}
		deps.ObjectUploader = uploader
	}
	if deps.DescriptionGen == nil {
		deps.DescriptionGen = agent.NewProductDescriptionClient(cfg.Agent)
	}
	if deps.ProductAuditor == nil && cfg.Agent.ProductAuditEnabled {
		deps.ProductAuditor = agent.NewProductAuditClient(cfg.Agent)
	}
	if deps.LiveAnalysisRequester == nil {
		deps.LiveAnalysisRequester = agent.NewLiveAnalysisClient(cfg.Agent)
	}
	if deps.LiveAgentHookInvoker == nil {
		deps.LiveAgentHookInvoker = agent.NewLiveAuctionHookClient(cfg.Agent)
	}
	if deps.LiveVoiceSynthesizer == nil {
		deps.LiveVoiceSynthesizer = ttsinfr.NewDoubaoClient(cfg.DoubaoTTS)
	}
	if deps.LiveVoiceBroadcaster == nil && (deps.Hub != nil || deps.RealtimeEventPublisher != nil) {
		deps.LiveVoiceBroadcaster = liveVoiceHubBroadcaster{hub: deps.Hub, eventPublisher: deps.RealtimeEventPublisher}
	}
	if deps.AuctionIDGen == nil || deps.OrderIDGen == nil {
		generator, err := idgen.NewSnowflake(cfg.IDGen.WorkerID)
		if err != nil {
			panic(fmt.Errorf("init ID generator: %w", err))
		}
		if deps.AuctionIDGen == nil {
			deps.AuctionIDGen = generator
		}
		if deps.OrderIDGen == nil {
			deps.OrderIDGen = generator
		}
	}
	if deps.EventLog != nil && deps.EventLog.Enabled() {
		deps.Hub.SetReplaySource(wstransport.NewRedisReplaySource(deps.EventLog, int64(cfg.WebSocket.ReplayLimit)))
	}
	if deps.WSHandshakeLimiter == nil {
		deps.WSHandshakeLimiter = wstransport.NewHandshakeLimiter(cfg.WebSocket.HandshakeRateLimitPerIP, cfg.WebSocket.HandshakeRateLimitPerUser, cfg.WebSocket.HandshakeRateLimitPerAuction)
	}
	if deps.MetricsRegistry == nil {
		deps.MetricsRegistry = metrics.NewNoop()
	}
	if deps.Tracing == nil {
		deps.Tracing = tracing.NewNoop()
	}
	return deps
}

func buildAppServices(cfg appconfig.Config, deps ServerDependencies) appServices {
	jwtManager := jwtpkg.NewManager(cfg.JWT.Secret, cfg.JWT.AccessTokenTTL.Std())
	authService := deps.AuthService
	if authService == nil {
		authService = authapp.NewAuthService(deps.UserRepo, jwtManager, repositoryPasswordHasher{})
	}
	realtimeEvents := deps.RealtimeEventPublisher
	auctionEvents := auctionEventPublisherAdapter{publisher: deps.Hub, eventPublisher: realtimeEvents}
	riskService := riskapp.NewRiskService(deps.RiskRepo, riskEventPublisherAdapter{publisher: deps.Hub, eventPublisher: realtimeEvents})
	if deps.BlacklistCache != nil {
		riskService.SetBlacklistCache(deps.BlacklistCache)
	}
	riskControlService := riskapp.NewRiskControlService(riskControlFromConfig(cfg.RiskControl))
	depositService := depositapp.NewDepositService(deps.DepositRepo, deps.AuctionRepo, deps.RealtimeStore, riskService, deps.TxManager)
	depositService.SetRiskControlService(riskControlService)
	depositService.SetParticipantNotifier(depositParticipantNotifier{publisher: auctionEvents})
	orderService := orderapp.NewOrderService(deps.OrderRepo, deps.TxManager)
	orderService.SetUserRepository(deps.UserRepo)
	liveAgentHookService := appruntime.NewLiveAgentHookService(deps.ConfigRepo, deps.UserRepo, deps.LiveAgentHookInvoker)
	liveAgentHookService.SetInvokeTimeout(cfg.Agent.Timeout.Std())
	var liveSessionService *livesessionapp.LiveSessionService
	// 终态回调需要 LiveSessionService，但 LiveSessionService 构造又依赖 AuctionService；
	// 用闭包延迟解引用以避免为 onClose 增加后注入 setter。
	onAuctionClosed := func(ctx context.Context, auctionID uint64) {
		if deps.AuctionSnapshotCache != nil {
			_ = deps.AuctionSnapshotCache.Invalidate(ctx, auctionID)
		}
		if liveSessionService != nil {
			liveSessionService.OnAuctionClosed(ctx, auctionID)
		}
	}
	hammerService := auctionapp.NewHammerServiceWithDeps(auctionapp.HammerServiceDeps{
		Auctions:      deps.AuctionRepo,
		Bids:          deps.BidRepo,
		Orders:        deps.OrderRepo,
		Deposits:      deps.DepositRepo,
		Realtime:      deps.RealtimeStore,
		Tx:            deps.TxManager,
		Publisher:     auctionEvents,
		OrderID:       deps.OrderIDGen,
		Metrics:       deps.MetricsRegistry,
		Tracer:        auctionTracerAdapter{provider: deps.Tracing},
		LiveAgentHook: liveAgentHookService,
		Events:        deps.SettlementEventPublisher,
		OnClose:       onAuctionClosed,
	})
	timer := appruntime.NewTimerScheduler(deps.RealtimeStore, hammerService, wsEnvelopePublisherAdapter{publisher: deps.Hub, eventPublisher: realtimeEvents}, time.Second)
	timer.SetHammerAntiSnipingGraceMs(int64(cfg.Auction.HammerAntiSnipingGraceMs))
	auctionService := auctionapp.NewAuctionServiceWithDeps(auctionapp.AuctionServiceDeps{
		Auctions:         deps.AuctionRepo,
		Bids:             deps.BidRepo,
		Deposits:         deps.DepositRepo,
		Tx:               deps.TxManager,
		Realtime:         deps.RealtimeStore,
		Publisher:        auctionEvents,
		Timer:            timer,
		IDGen:            deps.AuctionIDGen,
		ProductAuditor:   deps.ProductAuditor,
		AuditImageLoader: productAuditImageLoader{uploader: deps.ObjectUploader},
		LiveAgentHook:    liveAgentHookService,
		OnClose:          onAuctionClosed,
		AuctionConfig:    cfg.Auction,
		ProductAuditOn:   cfg.Agent.ProductAuditEnabled,
		ProductAuditSet:  true,
		AuctionSnapshots: deps.AuctionSnapshotCache,
		Tracer:           auctionTracerAdapter{provider: deps.Tracing},
	})
	liveAnalysisService := liveanalysisapp.NewLiveAnalysisService(deps.LiveAnalysisReportRepo, deps.LiveSessionRepo, deps.LiveAnalysisRequester, liveanalysisapp.LiveAnalysisOptions{
		CallbackURL:    cfg.Agent.LiveAnalysisCallbackURL,
		CallbackAPIKey: cfg.Agent.LiveAnalysisCallbackAPIKey,
	})
	aiAssistantService := aiapp.NewAIAssistantService(deps.UserRepo, aiAssistantHubNotifier{hub: deps.Hub, eventPublisher: realtimeEvents})
	liveSessionService = livesessionapp.NewLiveSessionServiceWithDeps(livesessionapp.LiveSessionServiceDeps{
		Sessions:        deps.LiveSessionRepo,
		Auctions:        deps.AuctionRepo,
		Tx:              deps.TxManager,
		Lock:            deps.LiveSessionLock,
		Auction:         auctionService,
		Bids:            deps.BidRepo,
		Orders:          deps.OrderRepo,
		Users:           deps.UserRepo,
		AuctionRealtime: deps.RealtimeStore,
		OnlineCounter:   deps.Hub,
		SessionRealtime: deps.LiveSessionRealtimeStore,
		OnEnded:         buildLiveSessionEndedHook(deps.Hub, realtimeEvents, liveAnalysisService),
		LiveAgentHook:   liveAgentHookService,
		LotEvents:       liveSessionLotHubNotifier{hub: deps.Hub, eventPublisher: realtimeEvents},
		AISwitch:        aiAssistantService,
	})
	hammerService.SetLiveSessionService(liveSessionService)
	bidService := auctionapp.NewBidServiceWithDeps(auctionapp.BidServiceDeps{
		Bids:             deps.BidRepo,
		Auctions:         deps.AuctionRepo,
		Realtime:         deps.RealtimeStore,
		Risk:             riskService,
		Publisher:        auctionEvents,
		Hammer:           hammerService,
		Sessions:         liveSessionService,
		Config:           cfg.Auction,
		Metrics:          deps.MetricsRegistry,
		Tracer:           auctionTracerAdapter{provider: deps.Tracing},
		LiveAgentHook:    liveAgentHookService,
		Configs:          deps.ConfigRepo,
		RiskControls:     riskControlService,
		Users:            deps.UserRepo,
		AuctionSnapshots: deps.AuctionSnapshotCache,
	})
	depositService.SetTelemetry(depositTelemetryAdapter{metrics: deps.MetricsRegistry})
	if deps.Hub != nil {
		deps.Hub.SetMetrics(deps.MetricsRegistry)
	}
	marketplaceService := marketplaceapp.NewMarketplaceService(deps.AuctionRepo, deps.LiveSessionRepo, deps.DepositRepo, deps.OrderRepo, deps.UserRepo)
	marketplaceService.SetRealtime(deps.RealtimeStore)
	marketplaceService.SetOnlineCounter(deps.Hub)
	adminService := adminapp.NewAdminService(deps.UserRepo, auctionService, hammerService, orderService, riskService, deps.AuditRepo)
	adminService.SetDashboardRepository(deps.AdminDashboardRepo)
	adminService.SetLookupRepositories(deps.LiveSessionRepo)
	adminService.SetConfigRepository(deps.ConfigRepo)
	adminService.SetFeatureFlagService(deps.FeatureFlags)
	mcpReadService := mcpapp.NewMCPReadService(mcpapp.MCPReadDependencies{
		Users:       deps.UserRepo,
		Auctions:    deps.AuctionRepo,
		Sessions:    deps.LiveSessionRepo,
		Bids:        deps.BidRepo,
		Orders:      deps.OrderRepo,
		Risk:        riskService,
		AuditLogs:   deps.AuditRepo,
		AuctionSvc:  auctionService,
		LiveSession: newMCPLiveSessionUseCaseAdapter(liveSessionService),
		OrderSvc:    orderService,
	})
	mcpControlService := mcpapp.NewMCPControlService(mcpapp.MCPLiveControlDependencies{
		Auctions:             deps.AuctionRepo,
		Sessions:             deps.LiveSessionRepo,
		LiveSessionSvc:       newMCPLiveSessionUseCaseAdapter(liveSessionService),
		AuctionSvc:           auctionService,
		HammerSvc:            hammerService,
		LiveVoiceSynthesizer: deps.LiveVoiceSynthesizer,
		LiveVoiceBroadcaster: deps.LiveVoiceBroadcaster,
		AIAssistant:          aiAssistantService,
	})
	return appServices{auth: authService, auction: auctionService, bid: bidService, deposit: depositService, hammer: hammerService, order: orderService, orderTimeout: orderService, admin: adminService, liveSession: liveSessionService, marketplace: marketplaceService, liveAnalysis: liveAnalysisService, aiAssistant: aiAssistantService, mcpRead: mcpReadService, mcpControl: mcpControlService, riskControl: riskControlService}
}

func riskControlFromConfig(cfg appconfig.RiskControlConfig) domain.RiskControlConfig {
	return domain.RiskControlConfig{Enabled: cfg.Enabled}
}

func newMemoryAdminDashboardRepository(
	auctions auctionports.AuctionRepository,
	sessions livesessionports.LiveSessionRepository,
	bids auctionports.BidRepository,
	orders orderports.OrderRepository,
	risk riskports.RiskRepository,
) *adminrepo.MemoryAdminDashboardRepository {
	return adminrepo.NewMemoryAdminDashboardRepository(
		auctionSnapshotAdapter{repo: auctions},
		liveSessionSnapshotAdapter{repo: sessions},
		bidSnapshotAdapter{repo: bids},
		orderSnapshotAdapter{repo: orders},
		riskEventSnapshotAdapter{repo: risk},
	)
}

type auctionSnapshotAdapter struct {
	repo auctionports.AuctionRepository
}

func (a auctionSnapshotAdapter) SnapshotAuctions() []domain.AuctionLot {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotAuctions() []domain.AuctionLot }); ok {
		return snapshotter.SnapshotAuctions()
	}
	return nil
}

type liveSessionSnapshotAdapter struct {
	repo livesessionports.LiveSessionRepository
}

func (a liveSessionSnapshotAdapter) SnapshotLiveSessions() []domain.LiveSession {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotLiveSessions() []domain.LiveSession }); ok {
		return snapshotter.SnapshotLiveSessions()
	}
	return nil
}

type bidSnapshotAdapter struct{ repo auctionports.BidRepository }

func (a bidSnapshotAdapter) SnapshotBids() []domain.BidRecord {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotBids() []domain.BidRecord }); ok {
		return snapshotter.SnapshotBids()
	}
	return nil
}

type orderSnapshotAdapter struct{ repo orderports.OrderRepository }

func (a orderSnapshotAdapter) SnapshotOrders() []domain.OrderDeal {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotOrders() []domain.OrderDeal }); ok {
		return snapshotter.SnapshotOrders()
	}
	return nil
}

type riskEventSnapshotAdapter struct{ repo riskports.RiskRepository }

func (a riskEventSnapshotAdapter) SnapshotRiskEvents() []domain.RiskEvent {
	if a.repo == nil {
		return nil
	}
	if snapshotter, ok := a.repo.(interface{ SnapshotRiskEvents() []domain.RiskEvent }); ok {
		return snapshotter.SnapshotRiskEvents()
	}
	if repo, ok := a.repo.(*riskrepo.MemoryRiskRepository); ok && repo != nil {
		return repo.SnapshotEvents()
	}
	return nil
}

type repositoryPasswordHasher struct{}

func (repositoryPasswordHasher) Matches(password, passwordHash string) bool {
	return userrepo.HashPassword(password) == passwordHash
}

var _ authports.PasswordHasher = repositoryPasswordHasher{}

type auctionEventPublisherAdapter struct {
	publisher interface {
		Broadcast(auctionID uint64, env wstransport.Envelope) int
	}
	eventPublisher RealtimeEventPublisher
}

func (a auctionEventPublisherAdapter) Broadcast(auctionID uint64, env auctionports.EventEnvelope) int {
	if a.publisher == nil && a.eventPublisher == nil {
		return 0
	}
	out := wstransport.Envelope{Type: env.Type, RequestID: env.RequestID, Seq: env.Seq, Payload: env.Payload}
	delivered := 0
	if a.eventPublisher != nil && env.Type == "bid.accepted" {
		return delivered
	}
	publishedThroughBus := false
	if a.eventPublisher != nil {
		if err := a.eventPublisher.PublishAuctionEvent(context.Background(), auctionID, env.Type, env.RequestID, env.Seq, env.Payload); err != nil && a.publisher != nil {
			delivered += a.publisher.Broadcast(auctionID, out)
		} else if err == nil {
			publishedThroughBus = true
		}
	} else if a.publisher != nil {
		delivered += a.publisher.Broadcast(auctionID, out)
	}
	if liveSessionID := liveSessionScopedEventLiveSessionID(env); liveSessionID != 0 {
		if publishedThroughBus {
			return delivered
		}
		if a.publisher != nil {
			if sessionPublisher, ok := a.publisher.(interface {
				BroadcastLiveSession(liveSessionID uint64, env wstransport.Envelope) int
			}); ok {
				delivered += sessionPublisher.BroadcastLiveSession(liveSessionID, out)
			}
		}
	}
	return delivered
}

func liveSessionScopedEventLiveSessionID(env auctionports.EventEnvelope) uint64 {
	if env.Type != "auction.started" && env.Type != "auction.closed" && env.Type != wstransport.TypeLiveSessionLotChanged {
		return 0
	}
	var payload struct {
		LiveSessionID uint64 `json:"liveSessionId"`
	}
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return 0
	}
	return payload.LiveSessionID
}

type depositParticipantNotifier struct {
	publisher interface {
		Broadcast(auctionID uint64, env auctionports.EventEnvelope) int
	}
}

func (n depositParticipantNotifier) NotifyParticipantUpdated(ctx context.Context, auctionID uint64, participantCount int) int {
	if n.publisher == nil || auctionID == 0 || participantCount < 0 {
		return 0
	}
	raw, err := json.Marshal(map[string]interface{}{
		"auctionId":        auctionID,
		"participantCount": participantCount,
		"serverTime":       time.Now().UTC().UnixMilli(),
	})
	if err != nil {
		return 0
	}
	return n.publisher.Broadcast(auctionID, auctionports.EventEnvelope{
		Type:    wstransport.TypeAuctionParticipantUpdated,
		Payload: raw,
	})
}

type riskEventPublisherAdapter struct {
	publisher interface {
		Broadcast(auctionID uint64, env wstransport.Envelope) int
	}
	eventPublisher RealtimeEventPublisher
}

func (a riskEventPublisherAdapter) Broadcast(auctionID uint64, env riskports.EventEnvelope) int {
	if a.publisher == nil && a.eventPublisher == nil {
		return 0
	}
	out := wstransport.Envelope{Type: env.Type, RequestID: env.RequestID, Payload: env.Payload}
	if a.eventPublisher != nil {
		if err := a.eventPublisher.PublishAuctionEvent(context.Background(), auctionID, env.Type, env.RequestID, 0, env.Payload); err == nil {
			return 0
		}
	}
	if a.publisher == nil {
		return 0
	}
	return a.publisher.Broadcast(auctionID, out)
}

type wsEnvelopePublisherAdapter struct {
	publisher interface {
		Broadcast(auctionID uint64, env wstransport.Envelope) int
	}
	eventPublisher RealtimeEventPublisher
}

func (a wsEnvelopePublisherAdapter) Broadcast(auctionID uint64, env wstransport.Envelope) int {
	if a.publisher == nil && a.eventPublisher == nil {
		return 0
	}
	if a.eventPublisher != nil {
		if err := a.eventPublisher.PublishAuctionEvent(context.Background(), auctionID, env.Type, env.RequestID, env.Seq, env.Payload); err == nil {
			return 0
		}
	}
	if a.publisher == nil {
		return 0
	}
	return a.publisher.Broadcast(auctionID, env)
}

type auctionTracerAdapter struct {
	provider *tracing.Provider
}

func (a auctionTracerAdapter) Start(ctx context.Context, name string, attrs ...auctionapp.AuctionAttr) (context.Context, auctionapp.AuctionSpan) {
	if ctx == nil {
		ctx = context.Background()
	}
	provider := a.provider
	if provider == nil {
		provider = tracing.NewNoop()
	}
	tracer := provider.Tracer("aieas_backend")
	startOpts := []traceapi.SpanStartOption{}
	if len(attrs) > 0 {
		startOpts = append(startOpts, traceapi.WithAttributes(toOTelAttrs(attrs)...))
	}
	nextCtx, span := tracer.Start(ctx, name, startOpts...)
	return nextCtx, auctionTraceSpanAdapter{inner: span}
}

type auctionTraceSpanAdapter struct {
	inner traceapi.Span
}

func (a auctionTraceSpanAdapter) End() {
	if a.inner != nil {
		a.inner.End()
	}
}

func (a auctionTraceSpanAdapter) SetAttributes(attrs ...auctionapp.AuctionAttr) {
	if a.inner != nil {
		a.inner.SetAttributes(toOTelAttrs(attrs)...)
	}
}

func (a auctionTraceSpanAdapter) RecordError(err error) {
	if a.inner != nil {
		a.inner.RecordError(err)
	}
}

func (a auctionTraceSpanAdapter) SetStatus(code auctionapp.AuctionStatusCode, description string) {
	if a.inner == nil {
		return
	}
	statusCode := tracecodes.Unset
	if code == auctionapp.AuctionStatusError {
		statusCode = tracecodes.Error
	}
	a.inner.SetStatus(statusCode, description)
}

func toOTelAttrs(attrs []auctionapp.AuctionAttr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "" {
			continue
		}
		switch value := attr.Value.(type) {
		case string:
			out = append(out, attribute.String(attr.Key, value))
		case int:
			out = append(out, attribute.Int(attr.Key, value))
		case int64:
			out = append(out, attribute.Int64(attr.Key, value))
		case bool:
			out = append(out, attribute.Bool(attr.Key, value))
		default:
			out = append(out, attribute.String(attr.Key, fmt.Sprint(value)))
		}
	}
	return out
}

type mcpLiveSessionUseCaseAdapter struct {
	svc *livesessionapp.LiveSessionService
}

func newMCPLiveSessionUseCaseAdapter(svc *livesessionapp.LiveSessionService) mcpports.LiveSessionUseCase {
	if svc == nil {
		return nil
	}
	return mcpLiveSessionUseCaseAdapter{svc: svc}
}

func (a mcpLiveSessionUseCaseAdapter) ListByMerchantFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error) {
	return a.svc.ListByMerchantFiltered(ctx, filter, actorID, actorRole)
}

func (a mcpLiveSessionUseCaseAdapter) ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	return a.svc.ListLots(ctx, sessionID, actorID, actorRole)
}

func (a mcpLiveSessionUseCaseAdapter) ListBidsPaged(ctx context.Context, sessionID uint64, sortBy string, limit, offset int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	return a.svc.ListBidsPaged(ctx, sessionID, sortBy, limit, offset, actorID, actorRole)
}

func (a mcpLiveSessionUseCaseAdapter) Stats(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (mcpports.LiveSessionStats, error) {
	stats, err := a.svc.Stats(ctx, sessionID, actorID, actorRole)
	if err != nil {
		return mcpports.LiveSessionStats{}, err
	}
	return mcpports.LiveSessionStats(stats), nil
}

func (a mcpLiveSessionUseCaseAdapter) MountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	return a.svc.MountAuction(ctx, sessionID, auctionID, actorID, actorRole)
}

func (a mcpLiveSessionUseCaseAdapter) UnmountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) error {
	return a.svc.UnmountAuction(ctx, sessionID, auctionID, actorID, actorRole)
}

func (a mcpLiveSessionUseCaseAdapter) ActivateAuctionWithOptions(ctx context.Context, in mcpports.ActivateLiveSessionAuctionInput) (domain.AuctionLot, error) {
	return a.svc.ActivateAuctionWithOptions(ctx, livesessionapp.ActivateLiveSessionAuctionInput(in))
}

func (a mcpLiveSessionUseCaseAdapter) DeactivateAuction(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error) {
	return a.svc.DeactivateAuction(ctx, sessionID, actorID, actorRole)
}
