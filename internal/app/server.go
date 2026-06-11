package app

import (
	"context"
	"time"

	appruntime "aieas_backend/internal/app/runtime"
	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	realtimeinfra "aieas_backend/internal/infra/realtime"
	redisinfra "aieas_backend/internal/infra/redis"
	adminapp "aieas_backend/internal/modules/admin/app"
	adminports "aieas_backend/internal/modules/admin/ports"
	adminrepo "aieas_backend/internal/modules/admin/repository"
	aiapp "aieas_backend/internal/modules/ai/app"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
	authapp "aieas_backend/internal/modules/auth/app"
	depositrepo "aieas_backend/internal/modules/deposit/repository"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
	livesessionports "aieas_backend/internal/modules/live_session/ports"
	marketplaceports "aieas_backend/internal/modules/marketplace/ports"
	mcpapp "aieas_backend/internal/modules/mcp/app"
	orderports "aieas_backend/internal/modules/order/ports"
	orderrepo "aieas_backend/internal/modules/order/repository"
	riskports "aieas_backend/internal/modules/risk/ports"
	riskrepo "aieas_backend/internal/modules/risk/repository"
	userports "aieas_backend/internal/modules/user/ports"
	userrepo "aieas_backend/internal/modules/user/repository"
	httptransport "aieas_backend/internal/transport/http"
	mcptransport "aieas_backend/internal/transport/mcp"
	wstransport "aieas_backend/internal/transport/ws"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func NewServer() *server.Hertz {
	return NewServerFromConfigPath(appconfig.DefaultPath)
}

func NewServerFromConfigPath(path string) *server.Hertz {
	cfg := appconfig.MustLoad(path)
	return NewServerWithConfig(cfg)
}

func NewServerWithConfig(cfg appconfig.Config) *server.Hertz {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	platform, deps, err := buildProductionServerDependencies(context.Background(), cfg)
	if err != nil {
		panic(err)
	}
	h := NewServerWithDependencies(cfg, deps)
	platform.registerShutdown(h)
	return h
}

type ServerDependencies struct {
	AuthService              *authapp.AuthService
	UserRepo                 userports.UserRepository
	MerchantFollowRepo       marketplaceports.MerchantFollowRepository
	AuctionRepo              auctionports.AuctionRepository
	LiveSessionRepo          livesessionports.LiveSessionRepository
	LiveAnalysisReportRepo   liveanalysisports.LiveAnalysisReportRepository
	ConfigRepo               adminports.ConfigRepository
	BidRepo                  auctionports.BidRepository
	DepositRepo              auctionports.DepositRepository
	OrderRepo                orderports.OrderRepository
	RiskRepo                 riskports.RiskRepository
	AuditRepo                adminports.AuditRepository
	AdminDashboardRepo       adminports.DashboardRepository
	RealtimeStore            auctionports.AuctionRealtimeStore
	LiveSessionRealtimeStore livesessionports.LiveSessionRealtimeStore
	LiveSessionLock          livesessionports.LiveSessionLock
	TxManager                auctionports.TxManager
	Hub                      *wstransport.Hub
	Idempotency              httptransport.IdempotencyStore
	EventLog                 *redisinfra.EventLog
	OnlineCounter            *redisinfra.OnlineCounter
	DistributedRateLimiter   httptransport.DistributedRateLimitStore
	FeatureFlags             *adminapp.FeatureFlagService
	PubSubClients            []wstransport.PubSubClient
	RealtimeEventPublisher   RealtimeEventPublisher
	WSHandshakeLimiter       *wstransport.HandshakeLimiter
	ObjectUploader           objectstorage.Uploader
	DescriptionGen           aiapp.ProductDescriptionGenerator
	ProductAuditor           auctionapp.ProductAuditor
	LiveAnalysisRequester    liveanalysisports.AsyncRequester
	LiveAgentHookInvoker     appruntime.LiveAgentHookInvoker
	LiveVoiceSynthesizer     mcpapp.LiveVoiceSynthesizer
	LiveVoiceBroadcaster     mcpapp.LiveVoiceBroadcaster
	AuctionIDGen             auctionports.AuctionIDGenerator
	OrderIDGen               auctionports.OrderIDGenerator
	// MetricsRegistry 与 Tracing 由 NewServerWithConfig 在启动时构造并注入。
	// 当外部调用方（测试、NewServerWithUserRepository）未注入时，
	// NewServerWithDependencies 会兜底成 noop Registry / Provider。
	MetricsRegistry *metrics.Registry
	Tracing         *tracing.Provider
	// ReadinessProbes 是 /readyz 检查的依赖列表（key=component，value=probe）。
	// nil/空时 /readyz 仍可用，仅返回固定 ok（无依赖时视为就绪）。
	ReadinessProbes map[string]httptransport.ReadinessProbe
	// BlacklistCache 是 RiskService 的黑名单查询缓存（L1+L2+singleflight+负缓存）。
	// nil 时 RiskService 直接读 MySQL。NewServerWithConfig 默认基于 RedisCacheClient
	// 注入 LayeredCache[bool]；测试场景可以传 nil。
	BlacklistCache riskports.BlacklistCache
	// AuctionSnapshotCache 是出价热路径的拍品运行快照缓存。
	// 开拍时写入 RedisCache，出价实例本地缓存 miss 后优先读取它，避免静态字段每次回源 MySQL。
	AuctionSnapshotCache auctionapp.AuctionSnapshotCache
	// DepositReconcilerEnabled 控制是否启动押金一致性巡检（P1-A）。
	// NewServerWithConfig 走"线上"路径默认 true；测试经 NewServerWithDependencies
	// 显式不传时默认 false，避免内存夹具里多跑一根 goroutine 干扰断言。
	DepositReconcilerEnabled bool
	// OrderTimeoutWorkerEnabled 控制是否启动订单 20 分钟超时关单 worker。
	// NewServerWithConfig 走线上路径默认 true；测试经 NewServerWithDependencies
	// 默认 false，避免后台 goroutine 干扰时间敏感断言。
	OrderTimeoutWorkerEnabled bool
	// StartupExpiredAuctionCleanupEnabled 控制启动时是否清理倒计时已结束但仍处于竞拍中的拍品。
	// NewServerWithConfig 走线上路径默认 true；测试经 NewServerWithDependencies
	// 默认 false，避免后台 goroutine 修改内存夹具。
	StartupExpiredAuctionCleanupEnabled bool
	// ScheduledAuctionStarterEnabled 控制是否启动预约开拍扫描 worker。
	// NewServerWithConfig 走线上路径默认 true；测试经 NewServerWithDependencies
	// 默认 false，避免后台 goroutine 修改内存夹具。
	ScheduledAuctionStarterEnabled bool
	BidEventKafkaProducer          appruntime.BidEventKafkaProducer
	BidEventKafkaConsumer          appruntime.BidEventKafkaConsumer
	SettlementEventPublisher       auctionports.SettlementEventPublisher
	// 异步竞价裁决依赖：命令消费者 + 命令发布器。仅在 kafka.enabled 时非 nil；
	// 任一缺失时 WS handler 强制走同步降级（绝不丢请求）。
	BidCommandConsumer  appruntime.BidCommandConsumer
	BidCommandPublisher httptransport.BidCommandPublisher
	// BidAsyncCoordinator 是异步竞价的进程内协调器（队列保护 + 结果重发）。
	// 由 NewServerWithDependencies 在 async 依赖齐备时构造，worker 与 WS handler 共享同一实例。
	BidAsyncCoordinator *wstransport.BidAsyncCoordinator
	// BidCommandInFlightTracker 是跨实例的拍品级 Kafka command in-flight 计数器。
	// 多实例落锤 drain 依赖它等待其他实例发布的出价完成 Lua 裁决。
	BidCommandInFlightTracker bidCommandInFlightTracker
}

func NewServerWithUserRepository(cfg appconfig.Config, userRepo userports.UserRepository) *server.Hertz {
	return NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:      userRepo,
		AuctionRepo:   auctionrepo.NewMemoryAuctionRepository(),
		BidRepo:       auctionrepo.NewMemoryBidRepository(),
		DepositRepo:   depositrepo.NewMemoryDepositRepository(),
		OrderRepo:     orderrepo.NewMemoryOrderRepository(),
		RiskRepo:      riskrepo.NewMemoryRiskRepository(),
		AuditRepo:     adminrepo.NewMemoryAuditRepository(),
		RealtimeStore: realtimeinfra.NewMemoryRealtimeStore(),
		TxManager:     mysqlinfra.NoopTxManager{},
		Hub:           wstransport.NewHub(),
	})
}

func NewServerWithDependencies(cfg appconfig.Config, deps ServerDependencies) *server.Hertz {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	deps = withDefaultServerDependencies(cfg, deps)
	// 异步竞价协调器：ws handler 用它登记 pending，ws-gateway 收到 bid.result
	// PubSub 后也用它定向投递并管理 ack 重发。
	if deps.BidAsyncCoordinator == nil && deps.Hub != nil && (deps.BidCommandPublisher != nil || deps.BidCommandConsumer != nil) {
		coord := wstransport.NewBidAsyncCoordinator(deps.Hub, cfg.Kafka.BidAsyncMaxPendingPerAuction, 0, 0)
		coord.SetMetrics(deps.MetricsRegistry)
		deps.BidAsyncCoordinator = coord
	}
	// 异步落锤过渡态：当 async 闭环齐备时构造 publisher 闸门 + in-flight 屏障，
	// 注入到 publisher 适配器（拒绝新命令）与 HammerService（屏障等待 + finalize）。
	asyncReadyForHammer := deps.BidCommandPublisher != nil && deps.BidAsyncCoordinator != nil
	var (
		hammerGate    *auctionapp.HammerPublisherGate
		hammerBarrier *auctionapp.InFlightBarrier
	)
	if asyncReadyForHammer {
		// 闸门宽限：默认 200ms，覆盖 ws handler 在 publish 前/后的入队竞态窗口。
		hammerGate = auctionapp.NewHammerPublisherGate(200 * time.Millisecond)
		hammerBarrier = auctionapp.NewInFlightBarrier(newHammerDrainCoordinatorSet(
			deps.BidAsyncCoordinator,
			deps.BidCommandInFlightTracker,
		), hammerGate, cfg.Auction.HammerDrainPollMs)
		hammerBarrier.SetMetrics(hammerBarrierMetricsAdapter{registry: deps.MetricsRegistry})
		// 把闸门注入命令发布器（如果是已知的 kafka 适配器实例）。
		if k, ok := deps.BidCommandPublisher.(*kafkaBidCommandPublisher); ok {
			k.SetGate(hammerGate)
			k.SetInFlightTracker(deps.BidCommandInFlightTracker)
			k.SetMetrics(deps.MetricsRegistry)
		}
	}
	services := buildAppServices(cfg, deps)
	if asyncReadyForHammer && services.hammer != nil {
		drainMaxWait := time.Duration(cfg.Auction.HammerDrainMaxWaitMs) * time.Millisecond
		services.hammer.SetAsyncBidWiring(true, hammerBarrier, hammerGate, drainMaxWait)
	}
	workerShutdown := startAppWorkers(cfg, deps, services)
	asyncBid := asyncBidWiring{coordinator: deps.BidAsyncCoordinator, publisher: deps.BidCommandPublisher}
	if asyncBid.coordinator != nil && asyncBid.publisher != nil && services.bid != nil {
		asyncBid.bids = services.bid
	}
	h := newServerWithServices(services.auth, services.auction, services.bid, deps.UserRepo, services.deposit, services.hammer, services.order, services.admin, services.liveSession, services.liveSession, deps.RealtimeStore, deps.LiveSessionRealtimeStore, services.marketplace, services.marketplace, services.liveAnalysis, services.aiAssistant, services.aiAssistant, services.mcpRead, services.mcpControl, services.riskControl, deps.AuditRepo, deps.Hub, deps.Idempotency, deps.ObjectUploader, deps.DescriptionGen, deps.MetricsRegistry, deps.Tracing, deps.ReadinessProbes, deps.DistributedRateLimiter, deps.FeatureFlags, deps.WSHandshakeLimiter, asyncBid, cfg)
	if deps.Hub != nil {
		h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
			_ = deps.Hub.Drain(ctx, cfg.WebSocket.DrainTimeout.Std())
		})
	}
	if workerShutdown.stopWorkers != nil {
		h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
			workerShutdown.stop(ctx)
		})
	}
	return h
}

func NewServerWithAuth(authService *authapp.AuthService) *server.Hertz {
	cfg := appconfig.Default()
	return NewServerWithDependencies(cfg, ServerDependencies{
		AuthService:    authService,
		UserRepo:       userrepo.NewSeedUserRepository(),
		ObjectUploader: objectstorage.DisabledUploader{},
		DescriptionGen: aiapp.DisabledProductDescriptionGenerator{},
	})
}

func newServerWithServices(
	authService httptransport.AuthUseCase,
	auctionService httptransport.AuctionUseCase,
	bidService httptransport.WSBidUseCase,
	userProfiles httptransport.WSUserProfileLookup,
	depositService httptransport.DepositUseCase,
	hammerService httptransport.HammerUseCase,
	orderService httptransport.OrderUseCase,
	adminService httptransport.AdminUseCase,
	liveSessionService httptransport.LiveSessionUseCase,
	wsLiveSessionLookup httptransport.WSLiveSessionLookupUseCase,
	realtimeStore auctionports.AuctionRealtimeStore,
	liveSessionRealtimeStore livesessionports.LiveSessionRealtimeStore,
	marketplaceService httptransport.MarketplaceUseCase,
	marketplacePresenter httptransport.MarketplaceLiveSessionPresenter,
	liveAnalysisService httptransport.LiveAnalysisUseCase,
	aiAssistantService httptransport.AIAssistantUseCase,
	aiAssistantNotifier httptransport.AIAssistantStatusNotifier,
	mcpReadService mcptransport.MCPReadUseCase,
	mcpControlService mcptransport.MCPControlUseCase,
	riskControlService httptransport.RiskControlUseCase,
	auditRepo adminports.AuditRepository,
	hub *wstransport.Hub,
	idempotencyStore httptransport.IdempotencyStore,
	objectUploader objectstorage.Uploader,
	descriptionGen httptransport.ProductDescriptionGenerator,
	metricsRegistry *metrics.Registry,
	tracingProvider *tracing.Provider,
	readinessProbes map[string]httptransport.ReadinessProbe,
	distributedRateLimiter httptransport.DistributedRateLimitStore,
	featureFlags *adminapp.FeatureFlagService,
	wsHandshakeLimiter *wstransport.HandshakeLimiter,
	asyncBid asyncBidWiring,
	cfg appconfig.Config,
) *server.Hertz {
	logger := buildLogger(cfg.Observability)
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoop()
	}
	if tracingProvider == nil {
		tracingProvider = tracing.NewNoop()
	}
	h := server.Default(serverOptions(cfg.Server)...)
	// 中间件顺序固定：Recovery → CORS → RequestID → Tracing → Metrics → RateLimiter → Audit。
	// Tracing 必须先于 Metrics，让 metric 的请求生命周期完全包在 span 内部，
	// 也让 metric label 能从 routeLabel 拿到（FullPath 在路由匹配后才有值）。
	rateLimiter := httptransport.NewRateLimiter(240, timeMinute())
	if riskControlService != nil {
		rateLimiter.SetEnabledFunc(riskControlService.Enabled)
	}
	if distributedRateLimiter != nil {
		rateLimiter.SetDistributedStore(distributedRateLimiter)
	}
	if featureFlags != nil {
		rateLimiter.SetDistributedEnabledFunc(func(ctx context.Context) bool {
			return featureFlags.Decide(ctx, domain.FeatureFlagDistributedRateLimit, "")
		})
	}
	h.Use(
		httptransport.RecoveryMiddleware(logger),
		httptransport.CORSMiddleware(corsOptions(cfg.Server.CORS)),
		httptransport.RequestIDMiddleware(),
		httptransport.TracingMiddleware(tracingProvider),
		httptransport.MetricsMiddleware(metricsRegistry),
		rateLimiter.Middleware(),
		httptransport.AuditMiddleware(auditRepo, logger),
	)

	readinessProbes = filterReadinessProbesForRole(readinessProbes, cfg)
	readinessProbes = withWSDrainingReadinessProbe(readinessProbes, hub, cfg)
	registerObservabilityRoutes(h, cfg.Observability, metricsRegistry, readinessProbes)

	h.GET("/ping", func(ctx context.Context, c *hertzapp.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"message": "pong"})
	})
	registerAppRoutes(h, routeWiring{
		authService:              authService,
		auctionService:           auctionService,
		bidService:               bidService,
		userProfiles:             userProfiles,
		depositService:           depositService,
		hammerService:            hammerService,
		orderService:             orderService,
		adminService:             adminService,
		liveSessionService:       liveSessionService,
		wsLiveSessionLookup:      wsLiveSessionLookup,
		realtimeStore:            realtimeStore,
		liveSessionRealtimeStore: liveSessionRealtimeStore,
		marketplaceService:       marketplaceService,
		marketplacePresenter:     marketplacePresenter,
		liveAnalysisService:      liveAnalysisService,
		aiAssistantService:       aiAssistantService,
		aiAssistantNotifier:      aiAssistantNotifier,
		mcpReadService:           mcpReadService,
		mcpControlService:        mcpControlService,
		hub:                      hub,
		idempotencyStore:         idempotencyStore,
		objectUploader:           objectUploader,
		descriptionGen:           descriptionGen,
		metricsRegistry:          metricsRegistry,
		wsHandshakeLimiter:       wsHandshakeLimiter,
		asyncBid:                 asyncBid,
		cfg:                      cfg,
	})

	return h
}

func corsOptions(cfg appconfig.CORSConfig) httptransport.CORSOptions {
	return httptransport.CORSOptions{
		Enabled:          cfg.Enabled,
		AllowOrigins:     cfg.AllowOrigins,
		AllowMethods:     cfg.AllowMethods,
		AllowHeaders:     cfg.AllowHeaders,
		ExposeHeaders:    cfg.ExposeHeaders,
		AllowCredentials: cfg.AllowCredentials,
		MaxAgeSeconds:    cfg.MaxAgeSeconds,
	}
}

func timeMinute() time.Duration {
	return time.Minute
}
