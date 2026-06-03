package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/agent"
	"aieas_backend/internal/infra/cache"
	"aieas_backend/internal/infra/idgen"
	kafkainfra "aieas_backend/internal/infra/kafka"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/infra/observability"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	redisinfra "aieas_backend/internal/infra/redis"
	ttsinfr "aieas_backend/internal/infra/tts"
	"aieas_backend/internal/repository"
	"aieas_backend/internal/service"
	httptransport "aieas_backend/internal/transport/http"
	mcptransport "aieas_backend/internal/transport/mcp"
	wstransport "aieas_backend/internal/transport/ws"
	jwtpkg "aieas_backend/pkg/jwt"

	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"golang.org/x/term"
	"gorm.io/gorm"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	hertzconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func NewServer() *server.Hertz {
	cfg := appconfig.MustLoad(appconfig.DefaultPath)
	return NewServerWithConfig(cfg)
}

func NewServerWithConfig(cfg appconfig.Config) *server.Hertz {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	logger := buildLogger(cfg.Observability)
	metricsRegistry := metrics.New(metrics.Options{
		Enabled:   cfg.Observability.Metrics.Enabled,
		Namespace: cfg.Observability.Metrics.Namespace,
	})
	tracingProvider, err := tracing.Setup(context.Background(), tracing.Config{
		Enabled:     cfg.Observability.Tracing.Enabled,
		Exporter:    cfg.Observability.Tracing.Exporter,
		Endpoint:    cfg.Observability.Tracing.Endpoint,
		Insecure:    cfg.Observability.Tracing.Insecure,
		ServiceName: cfg.Observability.Tracing.ServiceName,
		Sampler:     cfg.Observability.Tracing.Sampler,
		SampleRatio: cfg.Observability.Tracing.SampleRatio,
	})
	if err != nil {
		// 启用 trace 但 exporter 初始化失败：降级到 noop，但保留日志告警。
		logger.Warn("tracing setup failed, falling back to noop", "error", err)
	}
	db, shardedRT, rdbCache, err := openClients(context.Background(), cfg, logger, metricsRegistry, tracingProvider)
	if err != nil {
		panic(err)
	}
	scripts := redisinfra.NewShardedScriptRegistry(shardedRT, redisinfra.DefaultScripts())
	scripts.SetMetrics(metricsRegistry)
	if err := scripts.LoadAll(context.Background()); err != nil {
		_ = shardedRT.Close()
		_ = rdbCache.Close()
		_ = mysqlinfra.Close(db)
		panic(fmt.Errorf("load redis scripts: %w", err))
	}
	keys := redisinfra.NewKeyBuilder("")
	realtimeStore := redisinfra.NewAuctionRealtimeStore(shardedRT, scripts, keys)
	onlineCounter := redisinfra.NewOnlineCounter(shardedRT, keys, redisinfra.DefaultOnlineCounterTTL)
	eventLog := redisinfra.NewEventLog(shardedRT, keys)
	liveSessionRealtimeStore := redisinfra.NewLiveSessionRealtimeStore(shardedRT, keys)
	kafkaProducer, kafkaBidReader, err := openKafkaClients(cfg)
	if err != nil {
		_ = shardedRT.Close()
		_ = rdbCache.Close()
		_ = mysqlinfra.Close(db)
		panic(err)
	}

	// BlacklistCache：把黑名单查询挂到 LayeredCache（L1+L2+singleflight+负缓存），
	// 通过 SetBlacklistCache 注入到 RiskService（在 NewServerWithDependencies 内）。
	blacklistCache := newBlacklistLayeredCache(rdbCache)

	userRepo := repository.NewMySQLUserRepository(db)
	deps := ServerDependencies{
		UserRepo:                  userRepo,
		AuctionRepo:               repository.NewMySQLAuctionRepository(db),
		LiveSessionRepo:           repository.NewMySQLLiveSessionRepository(db),
		LiveAnalysisReportRepo:    repository.NewMySQLLiveAnalysisReportRepository(db),
		ConfigRepo:                repository.NewMySQLConfigRepository(db),
		BidRepo:                   repository.NewMySQLBidRepository(db),
		DepositRepo:               repository.NewMySQLDepositRepository(db),
		OrderRepo:                 repository.NewMySQLOrderRepository(db),
		RiskRepo:                  repository.NewMySQLRiskRepository(db),
		AuditRepo:                 repository.NewMySQLAuditRepository(db),
		AdminDashboardRepo:        repository.NewMySQLAdminDashboardRepository(db),
		RealtimeStore:             realtimeStore,
		LiveSessionRealtimeStore:  liveSessionRealtimeStore,
		LiveSessionLock:           redisinfra.NewLiveSessionLock(shardedRT, keys),
		TxManager:                 repository.NewGORMTxManager(db),
		Hub:                       wstransport.NewHubWithOnlineCounter(onlineCounter),
		Idempotency:               httptransport.NewRedisIdempotencyStore(rdbCache, "idempotency"),
		EventLog:                  eventLog,
		OnlineCounter:             onlineCounter,
		DistributedRateLimiter:    redisinfra.NewDistributedRateLimiter(scripts, keys),
		FeatureFlags:              service.NewFeatureFlagService(repository.NewMySQLConfigRepository(db), rdbCache),
		PubSubClients:             pubSubClientsFromShards(shardedRT),
		MetricsRegistry:           metricsRegistry,
		Tracing:                   tracingProvider,
		ReadinessProbes:           buildReadinessProbes(db, shardedRT, rdbCache, scripts, kafkaProducer),
		BlacklistCache:            blacklistCache,
		DepositReconcilerEnabled:  true,
		OrderTimeoutWorkerEnabled: true,
		BidEventKafkaProducer:     kafkaProducer,
		BidEventKafkaConsumer:     kafkaBidReader,
		SettlementEventPublisher:  kafkaProducer,
	}
	h := NewServerWithDependencies(cfg, deps)
	poolStatsCtx, stopPoolStats := context.WithCancel(context.Background())
	redisinfra.StartPoolStatsCollector(poolStatsCtx, metricsRegistry, shardedRT, rdbCache)
	h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
		_ = ctx
		stopPoolStats()
	})
	h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
		_ = shardedRT.Close()
		_ = rdbCache.Close()
		if kafkaBidReader != nil {
			_ = kafkaBidReader.Close()
		}
		if kafkaProducer != nil {
			_ = kafkaProducer.Close()
		}
		_ = mysqlinfra.Close(db)
		if tracingProvider != nil {
			_ = tracingProvider.Shutdown(ctx)
		}
	})
	return h
}

type ServerDependencies struct {
	UserRepo                 repository.UserRepository
	AuctionRepo              repository.AuctionRepository
	LiveSessionRepo          repository.LiveSessionRepository
	LiveAnalysisReportRepo   repository.LiveAnalysisReportRepository
	ConfigRepo               repository.ConfigRepository
	BidRepo                  repository.BidRepository
	DepositRepo              repository.DepositRepository
	OrderRepo                repository.OrderRepository
	RiskRepo                 repository.RiskRepository
	AuditRepo                repository.AuditRepository
	AdminDashboardRepo       repository.AdminDashboardRepository
	RealtimeStore            repository.AuctionRealtimeStore
	LiveSessionRealtimeStore repository.LiveSessionRealtimeStore
	LiveSessionLock          repository.LiveSessionLock
	TxManager                repository.TxManager
	Hub                      *wstransport.Hub
	Idempotency              httptransport.IdempotencyStore
	EventLog                 *redisinfra.EventLog
	OnlineCounter            *redisinfra.OnlineCounter
	DistributedRateLimiter   httptransport.DistributedRateLimitStore
	FeatureFlags             *service.FeatureFlagService
	PubSubClients            []wstransport.PubSubClient
	ObjectUploader           objectstorage.Uploader
	DescriptionGen           service.ProductDescriptionGenerator
	ProductAuditor           service.ProductAuditor
	LiveAnalysisRequester    service.LiveAnalysisRequester
	LiveAgentHookInvoker     service.LiveAgentHookInvoker
	LiveVoiceSynthesizer     service.LiveVoiceSynthesizer
	LiveVoiceBroadcaster     service.LiveVoiceBroadcaster
	AuctionIDGen             service.AuctionIDGenerator
	OrderIDGen               service.OrderIDGenerator
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
	BlacklistCache service.BlacklistCache
	// DepositReconcilerEnabled 控制是否启动押金一致性巡检（P1-A）。
	// NewServerWithConfig 走"线上"路径默认 true；测试经 NewServerWithDependencies
	// 显式不传时默认 false，避免内存夹具里多跑一根 goroutine 干扰断言。
	DepositReconcilerEnabled bool
	// OrderTimeoutWorkerEnabled 控制是否启动订单 20 分钟超时关单 worker。
	// NewServerWithConfig 走线上路径默认 true；测试经 NewServerWithDependencies
	// 默认 false，避免后台 goroutine 干扰时间敏感断言。
	OrderTimeoutWorkerEnabled bool
	BidEventKafkaProducer     service.BidEventKafkaProducer
	BidEventKafkaConsumer     service.BidEventKafkaConsumer
	SettlementEventPublisher  service.SettlementEventPublisher
}

func NewServerWithUserRepository(cfg appconfig.Config, userRepo repository.UserRepository) *server.Hertz {
	return NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:      userRepo,
		AuctionRepo:   repository.NewMemoryAuctionRepository(),
		BidRepo:       repository.NewMemoryBidRepository(),
		DepositRepo:   repository.NewMemoryDepositRepository(),
		OrderRepo:     repository.NewMemoryOrderRepository(),
		RiskRepo:      repository.NewMemoryRiskRepository(),
		AuditRepo:     repository.NewMemoryAuditRepository(),
		RealtimeStore: repository.NewMemoryRealtimeStore(),
		TxManager:     repository.NoopTxManager{},
		Hub:           wstransport.NewHub(),
	})
}

func NewServerWithDependencies(cfg appconfig.Config, deps ServerDependencies) *server.Hertz {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	if deps.UserRepo == nil {
		deps.UserRepo = repository.NewSeedUserRepository()
	}
	if deps.AuctionRepo == nil {
		deps.AuctionRepo = repository.NewMemoryAuctionRepository()
	}
	if deps.LiveSessionRepo == nil {
		deps.LiveSessionRepo = repository.NewMemoryLiveSessionRepository()
	}
	if deps.LiveAnalysisReportRepo == nil {
		deps.LiveAnalysisReportRepo = repository.NewMemoryLiveAnalysisReportRepository()
	}
	if deps.ConfigRepo == nil {
		deps.ConfigRepo = repository.NewMemoryConfigRepository()
	}
	if deps.BidRepo == nil {
		deps.BidRepo = repository.NewMemoryBidRepository()
	}
	if deps.DepositRepo == nil {
		deps.DepositRepo = repository.NewMemoryDepositRepository()
	}
	if deps.OrderRepo == nil {
		deps.OrderRepo = repository.NewMemoryOrderRepository()
	}
	if deps.RiskRepo == nil {
		deps.RiskRepo = repository.NewMemoryRiskRepository()
	}
	if deps.FeatureFlags == nil {
		deps.FeatureFlags = service.NewFeatureFlagService(deps.ConfigRepo, nil)
	}
	if deps.AuditRepo == nil {
		deps.AuditRepo = repository.NewMemoryAuditRepository()
	}
	if deps.AdminDashboardRepo == nil {
		deps.AdminDashboardRepo = repository.NewMemoryAdminDashboardRepository(deps.AuctionRepo, deps.LiveSessionRepo, deps.BidRepo, deps.OrderRepo, deps.RiskRepo)
	}
	if deps.RealtimeStore == nil {
		deps.RealtimeStore = repository.NewMemoryRealtimeStore()
	}
	if deps.LiveSessionRealtimeStore == nil {
		deps.LiveSessionRealtimeStore = repository.NewMemoryLiveSessionRealtimeStore()
	}
	if deps.LiveSessionLock == nil {
		deps.LiveSessionLock = repository.NewMemoryLiveSessionLock()
	}
	if deps.TxManager == nil {
		deps.TxManager = repository.NoopTxManager{}
	}
	if deps.Hub == nil {
		deps.Hub = wstransport.NewHub()
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
	} else if deps.ProductAuditor == nil {
		deps.ProductAuditor = service.DisabledProductAuditor{}
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
	if deps.LiveVoiceBroadcaster == nil && deps.Hub != nil {
		deps.LiveVoiceBroadcaster = liveVoiceHubBroadcaster{hub: deps.Hub}
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
		deps.Hub.SetReplaySource(wstransport.NewRedisReplaySource(deps.EventLog, 256))
	}
	if deps.MetricsRegistry == nil {
		deps.MetricsRegistry = metrics.NewNoop()
	}
	if deps.Tracing == nil {
		deps.Tracing = tracing.NewNoop()
	}
	jwtManager := jwtpkg.NewManager(cfg.JWT.Secret, cfg.JWT.AccessTokenTTL.Std())
	authService := service.NewAuthService(deps.UserRepo, jwtManager)
	auctionService := service.NewAuctionService(deps.AuctionRepo, deps.TxManager)
	auctionService.SetProductAuditEnabled(cfg.Agent.ProductAuditEnabled)
	auctionService.SetProductAuditor(deps.ProductAuditor)
	auctionService.SetProductAuditImageLoader(productAuditImageLoader{uploader: deps.ObjectUploader})
	riskService := service.NewRiskService(deps.RiskRepo, deps.RealtimeStore, deps.Hub)
	if deps.BlacklistCache != nil {
		riskService.SetBlacklistCache(deps.BlacklistCache)
	}
	riskControlService := service.NewRiskControlService(riskControlFromConfig(cfg.RiskControl))
	depositService := service.NewDepositService(deps.DepositRepo, deps.AuctionRepo, deps.RealtimeStore, riskService, deps.TxManager)
	depositService.SetRiskControlService(riskControlService)
	orderService := service.NewOrderService(deps.OrderRepo, deps.TxManager)
	orderService.SetUserRepository(deps.UserRepo)
	hammerService := service.NewHammerService(deps.AuctionRepo, deps.OrderRepo, deps.DepositRepo, deps.RealtimeStore, deps.TxManager, deps.Hub)
	timer := service.NewTimerScheduler(deps.RealtimeStore, hammerService, deps.Hub, time.Second)
	auctionService.SetRealtime(deps.RealtimeStore)
	auctionService.SetBidRepository(deps.BidRepo)
	auctionService.SetPublisher(deps.Hub)
	auctionService.SetTimer(timer)
	auctionService.SetAuctionConfig(cfg.Auction)
	auctionService.SetIDGenerator(deps.AuctionIDGen)
	hammerService.SetOrderIDGenerator(deps.OrderIDGen)
	liveSessionService := service.NewLiveSessionService(deps.LiveSessionRepo, deps.AuctionRepo)
	liveSessionService.SetReadDeps(deps.BidRepo, deps.OrderRepo)
	liveSessionService.SetUserRepository(deps.UserRepo)
	liveSessionService.SetWriteDeps(deps.TxManager, deps.LiveSessionLock, auctionService)
	liveSessionService.SetStatsDeps(deps.BidRepo, deps.RealtimeStore, deps.Hub)
	liveSessionService.SetRealtimeStore(deps.LiveSessionRealtimeStore)
	liveAnalysisService := service.NewLiveAnalysisService(deps.LiveAnalysisReportRepo, deps.LiveSessionRepo, deps.LiveAnalysisRequester, service.LiveAnalysisOptions{
		CallbackURL:    cfg.Agent.LiveAnalysisCallbackURL,
		CallbackAPIKey: cfg.Agent.LiveAnalysisCallbackAPIKey,
	})
	aiAssistantService := service.NewAIAssistantService(deps.UserRepo, aiAssistantHubNotifier{hub: deps.Hub})
	liveAgentHookService := service.NewLiveAgentHookService(deps.ConfigRepo, deps.UserRepo, deps.LiveAgentHookInvoker)
	liveSessionService.SetOnEnded(buildLiveSessionEndedHook(deps.Hub, liveAnalysisService))
	liveSessionService.SetLiveAgentHookService(liveAgentHookService)
	liveSessionService.SetAIAssistantSwitchNotifier(aiAssistantService)
	auctionService.SetLiveAgentHookService(liveAgentHookService)
	hammerService.SetLiveSessionService(liveSessionService)
	hammerService.SetLiveAgentHookService(liveAgentHookService)
	hammerService.SetSettlementEventPublisher(deps.SettlementEventPublisher)
	auctionService.SetOnClose(func(ctx context.Context, auctionID uint64) {
		liveSessionService.OnAuctionClosed(ctx, auctionID)
	})
	hammerService.SetOnClose(func(ctx context.Context, auctionID uint64) {
		liveSessionService.OnAuctionClosed(ctx, auctionID)
	})
	bidService := service.NewBidService(deps.BidRepo, deps.AuctionRepo, deps.RealtimeStore, riskService, deps.Hub, cfg.Auction)
	bidService.SetLiveSessionService(liveSessionService)
	bidService.SetHammerService(hammerService)
	bidService.SetLiveAgentHookService(liveAgentHookService)
	bidService.SetConfigRepository(deps.ConfigRepo)
	bidService.SetRiskControlService(riskControlService)
	bidService.SetUserRepository(deps.UserRepo)
	// 业务埋点：把 metrics registry 注入到关键服务/Hub。nil 安全（兜底为 noop）。
	bidService.SetMetrics(deps.MetricsRegistry)
	hammerService.SetMetrics(deps.MetricsRegistry)
	depositService.SetMetrics(deps.MetricsRegistry)
	if deps.Hub != nil {
		deps.Hub.SetMetrics(deps.MetricsRegistry)
	}
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	deps.FeatureFlags.StartInvalidationSubscriber(workerCtx)
	if deps.OrderTimeoutWorkerEnabled {
		orderService.StartTimeoutWorker(workerCtx, service.DefaultOrderTimeoutScanInterval, service.DefaultOrderTimeoutScanBatchSize)
	}
	if deps.EventLog != nil && deps.EventLog.Enabled() {
		wstransport.NewEventRelay(deps.EventLog, deps.Hub, 200*time.Millisecond).Start(workerCtx)
		for _, pubSubClient := range deps.PubSubClients {
			wstransport.NewPubSubBroadcaster(pubSubClient, deps.Hub).Start(workerCtx)
		}
		if deps.BidEventKafkaProducer != nil && deps.BidEventKafkaConsumer != nil {
			bridge := service.NewRedisBidEventKafkaBridge(deps.EventLog, deps.BidEventKafkaProducer, cfg.Kafka.BidBridgeGroup, "")
			bridge.SetMetrics(deps.MetricsRegistry)
			bridge.Start(workerCtx)
			writer := service.NewKafkaBidRecordWriter(deps.BidRepo, deps.BidEventKafkaConsumer)
			writer.SetMetrics(deps.MetricsRegistry)
			writer.Start(workerCtx)
		} else {
			writer := service.NewBidRecordWriter(deps.BidRepo, deps.EventLog, "")
			writer.SetMetrics(deps.MetricsRegistry)
			writer.Start(workerCtx)
		}
		reconciler := service.NewBidRecordReconciler(deps.BidRepo, deps.EventLog)
		reconciler.SetMetrics(deps.MetricsRegistry)
		reconciler.Start(workerCtx, time.Minute)
		if deps.OnlineCounter != nil {
			deps.OnlineCounter.StartJanitor(workerCtx, time.Minute)
		}
	}
	// P1-A：押金一致性巡检。仅在显式 enable 时启动；测试场景默认关闭，
	// 避免内存夹具里多跑一根 goroutine 干扰断言。
	var depositReconciler *service.DepositReconciler
	if deps.DepositReconcilerEnabled {
		depositReconciler = service.NewDepositReconciler(deps.AuctionRepo, deps.DepositRepo, deps.RealtimeStore, 30*time.Second)
		depositReconciler.SetMetrics(deps.MetricsRegistry)
		depositReconciler.Start(context.Background())
	}
	adminService := service.NewAdminService(deps.UserRepo, auctionService, hammerService, orderService, riskService, deps.AuditRepo)
	adminService.SetDashboardRepository(deps.AdminDashboardRepo)
	adminService.SetLookupRepositories(deps.LiveSessionRepo)
	adminService.SetConfigRepository(deps.ConfigRepo)
	adminService.SetFeatureFlagService(deps.FeatureFlags)
	mcpReadService := service.NewMCPReadService(service.MCPReadDependencies{
		Users:       deps.UserRepo,
		Auctions:    deps.AuctionRepo,
		Sessions:    deps.LiveSessionRepo,
		Bids:        deps.BidRepo,
		Orders:      deps.OrderRepo,
		Risk:        riskService,
		AuditLogs:   deps.AuditRepo,
		AuctionSvc:  auctionService,
		LiveSession: liveSessionService,
		OrderSvc:    orderService,
	})
	mcpControlService := service.NewMCPControlService(service.MCPLiveControlDependencies{
		Auctions:             deps.AuctionRepo,
		Sessions:             deps.LiveSessionRepo,
		LiveSessionSvc:       liveSessionService,
		AuctionSvc:           auctionService,
		HammerSvc:            hammerService,
		LiveVoiceSynthesizer: deps.LiveVoiceSynthesizer,
		LiveVoiceBroadcaster: deps.LiveVoiceBroadcaster,
		AIAssistant:          aiAssistantService,
	})
	h := newServerWithServices(authService, auctionService, bidService, depositService, hammerService, orderService, adminService, liveSessionService, liveAnalysisService, aiAssistantService, mcpReadService, mcpControlService, riskControlService, deps.AuditRepo, deps.Hub, deps.Idempotency, deps.ObjectUploader, deps.DescriptionGen, deps.MetricsRegistry, deps.Tracing, deps.ReadinessProbes, deps.DistributedRateLimiter, deps.FeatureFlags, cfg)
	if stopWorkers != nil {
		h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
			_ = ctx
			stopWorkers()
		})
	}
	if depositReconciler != nil {
		h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
			_ = ctx
			depositReconciler.Stop()
		})
	}
	return h
}

func NewServerWithAuth(authService *service.AuthService) *server.Hertz {
	cfg := appconfig.Default()
	userRepo := repository.NewSeedUserRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	configRepo := repository.NewMemoryConfigRepository()
	realtimeStore := repository.NewMemoryRealtimeStore()
	hub := wstransport.NewHub()
	riskService := service.NewRiskService(riskRepo, realtimeStore, hub)
	riskControlService := service.NewRiskControlService(riskControlFromConfig(cfg.RiskControl))
	depositService := service.NewDepositService(depositRepo, auctionRepo, realtimeStore, riskService, repository.NoopTxManager{})
	depositService.SetRiskControlService(riskControlService)
	orderService := service.NewOrderService(orderRepo, repository.NoopTxManager{})
	orderService.SetUserRepository(userRepo)
	hammerService := service.NewHammerService(auctionRepo, orderRepo, depositRepo, realtimeStore, repository.NoopTxManager{}, hub)
	auctionService := service.NewAuctionService(auctionRepo, repository.NoopTxManager{})
	auctionService.SetRealtime(realtimeStore)
	auctionService.SetPublisher(hub)
	auctionService.SetTimer(service.NewTimerScheduler(realtimeStore, hammerService, hub, time.Second))
	generator, err := idgen.NewSnowflake(cfg.IDGen.WorkerID)
	if err != nil {
		panic(fmt.Errorf("init ID generator: %w", err))
	}
	auctionService.SetIDGenerator(generator)
	hammerService.SetOrderIDGenerator(generator)
	bidService := service.NewBidService(bidRepo, auctionRepo, realtimeStore, riskService, hub, cfg.Auction)
	bidService.SetHammerService(hammerService)
	bidService.SetConfigRepository(configRepo)
	bidService.SetRiskControlService(riskControlService)
	auditRepo := repository.NewMemoryAuditRepository()
	adminService := service.NewAdminService(userRepo, auctionService, hammerService, orderService, riskService, auditRepo)
	adminService.SetConfigRepository(configRepo)
	liveSessionLock := repository.NewMemoryLiveSessionLock()
	liveSessionRepo := repository.NewMemoryLiveSessionRepository()
	adminService.SetLookupRepositories(liveSessionRepo)
	liveSessionService := service.NewLiveSessionService(liveSessionRepo, auctionRepo)
	liveSessionService.SetReadDeps(bidRepo, orderRepo)
	liveSessionService.SetWriteDeps(repository.NoopTxManager{}, liveSessionLock, auctionService)
	liveSessionService.SetStatsDeps(bidRepo, realtimeStore, hub)
	liveAnalysisService := service.NewLiveAnalysisService(repository.NewMemoryLiveAnalysisReportRepository(), liveSessionRepo, service.DisabledLiveAnalysisRequester{}, service.LiveAnalysisOptions{
		CallbackURL:    cfg.Agent.LiveAnalysisCallbackURL,
		CallbackAPIKey: cfg.Agent.LiveAnalysisCallbackAPIKey,
	})
	aiAssistantService := service.NewAIAssistantService(userRepo, aiAssistantHubNotifier{hub: hub})
	liveAgentHookService := service.NewLiveAgentHookService(repository.NewMemoryConfigRepository(), userRepo, service.DisabledLiveAgentHookInvoker{})
	liveSessionService.SetOnEnded(buildLiveSessionEndedHook(hub, liveAnalysisService))
	liveSessionService.SetLiveAgentHookService(liveAgentHookService)
	liveSessionService.SetAIAssistantSwitchNotifier(aiAssistantService)
	liveSessionService.SetUserRepository(userRepo)
	auctionService.SetLiveAgentHookService(liveAgentHookService)
	hammerService.SetLiveSessionService(liveSessionService)
	hammerService.SetLiveAgentHookService(liveAgentHookService)
	bidService.SetLiveSessionService(liveSessionService)
	bidService.SetLiveAgentHookService(liveAgentHookService)
	bidService.SetUserRepository(userRepo)
	auctionService.SetOnClose(func(ctx context.Context, auctionID uint64) {
		liveSessionService.OnAuctionClosed(ctx, auctionID)
	})
	hammerService.SetOnClose(func(ctx context.Context, auctionID uint64) {
		liveSessionService.OnAuctionClosed(ctx, auctionID)
	})
	mcpReadService := service.NewMCPReadService(service.MCPReadDependencies{
		Users:       repository.NewSeedUserRepository(),
		Auctions:    auctionRepo,
		Sessions:    liveSessionRepo,
		Bids:        bidRepo,
		Orders:      orderRepo,
		Risk:        riskService,
		AuditLogs:   auditRepo,
		AuctionSvc:  auctionService,
		LiveSession: liveSessionService,
		OrderSvc:    orderService,
	})
	mcpControlService := service.NewMCPControlService(service.MCPLiveControlDependencies{
		Auctions:             auctionRepo,
		Sessions:             liveSessionRepo,
		LiveSessionSvc:       liveSessionService,
		AuctionSvc:           auctionService,
		HammerSvc:            hammerService,
		LiveVoiceSynthesizer: ttsinfr.NewDoubaoClient(cfg.DoubaoTTS),
		LiveVoiceBroadcaster: liveVoiceHubBroadcaster{hub: hub},
		AIAssistant:          aiAssistantService,
	})
	return newServerWithServices(
		authService,
		auctionService,
		bidService,
		depositService,
		hammerService,
		orderService,
		adminService,
		liveSessionService,
		liveAnalysisService,
		aiAssistantService,
		mcpReadService,
		mcpControlService,
		riskControlService,
		auditRepo,
		hub,
		nil,
		objectstorage.DisabledUploader{},
		service.DisabledProductDescriptionGenerator{},
		metrics.NewNoop(),
		tracing.NewNoop(),
		nil,
		nil,
		service.NewFeatureFlagService(configRepo, nil),
		cfg,
	)
}

func newServerWithServices(
	authService *service.AuthService,
	auctionService *service.AuctionService,
	bidService *service.BidService,
	depositService *service.DepositService,
	hammerService *service.HammerService,
	orderService *service.OrderService,
	adminService *service.AdminService,
	liveSessionService *service.LiveSessionService,
	liveAnalysisService *service.LiveAnalysisService,
	aiAssistantService *service.AIAssistantService,
	mcpReadService *service.MCPReadService,
	mcpControlService *service.MCPControlService,
	riskControlService *service.RiskControlService,
	auditRepo repository.AuditRepository,
	hub *wstransport.Hub,
	idempotencyStore httptransport.IdempotencyStore,
	objectUploader objectstorage.Uploader,
	descriptionGen service.ProductDescriptionGenerator,
	metricsRegistry *metrics.Registry,
	tracingProvider *tracing.Provider,
	readinessProbes map[string]httptransport.ReadinessProbe,
	distributedRateLimiter httptransport.DistributedRateLimitStore,
	featureFlags *service.FeatureFlagService,
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
	// 中间件顺序固定：Recovery → RequestID → Tracing → Metrics → RateLimiter → Audit。
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
		httptransport.RequestIDMiddleware(),
		httptransport.TracingMiddleware(tracingProvider),
		httptransport.MetricsMiddleware(metricsRegistry),
		rateLimiter.Middleware(),
		httptransport.AuditMiddleware(auditRepo, logger),
	)

	registerObservabilityRoutes(h, cfg.Observability, metricsRegistry, readinessProbes)

	h.GET("/ping", func(ctx context.Context, c *hertzapp.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"message": "pong"})
	})
	mcpReadHandler := mcptransport.NewReadHandler(mcpReadService, mcptransport.APIKeyAuthConfig{
		APIKey: cfg.MCP.Read.APIKey,
		Actor: service.MCPActor{
			ID:   cfg.MCP.Read.ActorID,
			Role: domain.Role(cfg.MCP.Read.ActorRole),
		},
	})
	mcpReadHandler.SetMetrics(metricsRegistry)
	mcpReadHandler.SetAIAssistant(aiAssistantService)
	mcpControlHandler := mcptransport.NewControlHandler(mcpControlService, mcptransport.APIKeyAuthConfig{
		APIKey: cfg.MCP.Control.APIKey,
		Actor: service.MCPActor{
			ID:   cfg.MCP.Control.ActorID,
			Role: domain.Role(cfg.MCP.Control.ActorRole),
		},
	})
	mcpControlHandler.SetMetrics(metricsRegistry)
	h.POST("/mcp/read", mcpReadHandler.Post)
	h.GET("/mcp/read", mcpReadHandler.Get)
	h.POST("/mcp/control", mcpControlHandler.Post)
	h.GET("/mcp/control", mcpControlHandler.Get)

	authHandler := httptransport.NewAuthHandler(authService, objectUploader)
	auctionHandler := httptransport.NewAuctionHandler(auctionService, depositService, hammerService, objectUploader, descriptionGen, cfg.Agent.LiveAnalysisCallbackAPIKey)
	orderHandler := httptransport.NewOrderHandler(orderService)
	adminHandler := httptransport.NewAdminHandler(adminService)
	liveSessionHandler := httptransport.NewLiveSessionHandler(liveSessionService, objectUploader)
	liveAnalysisHandler := httptransport.NewLiveAnalysisHandler(liveAnalysisService, cfg.Agent.LiveAnalysisCallbackAPIKey)
	aiAssistantHandler := httptransport.NewAIAssistantHandler(aiAssistantService)
	wsHandler := httptransport.NewWSHandler(hub, bidService, cfg.WebSocket.SendBufferSize, cfg.WebSocket.ReadLimitBytes, cfg.WebSocket.PingInterval.Std(), cfg.WebSocket.PongTimeout.Std())
	wsHandler.SetLiveSessionService(liveSessionService)
	wsHandler.SetAuctionService(auctionService)
	idempotencyTTL := cfg.Idempotency.TTL.Std()
	if idempotencyStore == nil {
		idempotencyStore = httptransport.NewMemoryIdempotencyStore(idempotencyTTL)
	}

	v1 := h.Group("/api/v1")
	{
		v1.POST("/auth/login", authHandler.Login)
		v1.POST("/auth/refresh", authHandler.Refresh)
		v1.POST("/admin/auth/login", authHandler.AdminLogin)

		v1.GET("/images/*key", auctionHandler.Image)
		v1.POST("/live-analysis/callback", liveAnalysisHandler.Callback)
		v1.POST("/auctions/audit/callback", auctionHandler.AuditCallback)

		protected := v1.Group("/auth", authHandler.AuthMiddleware())
		protected.GET("/me", authHandler.Me)
		protected.PATCH("/me", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, authHandler.UpdateProfile))
		protected.POST("/me/avatar", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, authHandler.UploadAvatar))
		protected.POST("/logout", authHandler.Logout)

		v1.GET("/audit-logs", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin), adminHandler.ListOwnAuditLogs)

		auctionState := v1.Group("/auctions", authHandler.AuthMiddleware())
		auctionState.GET("/:id/state", auctionHandler.State)
		auctionState.POST("/:id/enroll", httptransport.RoleAuth(domain.RoleBuyer), auctionHandler.Enroll)

		auctions := v1.Group("/auctions", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		auctions.POST("/description/optimize", auctionHandler.OptimizeDescription)
		auctions.POST("/images", auctionHandler.UploadImages)
		auctions.POST("", auctionHandler.Create)
		auctions.GET("", auctionHandler.List)
		auctions.GET("/:id", auctionHandler.Get)
		auctions.PATCH("/:id", auctionHandler.Update)
		auctions.DELETE("/:id", auctionHandler.Delete)
		auctions.POST("/:id/start", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, auctionHandler.Start))
		auctions.POST("/:id/cancel", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, auctionHandler.Cancel))
		auctions.POST("/:id/hammer", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, auctionHandler.Hammer))

		liveSessionsPublic := v1.Group("/live-sessions", authHandler.AuthMiddleware())
		liveSessionsPublic.GET("", liveSessionHandler.List)
		liveSessionsPublic.GET("/:id", liveSessionHandler.Get)
		liveSessionsPublic.GET("/:id/lots", liveSessionHandler.Lots)
		liveSessionsPublic.GET("/:id/bids", liveSessionHandler.Bids)
		liveSessionsPublic.GET("/:id/orders", liveSessionHandler.Orders)
		liveSessionsPublic.GET("/:id/stats", liveSessionHandler.Stats)

		merchantSessions := v1.Group("/merchants", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		merchantSessions.GET("/:merchantId/live-sessions", liveSessionHandler.ListByMerchant)

		liveAnalysis := v1.Group("/live-analysis", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		liveAnalysis.POST("/reports", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveAnalysisHandler.CreateReport))
		liveAnalysis.GET("/reports/:liveSessionId", liveAnalysisHandler.GetReport)

		aiAssistant := v1.Group("/ai-assistant", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		aiAssistant.GET("/permission", aiAssistantHandler.Permission)
		aiAssistant.PATCH("/permission", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, aiAssistantHandler.UpdatePermission))
		aiAssistant.POST("/approvals/:requestId/decision", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, aiAssistantHandler.DecideApproval))

		liveSessions := v1.Group("/live-sessions", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		liveSessions.POST("", liveSessionHandler.Create)
		liveSessions.PATCH("/:id", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.Update))
		liveSessions.POST("/:id/start", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.Start))
		liveSessions.POST("/:id/end", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.End))
		liveSessions.POST("/:id/lots", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.MountLot))
		liveSessions.DELETE("/:id/lots/:auctionId", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.UnmountLot))
		liveSessions.POST("/:id/activate", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.Activate))
		liveSessions.POST("/:id/deactivate", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.Deactivate))
		liveSessions.POST("/:id/cover", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.UploadCover))
		liveSessions.GET("/:id/agent-hook", liveSessionHandler.AgentHookConfig)
		liveSessions.PATCH("/:id/agent-hook", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveSessionHandler.UpdateAgentHookConfig))

		orders := v1.Group("/orders", authHandler.AuthMiddleware())
		orders.GET("", orderHandler.List)
		orders.GET("/mine", orderHandler.Mine)
		orders.GET("/:id", orderHandler.Get)
		orders.POST("/:id/pay", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, orderHandler.Pay))

		admin := v1.Group("/admin", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleAdmin))
		admin.GET("/auctions", adminHandler.ListAuctions)
		admin.POST("/auctions/:id/audit", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.AuditAuction))
		admin.POST("/auctions/:id/cancel", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.CancelAuction))
		admin.POST("/auctions/:id/close", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.CloseAuction))
		admin.GET("/users", adminHandler.ListUsers)
		admin.PATCH("/users/:id", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.UpdateUser))
		admin.POST("/blacklist", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.AddBlacklist))
		admin.DELETE("/blacklist/:user_id", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.RemoveBlacklist))
		admin.GET("/blacklist", adminHandler.ListBlacklist)
		admin.GET("/risk/blacklist-strategy", adminHandler.BlacklistStrategyConfig)
		admin.PUT("/risk/blacklist-strategy", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.UpdateBlacklistStrategyConfig))
		admin.GET("/feature-flags/:key", adminHandler.FeatureFlag)
		admin.PUT("/feature-flags/:key", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.UpdateFeatureFlag))
		admin.GET("/orders", adminHandler.ListOrders)
		admin.GET("/dashboard/metrics", adminHandler.DashboardMetrics)
		admin.GET("/audit-logs", adminHandler.ListAuditLogs)
		admin.GET("/risk/events", adminHandler.ListRiskEvents)
		admin.PATCH("/risk/events/:id", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.HandleRiskEvent))
	}
	h.GET("/ws/auctions/:auction_id", authHandler.AuthMiddleware(), wsHandler.Auction)
	h.GET("/ws/live-sessions/:session_id", authHandler.AuthMiddleware(), wsHandler.LiveSession)

	return h
}

func timeMinute() time.Duration {
	return time.Minute
}

func riskControlFromConfig(cfg appconfig.RiskControlConfig) domain.RiskControlConfig {
	return domain.RiskControlConfig{Enabled: cfg.Enabled}
}

func pubSubClientsFromShards(sharded *redisinfra.ShardedRTClient) []wstransport.PubSubClient {
	if sharded == nil || sharded.Len() == 0 {
		return nil
	}
	shards := sharded.Shards()
	clients := make([]wstransport.PubSubClient, 0, len(shards))
	for _, shard := range shards {
		if shard != nil {
			clients = append(clients, shard)
		}
	}
	return clients
}

// openKafkaClients 构造 Kafka producer 与 bid event consumer。Kafka disabled 时返回 nil。
func openKafkaClients(cfg appconfig.Config) (*kafkainfra.Producer, *kafkainfra.BidEventReader, error) {
	if !cfg.Kafka.Enabled {
		return nil, nil, nil
	}
	producer, err := kafkainfra.NewProducer(kafkainfra.ProducerConfig{
		Brokers:            cfg.Kafka.Brokers,
		ClientID:           cfg.Kafka.ClientID,
		BidEventsTopic:     cfg.Kafka.BidEventsTopic,
		AuctionEventsTopic: cfg.Kafka.AuctionEventsTopic,
		OrderEventsTopic:   cfg.Kafka.OrderEventsTopic,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("init kafka producer: %w", err)
	}
	reader, err := kafkainfra.NewBidEventReader(kafkainfra.BidEventReaderConfig{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.BidRecordGroup,
		Topic:   cfg.Kafka.BidEventsTopic,
	})
	if err != nil {
		_ = producer.Close()
		return nil, nil, fmt.Errorf("init kafka bid reader: %w", err)
	}
	return producer, reader, nil
}

// buildReadinessProbes 构造默认 /readyz 依赖检查：mysql.ping、redis.ping、
// redis.scripts、kafka.ping。任何一个 nil 时跳过该 component（noop）。
//
// 设计取舍：probe 闭包内复用上层已构建的 *gorm.DB / *ShardedRTClient / *RedisCacheClient，
// 避免每次 readyz 都重新打开连接。Ping 用各自带 ctx 的 API，让 ReadinessHandler
// 的 timeout 控制生效。RT 是 sharded：每个 shard 都要 ping 通才算就绪。
func buildReadinessProbes(db *gorm.DB, shardedRT *redisinfra.ShardedRTClient, rdbCache *redisinfra.RedisCacheClient, scripts *redisinfra.ScriptRegistry, kafkaProducer *kafkainfra.Producer) map[string]httptransport.ReadinessProbe {
	probes := make(map[string]httptransport.ReadinessProbe, 5)
	if db != nil {
		probes["mysql"] = func(ctx context.Context) error {
			sqlDB, err := db.DB()
			if err != nil {
				return fmt.Errorf("unwrap mysql: %w", err)
			}
			return sqlDB.PingContext(ctx)
		}
	}
	if shardedRT != nil && shardedRT.Len() > 0 {
		shards := shardedRT.Shards()
		probes["redis_rt"] = func(ctx context.Context) error {
			for i, shard := range shards {
				if shard == nil {
					continue
				}
				if err := shard.Ping(ctx).Err(); err != nil {
					return fmt.Errorf("redis rt shard %d: %w", i, err)
				}
			}
			return nil
		}
	}
	if rdbCache != nil {
		probes["redis_cache"] = func(ctx context.Context) error {
			return rdbCache.Ping(ctx).Err()
		}
	}
	if scripts != nil {
		probes["scripts"] = func(ctx context.Context) error {
			if !scripts.Loaded() {
				return fmt.Errorf("redis lua scripts not loaded")
			}
			return nil
		}
	}
	if kafkaProducer != nil {
		probes["kafka"] = func(ctx context.Context) error {
			return kafkaProducer.Ping(ctx)
		}
	}
	return probes
}

// registerObservabilityRoutes 注册 /metrics、/healthz、/readyz 三类运维端点。
//
// 这些路径已在 transport/http.IsObservabilitySkipPath 中登记，所以会自动跳过
// MetricsMiddleware / RateLimiter / AuditMiddleware（C2、C5 约定的运维旁路）。
//
//   - /metrics：Prometheus 文本格式，可选 Bearer token 鉴权（MetricsAuth）；
//     metrics 禁用时由 Registry.Handler() 自身返回 503。
//   - /healthz：纯 liveness 探针。进程存活即 200，不依赖任何下游。
//   - /readyz：依次执行 ReadinessProbes（mysql/redis/scripts 等）；任意失败 503。
func registerObservabilityRoutes(h *server.Hertz, cfg appconfig.ObservabilityConfig, registry *metrics.Registry, probes map[string]httptransport.ReadinessProbe) {
	livenessPath := strings.TrimSpace(cfg.Health.LivenessPath)
	if livenessPath == "" {
		livenessPath = "/healthz"
	}
	readinessPath := strings.TrimSpace(cfg.Health.ReadinessPath)
	if readinessPath == "" {
		readinessPath = "/readyz"
	}
	h.GET(livenessPath, func(ctx context.Context, c *hertzapp.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"status": "ok"})
	})
	h.GET(readinessPath, httptransport.ReadinessHandler(3*time.Second, probes))

	if !cfg.Metrics.Enabled {
		return
	}
	metricsPath := strings.TrimSpace(cfg.Metrics.Path)
	if metricsPath == "" {
		metricsPath = strings.TrimSpace(cfg.MetricsPath)
	}
	if metricsPath == "" {
		metricsPath = "/metrics"
	}
	handler := registry.Handler()
	auth := httptransport.MetricsAuth(cfg.Metrics.AuthToken)
	h.GET(metricsPath, auth, func(ctx context.Context, c *hertzapp.RequestContext) {
		// 用 net/http 适配桥接 promhttp 的 Handler：把 hertz 请求转写成 stdlib
		// http.Request → 调用 promhttp Handler → 再把响应写回 hertz Response。
		serveStdHTTP(c, handler)
	})
}

// serveStdHTTP 把一个 net/http.Handler 适配到 hertz RequestContext。
// 仅用于运维端点（/metrics）：拷贝必要的 method/path/header/body，调用 handler，
// 再把状态码 / header / body 写回 hertz 响应。
func serveStdHTTP(c *hertzapp.RequestContext, handler http.Handler) {
	req, err := http.NewRequest(string(c.Method()), string(c.Request.URI().FullURI()), nil)
	if err != nil {
		c.AbortWithStatus(consts.StatusInternalServerError)
		return
	}
	c.Request.Header.VisitAll(func(k, v []byte) {
		req.Header.Add(string(k), string(v))
	})
	rec := newResponseRecorder()
	handler.ServeHTTP(rec, req)
	for k, vs := range rec.Header() {
		for _, v := range vs {
			c.Response.Header.Add(k, v)
		}
	}
	c.Response.SetStatusCode(rec.statusCode)
	c.Response.SetBodyRaw(rec.body)
}

// responseRecorder 是 http.ResponseWriter 的最小化实现：仅缓存 status / header / body，
// 用于把 promhttp Handler 的输出转写到 hertz Response。
type responseRecorder struct {
	header     http.Header
	body       []byte
	statusCode int
	wroteHead  bool
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{header: make(http.Header), statusCode: consts.StatusOK}
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHead {
		r.WriteHeader(consts.StatusOK)
	}
	r.body = append(r.body, p...)
	return len(p), nil
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHead {
		return
	}
	r.statusCode = code
	r.wroteHead = true
}

// buildLiveSessionEndedHook 构造 LiveSession 闭播完成后的回调。
//
// 闭播路径会在 LiveSessionService.CloseSession 完成 MySQL 状态机切换后异步触发：
// 通过 Hub.BroadcastSessionEnd 把 live_session.ended 事件推送给所有订阅了该 sessionID
// 的客户端，并触发本场直播的 AI 总结报告生成。
//
// hub 和 liveAnalysis 都为空时返回 nil，使 LiveSessionService 跳过回调注入。
func buildLiveSessionEndedHook(hub *wstransport.Hub, liveAnalysis *service.LiveAnalysisService) func(ctx context.Context, session domain.LiveSession) {
	if hub == nil && liveAnalysis == nil {
		return nil
	}
	return func(ctx context.Context, session domain.LiveSession) {
		if session.ID == 0 {
			return
		}
		if hub != nil {
			payload, _ := json.Marshal(map[string]interface{}{
				"liveSessionId": session.ID,
				"status":        session.Status,
			})
			hub.BroadcastSessionEnd(session.ID, payload)
		}
		if liveAnalysis != nil {
			_, _ = liveAnalysis.StartReportForSession(ctx, session)
		}
	}
}

type liveVoiceHubBroadcaster struct {
	hub *wstransport.Hub
}

type aiAssistantHubNotifier struct {
	hub *wstransport.Hub
}

func (n aiAssistantHubNotifier) NotifyAIAssistantEvent(ctx context.Context, liveSessionID uint64, event service.AIAssistantEvent) (int, error) {
	_ = ctx
	if n.hub == nil || liveSessionID == 0 {
		return 0, nil
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	eventType := wstransport.TypeAIAssistantStatus
	switch event.Kind {
	case "permission":
		if event.Status == "pending" {
			eventType = wstransport.TypeAIAssistantPermissionRequest
		}
	case "broadcast":
		eventType = wstransport.TypeAIAssistantBroadcast
	case "switch":
		eventType = wstransport.TypeAIAssistantSwitch
	}
	return n.hub.BroadcastLiveSession(liveSessionID, wstransport.Envelope{
		Type:      eventType,
		RequestID: event.RequestID,
		Payload:   raw,
	}), nil
}

func (b liveVoiceHubBroadcaster) BroadcastLiveVoice(ctx context.Context, liveSessionID uint64, payload service.LiveVoiceBroadcastPayload) (int, error) {
	_ = ctx
	if b.hub == nil || liveSessionID == 0 {
		return 0, nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	return b.hub.BroadcastLiveSession(liveSessionID, wstransport.Envelope{
		Type:      wstransport.TypeLiveVoiceBroadcast,
		RequestID: payload.RequestID,
		Payload:   raw,
	}), nil
}

// newBlacklistLayeredCache 构造默认的黑名单缓存：基于 LayeredCache[bool] +
// JSONCodec，通过 blacklistCacheAdapter 适配到 service.BlacklistCache。
//
// 命中策略（与 RiskService.IsBlacklisted 的 loader 协同）：
//   - 命中黑名单（hit=true）→ 写入正向缓存，TTL=5min；
//   - 不在黑名单（found=false）→ 写入负缓存，TTL=30s（短 TTL 避免长时间错误屏蔽新加入项）；
//   - 缓存层故障 → RiskService 自身做 fail-open，这里只透传错误。
//
// 通过 Invalidate 与 RiskService.AddBlacklist / RemoveBlacklist 配对，确保
// 写后立即对当前进程内 L1 失效，L2 由后续 GetOrLoad 自然刷新。
func newBlacklistLayeredCache(rdbCache *redisinfra.RedisCacheClient) service.BlacklistCache {
	if rdbCache == nil {
		return nil
	}
	lc := cache.New[bool](rdbCache, cache.JSONCodec[bool]{}, cache.Options{
		Name:        "blacklist",
		L1Capacity:  4096,
		TTL:         5 * time.Minute,
		L1TTL:       30 * time.Second,
		NegativeTTL: 30 * time.Second,
	})
	return &blacklistCacheAdapter{inner: lc}
}

// blacklistCacheAdapter 把 *cache.LayeredCache[bool] 适配为 service.BlacklistCache。
//
// 适配点：
//   - GetOrLoad 的 loader 签名差异；
//   - cache.ErrNegativeHit → 视为非黑名单（false, nil）：负缓存对黑名单语义就是
//     "不在名单内"，service 层无需感知缓存内部错误。
type blacklistCacheAdapter struct {
	inner *cache.LayeredCache[bool]
}

func (a *blacklistCacheAdapter) GetOrLoad(ctx context.Context, userID string, loader func(ctx context.Context) (bool, bool, error)) (bool, error) {
	value, _, err := a.inner.GetOrLoad(ctx, userID, cache.Loader[bool](loader))
	if err != nil {
		if errors.Is(err, cache.ErrNegativeHit) {
			return false, nil
		}
		return false, err
	}
	return value, nil
}

func (a *blacklistCacheAdapter) Invalidate(ctx context.Context, userIDs ...string) error {
	return a.inner.Invalidate(ctx, userIDs...)
}

func serverOptions(cfg appconfig.ServerConfig) []hertzconfig.Option {
	options := make([]hertzconfig.Option, 0, 4)
	if cfg.Addr != "" {
		options = append(options, server.WithHostPorts(cfg.Addr))
	}
	if cfg.ReadTimeout.Std() > 0 {
		options = append(options, server.WithReadTimeout(cfg.ReadTimeout.Std()))
	}
	if cfg.WriteTimeout.Std() > 0 {
		options = append(options, server.WithWriteTimeout(cfg.WriteTimeout.Std()))
	}
	if cfg.ShutdownTimeout.Std() > 0 {
		options = append(options, server.WithExitWaitTime(cfg.ShutdownTimeout.Std()))
	}
	return options
}

type productAuditImageLoader struct {
	uploader objectstorage.Uploader
}

func (l productAuditImageLoader) LoadProductAuditImage(ctx context.Context, imageURL string) (service.ProductAuditImage, error) {
	if l.uploader == nil {
		return service.ProductAuditImage{}, objectstorage.ErrDisabled
	}
	key, err := objectKeyFromProxyImageURL(imageURL)
	if err != nil {
		return service.ProductAuditImage{}, err
	}
	out, err := l.uploader.Download(ctx, key)
	if err != nil {
		return service.ProductAuditImage{}, err
	}
	defer out.Content.Close()
	image, err := io.ReadAll(out.Content)
	if err != nil {
		return service.ProductAuditImage{}, err
	}
	contentType := strings.TrimSpace(out.ContentType)
	if contentType == "" {
		contentType = mime.TypeByExtension(strings.ToLower(filepath.Ext(key)))
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return service.ProductAuditImage{
		ImageName:   filepath.Base(key),
		ContentType: contentType,
		ImageSize:   int64(len(image)),
		Image:       image,
	}, nil
}

func objectKeyFromProxyImageURL(imageURL string) (string, error) {
	value := strings.TrimSpace(imageURL)
	if value == "" {
		return "", domain.ErrInvalidArgument
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Path != "" {
		value = parsed.Path
	}
	prefix := objectstorage.ProxyPathPrefix()
	if !strings.HasPrefix(value, prefix) {
		return "", domain.ErrInvalidArgument
	}
	key := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	if key == "" || strings.Contains(key, "..") {
		return "", domain.ErrInvalidArgument
	}
	return key, nil
}

func openClients(ctx context.Context, cfg appconfig.Config, logger *slog.Logger, metricsRegistry *metrics.Registry, tracingProvider *tracing.Provider) (*gorm.DB, *redisinfra.ShardedRTClient, *redisinfra.RedisCacheClient, error) {
	gormLogger := observability.NewGormLogger(
		logger,
		time.Duration(cfg.Observability.SlowSQLThresholdMs)*time.Millisecond,
		true,
	)
	db, err := mysqlinfra.Open(ctx, cfg.MySQL, gormLogger)
	if err != nil {
		return nil, nil, nil, err
	}
	// GORM tracing：仅在 trace enabled 时挂 otelgorm，避免 noop tracer 也吃一份 plugin 内存。
	if tracingProvider != nil && tracingProvider.Enabled() {
		if err := db.Use(otelgorm.NewPlugin()); err != nil {
			logger.Warn("install otelgorm plugin failed", "error", err)
		}
	}

	// Redis 拆分：RT 实例服务实时路径（拍卖/出价/Stream/锁/计数），按聚合根
	// fnv32 路由到具体 shard；Cache 实例服务查询缓存（L2）。两者使用独立配置 +
	// 独立 hook 实例标签。RT 多 shard 时每个 shard 都挂上 instance="rt-<idx>"
	// 的 hook，便于 Prometheus / 链路追踪按 shard 维度区分。
	shardedRT, err := redisinfra.NewShardedRTClient(ctx, cfg.Redis.RT.Shards)
	if err != nil {
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, fmt.Errorf("open redis rt: %w", err)
	}
	cacheClient, err := redisinfra.OpenCache(ctx, cfg.Redis.Cache)
	if err != nil {
		_ = shardedRT.Close()
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, fmt.Errorf("open redis cache: %w", err)
	}
	// Redis tracing / metrics 钩子：分别为每个 RT shard 与 Cache 挂上各自 instance 标签的 hook。
	for i, shard := range shardedRT.Shards() {
		instance := fmt.Sprintf("rt-%d", i)
		if tracingProvider != nil && tracingProvider.Enabled() {
			if err := redisotel.InstrumentTracing(shard.Client); err != nil {
				logger.Warn("install redisotel tracing on rt shard failed", "shard", i, "error", err)
			}
		}
		shard.AddHook(redisinfra.NewMetricsHook(metricsRegistry, instance))
	}
	if tracingProvider != nil && tracingProvider.Enabled() {
		if err := redisotel.InstrumentTracing(cacheClient.Client); err != nil {
			logger.Warn("install redisotel tracing on cache failed", "error", err)
		}
	}
	cacheClient.AddHook(redisinfra.NewMetricsHook(metricsRegistry, "cache"))
	return db, shardedRT, cacheClient, nil
}

// buildLogger 根据 ObservabilityConfig 构建 slog.Logger，自动检测 stdout 是否为 TTY。
func buildLogger(cfg appconfig.ObservabilityConfig) *slog.Logger {
	tty := isStdoutTTY()
	return observability.NewWithOptions(cfg.LogLevel, cfg.Format, tty)
}

func isStdoutTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}
