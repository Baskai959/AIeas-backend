package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/agent"
	"aieas_backend/internal/infra/idgen"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/infra/observability"
	redisinfra "aieas_backend/internal/infra/redis"
	"aieas_backend/internal/repository"
	"aieas_backend/internal/service"
	httptransport "aieas_backend/internal/transport/http"
	mcptransport "aieas_backend/internal/transport/mcp"
	wstransport "aieas_backend/internal/transport/ws"
	jwtpkg "aieas_backend/pkg/jwt"

	redisgo "github.com/redis/go-redis/v9"
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
	db, rdb, err := openClients(context.Background(), cfg, logger)
	if err != nil {
		panic(err)
	}
	scripts := redisinfra.NewScriptRegistry(rdb, redisinfra.DefaultScripts())
	if err := scripts.LoadAll(context.Background()); err != nil {
		_ = rdb.Close()
		_ = mysqlinfra.Close(db)
		panic(fmt.Errorf("load redis scripts: %w", err))
	}
	realtimeStore := redisinfra.NewAuctionRealtimeStore(rdb, scripts, redisinfra.NewKeyBuilder(""))
	keys := redisinfra.NewKeyBuilder("")
	onlineCounter := redisinfra.NewOnlineCounter(rdb, keys, 24*time.Hour)
	eventLog := redisinfra.NewEventLog(rdb, keys)
	liveSessionRealtimeStore := redisinfra.NewLiveSessionRealtimeStore(rdb, keys)

	userRepo := repository.NewMySQLUserRepository(db)
	deps := ServerDependencies{
		UserRepo:                 userRepo,
		ItemRepo:                 repository.NewMySQLItemRepository(db),
		AuctionRepo:              repository.NewMySQLAuctionRepository(db),
		LiveRoomRepo:             repository.NewMySQLLiveRoomRepository(db),
		LiveSessionRepo:          repository.NewMySQLLiveSessionRepository(db),
		BidRepo:                  repository.NewMySQLBidRepository(db),
		DepositRepo:              repository.NewMySQLDepositRepository(db),
		OrderRepo:                repository.NewMySQLOrderRepository(db),
		RiskRepo:                 repository.NewMySQLRiskRepository(db),
		AuditRepo:                repository.NewMySQLAuditRepository(db),
		RealtimeStore:            realtimeStore,
		LiveSessionRealtimeStore: liveSessionRealtimeStore,
		LiveRoomLock:             redisinfra.NewLiveRoomLock(rdb, keys),
		TxManager:                repository.NewGORMTxManager(db),
		Hub:                      wstransport.NewHubWithOnlineCounter(onlineCounter),
		Idempotency:              httptransport.NewRedisIdempotencyStore(rdb, "idempotency"),
		EventLog:                 eventLog,
		OnlineCounter:            onlineCounter,
	}
	h := NewServerWithDependencies(cfg, deps)
	h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
		_ = rdb.Close()
		_ = mysqlinfra.Close(db)
	})
	return h
}

type ServerDependencies struct {
	UserRepo                 repository.UserRepository
	ItemRepo                 repository.ItemRepository
	AuctionRepo              repository.AuctionRepository
	LiveRoomRepo             repository.LiveRoomRepository
	LiveSessionRepo          repository.LiveSessionRepository
	BidRepo                  repository.BidRepository
	DepositRepo              repository.DepositRepository
	OrderRepo                repository.OrderRepository
	RiskRepo                 repository.RiskRepository
	AuditRepo                repository.AuditRepository
	RealtimeStore            repository.AuctionRealtimeStore
	LiveSessionRealtimeStore repository.LiveSessionRealtimeStore
	LiveRoomLock             repository.LiveRoomLock
	TxManager                repository.TxManager
	Hub                      *wstransport.Hub
	Idempotency              httptransport.IdempotencyStore
	EventLog                 *redisinfra.EventLog
	OnlineCounter            *redisinfra.OnlineCounter
	ObjectUploader           objectstorage.Uploader
	DescriptionGen           service.ProductDescriptionGenerator
	ProductAuditor           service.ProductAuditor
	AuctionIDGen             service.AuctionIDGenerator
	OrderIDGen               service.OrderIDGenerator
}

func NewServerWithUserRepository(cfg appconfig.Config, userRepo repository.UserRepository) *server.Hertz {
	return NewServerWithDependencies(cfg, ServerDependencies{
		UserRepo:      userRepo,
		ItemRepo:      repository.NewMemoryItemRepository(),
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
	if deps.ItemRepo == nil {
		deps.ItemRepo = repository.NewMemoryItemRepository()
	}
	if deps.AuctionRepo == nil {
		deps.AuctionRepo = repository.NewMemoryAuctionRepository()
	}
	if deps.LiveRoomRepo == nil {
		deps.LiveRoomRepo = repository.NewMemoryLiveRoomRepository()
	}
	if deps.LiveSessionRepo == nil {
		deps.LiveSessionRepo = repository.NewMemoryLiveSessionRepository()
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
	if deps.AuditRepo == nil {
		deps.AuditRepo = repository.NewMemoryAuditRepository()
	}
	if deps.RealtimeStore == nil {
		deps.RealtimeStore = repository.NewMemoryRealtimeStore()
	}
	if deps.LiveSessionRealtimeStore == nil {
		deps.LiveSessionRealtimeStore = repository.NewMemoryLiveSessionRealtimeStore()
	}
	if deps.LiveRoomLock == nil {
		deps.LiveRoomLock = repository.NewMemoryLiveRoomLock()
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
	if deps.ProductAuditor == nil {
		deps.ProductAuditor = agent.NewProductAuditClient(cfg.Agent)
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
	jwtManager := jwtpkg.NewManager(cfg.JWT.Secret, cfg.JWT.AccessTokenTTL.Std())
	authService := service.NewAuthService(deps.UserRepo, jwtManager)
	itemService := service.NewItemService(deps.ItemRepo)
	itemService.SetAuctionRepository(deps.AuctionRepo)
	itemService.SetProductAuditor(deps.ProductAuditor)
	auctionService := service.NewAuctionService(deps.AuctionRepo, deps.ItemRepo, deps.TxManager)
	riskService := service.NewRiskService(deps.RiskRepo, deps.RealtimeStore, deps.Hub)
	depositService := service.NewDepositService(deps.DepositRepo, deps.AuctionRepo, deps.RealtimeStore, riskService, deps.TxManager)
	orderService := service.NewOrderService(deps.OrderRepo, deps.TxManager)
	hammerService := service.NewHammerService(deps.AuctionRepo, deps.OrderRepo, deps.DepositRepo, deps.RealtimeStore, deps.TxManager, deps.Hub)
	timer := service.NewTimerScheduler(deps.RealtimeStore, hammerService, deps.Hub, time.Second)
	auctionService.SetRealtime(deps.RealtimeStore)
	auctionService.SetPublisher(deps.Hub)
	auctionService.SetTimer(timer)
	auctionService.SetAuctionConfig(cfg.Auction)
	auctionService.SetIDGenerator(deps.AuctionIDGen)
	hammerService.SetOrderIDGenerator(deps.OrderIDGen)
	liveRoomService := service.NewLiveRoomService(deps.LiveRoomRepo, deps.AuctionRepo, deps.TxManager, deps.LiveRoomLock)
	liveRoomService.SetAuctionService(auctionService)
	liveRoomService.SetHammerService(hammerService)
	liveRoomService.SetStatsDeps(deps.BidRepo, deps.RealtimeStore, deps.Hub)
	liveSessionService := service.NewLiveSessionService(deps.LiveSessionRepo, deps.LiveRoomRepo, deps.AuctionRepo)
	liveSessionService.SetReadDeps(deps.BidRepo, deps.OrderRepo)
	liveSessionService.SetRealtimeStore(deps.LiveSessionRealtimeStore)
	liveSessionService.SetOnEnded(buildLiveSessionEndedHook(deps.Hub))
	liveRoomService.SetLiveSessionService(liveSessionService)
	hammerService.SetLiveSessionService(liveSessionService)
	auctionService.SetOnClose(liveRoomService.OnAuctionClosed)
	hammerService.SetOnClose(liveRoomService.OnAuctionClosed)
	bidService := service.NewBidService(deps.BidRepo, deps.AuctionRepo, deps.RealtimeStore, riskService, deps.Hub, cfg.Auction)
	bidService.SetLiveSessionService(liveSessionService)
	var stopWorkers context.CancelFunc
	if deps.EventLog != nil && deps.EventLog.Enabled() {
		ctx, cancel := context.WithCancel(context.Background())
		stopWorkers = cancel
		wstransport.NewEventRelay(deps.EventLog, deps.Hub, 200*time.Millisecond).Start(ctx)
		service.NewBidRecordWriter(deps.BidRepo, deps.EventLog, "").Start(ctx)
		service.NewBidRecordReconciler(deps.BidRepo, deps.EventLog).Start(ctx, time.Minute)
		if deps.OnlineCounter != nil {
			deps.OnlineCounter.StartJanitor(ctx, time.Minute)
		}
	}
	adminService := service.NewAdminService(deps.UserRepo, auctionService, hammerService, orderService, riskService, deps.AuditRepo)
	mcpReadService := service.NewMCPReadService(service.MCPReadDependencies{
		Users:       deps.UserRepo,
		Items:       deps.ItemRepo,
		Auctions:    deps.AuctionRepo,
		Rooms:       deps.LiveRoomRepo,
		Sessions:    deps.LiveSessionRepo,
		Bids:        deps.BidRepo,
		Orders:      deps.OrderRepo,
		Risk:        riskService,
		AuditLogs:   deps.AuditRepo,
		AuctionSvc:  auctionService,
		LiveRoomSvc: liveRoomService,
		LiveSession: liveSessionService,
		OrderSvc:    orderService,
	})
	h := newServerWithServices(authService, itemService, auctionService, bidService, depositService, hammerService, orderService, adminService, liveRoomService, liveSessionService, mcpReadService, deps.AuditRepo, deps.Hub, deps.Idempotency, deps.ObjectUploader, deps.DescriptionGen, cfg)
	if stopWorkers != nil {
		h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
			_ = ctx
			stopWorkers()
		})
	}
	return h
}

func NewServerWithAuth(authService *service.AuthService) *server.Hertz {
	cfg := appconfig.Default()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	realtimeStore := repository.NewMemoryRealtimeStore()
	hub := wstransport.NewHub()
	riskService := service.NewRiskService(riskRepo, realtimeStore, hub)
	depositService := service.NewDepositService(depositRepo, auctionRepo, realtimeStore, riskService, repository.NoopTxManager{})
	orderService := service.NewOrderService(orderRepo, repository.NoopTxManager{})
	hammerService := service.NewHammerService(auctionRepo, orderRepo, depositRepo, realtimeStore, repository.NoopTxManager{}, hub)
	auctionService := service.NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})
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
	auditRepo := repository.NewMemoryAuditRepository()
	adminService := service.NewAdminService(repository.NewSeedUserRepository(), auctionService, hammerService, orderService, riskService, auditRepo)
	liveRoomRepo := repository.NewMemoryLiveRoomRepository()
	liveRoomLock := repository.NewMemoryLiveRoomLock()
	liveRoomService := service.NewLiveRoomService(liveRoomRepo, auctionRepo, repository.NoopTxManager{}, liveRoomLock)
	liveRoomService.SetAuctionService(auctionService)
	liveRoomService.SetHammerService(hammerService)
	liveRoomService.SetStatsDeps(bidRepo, realtimeStore, hub)
	liveSessionRepo := repository.NewMemoryLiveSessionRepository()
	liveSessionService := service.NewLiveSessionService(liveSessionRepo, liveRoomRepo, auctionRepo)
	liveSessionService.SetReadDeps(bidRepo, orderRepo)
	liveSessionService.SetOnEnded(buildLiveSessionEndedHook(hub))
	liveRoomService.SetLiveSessionService(liveSessionService)
	hammerService.SetLiveSessionService(liveSessionService)
	bidService.SetLiveSessionService(liveSessionService)
	auctionService.SetOnClose(liveRoomService.OnAuctionClosed)
	hammerService.SetOnClose(liveRoomService.OnAuctionClosed)
	itemService := service.NewItemService(itemRepo)
	mcpReadService := service.NewMCPReadService(service.MCPReadDependencies{
		Users:       repository.NewSeedUserRepository(),
		Items:       itemRepo,
		Auctions:    auctionRepo,
		Rooms:       liveRoomRepo,
		Sessions:    liveSessionRepo,
		Bids:        bidRepo,
		Orders:      orderRepo,
		Risk:        riskService,
		AuditLogs:   auditRepo,
		AuctionSvc:  auctionService,
		LiveRoomSvc: liveRoomService,
		LiveSession: liveSessionService,
		OrderSvc:    orderService,
	})
	return newServerWithServices(
		authService,
		itemService,
		auctionService,
		bidService,
		depositService,
		hammerService,
		orderService,
		adminService,
		liveRoomService,
		liveSessionService,
		mcpReadService,
		auditRepo,
		hub,
		nil,
		objectstorage.DisabledUploader{},
		service.DisabledProductDescriptionGenerator{},
		cfg,
	)
}

func newServerWithServices(
	authService *service.AuthService,
	itemService *service.ItemService,
	auctionService *service.AuctionService,
	bidService *service.BidService,
	depositService *service.DepositService,
	hammerService *service.HammerService,
	orderService *service.OrderService,
	adminService *service.AdminService,
	liveRoomService *service.LiveRoomService,
	liveSessionService *service.LiveSessionService,
	mcpReadService *service.MCPReadService,
	auditRepo repository.AuditRepository,
	hub *wstransport.Hub,
	idempotencyStore httptransport.IdempotencyStore,
	objectUploader objectstorage.Uploader,
	descriptionGen service.ProductDescriptionGenerator,
	cfg appconfig.Config,
) *server.Hertz {
	logger := buildLogger(cfg.Observability)
	h := server.Default(serverOptions(cfg.Server)...)
	h.Use(
		httptransport.RecoveryMiddleware(logger),
		httptransport.RequestIDMiddleware(),
		httptransport.NewRateLimiter(240, timeMinute()).Middleware(),
		httptransport.AuditMiddleware(auditRepo, logger),
	)

	h.GET("/ping", func(ctx context.Context, c *hertzapp.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"message": "pong"})
	})
	mcpHandler := mcptransport.NewHandler(authService, mcpReadService)
	h.POST("/mcp", mcpHandler.Post)
	h.GET("/mcp", mcpHandler.Get)

	authHandler := httptransport.NewAuthHandler(authService)
	itemHandler := httptransport.NewItemHandler(itemService, objectUploader, descriptionGen)
	auctionHandler := httptransport.NewAuctionHandler(auctionService, depositService, hammerService)
	orderHandler := httptransport.NewOrderHandler(orderService)
	adminHandler := httptransport.NewAdminHandler(adminService)
	liveRoomHandler := httptransport.NewLiveRoomHandler(liveRoomService)
	liveSessionHandler := httptransport.NewLiveSessionHandler(liveSessionService)
	wsHandler := httptransport.NewWSHandler(hub, bidService, cfg.WebSocket.SendBufferSize, cfg.WebSocket.ReadLimitBytes, cfg.WebSocket.PingInterval.Std(), cfg.WebSocket.PongTimeout.Std())
	wsHandler.SetLiveRoomService(liveRoomService)
	idempotencyTTL := cfg.Idempotency.TTL.Std()
	if idempotencyStore == nil {
		idempotencyStore = httptransport.NewMemoryIdempotencyStore(idempotencyTTL)
	}

	v1 := h.Group("/api/v1")
	{
		v1.POST("/auth/login", authHandler.Login)
		v1.POST("/auth/refresh", authHandler.Refresh)
		v1.POST("/admin/auth/login", authHandler.AdminLogin)

		v1.GET("/images/*key", itemHandler.Image)

		protected := v1.Group("/auth", authHandler.AuthMiddleware())
		protected.GET("/me", authHandler.Me)
		protected.POST("/logout", authHandler.Logout)

		v1.GET("/audit-logs", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin), adminHandler.ListOwnAuditLogs)

		items := v1.Group("/items", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		items.POST("/description/optimize", itemHandler.OptimizeDescription)
		items.POST("", itemHandler.Create)
		items.GET("", itemHandler.List)
		items.GET("/:id", itemHandler.Get)
		items.PATCH("/:id", itemHandler.Update)
		items.DELETE("/:id", itemHandler.Delete)

		auctionState := v1.Group("/auctions", authHandler.AuthMiddleware())
		auctionState.GET("/:id/state", auctionHandler.State)
		auctionState.POST("/:id/enroll", httptransport.RoleAuth(domain.RoleBuyer), auctionHandler.Enroll)

		auctions := v1.Group("/auctions", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		auctions.POST("", auctionHandler.Create)
		auctions.GET("", auctionHandler.List)
		auctions.GET("/:id", auctionHandler.Get)
		auctions.PATCH("/:id", auctionHandler.Update)
		auctions.DELETE("/:id", auctionHandler.Delete)
		auctions.POST("/:id/start", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, auctionHandler.Start))
		auctions.POST("/:id/cancel", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, auctionHandler.Cancel))
		auctions.POST("/:id/hammer", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, auctionHandler.Hammer))

		liveRoomsPublic := v1.Group("/live-rooms", authHandler.AuthMiddleware())
		liveRoomsPublic.GET("", liveRoomHandler.List)
		liveRoomsPublic.GET("/:id", liveRoomHandler.Get)
		liveRoomsPublic.GET("/:id/lots", liveRoomHandler.Lots)
		liveRoomsPublic.GET("/:id/stats", liveRoomHandler.Stats)
		liveRoomsPublic.GET("/:id/sessions", liveSessionHandler.ListByRoom)

		liveSessionsPublic := v1.Group("/live-sessions", authHandler.AuthMiddleware())
		liveSessionsPublic.GET("/:sessionId", liveSessionHandler.Get)
		liveSessionsPublic.GET("/:sessionId/lots", liveSessionHandler.Lots)
		liveSessionsPublic.GET("/:sessionId/bids", liveSessionHandler.Bids)
		liveSessionsPublic.GET("/:sessionId/orders", liveSessionHandler.Orders)

		merchantSessions := v1.Group("/merchants", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		merchantSessions.GET("/:merchantId/live-sessions", liveSessionHandler.ListByMerchant)

		liveRooms := v1.Group("/live-rooms", authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		liveRooms.POST("", liveRoomHandler.Create)
		liveRooms.PATCH("/:id", liveRoomHandler.Update)
		liveRooms.DELETE("/:id", liveRoomHandler.Delete)
		liveRooms.POST("/:id/activate", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveRoomHandler.Activate))
		liveRooms.POST("/:id/deactivate", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveRoomHandler.Deactivate))
		liveRooms.POST("/:id/lots", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, liveRoomHandler.MountLot))
		liveRooms.DELETE("/:id/lots/:auctionId", liveRoomHandler.UnmountLot)

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
		admin.GET("/orders", adminHandler.ListOrders)
		admin.GET("/audit-logs", adminHandler.ListAuditLogs)
		admin.GET("/risk/events", adminHandler.ListRiskEvents)
		admin.PATCH("/risk/events/:id", httptransport.WithIdempotency(idempotencyStore, idempotencyTTL, adminHandler.HandleRiskEvent))
	}
	h.GET("/ws/auctions/:auction_id", authHandler.AuthMiddleware(), wsHandler.Auction)
	h.GET("/ws/live-rooms/:room_id", authHandler.AuthMiddleware(), wsHandler.LiveRoom)
	if cfg.Observability.MetricsPath != "" {
		h.GET(cfg.Observability.MetricsPath, func(ctx context.Context, c *hertzapp.RequestContext) {
			httptransport.WriteSuccess(c, map[string]string{"status": "ok"})
		})
	}

	return h
}

func timeMinute() time.Duration {
	return time.Minute
}

// buildLiveSessionEndedHook 构造 LiveSession 闭播完成后用于通知 WS 订阅方的回调。
//
// 闭播路径会在 LiveSessionService.CloseSession 完成 MySQL 状态机切换后异步触发：
// 通过 Hub.BroadcastSessionEnd 把 live_session.ended 事件推送给所有订阅了该 sessionID
// 的客户端，并把它们从 session 反查表中清理掉（同房间的 ws 仍由 Unsubscribe 路径独立清理）。
//
// hub 为 nil 时返回 nil，使 LiveSessionService 跳过回调注入。
func buildLiveSessionEndedHook(hub *wstransport.Hub) func(ctx context.Context, session domain.LiveSession) {
	if hub == nil {
		return nil
	}
	return func(_ context.Context, session domain.LiveSession) {
		if session.ID == 0 {
			return
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"liveSessionId": session.ID,
			"liveRoomId":    session.LiveRoomID,
			"status":        session.Status,
		})
		hub.BroadcastSessionEnd(session.ID, payload)
	}
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

func openClients(ctx context.Context, cfg appconfig.Config, logger *slog.Logger) (*gorm.DB, *redisgo.Client, error) {
	gormLogger := observability.NewGormLogger(
		logger,
		time.Duration(cfg.Observability.SlowSQLThresholdMs)*time.Millisecond,
		true,
	)
	db, err := mysqlinfra.Open(ctx, cfg.MySQL, gormLogger)
	if err != nil {
		return nil, nil, err
	}

	rdb, err := redisinfra.Open(ctx, cfg.Redis)
	if err != nil {
		_ = mysqlinfra.Close(db)
		return nil, nil, err
	}
	return db, rdb, nil
}

// buildLogger 根据 ObservabilityConfig 构建 slog.Logger，自动检测 stdout 是否为 TTY。
func buildLogger(cfg appconfig.ObservabilityConfig) *slog.Logger {
	tty := isStdoutTTY()
	return observability.NewWithOptions(cfg.LogLevel, cfg.Format, tty)
}

func isStdoutTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}
