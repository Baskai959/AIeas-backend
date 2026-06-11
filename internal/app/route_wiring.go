package app

import (
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/infra/observability/metrics"
	auctionports "aieas_backend/internal/modules/auction/ports"
	livesessionports "aieas_backend/internal/modules/live_session/ports"
	mcpapp "aieas_backend/internal/modules/mcp/app"
	httptransport "aieas_backend/internal/transport/http"
	mcptransport "aieas_backend/internal/transport/mcp"
	wstransport "aieas_backend/internal/transport/ws"

	"github.com/cloudwego/hertz/pkg/app/server"
)

type routeWiring struct {
	authService              httptransport.AuthUseCase
	auctionService           httptransport.AuctionUseCase
	bidService               httptransport.WSBidUseCase
	userProfiles             httptransport.WSUserProfileLookup
	depositService           httptransport.DepositUseCase
	hammerService            httptransport.HammerUseCase
	orderService             httptransport.OrderUseCase
	adminService             httptransport.AdminUseCase
	liveSessionService       httptransport.LiveSessionUseCase
	wsLiveSessionLookup      httptransport.WSLiveSessionLookupUseCase
	realtimeStore            auctionports.AuctionRealtimeStore
	liveSessionRealtimeStore livesessionports.LiveSessionRealtimeStore
	marketplaceService       httptransport.MarketplaceUseCase
	marketplacePresenter     httptransport.MarketplaceLiveSessionPresenter
	liveAnalysisService      httptransport.LiveAnalysisUseCase
	aiAssistantService       httptransport.AIAssistantUseCase
	aiAssistantNotifier      httptransport.AIAssistantStatusNotifier
	mcpReadService           mcptransport.MCPReadUseCase
	mcpControlService        mcptransport.MCPControlUseCase
	hub                      *wstransport.Hub
	idempotencyStore         httptransport.IdempotencyStore
	objectUploader           objectstorage.Uploader
	descriptionGen           httptransport.ProductDescriptionGenerator
	metricsRegistry          *metrics.Registry
	wsHandshakeLimiter       *wstransport.HandshakeLimiter
	asyncBid                 asyncBidWiring
	cfg                      appconfig.Config
}

// asyncBidWiring 聚合异步竞价的 WS handler 依赖。任一为 nil 时 handler 走同步降级。
type asyncBidWiring struct {
	bids        httptransport.WSAsyncBidUseCase
	publisher   httptransport.BidCommandPublisher
	coordinator *wstransport.BidAsyncCoordinator
}

func registerAppRoutes(h *server.Hertz, wiring routeWiring) {
	registerAPIRoutes := shouldRegisterAPIRoutes(wiring.cfg)
	registerWSRoutes := shouldRegisterWSRoutes(wiring.cfg)

	if registerAPIRoutes {
		registerMCPRoutes(h, wiring)
	}

	imageUploader := objectStorageImageUploader{uploader: wiring.objectUploader}
	authHandler := httptransport.NewAuthHandler(wiring.authService, imageUploader)
	auctionHandler := httptransport.NewAuctionHandler(wiring.auctionService, wiring.auctionService, wiring.depositService, wiring.hammerService, imageUploader, wiring.descriptionGen, wiring.cfg.Agent.LiveAnalysisCallbackAPIKey)
	if rankingService, ok := wiring.bidService.(httptransport.WSAuctionRankingUseCase); ok {
		auctionHandler.SetRankingService(rankingService)
	}
	orderHandler := httptransport.NewOrderHandler(wiring.orderService)
	adminHandler := httptransport.NewAdminHandler(wiring.adminService)
	marketplaceHandler := httptransport.NewMarketplaceHandler(wiring.marketplaceService)
	liveSessionHandler := httptransport.NewLiveSessionHandler(wiring.liveSessionService, wiring.liveSessionService, imageUploader)
	liveSessionHandler.SetMarketplaceService(wiring.marketplacePresenter)
	liveAnalysisHandler := httptransport.NewLiveAnalysisHandler(wiring.liveAnalysisService, wiring.cfg.Agent.LiveAnalysisCallbackAPIKey)
	aiAssistantHandler := httptransport.NewAIAssistantHandler(wiring.aiAssistantService)
	wsHandler := newWiredWSHandler(wiring)
	idempotencyTTL := wiring.cfg.Idempotency.TTL.Std()
	idempotencyStore := wiring.idempotencyStore
	if idempotencyStore == nil {
		idempotencyStore = httptransport.NewMemoryIdempotencyStore(idempotencyTTL)
	}

	if registerAPIRoutes {
		registerRESTAPIRoutes(h, restRouteWiring{
			authHandler:         authHandler,
			auctionHandler:      auctionHandler,
			orderHandler:        orderHandler,
			adminHandler:        adminHandler,
			marketplaceHandler:  marketplaceHandler,
			liveSessionHandler:  liveSessionHandler,
			liveAnalysisHandler: liveAnalysisHandler,
			aiAssistantHandler:  aiAssistantHandler,
			idempotencyStore:    idempotencyStore,
			idempotencyTTL:      idempotencyTTL,
		})
	}
	if registerWSRoutes {
		h.GET("/ws/auctions/:auction_id", authHandler.AuthMiddleware(), wsHandler.Auction)
		h.GET("/ws/live-sessions/:session_id", authHandler.AuthMiddleware(), wsHandler.LiveSession)
		h.GET("/ws/live-rooms/:room_id", authHandler.AuthMiddleware(), wsHandler.LiveSession)
	}
}

func registerMCPRoutes(h *server.Hertz, wiring routeWiring) {
	mcpReadHandler := mcptransport.NewReadHandler(wiring.mcpReadService, mcptransport.APIKeyAuthConfig{
		APIKey: wiring.cfg.MCP.Read.APIKey,
		Actor: mcpapp.MCPActor{
			ID:   wiring.cfg.MCP.Read.ActorID,
			Role: domain.Role(wiring.cfg.MCP.Read.ActorRole),
		},
	})
	mcpReadHandler.SetMetrics(wiring.metricsRegistry)
	mcpReadHandler.SetAIAssistant(wiring.aiAssistantNotifier)
	mcpControlHandler := mcptransport.NewControlHandler(wiring.mcpControlService, mcptransport.APIKeyAuthConfig{
		APIKey: wiring.cfg.MCP.Control.APIKey,
		Actor: mcpapp.MCPActor{
			ID:   wiring.cfg.MCP.Control.ActorID,
			Role: domain.Role(wiring.cfg.MCP.Control.ActorRole),
		},
	})
	mcpControlHandler.SetMetrics(wiring.metricsRegistry)
	h.POST("/mcp/read", mcpReadHandler.Post)
	h.GET("/mcp/read", mcpReadHandler.Get)
	h.POST("/mcp/control", mcpControlHandler.Post)
	h.GET("/mcp/control", mcpControlHandler.Get)
}

func newWiredWSHandler(wiring routeWiring) *httptransport.WSHandler {
	wsHandler := httptransport.NewWSHandler(wiring.hub, wiring.bidService, wiring.cfg.WebSocket.SendBufferSize, wiring.cfg.WebSocket.ReadLimitBytes, wiring.cfg.WebSocket.PingInterval.Std(), wiring.cfg.WebSocket.PongTimeout.Std(), wiring.cfg.WebSocket.WriteTimeout.Std(), wiring.cfg.WebSocket.PingJitter.Std(), wiring.cfg.WebSocket.CloseGrace.Std(), wiring.wsHandshakeLimiter, wiring.metricsRegistry)
	wsHandler.SetLiveSessionService(wiring.wsLiveSessionLookup)
	wsHandler.SetAuctionService(wiring.auctionService)
	if rankingService, ok := wiring.bidService.(httptransport.WSAuctionRankingUseCase); ok {
		wsHandler.SetAuctionRankingService(rankingService)
	}
	wsHandler.SetUserProfileLookup(wiring.userProfiles)
	wsHandler.SetRealtimeSnapshotProvider(wiring.realtimeStore)
	wsHandler.SetLiveSessionRealtimeStore(wiring.liveSessionRealtimeStore)
	wsHandler.SetBidModeMetrics(wiring.metricsRegistry)
	// 异步竞价：仅当 async 依赖齐备（kafka.enabled）时注入并切 async 模式；
	// 否则保持同步（newServerWithDependencies 仅在依赖齐备时构造 coordinator）。
	asyncReady := wiring.asyncBid.bids != nil && wiring.asyncBid.publisher != nil && wiring.asyncBid.coordinator != nil
	if asyncReady {
		wsHandler.SetAsyncBidDependencies(wiring.asyncBid.bids, wiring.asyncBid.publisher, wiring.asyncBid.coordinator)
	}
	if isWSGatewayMode(wiring.cfg) {
		wsHandler.SetBidPlaceMode(httptransport.WSBidPlaceDisabled)
		wsHandler.SetAllowDBSnapshotFallback(false)
		wsHandler.SetLiveSessionLookupMode(httptransport.WSLiveSessionLookupRealtime)
	} else if asyncReady {
		wsHandler.SetBidPlaceMode(httptransport.WSBidPlaceAsync)
		wsHandler.SetAllowDBSnapshotFallback(true)
		wsHandler.SetLiveSessionLookupMode(httptransport.WSLiveSessionLookupService)
	} else {
		wsHandler.SetBidPlaceMode(httptransport.WSBidPlaceLocal)
		wsHandler.SetAllowDBSnapshotFallback(true)
		wsHandler.SetLiveSessionLookupMode(httptransport.WSLiveSessionLookupService)
	}
	return wsHandler
}

type restRouteWiring struct {
	authHandler         *httptransport.AuthHandler
	auctionHandler      *httptransport.AuctionHandler
	orderHandler        *httptransport.OrderHandler
	adminHandler        *httptransport.AdminHandler
	marketplaceHandler  *httptransport.MarketplaceHandler
	liveSessionHandler  *httptransport.LiveSessionHandler
	liveAnalysisHandler *httptransport.LiveAnalysisHandler
	aiAssistantHandler  *httptransport.AIAssistantHandler
	idempotencyStore    httptransport.IdempotencyStore
	idempotencyTTL      time.Duration
}

func registerRESTAPIRoutes(h *server.Hertz, wiring restRouteWiring) {
	v1 := h.Group("/api/v1")
	{
		v1.POST("/auth/login", wiring.authHandler.Login)
		v1.POST("/auth/refresh", wiring.authHandler.Refresh)
		v1.POST("/admin/auth/login", wiring.authHandler.AdminLogin)

		v1.GET("/images/*key", wiring.auctionHandler.Image)
		v1.POST("/live-analysis/callback", wiring.liveAnalysisHandler.Callback)
		v1.POST("/auctions/audit/callback", wiring.auctionHandler.AuditCallback)

		protected := v1.Group("/auth", wiring.authHandler.AuthMiddleware())
		protected.GET("/me", wiring.authHandler.Me)
		protected.PATCH("/me", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.authHandler.UpdateProfile))
		protected.POST("/me/avatar", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.authHandler.UploadAvatar))
		protected.POST("/logout", wiring.authHandler.Logout)

		v1.GET("/audit-logs", wiring.authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin), wiring.adminHandler.ListOwnAuditLogs)

		marketplace := v1.Group("", wiring.authHandler.AuthMiddleware())
		marketplace.GET("/search/lots", wiring.marketplaceHandler.SearchLots)
		marketplace.GET("/lots/:id", wiring.marketplaceHandler.Lot)
		marketplace.GET("/categories", wiring.marketplaceHandler.Categories)
		marketplace.GET("/search/merchants", wiring.marketplaceHandler.SearchMerchants)
		marketplace.GET("/merchants/:id", wiring.marketplaceHandler.Merchant)
		marketplace.POST("/merchants/:id/follow", httptransport.RoleAuth(domain.RoleBuyer), httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.marketplaceHandler.FollowMerchant))
		marketplace.DELETE("/merchants/:id/follow", httptransport.RoleAuth(domain.RoleBuyer), httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.marketplaceHandler.UnfollowMerchant))
		marketplace.GET("/merchant-follows/mine", httptransport.RoleAuth(domain.RoleBuyer), wiring.marketplaceHandler.MyFollowedMerchants)
		marketplace.GET("/auction-participations/mine", httptransport.RoleAuth(domain.RoleBuyer), wiring.marketplaceHandler.MyParticipations)

		auctionState := v1.Group("/auctions", wiring.authHandler.AuthMiddleware())
		auctionState.GET("/:id/state", wiring.auctionHandler.State)
		auctionState.GET("/:id/ranking", wiring.auctionHandler.Ranking)
		auctionState.POST("/:id/enroll", httptransport.RoleAuth(domain.RoleBuyer), wiring.auctionHandler.Enroll)

		auctions := v1.Group("/auctions", wiring.authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		auctions.POST("/description/optimize", wiring.auctionHandler.OptimizeDescription)
		auctions.POST("/images", wiring.auctionHandler.UploadImages)
		auctions.POST("", wiring.auctionHandler.Create)
		auctions.GET("", wiring.auctionHandler.List)
		auctions.GET("/:id", wiring.auctionHandler.Get)
		auctions.PATCH("/:id", wiring.auctionHandler.Update)
		auctions.DELETE("/:id", wiring.auctionHandler.Delete)
		auctions.POST("/:id/start", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.auctionHandler.Start))
		auctions.POST("/:id/cancel", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.auctionHandler.Cancel))
		auctions.POST("/:id/hammer", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.auctionHandler.Hammer))

		liveSessionsPublic := v1.Group("/live-sessions", wiring.authHandler.AuthMiddleware())
		liveSessionsPublic.GET("", wiring.liveSessionHandler.List)
		liveSessionsPublic.GET("/:id", wiring.liveSessionHandler.Get)
		liveSessionsPublic.GET("/:id/lots", wiring.liveSessionHandler.Lots)
		liveSessionsPublic.GET("/:id/bids", wiring.liveSessionHandler.Bids)
		liveSessionsPublic.GET("/:id/orders", wiring.liveSessionHandler.Orders)
		liveSessionsPublic.GET("/:id/stats", wiring.liveSessionHandler.Stats)

		merchantSessions := v1.Group("/merchants", wiring.authHandler.AuthMiddleware())
		merchantSessions.GET("/:merchantId/live-sessions", wiring.liveSessionHandler.ListByMerchant)

		liveAnalysis := v1.Group("/live-analysis", wiring.authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		liveAnalysis.POST("/reports", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveAnalysisHandler.CreateReport))
		liveAnalysis.GET("/reports/:liveSessionId", wiring.liveAnalysisHandler.GetReport)

		aiAssistant := v1.Group("/ai-assistant", wiring.authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		aiAssistant.GET("/permission", wiring.aiAssistantHandler.Permission)
		aiAssistant.PATCH("/permission", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.aiAssistantHandler.UpdatePermission))
		aiAssistant.POST("/approvals/:requestId/decision", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.aiAssistantHandler.DecideApproval))

		liveSessions := v1.Group("/live-sessions", wiring.authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleMerchant, domain.RoleAdmin))
		liveSessions.POST("", wiring.liveSessionHandler.Create)
		liveSessions.PATCH("/:id", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.Update))
		liveSessions.POST("/:id/start", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.Start))
		liveSessions.POST("/:id/end", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.End))
		liveSessions.POST("/:id/lots", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.MountLot))
		liveSessions.DELETE("/:id/lots/:auctionId", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.UnmountLot))
		liveSessions.POST("/:id/activate", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.Activate))
		liveSessions.POST("/:id/deactivate", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.Deactivate))
		liveSessions.POST("/:id/cover", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.UploadCover))
		liveSessions.GET("/:id/agent-hook", wiring.liveSessionHandler.AgentHookConfig)
		liveSessions.PATCH("/:id/agent-hook", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.liveSessionHandler.UpdateAgentHookConfig))

		orders := v1.Group("/orders", wiring.authHandler.AuthMiddleware())
		orders.GET("", wiring.orderHandler.List)
		orders.GET("/mine", wiring.orderHandler.Mine)
		orders.GET("/:id", wiring.orderHandler.Get)
		orders.POST("/:id/pay", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.orderHandler.Pay))
		orders.POST("/:id/ship", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.orderHandler.Ship))
		orders.POST("/:id/receive", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.orderHandler.Receive))

		admin := v1.Group("/admin", wiring.authHandler.AuthMiddleware(), httptransport.RoleAuth(domain.RoleAdmin))
		admin.GET("/auctions", wiring.adminHandler.ListAuctions)
		admin.POST("/auctions/:id/audit", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.AuditAuction))
		admin.POST("/auctions/:id/cancel", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.CancelAuction))
		admin.POST("/auctions/:id/close", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.CloseAuction))
		admin.GET("/users", wiring.adminHandler.ListUsers)
		admin.PATCH("/users/:id", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.UpdateUser))
		admin.POST("/blacklist", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.AddBlacklist))
		admin.DELETE("/blacklist/:user_id", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.RemoveBlacklist))
		admin.GET("/blacklist", wiring.adminHandler.ListBlacklist)
		admin.GET("/risk/blacklist-strategy", wiring.adminHandler.BlacklistStrategyConfig)
		admin.PUT("/risk/blacklist-strategy", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.UpdateBlacklistStrategyConfig))
		admin.GET("/feature-flags/:key", wiring.adminHandler.FeatureFlag)
		admin.PUT("/feature-flags/:key", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.UpdateFeatureFlag))
		admin.GET("/orders", wiring.adminHandler.ListOrders)
		admin.GET("/dashboard/metrics", wiring.adminHandler.DashboardMetrics)
		admin.GET("/audit-logs", wiring.adminHandler.ListAuditLogs)
		admin.GET("/risk/events", wiring.adminHandler.ListRiskEvents)
		admin.PATCH("/risk/events/:id", httptransport.WithIdempotency(wiring.idempotencyStore, wiring.idempotencyTTLValue(), wiring.adminHandler.HandleRiskEvent))
	}
}

func (w restRouteWiring) idempotencyTTLValue() time.Duration {
	return w.idempotencyTTL
}
