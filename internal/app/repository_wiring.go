package app

import (
	"context"
	"errors"
	"strconv"
	"time"

	appruntime "aieas_backend/internal/app/runtime"
	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/infra/cache"
	kafkainfra "aieas_backend/internal/infra/kafka"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	redisinfra "aieas_backend/internal/infra/redis"
	adminapp "aieas_backend/internal/modules/admin/app"
	adminrepo "aieas_backend/internal/modules/admin/repository"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
	depositrepo "aieas_backend/internal/modules/deposit/repository"
	liveanalysisrepo "aieas_backend/internal/modules/live_analysis/repository"
	livesessionrepo "aieas_backend/internal/modules/live_session/repository"
	orderrepo "aieas_backend/internal/modules/order/repository"
	riskports "aieas_backend/internal/modules/risk/ports"
	riskrepo "aieas_backend/internal/modules/risk/repository"
	userrepo "aieas_backend/internal/modules/user/repository"
	httptransport "aieas_backend/internal/transport/http"
	wstransport "aieas_backend/internal/transport/ws"

	redisgo "github.com/redis/go-redis/v9"
)

func buildProductionServerDependencies(ctx context.Context, cfg appconfig.Config) (*platformDeps, ServerDependencies, error) {
	platform, err := buildPlatformDeps(ctx, cfg)
	if err != nil {
		return nil, ServerDependencies{}, err
	}
	deps := buildMySQLServerDependencies(cfg, platform)
	return platform, deps, nil
}

func buildMySQLServerDependencies(cfg appconfig.Config, platform *platformDeps) ServerDependencies {
	realtimeStore := redisinfra.NewAuctionRealtimeStore(platform.shardedRT, platform.scripts, platform.keys)
	realtimeStore.SetPublishShardedRT(platform.publishShardedRT)
	realtimeStore.SetRankingShardedRT(platform.rankingShardedRT)
	onlineCounter := redisinfra.NewOnlineCounterOnCache(platform.rdbCache, platform.keys, redisinfra.DefaultOnlineCounterTTL)
	// EventLog 是后台 worker 主要消费方（XREADGROUP / ranking 更新 / DLQ），
	// 改用 worker 专用 pool；accepted 排行榜更新再写到 ranking 专用 pool，
	// 避免异步展示链路占用主链路出价 Redis。
	eventLog := redisinfra.NewEventLog(platform.workerShardedRT, platform.keys)
	eventLog.SetRankingShardedRT(platform.rankingShardedRT)
	liveSessionRealtimeStore := redisinfra.NewLiveSessionRealtimeStore(platform.shardedRT, platform.keys)
	bidCommandInFlight := redisinfra.NewBidCommandInFlightTracker(platform.workerShardedRT, platform.keys, redisinfra.DefaultBidCommandInFlightTTL)

	userRepo := userrepo.NewMySQLUserRepository(platform.db)
	return ServerDependencies{
		UserRepo:                            userRepo,
		MerchantFollowRepo:                  userRepo,
		AuctionRepo:                         auctionrepo.NewMySQLAuctionRepository(platform.db, mysqlinfra.ResolveDB),
		LiveSessionRepo:                     livesessionrepo.NewMySQLLiveSessionRepository(platform.db, mysqlinfra.ResolveDB),
		LiveAnalysisReportRepo:              liveanalysisrepo.NewMySQLLiveAnalysisReportRepository(platform.db, mysqlinfra.ResolveDB),
		ConfigRepo:                          adminrepo.NewMySQLConfigRepository(platform.db, mysqlinfra.ResolveDB),
		BidRepo:                             auctionrepo.NewMySQLBidRepository(platform.db, mysqlinfra.ResolveDB),
		DepositRepo:                         depositrepo.NewMySQLDepositRepository(platform.db, mysqlinfra.ResolveDB),
		OrderRepo:                           orderrepo.NewMySQLOrderRepository(platform.db, mysqlinfra.ResolveDB),
		RiskRepo:                            riskrepo.NewMySQLRiskRepository(platform.db, mysqlinfra.ResolveDB),
		AuditRepo:                           adminrepo.NewMySQLAuditRepository(platform.db, mysqlinfra.ResolveDB),
		AdminDashboardRepo:                  adminrepo.NewMySQLAdminDashboardRepository(platform.db, mysqlinfra.ResolveDB),
		RealtimeStore:                       realtimeStore,
		LiveSessionRealtimeStore:            liveSessionRealtimeStore,
		LiveSessionLock:                     redisinfra.NewLiveSessionLock(platform.shardedRT, platform.keys),
		TxManager:                           mysqlinfra.NewGORMTxManager(platform.db),
		Hub:                                 wstransport.NewHubWithOnlineCounter(onlineCounter),
		Idempotency:                         httptransport.NewRedisIdempotencyStore(platform.rdbCache, "idempotency"),
		EventLog:                            eventLog,
		OnlineCounter:                       onlineCounter,
		DistributedRateLimiter:              redisinfra.NewDistributedRateLimiter(platform.scripts, platform.keys),
		FeatureFlags:                        adminapp.NewFeatureFlagService(adminrepo.NewMySQLConfigRepository(platform.db, mysqlinfra.ResolveDB), redisConfigInvalidationBus{client: platform.rdbCache}),
		PubSubClients:                       pubSubClientsFromShards(platform.publishShardedRT),
		RealtimeEventPublisher:              redisinfra.NewRealtimeEventPublisher(platform.publishShardedRT),
		WSHandshakeLimiter:                  wstransport.NewHandshakeLimiter(cfg.WebSocket.HandshakeRateLimitPerIP, cfg.WebSocket.HandshakeRateLimitPerUser, cfg.WebSocket.HandshakeRateLimitPerAuction),
		MetricsRegistry:                     platform.metricsRegistry,
		Tracing:                             platform.tracingProvider,
		ReadinessProbes:                     buildReadinessProbes(platform.db, platform.shardedRT, platform.rdbCache, platform.scripts, platform.kafkaProducer),
		BlacklistCache:                      newBlacklistLayeredCache(platform.rdbCache),
		AuctionSnapshotCache:                newAuctionSnapshotLayeredCache(platform.rdbCache),
		DepositReconcilerEnabled:            true,
		OrderTimeoutWorkerEnabled:           true,
		StartupExpiredAuctionCleanupEnabled: true,
		ScheduledAuctionStarterEnabled:      true,
		BidEventKafkaProducer:               platform.kafkaProducer,
		BidEventKafkaConsumer:               platform.kafkaBidReader,
		SettlementEventPublisher:            platform.kafkaProducer,
		BidCommandConsumer:                  kafkaBidCommandConsumer(platform.kafkaCmdReader),
		BidCommandPublisher:                 kafkaBidCommandPublisherOrNil(platform.kafkaProducer, bidCommandInFlight),
		BidCommandInFlightTracker:           bidCommandInFlight,
	}
}

// kafkaBidCommandConsumer 仅在 reader 非 nil 时返回非 nil 接口值，避免 typed-nil。
func kafkaBidCommandConsumer(reader *kafkainfra.BidCommandReader) appruntime.BidCommandConsumer {
	if reader == nil {
		return nil
	}
	return reader
}

// kafkaBidCommandPublisherOrNil 仅在 producer 非 nil 时返回非 nil 接口值。
func kafkaBidCommandPublisherOrNil(producer *kafkainfra.Producer, tracker bidCommandInFlightTracker) httptransport.BidCommandPublisher {
	if producer == nil {
		return nil
	}
	pub := newKafkaBidCommandPublisher(producer)
	pub.SetInFlightTracker(tracker)
	return pub
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

// newBlacklistLayeredCache 构造默认的黑名单缓存：基于 LayeredCache[bool] +
// JSONCodec，通过 blacklistCacheAdapter 适配到 riskports.BlacklistCache。
//
// 命中策略（与 RiskService.IsBlacklisted 的 loader 协同）：
//   - 命中黑名单（hit=true）→ 写入正向缓存，TTL=5min；
//   - 不在黑名单（found=false）→ 写入负缓存，TTL=30s（短 TTL 避免长时间错误屏蔽新加入项）；
//   - 缓存层故障 → RiskService 自身做 fail-open，这里只透传错误。
//
// 通过 Invalidate 与 RiskService.AddBlacklist / RemoveBlacklist 配对，确保
// 写后立即对当前进程内 L1 失效，L2 由后续 GetOrLoad 自然刷新。
func newBlacklistLayeredCache(rdbCache *redisinfra.RedisCacheClient) riskports.BlacklistCache {
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

// blacklistCacheAdapter 把 *cache.LayeredCache[bool] 适配为 riskports.BlacklistCache。
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

func newAuctionSnapshotLayeredCache(rdbCache *redisinfra.RedisCacheClient) auctionapp.AuctionSnapshotCache {
	if rdbCache == nil {
		return nil
	}
	lc := cache.New[auctionapp.AuctionRuntimeSnapshot](rdbCache, cache.JSONCodec[auctionapp.AuctionRuntimeSnapshot]{}, cache.Options{
		Name:        "auction_snapshot",
		L1Capacity:  4096,
		TTL:         time.Hour,
		L1TTL:       10 * time.Second,
		NegativeTTL: 30 * time.Second,
	})
	return &auctionSnapshotCacheAdapter{inner: lc}
}

type auctionSnapshotCacheAdapter struct {
	inner *cache.LayeredCache[auctionapp.AuctionRuntimeSnapshot]
}

func (a *auctionSnapshotCacheAdapter) Get(ctx context.Context, auctionID uint64) (auctionapp.AuctionRuntimeSnapshot, string, bool, error) {
	value, source, err := a.inner.Get(ctx, auctionSnapshotCacheKey(auctionID))
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) || errors.Is(err, cache.ErrNegativeHit) {
			return auctionapp.AuctionRuntimeSnapshot{}, string(source), false, nil
		}
		return auctionapp.AuctionRuntimeSnapshot{}, string(source), false, err
	}
	return value, string(source), true, nil
}

func (a *auctionSnapshotCacheAdapter) Set(ctx context.Context, snapshot auctionapp.AuctionRuntimeSnapshot, ttl time.Duration) error {
	if snapshot.AuctionID == 0 {
		return nil
	}
	return a.inner.Set(ctx, auctionSnapshotCacheKey(snapshot.AuctionID), snapshot, ttl)
}

func (a *auctionSnapshotCacheAdapter) Invalidate(ctx context.Context, auctionID uint64) error {
	if auctionID == 0 {
		return nil
	}
	return a.inner.Invalidate(ctx, auctionSnapshotCacheKey(auctionID))
}

func auctionSnapshotCacheKey(auctionID uint64) string {
	return strconv.FormatUint(auctionID, 10)
}

type redisConfigInvalidationBus struct {
	client *redisinfra.RedisCacheClient
}

func (b redisConfigInvalidationBus) Publish(ctx context.Context, channel string, message string) error {
	if b.client == nil {
		return nil
	}
	return b.client.Publish(ctx, channel, message).Err()
}

func (b redisConfigInvalidationBus) Subscribe(ctx context.Context, patterns ...string) (adminapp.ConfigInvalidationSubscription, error) {
	if b.client == nil {
		return nil, nil
	}
	pubsub := b.client.PSubscribe(ctx, patterns...)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}
	return newRedisConfigInvalidationSubscription(pubsub), nil
}

type redisConfigInvalidationSubscription struct {
	pubsub *redisgo.PubSub
	ch     <-chan adminapp.ConfigInvalidationMessage
}

func newRedisConfigInvalidationSubscription(pubsub *redisgo.PubSub) adminapp.ConfigInvalidationSubscription {
	if pubsub == nil {
		return nil
	}
	out := make(chan adminapp.ConfigInvalidationMessage, 16)
	go func() {
		defer close(out)
		for msg := range pubsub.Channel() {
			if msg == nil {
				continue
			}
			out <- adminapp.ConfigInvalidationMessage{Payload: msg.Payload}
		}
	}()
	return redisConfigInvalidationSubscription{pubsub: pubsub, ch: out}
}

func (s redisConfigInvalidationSubscription) Channel() <-chan adminapp.ConfigInvalidationMessage {
	return s.ch
}

func (s redisConfigInvalidationSubscription) Close() error {
	if s.pubsub == nil {
		return nil
	}
	return s.pubsub.Close()
}
