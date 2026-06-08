package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	appconfig "aieas_backend/internal/config"
	kafkainfra "aieas_backend/internal/infra/kafka"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	"aieas_backend/internal/infra/observability"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	redisinfra "aieas_backend/internal/infra/redis"
	httptransport "aieas_backend/internal/transport/http"

	"github.com/cloudwego/hertz/pkg/app/server"
	hertzconfig "github.com/cloudwego/hertz/pkg/common/config"
	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"golang.org/x/term"
	"gorm.io/gorm"
)

type platformDeps struct {
	logger           *slog.Logger
	metricsRegistry  *metrics.Registry
	tracingProvider  *tracing.Provider
	db               *gorm.DB
	shardedRT        *redisinfra.ShardedRTClient
	workerShardedRT  *redisinfra.ShardedRTClient
	publishShardedRT *redisinfra.ShardedRTClient
	rankingShardedRT *redisinfra.ShardedRTClient
	rdbCache         *redisinfra.RedisCacheClient
	scripts          *redisinfra.ScriptRegistry
	keys             redisinfra.KeyBuilder
	kafkaProducer    *kafkainfra.Producer
	kafkaBidReader   *kafkainfra.BidEventReader
	kafkaCmdReader   *kafkainfra.BidCommandReader
}

func buildPlatformDeps(ctx context.Context, cfg appconfig.Config) (*platformDeps, error) {
	logger := buildLogger(cfg.Observability)
	metricsRegistry := metrics.New(metrics.Options{
		Enabled:   cfg.Observability.Metrics.Enabled,
		Namespace: cfg.Observability.Metrics.Namespace,
	})
	tracingProvider, err := tracing.Setup(ctx, tracing.Config{
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
	db, shardedRT, workerShardedRT, publishShardedRT, rankingShardedRT, rdbCache, err := openClients(ctx, cfg, logger, metricsRegistry, tracingProvider)
	if err != nil {
		return nil, err
	}
	p := &platformDeps{
		logger:           logger,
		metricsRegistry:  metricsRegistry,
		tracingProvider:  tracingProvider,
		db:               db,
		shardedRT:        shardedRT,
		workerShardedRT:  workerShardedRT,
		publishShardedRT: publishShardedRT,
		rankingShardedRT: rankingShardedRT,
		rdbCache:         rdbCache,
		keys:             redisinfra.NewKeyBuilder(""),
	}
	p.scripts = redisinfra.NewShardedScriptRegistry(shardedRT, redisinfra.DefaultScripts())
	p.scripts.SetMetrics(metricsRegistry)
	if err := p.scripts.LoadAll(ctx); err != nil {
		p.close(ctx)
		return nil, fmt.Errorf("load redis scripts: %w", err)
	}
	kafkaProducer, kafkaBidReader, kafkaCmdReader, err := openKafkaClients(cfg)
	if err != nil {
		p.close(ctx)
		return nil, err
	}
	p.kafkaProducer = kafkaProducer
	p.kafkaBidReader = kafkaBidReader
	p.kafkaCmdReader = kafkaCmdReader
	return p, nil
}

func (p *platformDeps) registerShutdown(h *server.Hertz) {
	if p == nil || h == nil {
		return
	}
	poolStatsCtx, stopPoolStats := context.WithCancel(context.Background())
	redisinfra.StartPoolStatsCollector(poolStatsCtx, p.metricsRegistry, p.rdbCache,
		redisinfra.RedisPoolStatsGroup{Prefix: "rt", Sharded: p.shardedRT},
		redisinfra.RedisPoolStatsGroup{Prefix: "rt-worker", Sharded: p.workerShardedRT},
		redisinfra.RedisPoolStatsGroup{Prefix: "pubsub", Sharded: p.publishShardedRT},
		redisinfra.RedisPoolStatsGroup{Prefix: "ranking", Sharded: p.rankingShardedRT},
	)
	h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
		_ = ctx
		stopPoolStats()
	})
	// 关闭顺序：worker 池先关（worker goroutine 已在 NewServerWithDependencies 注册的
	// stopWorkers shutdown hook 中先于本 hook 退出），再关主链路池 + cache + mysql。
	h.OnShutdown = append(h.OnShutdown, p.close)
}

func (p *platformDeps) close(ctx context.Context) {
	if p == nil {
		return
	}
	_ = p.workerShardedRT.Close()
	_ = p.publishShardedRT.Close()
	_ = p.rankingShardedRT.Close()
	_ = p.shardedRT.Close()
	_ = p.rdbCache.Close()
	if p.kafkaBidReader != nil {
		_ = p.kafkaBidReader.Close()
	}
	if p.kafkaCmdReader != nil {
		_ = p.kafkaCmdReader.Close()
	}
	if p.kafkaProducer != nil {
		_ = p.kafkaProducer.Close()
	}
	_ = mysqlinfra.Close(p.db)
	if p.tracingProvider != nil {
		_ = p.tracingProvider.Shutdown(ctx)
	}
}

// openKafkaClients 构造 Kafka producer 与 bid event / bid command consumer。Kafka disabled 时返回 nil。
func openKafkaClients(cfg appconfig.Config) (*kafkainfra.Producer, *kafkainfra.BidEventReader, *kafkainfra.BidCommandReader, error) {
	if !cfg.Kafka.Enabled {
		return nil, nil, nil, nil
	}
	producer, err := kafkainfra.NewProducer(kafkainfra.ProducerConfig{
		Brokers:            cfg.Kafka.Brokers,
		ClientID:           cfg.Kafka.ClientID,
		BidEventsTopic:     cfg.Kafka.BidEventsTopic,
		BidCommandsTopic:   cfg.Kafka.BidCommandsTopic,
		AuctionEventsTopic: cfg.Kafka.AuctionEventsTopic,
		OrderEventsTopic:   cfg.Kafka.OrderEventsTopic,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("init kafka producer: %w", err)
	}
	reader, err := kafkainfra.NewBidEventReader(kafkainfra.BidEventReaderConfig{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.BidRecordGroup,
		Topic:   cfg.Kafka.BidEventsTopic,
	})
	if err != nil {
		_ = producer.Close()
		return nil, nil, nil, fmt.Errorf("init kafka bid reader: %w", err)
	}
	cmdReader, err := kafkainfra.NewBidCommandReader(kafkainfra.BidCommandReaderConfig{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.BidDecisionGroup,
		Topic:   cfg.Kafka.BidCommandsTopic,
	})
	if err != nil {
		_ = reader.Close()
		_ = producer.Close()
		return nil, nil, nil, fmt.Errorf("init kafka bid command reader: %w", err)
	}
	return producer, reader, cmdReader, nil
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

func serverOptions(cfg appconfig.ServerConfig) []hertzconfig.Option {
	options := make([]hertzconfig.Option, 0, 5)
	if cfg.Addr != "" {
		options = append(options, server.WithHostPorts(cfg.Addr))
	}
	if cfg.MaxRequestBodySizeBytes > 0 {
		options = append(options, server.WithMaxRequestBodySize(cfg.MaxRequestBodySizeBytes))
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

func openClients(ctx context.Context, cfg appconfig.Config, logger *slog.Logger, metricsRegistry *metrics.Registry, tracingProvider *tracing.Provider) (*gorm.DB, *redisinfra.ShardedRTClient, *redisinfra.ShardedRTClient, *redisinfra.ShardedRTClient, *redisinfra.ShardedRTClient, *redisinfra.RedisCacheClient, error) {
	gormLogger := observability.NewGormLogger(
		logger,
		time.Duration(cfg.Observability.SlowSQLThresholdMs)*time.Millisecond,
		true,
	)
	db, err := mysqlinfra.Open(ctx, cfg.MySQL, gormLogger)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	// GORM tracing：仅在 trace enabled 时挂 otelgorm，避免 noop tracer 也吃一份 plugin 内存。
	if tracingProvider != nil && tracingProvider.Enabled() {
		if err := db.Use(otelgorm.NewPlugin()); err != nil {
			logger.Warn("install otelgorm plugin failed", "error", err)
		}
	}

	// Redis 拆分：RT 实例服务实时路径（拍卖/出价/Stream/锁），按聚合根
	// fnv32 路由到具体 shard；Cache 实例服务查询缓存、在线人数、PubSub 与
	// 异步排行榜。两者使用独立配置 + 独立 hook 实例标签。RT 多 shard 时每个
	// shard 都挂上 instance="rt-<idx>" 的 hook，便于 Prometheus / 链路追踪按
	// shard 维度区分。
	// 同时给后台 worker 单独再开一份 ShardedRTClient（同 addr，独立 pool），避免
	// stream 消费 / DLQ 写入占用主链路 pool 的连接。
	shardedRT, err := redisinfra.NewShardedRTClient(ctx, cfg.Redis.RT.Shards)
	if err != nil {
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("open redis rt: %w", err)
	}
	workerShardedRT, err := redisinfra.NewShardedRTClient(ctx, cfg.Redis.RT.Shards)
	if err != nil {
		_ = shardedRT.Close()
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("open redis rt worker pool: %w", err)
	}
	cacheShardConfig := []appconfig.RedisInstanceConfig{cfg.Redis.Cache}
	publishShardedRT, err := redisinfra.NewShardedRTClient(ctx, cacheShardConfig)
	if err != nil {
		_ = workerShardedRT.Close()
		_ = shardedRT.Close()
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("open redis pubsub pool: %w", err)
	}
	rankingShardedRT, err := redisinfra.NewShardedRTClient(ctx, cacheShardConfig)
	if err != nil {
		_ = publishShardedRT.Close()
		_ = workerShardedRT.Close()
		_ = shardedRT.Close()
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("open redis ranking pool: %w", err)
	}
	cacheClient, err := redisinfra.OpenCache(ctx, cfg.Redis.Cache)
	if err != nil {
		_ = shardedRT.Close()
		_ = workerShardedRT.Close()
		_ = publishShardedRT.Close()
		_ = rankingShardedRT.Close()
		_ = mysqlinfra.Close(db)
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("open redis cache: %w", err)
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
	for i, shard := range workerShardedRT.Shards() {
		instance := fmt.Sprintf("rt-worker-%d", i)
		if tracingProvider != nil && tracingProvider.Enabled() {
			if err := redisotel.InstrumentTracing(shard.Client); err != nil {
				logger.Warn("install redisotel tracing on rt worker shard failed", "shard", i, "error", err)
			}
		}
		shard.AddHook(redisinfra.NewMetricsHook(metricsRegistry, instance))
	}
	for i, shard := range publishShardedRT.Shards() {
		instance := fmt.Sprintf("pubsub-%d", i)
		if tracingProvider != nil && tracingProvider.Enabled() {
			if err := redisotel.InstrumentTracing(shard.Client); err != nil {
				logger.Warn("install redisotel tracing on pubsub shard failed", "shard", i, "error", err)
			}
		}
		shard.AddHook(redisinfra.NewMetricsHook(metricsRegistry, instance))
	}
	for i, shard := range rankingShardedRT.Shards() {
		instance := fmt.Sprintf("ranking-%d", i)
		if tracingProvider != nil && tracingProvider.Enabled() {
			if err := redisotel.InstrumentTracing(shard.Client); err != nil {
				logger.Warn("install redisotel tracing on ranking shard failed", "shard", i, "error", err)
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
	return db, shardedRT, workerShardedRT, publishShardedRT, rankingShardedRT, cacheClient, nil
}

// buildLogger 根据 ObservabilityConfig 构建 slog.Logger，自动检测 stdout 是否为 TTY。
func buildLogger(cfg appconfig.ObservabilityConfig) *slog.Logger {
	tty := isStdoutTTY()
	return observability.NewWithOptions(cfg.LogLevel, cfg.Format, tty)
}

func isStdoutTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}
