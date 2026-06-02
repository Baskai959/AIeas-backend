package config

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultPath = "configs/config.yaml"

type Duration time.Duration

func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	raw := strings.TrimSpace(value.Value)
	if raw == "" {
		*d = 0
		return nil
	}
	if value.Tag == "!!int" {
		nanos, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("parse duration %q: %w", raw, err)
		}
		*d = Duration(time.Duration(nanos))
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", raw, err)
	}
	*d = Duration(parsed)
	return nil
}

type Config struct {
	Server        ServerConfig        `yaml:"server"`
	MySQL         MySQLConfig         `yaml:"mysql"`
	Redis         RedisConfig         `yaml:"redis"`
	Idempotency   IdempotencyConfig   `yaml:"idempotency"`
	JWT           JWTConfig           `yaml:"jwt"`
	IDGen         IDGenConfig         `yaml:"idgen"`
	Auction       AuctionConfig       `yaml:"auction"`
	RiskControl   RiskControlConfig   `yaml:"riskControl"`
	WebSocket     WebSocketConfig     `yaml:"websocket"`
	ObjectStorage ObjectStorageConfig `yaml:"objectStorage"`
	Agent         AgentConfig         `yaml:"agent"`
	DoubaoTTS     DoubaoTTSConfig     `yaml:"doubaoTTS"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	MCP           MCPConfig           `yaml:"mcp"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type ServerConfig struct {
	Addr            string   `yaml:"addr"`
	ReadTimeout     Duration `yaml:"readTimeout"`
	WriteTimeout    Duration `yaml:"writeTimeout"`
	ShutdownTimeout Duration `yaml:"shutdownTimeout"`
}

type MySQLConfig struct {
	DSN             string   `yaml:"dsn"`
	MaxOpenConns    int      `yaml:"maxOpenConns"`
	MaxIdleConns    int      `yaml:"maxIdleConns"`
	ConnMaxLifetime Duration `yaml:"connMaxLifetime"`
}

type RedisConfig struct {
	RT    RedisRTConfig       `yaml:"rt"`
	Cache RedisInstanceConfig `yaml:"cache"`
}

// RedisRTConfig 描述实时路径 Redis 实例的分片列表。
//
// Shards 至少 1 个；多 shard 时由应用层按聚合根（auctionID/sessionID/roomID）
// 的 fnv32 哈希路由到具体 shard，保证同一聚合根的所有 key 落到同一 shard 以
// 满足 Lua EVAL 与 multi-key 命令的同 slot 约束。全局 key（如 ws:instances）
// 固定到 shard 0。
type RedisRTConfig struct {
	Shards []RedisInstanceConfig `yaml:"shards"`
}

// RedisInstanceConfig 是单个 Redis 实例的连接参数。
type RedisInstanceConfig struct {
	Addr     string `yaml:"addr"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	PoolSize int    `yaml:"poolSize"`
}

type IdempotencyConfig struct {
	TTL Duration `yaml:"ttl"`
}

type JWTConfig struct {
	Issuer         string   `yaml:"issuer"`
	Secret         string   `yaml:"secret"`
	AccessTokenTTL Duration `yaml:"accessTokenTTL"`
}

type IDGenConfig struct {
	WorkerID int `yaml:"workerID"`
}

type AuctionConfig struct {
	MinIncrementCent  int64    `yaml:"minIncrementCent"`
	AntiSnipeMs       int64    `yaml:"antiSnipeMs"`
	ExtendMs          int64    `yaml:"extendMs"`
	MaxExtendCount    int      `yaml:"maxExtendCount"`
	FreqLimitCount    int      `yaml:"freqLimitCount"`
	FreqWindowMs      int64    `yaml:"freqWindowMs"`
	BidIdempotencyTTL Duration `yaml:"bidIdempotencyTTL"`
}

type RiskControlConfig struct {
	Enabled bool `yaml:"enabled"`
}

type WebSocketConfig struct {
	ReadLimitBytes int      `yaml:"readLimitBytes"`
	SendBufferSize int      `yaml:"sendBufferSize"`
	PingInterval   Duration `yaml:"pingInterval"`
	PongTimeout    Duration `yaml:"pongTimeout"`
}

type ObjectStorageConfig struct {
	Enabled      bool   `yaml:"enabled"`
	Endpoint     string `yaml:"endpoint"`
	Region       string `yaml:"region"`
	Bucket       string `yaml:"bucket"`
	BucketURL    string `yaml:"bucketURL"`
	AccessKey    string `yaml:"accessKey"`
	SecretKey    string `yaml:"secretKey"`
	ObjectPrefix string `yaml:"objectPrefix"`
}

type AgentConfig struct {
	ProductDescriptionURL      string   `yaml:"productDescriptionURL"`
	ProductAuditEnabled        bool     `yaml:"productAuditEnabled"`
	ProductAuditURL            string   `yaml:"productAuditURL"`
	ProductAuditCallbackURL    string   `yaml:"productAuditCallbackURL"`
	LiveAnalysisURL            string   `yaml:"liveAnalysisURL"`
	LiveAnalysisCallbackURL    string   `yaml:"liveAnalysisCallbackURL"`
	LiveAnalysisCallbackAPIKey string   `yaml:"liveAnalysisCallbackAPIKey"`
	LiveAuctionHookURL         string   `yaml:"liveAuctionHookURL"`
	Timeout                    Duration `yaml:"timeout"`
}

type DoubaoTTSConfig struct {
	AppID    string `yaml:"appID"`
	AckToken string `yaml:"ackToken"`
	Voice    string `yaml:"voice"`
}

type KafkaConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Brokers            []string `yaml:"brokers"`
	ClientID           string   `yaml:"clientID"`
	BidEventsTopic     string   `yaml:"bidEventsTopic"`
	AuctionEventsTopic string   `yaml:"auctionEventsTopic"`
	OrderEventsTopic   string   `yaml:"orderEventsTopic"`
	BidBridgeGroup     string   `yaml:"bidBridgeGroup"`
	BidRecordGroup     string   `yaml:"bidRecordGroup"`
}

type MCPConfig struct {
	Read    MCPAuthConfig `yaml:"read"`
	Control MCPAuthConfig `yaml:"control"`
}

type MCPAuthConfig struct {
	APIKey    string `yaml:"apiKey"`
	ActorID   string `yaml:"actorID"`
	ActorRole string `yaml:"actorRole"`
}

type ObservabilityConfig struct {
	LogLevel           string               `yaml:"logLevel"`
	MetricsPath        string               `yaml:"metricsPath"`
	Format             string               `yaml:"format"`
	SlowSQLThresholdMs int                  `yaml:"slowSQLThresholdMs"`
	Metrics            ObservabilityMetrics `yaml:"metrics"`
	Tracing            ObservabilityTracing `yaml:"tracing"`
	Health             ObservabilityHealth  `yaml:"health"`
}

// ObservabilityMetrics 控制 Prometheus /metrics 端点。
//
// Path 兼容历史字段 `observability.metricsPath`：当 Metrics.Path 为空时回退到
// 它，使既有部署不需要立刻迁移配置。
type ObservabilityMetrics struct {
	Enabled   bool   `yaml:"enabled"`
	Path      string `yaml:"path"`
	Namespace string `yaml:"namespace"`
	AuthToken string `yaml:"authToken"`
}

// ObservabilityTracing 控制 OpenTelemetry trace 链路。
//
// 默认 Enabled=false：无需 collector 即可启动；线上启用时需配置 Endpoint
// 与 Sampler。
type ObservabilityTracing struct {
	Enabled     bool    `yaml:"enabled"`
	Exporter    string  `yaml:"exporter"`
	Endpoint    string  `yaml:"endpoint"`
	Insecure    bool    `yaml:"insecure"`
	ServiceName string  `yaml:"serviceName"`
	Sampler     string  `yaml:"sampler"`
	SampleRatio float64 `yaml:"sampleRatio"`
}

// ObservabilityHealth 控制健康检查端点。
type ObservabilityHealth struct {
	LivenessPath  string `yaml:"livenessPath"`
	ReadinessPath string `yaml:"readinessPath"`
}

func Default() Config {
	return Config{
		Server: ServerConfig{
			Addr:            ":8080",
			ReadTimeout:     Duration(5 * time.Second),
			WriteTimeout:    Duration(10 * time.Second),
			ShutdownTimeout: Duration(20 * time.Second),
		},
		MySQL: MySQLConfig{
			DSN:             "auction:auction@tcp(mysql:3306)/auction?charset=utf8mb4&parseTime=true&loc=Local",
			MaxOpenConns:    100,
			MaxIdleConns:    20,
			ConnMaxLifetime: Duration(time.Hour),
		},
		Redis: RedisConfig{
			RT: RedisRTConfig{
				Shards: []RedisInstanceConfig{
					{
						Addr:     "127.0.0.1:6381",
						Username: "default",
						Password: "",
						DB:       0,
						PoolSize: 100,
					},
					{
						Addr:     "127.0.0.1:6382",
						Username: "default",
						Password: "",
						DB:       0,
						PoolSize: 100,
					},
				},
			},
			Cache: RedisInstanceConfig{
				Addr:     "127.0.0.1:6380",
				Username: "default",
				Password: "",
				DB:       0,
				PoolSize: 100,
			},
		},
		Idempotency: IdempotencyConfig{
			TTL: Duration(24 * time.Hour),
		},
		JWT: JWTConfig{
			Issuer:         "realtime-auction-master",
			Secret:         "change-me-in-prod",
			AccessTokenTTL: Duration(12 * time.Hour),
		},
		IDGen: IDGenConfig{
			WorkerID: 1,
		},
		Auction: AuctionConfig{
			MinIncrementCent:  100,
			AntiSnipeMs:       30000,
			ExtendMs:          30000,
			MaxExtendCount:    20,
			FreqLimitCount:    10,
			FreqWindowMs:      1000,
			BidIdempotencyTTL: Duration(30 * time.Second),
		},
		RiskControl: RiskControlConfig{
			Enabled: true,
		},
		WebSocket: WebSocketConfig{
			ReadLimitBytes: 65536,
			SendBufferSize: 256,
			PingInterval:   Duration(20 * time.Second),
			PongTimeout:    Duration(60 * time.Second),
		},
		ObjectStorage: ObjectStorageConfig{
			Enabled:      false,
			Endpoint:     "https://tos-cn-boe.volces.com",
			Region:       "cn-guilin-boe",
			Bucket:       "aieas",
			BucketURL:    "https://aieas.tos-cn-boe.volces.com",
			ObjectPrefix: "",
		},
		Agent: AgentConfig{
			ProductDescriptionURL:      "http://127.0.0.1:8000/api/v1/product-description",
			ProductAuditEnabled:        true,
			ProductAuditURL:            "http://127.0.0.1:8000/api/v1/product-audit",
			ProductAuditCallbackURL:    "http://127.0.0.1:8080/api/v1/auctions/audit/callback",
			LiveAnalysisURL:            "http://127.0.0.1:8000/api/v1/live-analysis/async",
			LiveAnalysisCallbackURL:    "http://127.0.0.1:8080/api/v1/live-analysis/callback",
			LiveAnalysisCallbackAPIKey: "change-me-in-local-dev-live-analysis-callback",
			LiveAuctionHookURL:         "http://127.0.0.1:8000/api/v1/live-auction-hook",
			Timeout:                    Duration(30 * time.Second),
		},
		DoubaoTTS: DoubaoTTSConfig{
			Voice: "zh_female_vv_jupiter_bigtts",
		},
		Kafka: KafkaConfig{
			Enabled:            false,
			Brokers:            []string{"127.0.0.1:9092"},
			ClientID:           "aieas-backend",
			BidEventsTopic:     "aieas.bid.events",
			AuctionEventsTopic: "aieas.auction.events",
			OrderEventsTopic:   "aieas.order.events",
			BidBridgeGroup:     "aieas-bid-kafka-bridge",
			BidRecordGroup:     "aieas-bid-record-writers",
		},
		MCP: MCPConfig{
			Read: MCPAuthConfig{
				APIKey:    "change-me-in-local-dev-mcp-read",
				ActorID:   "u_9001",
				ActorRole: "admin",
			},
			Control: MCPAuthConfig{
				APIKey:    "change-me-in-local-dev-mcp-control",
				ActorID:   "u_9001",
				ActorRole: "admin",
			},
		},
		Observability: ObservabilityConfig{
			LogLevel:           "info",
			MetricsPath:        "/metrics",
			Format:             "text",
			SlowSQLThresholdMs: 200,
			Metrics: ObservabilityMetrics{
				Enabled:   true,
				Path:      "/metrics",
				Namespace: "aieas",
				AuthToken: "",
			},
			Tracing: ObservabilityTracing{
				Enabled:     false,
				Exporter:    "otlphttp",
				Endpoint:    "",
				Insecure:    true,
				ServiceName: "aieas-backend",
				Sampler:     "parent_based_traceid_ratio",
				SampleRatio: 0.1,
			},
			Health: ObservabilityHealth{
				LivenessPath:  "/healthz",
				ReadinessPath: "/readyz",
			},
		},
	}
}

func Load(path string) (Config, error) {
	if path == "" {
		path = DefaultPath
	}

	cfg := Default()
	resolved, err := resolvePath(path)
	if err != nil {
		return Config{}, err
	}

	data, err := os.ReadFile(resolved)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", resolved, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", resolved, err)
	}
	if err := loadDotEnv(filepath.Dir(resolved)); err != nil {
		return Config{}, err
	}
	if err := cfg.applyEnv(); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func MustLoad(path string) Config {
	cfg, err := Load(path)
	if err != nil {
		panic(err)
	}
	return cfg
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Server.Addr) == "" {
		return fmt.Errorf("server.addr is required")
	}
	if strings.TrimSpace(c.JWT.Secret) == "" {
		return fmt.Errorf("jwt.secret is required")
	}
	if c.JWT.AccessTokenTTL.Std() <= 0 {
		return fmt.Errorf("jwt.accessTokenTTL must be positive")
	}
	if len(c.Redis.RT.Shards) == 0 {
		return fmt.Errorf("redis.rt.shards must contain at least one entry")
	}
	seenShardAddrs := make(map[string]struct{}, len(c.Redis.RT.Shards))
	for i, shard := range c.Redis.RT.Shards {
		if strings.TrimSpace(shard.Addr) == "" {
			return fmt.Errorf("redis.rt.shards[%d].addr is required", i)
		}
		if shard.DB < 0 {
			return fmt.Errorf("redis.rt.shards[%d].db must be non-negative", i)
		}
		if shard.PoolSize < 0 {
			return fmt.Errorf("redis.rt.shards[%d].poolSize must be non-negative", i)
		}
		// 同一进程内的 shard 必须互不相同（addr+db 组合）；同 addr 不同 DB 在
		// 极少数私有部署里可作为分片，仍需互不冲突。
		key := shard.Addr + "/" + strconv.Itoa(shard.DB)
		if _, dup := seenShardAddrs[key]; dup {
			return fmt.Errorf("redis.rt.shards[%d] duplicates a previous shard (addr=%s db=%d)", i, shard.Addr, shard.DB)
		}
		seenShardAddrs[key] = struct{}{}
	}
	if strings.TrimSpace(c.Redis.Cache.Addr) == "" {
		return fmt.Errorf("redis.cache.addr is required")
	}
	if c.Redis.Cache.DB < 0 {
		return fmt.Errorf("redis.cache.db must be non-negative")
	}
	if c.Redis.Cache.PoolSize < 0 {
		return fmt.Errorf("redis.cache.poolSize must be non-negative")
	}
	// 硬约束：RT shard 与 Cache 必须分别部署，避免 RT 被慢缓存阻塞。
	cacheKey := c.Redis.Cache.Addr + "/" + strconv.Itoa(c.Redis.Cache.DB)
	for i, shard := range c.Redis.RT.Shards {
		if shard.Addr+"/"+strconv.Itoa(shard.DB) == cacheKey {
			return fmt.Errorf("redis.rt.shards[%d] must differ from redis.cache (addr+db)", i)
		}
	}
	if c.Idempotency.TTL.Std() <= 0 {
		return fmt.Errorf("idempotency.ttl must be positive")
	}
	if c.Auction.BidIdempotencyTTL.Std() <= 0 {
		return fmt.Errorf("auction.bidIdempotencyTTL must be positive")
	}
	if c.IDGen.WorkerID < 0 || c.IDGen.WorkerID > 255 {
		return fmt.Errorf("idgen.workerID must be between 0 and 255")
	}
	switch strings.ToLower(strings.TrimSpace(c.Observability.Format)) {
	case "", "text", "json":
		// allow empty (defaults applied via Default()) plus the two supported values
	default:
		return fmt.Errorf("observability.format must be one of \"text\" or \"json\"")
	}
	if c.Observability.SlowSQLThresholdMs < 0 {
		return fmt.Errorf("observability.slowSQLThresholdMs must be non-negative")
	}
	if err := c.Observability.validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Agent.ProductDescriptionURL) == "" {
		return fmt.Errorf("agent.productDescriptionURL is required")
	}
	if err := validateHTTPURL(c.Agent.ProductDescriptionURL, "agent.productDescriptionURL"); err != nil {
		return err
	}
	if c.Agent.ProductAuditEnabled {
		if strings.TrimSpace(c.Agent.ProductAuditURL) == "" {
			return fmt.Errorf("agent.productAuditURL is required")
		}
		if err := validateHTTPURL(c.Agent.ProductAuditURL, "agent.productAuditURL"); err != nil {
			return err
		}
		if strings.TrimSpace(c.Agent.ProductAuditCallbackURL) == "" {
			return fmt.Errorf("agent.productAuditCallbackURL is required")
		}
		if err := validateHTTPURL(c.Agent.ProductAuditCallbackURL, "agent.productAuditCallbackURL"); err != nil {
			return err
		}
	}
	if strings.TrimSpace(c.Agent.LiveAnalysisURL) == "" {
		return fmt.Errorf("agent.liveAnalysisURL is required")
	}
	if err := validateHTTPURL(c.Agent.LiveAnalysisURL, "agent.liveAnalysisURL"); err != nil {
		return err
	}
	if strings.TrimSpace(c.Agent.LiveAnalysisCallbackURL) == "" {
		return fmt.Errorf("agent.liveAnalysisCallbackURL is required")
	}
	if err := validateHTTPURL(c.Agent.LiveAnalysisCallbackURL, "agent.liveAnalysisCallbackURL"); err != nil {
		return err
	}
	if strings.TrimSpace(c.Agent.LiveAnalysisCallbackAPIKey) == "" {
		return fmt.Errorf("agent.liveAnalysisCallbackAPIKey is required")
	}
	if strings.TrimSpace(c.Agent.LiveAuctionHookURL) != "" {
		if err := validateHTTPURL(c.Agent.LiveAuctionHookURL, "agent.liveAuctionHookURL"); err != nil {
			return err
		}
	}
	if c.Agent.Timeout.Std() <= 0 {
		return fmt.Errorf("agent.timeout must be positive")
	}
	if err := c.DoubaoTTS.validate(); err != nil {
		return err
	}
	c.Kafka.normalize()
	if err := c.Kafka.validate(); err != nil {
		return err
	}
	if err := validateMCPAuthConfig("mcp.read", c.MCP.Read); err != nil {
		return err
	}
	if err := validateMCPAuthConfig("mcp.control", c.MCP.Control); err != nil {
		return err
	}
	if c.ObjectStorage.Enabled {
		if strings.TrimSpace(c.ObjectStorage.Endpoint) == "" {
			return fmt.Errorf("objectStorage.endpoint is required when object storage is enabled")
		}
		if strings.TrimSpace(c.ObjectStorage.Region) == "" {
			return fmt.Errorf("objectStorage.region is required when object storage is enabled")
		}
		if strings.TrimSpace(c.ObjectStorage.Bucket) == "" {
			return fmt.Errorf("objectStorage.bucket is required when object storage is enabled")
		}
		if strings.TrimSpace(c.ObjectStorage.BucketURL) == "" {
			return fmt.Errorf("objectStorage.bucketURL is required when object storage is enabled")
		}
		if strings.TrimSpace(c.ObjectStorage.AccessKey) == "" {
			return fmt.Errorf("objectStorage.accessKey is required when object storage is enabled")
		}
		if strings.TrimSpace(c.ObjectStorage.SecretKey) == "" {
			return fmt.Errorf("objectStorage.secretKey is required when object storage is enabled")
		}
		for i, shard := range c.Redis.RT.Shards {
			if err := validateBucketURL(c.ObjectStorage.BucketURL, shard.Addr); err != nil {
				return fmt.Errorf("redis.rt.shards[%d]: %w", i, err)
			}
		}
		if err := validateBucketURL(c.ObjectStorage.BucketURL, c.Redis.Cache.Addr); err != nil {
			return err
		}
	}
	return nil
}

func validateMCPAuthConfig(prefix string, auth MCPAuthConfig) error {
	if strings.TrimSpace(auth.APIKey) == "" {
		return fmt.Errorf("%s.apiKey is required", prefix)
	}
	if strings.TrimSpace(auth.ActorID) == "" {
		return fmt.Errorf("%s.actorID is required", prefix)
	}
	switch strings.ToLower(strings.TrimSpace(auth.ActorRole)) {
	case "buyer", "merchant", "admin":
	default:
		return fmt.Errorf("%s.actorRole must be one of buyer, merchant, admin", prefix)
	}
	return nil
}

func (c DoubaoTTSConfig) validate() error {
	appID := strings.TrimSpace(c.AppID)
	ackToken := strings.TrimSpace(c.AckToken)
	if appID == "" && ackToken == "" {
		return nil
	}
	if appID == "" {
		return fmt.Errorf("doubaoTTS.appID is required when doubao TTS is enabled")
	}
	if ackToken == "" {
		return fmt.Errorf("doubaoTTS.ackToken is required when doubao TTS is enabled")
	}
	if strings.TrimSpace(c.Voice) == "" {
		return fmt.Errorf("doubaoTTS.voice is required when doubao TTS is enabled")
	}
	return nil
}

func (k *KafkaConfig) normalize() {
	if k == nil {
		return
	}
	if len(k.Brokers) == 0 {
		k.Brokers = []string{"127.0.0.1:9092"}
	}
	for i := range k.Brokers {
		k.Brokers[i] = strings.TrimSpace(k.Brokers[i])
	}
	if strings.TrimSpace(k.ClientID) == "" {
		k.ClientID = "aieas-backend"
	}
	if strings.TrimSpace(k.BidEventsTopic) == "" {
		k.BidEventsTopic = "aieas.bid.events"
	}
	if strings.TrimSpace(k.AuctionEventsTopic) == "" {
		k.AuctionEventsTopic = "aieas.auction.events"
	}
	if strings.TrimSpace(k.OrderEventsTopic) == "" {
		k.OrderEventsTopic = "aieas.order.events"
	}
	if strings.TrimSpace(k.BidBridgeGroup) == "" {
		k.BidBridgeGroup = "aieas-bid-kafka-bridge"
	}
	if strings.TrimSpace(k.BidRecordGroup) == "" {
		k.BidRecordGroup = "aieas-bid-record-writers"
	}
}

func (k KafkaConfig) validate() error {
	k.normalize()
	if !k.Enabled {
		return nil
	}
	if len(k.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must contain at least one broker when kafka enabled")
	}
	for i, broker := range k.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("kafka.brokers[%d] is required when kafka enabled", i)
		}
	}
	if strings.TrimSpace(k.BidEventsTopic) == "" {
		return fmt.Errorf("kafka.bidEventsTopic is required when kafka enabled")
	}
	if strings.TrimSpace(k.AuctionEventsTopic) == "" {
		return fmt.Errorf("kafka.auctionEventsTopic is required when kafka enabled")
	}
	if strings.TrimSpace(k.OrderEventsTopic) == "" {
		return fmt.Errorf("kafka.orderEventsTopic is required when kafka enabled")
	}
	if strings.TrimSpace(k.BidBridgeGroup) == "" {
		return fmt.Errorf("kafka.bidBridgeGroup is required when kafka enabled")
	}
	if strings.TrimSpace(k.BidRecordGroup) == "" {
		return fmt.Errorf("kafka.bidRecordGroup is required when kafka enabled")
	}
	return nil
}

func (c *Config) applyEnv() error {
	setString("SERVER_ADDR", &c.Server.Addr)
	if err := setDuration("SERVER_READ_TIMEOUT", &c.Server.ReadTimeout); err != nil {
		return err
	}
	if err := setDuration("SERVER_WRITE_TIMEOUT", &c.Server.WriteTimeout); err != nil {
		return err
	}
	if err := setDuration("SERVER_SHUTDOWN_TIMEOUT", &c.Server.ShutdownTimeout); err != nil {
		return err
	}

	setString("MYSQL_DSN", &c.MySQL.DSN)
	if err := setInt("MYSQL_MAX_OPEN_CONNS", &c.MySQL.MaxOpenConns); err != nil {
		return err
	}
	if err := setInt("MYSQL_MAX_IDLE_CONNS", &c.MySQL.MaxIdleConns); err != nil {
		return err
	}
	if err := setDuration("MYSQL_CONN_MAX_LIFETIME", &c.MySQL.ConnMaxLifetime); err != nil {
		return err
	}

	// RT 分片：支持 REDIS_RT_SHARD_<N>_* 环境变量与 yaml 中的 shards 列表合作。
	// - 如果 yaml 已定义 shards，env 仅按 index 覆盖对应字段；
	// - 如果 yaml 没有定义且 env 出现 REDIS_RT_SHARD_<N>_ADDR，则按需扩容。
	// 兼容历史 REDIS_RT_PRIMARY_*：若给出且 shards 为空/index 0 字段未填，
	// 则映射到 shards[0]。
	if err := c.applyRTShardEnv(); err != nil {
		return err
	}
	setString("REDIS_CACHE_ADDR", &c.Redis.Cache.Addr)
	setString("REDIS_CACHE_USERNAME", &c.Redis.Cache.Username)
	setString("REDIS_CACHE_PASSWORD", &c.Redis.Cache.Password)
	if err := setInt("REDIS_CACHE_DB", &c.Redis.Cache.DB); err != nil {
		return err
	}
	if err := setInt("REDIS_CACHE_POOL_SIZE", &c.Redis.Cache.PoolSize); err != nil {
		return err
	}
	if err := setDuration("IDEMPOTENCY_TTL", &c.Idempotency.TTL); err != nil {
		return err
	}

	setString("JWT_ISSUER", &c.JWT.Issuer)
	setString("JWT_SECRET", &c.JWT.Secret)
	if err := setDuration("JWT_ACCESS_TOKEN_TTL", &c.JWT.AccessTokenTTL); err != nil {
		return err
	}

	if err := setInt("IDGEN_WORKER_ID", &c.IDGen.WorkerID); err != nil {
		return err
	}

	if err := setInt64("AUCTION_MIN_INCREMENT_CENT", &c.Auction.MinIncrementCent); err != nil {
		return err
	}
	if err := setInt64("AUCTION_ANTI_SNIPE_MS", &c.Auction.AntiSnipeMs); err != nil {
		return err
	}
	if err := setInt64("AUCTION_EXTEND_MS", &c.Auction.ExtendMs); err != nil {
		return err
	}
	if err := setInt("AUCTION_MAX_EXTEND_COUNT", &c.Auction.MaxExtendCount); err != nil {
		return err
	}
	if err := setInt("AUCTION_FREQ_LIMIT_COUNT", &c.Auction.FreqLimitCount); err != nil {
		return err
	}
	if err := setInt64("AUCTION_FREQ_WINDOW_MS", &c.Auction.FreqWindowMs); err != nil {
		return err
	}
	if err := setDuration("AUCTION_BID_IDEMPOTENCY_TTL", &c.Auction.BidIdempotencyTTL); err != nil {
		return err
	}
	if err := setBool("RISK_CONTROL_ENABLED", &c.RiskControl.Enabled); err != nil {
		return err
	}

	if err := setInt("WEBSOCKET_READ_LIMIT_BYTES", &c.WebSocket.ReadLimitBytes); err != nil {
		return err
	}
	if err := setInt("WEBSOCKET_SEND_BUFFER_SIZE", &c.WebSocket.SendBufferSize); err != nil {
		return err
	}
	if err := setDuration("WEBSOCKET_PING_INTERVAL", &c.WebSocket.PingInterval); err != nil {
		return err
	}
	if err := setDuration("WEBSOCKET_PONG_TIMEOUT", &c.WebSocket.PongTimeout); err != nil {
		return err
	}

	if err := setBool("OBJECT_STORAGE_ENABLED", &c.ObjectStorage.Enabled); err != nil {
		return err
	}
	setString("OBJECT_STORAGE_ENDPOINT", &c.ObjectStorage.Endpoint)
	setString("OBJECT_STORAGE_REGION", &c.ObjectStorage.Region)
	setString("OBJECT_STORAGE_BUCKET", &c.ObjectStorage.Bucket)
	setString("OBJECT_STORAGE_BUCKET_URL", &c.ObjectStorage.BucketURL)
	setString("OBJECT_STORAGE_ACCESS_KEY", &c.ObjectStorage.AccessKey)
	setString("OBJECT_STORAGE_SECRET_KEY", &c.ObjectStorage.SecretKey)
	setString("OBJECT_STORAGE_OBJECT_PREFIX", &c.ObjectStorage.ObjectPrefix)

	setString("AGENT_PRODUCT_DESCRIPTION_URL", &c.Agent.ProductDescriptionURL)
	if err := setBool("AGENT_PRODUCT_AUDIT_ENABLED", &c.Agent.ProductAuditEnabled); err != nil {
		return err
	}
	setString("AGENT_PRODUCT_AUDIT_URL", &c.Agent.ProductAuditURL)
	setString("AGENT_PRODUCT_AUDIT_CALLBACK_URL", &c.Agent.ProductAuditCallbackURL)
	setString("AGENT_LIVE_ANALYSIS_URL", &c.Agent.LiveAnalysisURL)
	setString("AGENT_LIVE_ANALYSIS_CALLBACK_URL", &c.Agent.LiveAnalysisCallbackURL)
	setString("AGENT_LIVE_ANALYSIS_CALLBACK_API_KEY", &c.Agent.LiveAnalysisCallbackAPIKey)
	setString("AGENT_LIVE_AUCTION_HOOK_URL", &c.Agent.LiveAuctionHookURL)
	if err := setDuration("AGENT_TIMEOUT", &c.Agent.Timeout); err != nil {
		return err
	}

	setString("DOUBAO_TTS_APP_ID", &c.DoubaoTTS.AppID)
	setString("DOUBAO_TTS_ACK_TOKEN", &c.DoubaoTTS.AckToken)
	setString("DOUBAO_TTS_VOICE", &c.DoubaoTTS.Voice)

	if err := setBool("KAFKA_ENABLED", &c.Kafka.Enabled); err != nil {
		return err
	}
	setStringSlice("KAFKA_BROKERS", &c.Kafka.Brokers)
	setString("KAFKA_CLIENT_ID", &c.Kafka.ClientID)
	setString("KAFKA_BID_EVENTS_TOPIC", &c.Kafka.BidEventsTopic)
	setString("KAFKA_AUCTION_EVENTS_TOPIC", &c.Kafka.AuctionEventsTopic)
	setString("KAFKA_ORDER_EVENTS_TOPIC", &c.Kafka.OrderEventsTopic)
	setString("KAFKA_BID_BRIDGE_GROUP", &c.Kafka.BidBridgeGroup)
	setString("KAFKA_BID_RECORD_GROUP", &c.Kafka.BidRecordGroup)

	setString("MCP_READ_API_KEY", &c.MCP.Read.APIKey)
	setString("MCP_READ_ACTOR_ID", &c.MCP.Read.ActorID)
	setString("MCP_READ_ACTOR_ROLE", &c.MCP.Read.ActorRole)
	setString("MCP_CONTROL_API_KEY", &c.MCP.Control.APIKey)
	setString("MCP_CONTROL_ACTOR_ID", &c.MCP.Control.ActorID)
	setString("MCP_CONTROL_ACTOR_ROLE", &c.MCP.Control.ActorRole)

	setString("OBSERVABILITY_LOG_LEVEL", &c.Observability.LogLevel)
	setString("OBSERVABILITY_METRICS_PATH", &c.Observability.MetricsPath)
	setString("OBSERVABILITY_FORMAT", &c.Observability.Format)
	if err := setInt("OBSERVABILITY_SLOW_SQL_THRESHOLD_MS", &c.Observability.SlowSQLThresholdMs); err != nil {
		return err
	}
	if err := setBool("OBSERVABILITY_METRICS_ENABLED", &c.Observability.Metrics.Enabled); err != nil {
		return err
	}
	setString("OBSERVABILITY_METRICS_PATH_OVERRIDE", &c.Observability.Metrics.Path)
	setString("OBSERVABILITY_METRICS_NAMESPACE", &c.Observability.Metrics.Namespace)
	setString("OBSERVABILITY_METRICS_AUTH_TOKEN", &c.Observability.Metrics.AuthToken)
	if err := setBool("OBSERVABILITY_TRACING_ENABLED", &c.Observability.Tracing.Enabled); err != nil {
		return err
	}
	setString("OBSERVABILITY_TRACING_EXPORTER", &c.Observability.Tracing.Exporter)
	setString("OBSERVABILITY_TRACING_ENDPOINT", &c.Observability.Tracing.Endpoint)
	if err := setBool("OBSERVABILITY_TRACING_INSECURE", &c.Observability.Tracing.Insecure); err != nil {
		return err
	}
	setString("OBSERVABILITY_TRACING_SERVICE_NAME", &c.Observability.Tracing.ServiceName)
	setString("OBSERVABILITY_TRACING_SAMPLER", &c.Observability.Tracing.Sampler)
	if err := setFloat64("OBSERVABILITY_TRACING_SAMPLE_RATIO", &c.Observability.Tracing.SampleRatio); err != nil {
		return err
	}
	setString("OBSERVABILITY_HEALTH_LIVENESS_PATH", &c.Observability.Health.LivenessPath)
	setString("OBSERVABILITY_HEALTH_READINESS_PATH", &c.Observability.Health.ReadinessPath)
	c.Kafka.normalize()
	c.Observability.normalize()
	return nil
}

func resolvePath(path string) (string, error) {
	if filepath.IsAbs(path) {
		if _, err := os.Stat(path); err != nil {
			return "", fmt.Errorf("stat config %q: %w", path, err)
		}
		return path, nil
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		candidate := filepath.Join(wd, path)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			break
		}
		wd = parent
	}
	return "", fmt.Errorf("config %q not found from current directory or parents", path)
}

func setString(name string, target *string) {
	if value, ok := os.LookupEnv(name); ok {
		*target = value
	}
}

func setStringSlice(name string, target *[]string) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	*target = out
}

func loadDotEnv(startDir string) error {
	path := findUp(startDir, ".env")
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open .env %q: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("parse .env %q line %d: missing '='", path, lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("parse .env %q line %d: empty key", path, lineNo)
		}
		value = strings.TrimSpace(value)
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("set .env %q line %d: %w", path, lineNo, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read .env %q: %w", path, err)
	}
	return nil
}

func findUp(startDir, name string) string {
	dir := startDir
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return ""
		}
	}
	for {
		candidate := filepath.Join(dir, name)
		if stat, err := os.Stat(candidate); err == nil && !stat.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func validateBucketURL(bucketURL, redisAddr string) error {
	parsed, err := parseHTTPURL(bucketURL, "objectStorage.bucketURL")
	if err != nil {
		return err
	}
	redisHostname := redisHost(redisAddr)
	if redisHostname != "" && strings.EqualFold(parsed.Hostname(), redisHostname) {
		return fmt.Errorf("objectStorage.bucketURL must not point to redis addr")
	}
	return nil
}

func validateHTTPURL(rawURL, field string) error {
	_, err := parseHTTPURL(rawURL, field)
	return err
}

func parseHTTPURL(rawURL, field string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return nil, fmt.Errorf("%s must be a valid http or https URL", field)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%s must use http or https", field)
	}
	return parsed, nil
}

func redisHost(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return strings.Trim(host, "[]")
	}
	if strings.Count(addr, ":") == 1 {
		host, _, _ := strings.Cut(addr, ":")
		return strings.TrimSpace(host)
	}
	return strings.Trim(addr, "[]")
}

func setDuration(name string, target *Duration) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse env %s=%q as duration: %w", name, value, err)
	}
	*target = Duration(parsed)
	return nil
}

func setInt(name string, target *int) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("parse env %s=%q as int: %w", name, value, err)
	}
	*target = parsed
	return nil
}

func setInt64(name string, target *int64) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parse env %s=%q as int64: %w", name, value, err)
	}
	*target = parsed
	return nil
}

func setBool(name string, target *bool) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("parse env %s=%q as bool: %w", name, value, err)
	}
	*target = parsed
	return nil
}

func setFloat64(name string, target *float64) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("parse env %s=%q as float64: %w", name, value, err)
	}
	*target = parsed
	return nil
}

// applyRTShardEnv 处理 REDIS_RT_SHARD_<N>_* 环境变量。
//
// 规则：
//   - 当 REDIS_RT_SHARD_<N>_ADDR 显式存在（包含空字符串）时认为对该 index 做覆盖；
//     必要时把 c.Redis.RT.Shards 扩容到 N+1。
//   - 其余字段（USERNAME/PASSWORD/DB/POOL_SIZE）只有在 shards[N] 已存在时才生效。
//   - 如果 shards 当前为空且未显式给出任何 SHARD_<N>_ADDR，则保持 yaml 解析后的列表。
//   - 兼容旧名 REDIS_RT_PRIMARY_*：等价于 SHARD_0_*；显式 SHARD_0_* 优先。
//
// 上游 Validate 会再做最终校验（>=1 shard、地址非空、互不相同、与 cache 不重）。
func (c *Config) applyRTShardEnv() error {
	// 兼容历史 REDIS_RT_PRIMARY_* → SHARD_0_*。
	mapLegacy := func(legacy, modern string) {
		if _, ok := os.LookupEnv(modern); ok {
			return
		}
		if v, ok := os.LookupEnv(legacy); ok {
			_ = os.Setenv(modern, v)
		}
	}
	mapLegacy("REDIS_RT_PRIMARY_ADDR", "REDIS_RT_SHARD_0_ADDR")
	mapLegacy("REDIS_RT_PRIMARY_USERNAME", "REDIS_RT_SHARD_0_USERNAME")
	mapLegacy("REDIS_RT_PRIMARY_PASSWORD", "REDIS_RT_SHARD_0_PASSWORD")
	mapLegacy("REDIS_RT_PRIMARY_DB", "REDIS_RT_SHARD_0_DB")
	mapLegacy("REDIS_RT_PRIMARY_POOL_SIZE", "REDIS_RT_SHARD_0_POOL_SIZE")

	const prefix = "REDIS_RT_SHARD_"
	maxIndex := -1
	for _, kv := range os.Environ() {
		key, _, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		idxStr, _, ok := strings.Cut(rest, "_")
		if !ok {
			continue
		}
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 {
			continue
		}
		if idx > maxIndex {
			maxIndex = idx
		}
	}
	for i := 0; i <= maxIndex; i++ {
		addrKey := fmt.Sprintf("%s%d_ADDR", prefix, i)
		if _, ok := os.LookupEnv(addrKey); !ok {
			continue
		}
		for len(c.Redis.RT.Shards) <= i {
			c.Redis.RT.Shards = append(c.Redis.RT.Shards, RedisInstanceConfig{})
		}
	}
	for i := range c.Redis.RT.Shards {
		shard := &c.Redis.RT.Shards[i]
		setString(fmt.Sprintf("%s%d_ADDR", prefix, i), &shard.Addr)
		setString(fmt.Sprintf("%s%d_USERNAME", prefix, i), &shard.Username)
		setString(fmt.Sprintf("%s%d_PASSWORD", prefix, i), &shard.Password)
		if err := setInt(fmt.Sprintf("%s%d_DB", prefix, i), &shard.DB); err != nil {
			return err
		}
		if err := setInt(fmt.Sprintf("%s%d_POOL_SIZE", prefix, i), &shard.PoolSize); err != nil {
			return err
		}
	}
	return nil
}

// normalize 在 Validate 之前对 ObservabilityConfig 做兼容性归一化：
//   - Metrics.Path 为空时回退到历史的 MetricsPath；同步反向兜底。
//   - Health 路径与 Tracing 默认值在使用方为空时填回，避免“显式禁用”意外发生。
//
// 该方法只修正字符串字段的空值；不会改写用户显式设置的开关。
func (o *ObservabilityConfig) normalize() {
	if o == nil {
		return
	}
	if strings.TrimSpace(o.Metrics.Path) == "" {
		o.Metrics.Path = strings.TrimSpace(o.MetricsPath)
	}
	if strings.TrimSpace(o.Metrics.Path) == "" {
		o.Metrics.Path = "/metrics"
	}
	if strings.TrimSpace(o.MetricsPath) == "" {
		o.MetricsPath = o.Metrics.Path
	}
	if strings.TrimSpace(o.Metrics.Namespace) == "" {
		o.Metrics.Namespace = "aieas"
	}
	if strings.TrimSpace(o.Tracing.Exporter) == "" {
		o.Tracing.Exporter = "otlphttp"
	}
	if strings.TrimSpace(o.Tracing.ServiceName) == "" {
		o.Tracing.ServiceName = "aieas-backend"
	}
	if strings.TrimSpace(o.Tracing.Sampler) == "" {
		o.Tracing.Sampler = "parent_based_traceid_ratio"
	}
	if o.Tracing.SampleRatio == 0 {
		o.Tracing.SampleRatio = 0.1
	}
	if strings.TrimSpace(o.Health.LivenessPath) == "" {
		o.Health.LivenessPath = "/healthz"
	}
	if strings.TrimSpace(o.Health.ReadinessPath) == "" {
		o.Health.ReadinessPath = "/readyz"
	}
}

// validate 校验观测性子结构的必填项 / 取值范围。
func (o ObservabilityConfig) validate() error {
	if o.Metrics.Enabled {
		path := strings.TrimSpace(o.Metrics.Path)
		if path == "" || !strings.HasPrefix(path, "/") {
			return fmt.Errorf("observability.metrics.path must start with '/'")
		}
		if strings.TrimSpace(o.Metrics.Namespace) == "" {
			return fmt.Errorf("observability.metrics.namespace is required when metrics enabled")
		}
	}
	if o.Tracing.Enabled {
		switch strings.ToLower(strings.TrimSpace(o.Tracing.Exporter)) {
		case "otlphttp", "otlp", "otlpgrpc", "stdout", "noop":
		default:
			return fmt.Errorf("observability.tracing.exporter must be one of otlphttp|otlpgrpc|stdout|noop")
		}
		if strings.TrimSpace(o.Tracing.ServiceName) == "" {
			return fmt.Errorf("observability.tracing.serviceName is required when tracing enabled")
		}
		switch strings.ToLower(strings.TrimSpace(o.Tracing.Sampler)) {
		case "always_on", "always_off", "traceidratio", "parent_based_always_on", "parent_based_traceid_ratio":
		default:
			return fmt.Errorf("observability.tracing.sampler is invalid")
		}
		if o.Tracing.SampleRatio < 0 || o.Tracing.SampleRatio > 1 {
			return fmt.Errorf("observability.tracing.sampleRatio must be in [0,1]")
		}
		switch strings.ToLower(strings.TrimSpace(o.Tracing.Exporter)) {
		case "stdout", "noop":
			// stdout / noop exporter 不需要 endpoint
		default:
			if strings.TrimSpace(o.Tracing.Endpoint) == "" {
				return fmt.Errorf("observability.tracing.endpoint is required when tracing enabled")
			}
		}
	}
	if path := strings.TrimSpace(o.Health.LivenessPath); path != "" && !strings.HasPrefix(path, "/") {
		return fmt.Errorf("observability.health.livenessPath must start with '/'")
	}
	if path := strings.TrimSpace(o.Health.ReadinessPath); path != "" && !strings.HasPrefix(path, "/") {
		return fmt.Errorf("observability.health.readinessPath must start with '/'")
	}
	return nil
}
