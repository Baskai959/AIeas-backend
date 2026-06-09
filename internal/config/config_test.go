package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadParsesYAMLAndAppliesSecretEnvOverridesOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
server:
  addr: ":9090"
  readTimeout: 2s
mysql:
  maxOpenConns: 12
redis:
  rt:
    shards:
      - addr: "127.0.0.1:6381"
        db: 2
      - addr: "127.0.0.1:6382"
        db: 2
  cache:
    addr: "127.0.0.1:6380"
    db: 1
idempotency:
  ttl: 2h
jwt:
  secret: "from-file"
  accessTokenTTL: 30m
objectStorage:
  enabled: true
  endpoint: "https://tos-cn-boe.volces.com"
  region: "cn-guilin-boe"
  bucket: "aieas"
  bucketURL: "https://aieas.tos-cn-boe.volces.com"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SERVER_ADDR", ":7070")
	t.Setenv("JWT_SECRET", "from-env")
	t.Setenv("REDIS_RT_SHARD_0_PASSWORD", "rt-password-from-env")
	t.Setenv("REDIS_RT_SHARD_0_DB", "3")
	t.Setenv("REDIS_CACHE_PASSWORD", "cache-password-from-env")
	t.Setenv("IDEMPOTENCY_TTL", "30m")
	t.Setenv("AUCTION_BID_IDEMPOTENCY_TTL", "45s")
	t.Setenv("WEBSOCKET_HANDSHAKE_RATE_LIMIT_PER_IP", "0")
	t.Setenv("OBJECT_STORAGE_ENABLED", "false")
	t.Setenv("OBJECT_STORAGE_ENDPOINT", "https://ignored.example.com")
	t.Setenv("OBJECT_STORAGE_ACCESS_KEY", "ak")
	t.Setenv("OBJECT_STORAGE_SECRET_KEY", "sk")
	t.Setenv("AGENT_PRODUCT_DESCRIPTION_URL", "http://127.0.0.1:9000/api/v1/product-description")
	t.Setenv("AGENT_PRODUCT_AUDIT_ENABLED", "false")
	t.Setenv("AGENT_PRODUCT_AUDIT_URL", "http://127.0.0.1:9000/api/v1/product-audit")
	t.Setenv("AGENT_PRODUCT_AUDIT_CALLBACK_URL", "http://127.0.0.1:7070/api/v1/auctions/audit/callback")
	t.Setenv("AGENT_LIVE_ANALYSIS_URL", "http://127.0.0.1:9000/api/v1/live-analysis/async")
	t.Setenv("AGENT_LIVE_ANALYSIS_CALLBACK_URL", "http://127.0.0.1:7070/api/v1/live-analysis/callback")
	t.Setenv("AGENT_LIVE_ANALYSIS_CALLBACK_API_KEY", "callback-from-env")
	t.Setenv("AGENT_LIVE_AUCTION_HOOK_URL", "http://127.0.0.1:9000/api/v1/live-auction-hook")
	t.Setenv("AGENT_TIMEOUT", "5s")
	t.Setenv("DOUBAO_TTS_APP_ID", "doubao-appid")
	t.Setenv("DOUBAO_TTS_ACK_TOKEN", "doubao-acktoken")
	t.Setenv("DOUBAO_TTS_VOICE", "ignored_voice")
	t.Setenv("MCP_READ_API_KEY", "mcp-read-from-env")
	t.Setenv("MCP_READ_ACTOR_ID", "u_ignored")
	t.Setenv("MCP_READ_ACTOR_ROLE", "buyer")
	t.Setenv("MCP_CONTROL_API_KEY", "mcp-control-from-env")
	t.Setenv("MCP_CONTROL_ACTOR_ID", "u_ignored")
	t.Setenv("MCP_CONTROL_ACTOR_ROLE", "buyer")
	t.Setenv("KAFKA_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092,127.0.0.1:9093")
	t.Setenv("KAFKA_BID_EVENTS_TOPIC", "test.bid.events")
	t.Setenv("OBSERVABILITY_FORMAT", "json")
	t.Setenv("OBSERVABILITY_METRICS_AUTH_TOKEN", "metrics-token-from-env")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Server.Addr != ":9090" {
		t.Fatalf("expected server addr from yaml, got %q", cfg.Server.Addr)
	}
	if cfg.Server.ReadTimeout.Std() != 2*time.Second {
		t.Fatalf("expected read timeout 2s, got %s", cfg.Server.ReadTimeout.Std())
	}
	if cfg.MySQL.MaxOpenConns != 12 {
		t.Fatalf("expected mysql max open conns 12, got %d", cfg.MySQL.MaxOpenConns)
	}
	if cfg.Redis.RT.Shards[0].Addr != "127.0.0.1:6381" || cfg.Redis.RT.Shards[0].DB != 2 || cfg.Redis.RT.Shards[0].Password != "rt-password-from-env" {
		t.Fatalf("unexpected redis rt shard[0] config: %+v", cfg.Redis.RT.Shards[0])
	}
	if len(cfg.Redis.RT.Shards) < 2 || cfg.Redis.RT.Shards[1].Addr != "127.0.0.1:6382" {
		t.Fatalf("unexpected redis rt shard[1] config: %+v", cfg.Redis.RT.Shards)
	}
	if cfg.Redis.Cache.Addr != "127.0.0.1:6380" || cfg.Redis.Cache.DB != 1 || cfg.Redis.Cache.Password != "cache-password-from-env" {
		t.Fatalf("unexpected redis cache config: %+v", cfg.Redis.Cache)
	}
	if cfg.JWT.Secret != "from-env" || cfg.JWT.AccessTokenTTL.Std() != 30*time.Minute {
		t.Fatalf("unexpected jwt config: %+v", cfg.JWT)
	}
	if cfg.Idempotency.TTL.Std() != 2*time.Hour {
		t.Fatalf("expected idempotency ttl from yaml, got %s", cfg.Idempotency.TTL.Std())
	}
	if cfg.Auction.BidIdempotencyTTL.Std() != 30*time.Second {
		t.Fatalf("expected auction bid idempotency ttl from config/default, got %s", cfg.Auction.BidIdempotencyTTL.Std())
	}
	if !cfg.ObjectStorage.Enabled || cfg.ObjectStorage.Endpoint != "https://tos-cn-boe.volces.com" || cfg.ObjectStorage.AccessKey != "ak" || cfg.ObjectStorage.SecretKey != "sk" {
		t.Fatalf("unexpected object storage config: %+v", cfg.ObjectStorage)
	}
	if cfg.Agent.ProductDescriptionURL != "http://127.0.0.1:8000/api/v1/product-description" ||
		cfg.Agent.ProductDescriptionTimeout.Std() != 2*time.Minute ||
		!cfg.Agent.ProductAuditEnabled ||
		cfg.Agent.LiveAnalysisCallbackAPIKey != "callback-from-env" ||
		cfg.Agent.Timeout.Std() != 45*time.Second {
		t.Fatalf("unexpected agent config: %+v", cfg.Agent)
	}
	if cfg.MCP.Read.APIKey != "mcp-read-from-env" || cfg.MCP.Read.ActorID != "u_9001" || cfg.MCP.Read.ActorRole != "admin" ||
		cfg.MCP.Control.APIKey != "mcp-control-from-env" || cfg.MCP.Control.ActorID != "u_9001" || cfg.MCP.Control.ActorRole != "admin" {
		t.Fatalf("unexpected mcp config: %+v", cfg.MCP)
	}
	if cfg.DoubaoTTS.AppID != "doubao-appid" || cfg.DoubaoTTS.AckToken != "doubao-acktoken" || cfg.DoubaoTTS.Voice != "zh_female_vv_jupiter_bigtts" {
		t.Fatalf("unexpected doubao tts config: %+v", cfg.DoubaoTTS)
	}
	if cfg.Kafka.Enabled || len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.BidEventsTopic != "aieas.bid.events" {
		t.Fatalf("unexpected kafka config: %+v", cfg.Kafka)
	}
	if cfg.WebSocket.HandshakeRateLimitPerIP != 60 {
		t.Fatalf("expected websocket handshake limit from config/default, got %d", cfg.WebSocket.HandshakeRateLimitPerIP)
	}
	if cfg.Observability.Format != "text" || cfg.Observability.Metrics.AuthToken != "metrics-token-from-env" {
		t.Fatalf("unexpected observability config: %+v", cfg.Observability)
	}
}

func TestLoadAppliesDotEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
jwt:
  secret: "from-file"
  accessTokenTTL: 30m
redis:
  rt:
    shards:
      - addr: "127.0.0.1:6381"
        password: "from-file"
      - addr: "127.0.0.1:6382"
        password: "from-file"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("JWT_SECRET=from-dotenv\nREDIS_RT_SHARD_0_PASSWORD=redis-from-dotenv\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	t.Setenv("JWT_SECRET", "")
	t.Setenv("REDIS_RT_SHARD_0_PASSWORD", "")
	os.Unsetenv("JWT_SECRET")
	os.Unsetenv("REDIS_RT_SHARD_0_PASSWORD")
	t.Cleanup(func() {
		os.Unsetenv("JWT_SECRET")
		os.Unsetenv("REDIS_RT_SHARD_0_PASSWORD")
	})

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.JWT.Secret != "from-dotenv" {
		t.Fatalf("expected JWT secret from .env, got %q", cfg.JWT.Secret)
	}
	if cfg.Redis.RT.Shards[0].Password != "redis-from-dotenv" {
		t.Fatalf("expected Redis password from .env, got %q", cfg.Redis.RT.Shards[0].Password)
	}
}

func TestLoadIgnoresEmptySecretEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
jwt:
  secret: "jwt-from-file"
  accessTokenTTL: 30m
redis:
  rt:
    shards:
      - addr: "127.0.0.1:6381"
        password: "rt-from-file"
      - addr: "127.0.0.1:6382"
        password: "rt2-from-file"
  cache:
    addr: "127.0.0.1:6380"
    password: "cache-from-file"
objectStorage:
  enabled: true
  endpoint: "https://tos-cn-boe.volces.com"
  region: "cn-guilin-boe"
  bucket: "aieas"
  bucketURL: "https://aieas.tos-cn-boe.volces.com"
  accessKey: "ak-from-file"
  secretKey: "sk-from-file"
agent:
  liveAnalysisCallbackAPIKey: "callback-from-file"
mcp:
  read:
    apiKey: "read-from-file"
    actorID: "u_9001"
    actorRole: "admin"
  control:
    apiKey: "control-from-file"
    actorID: "u_9001"
    actorRole: "admin"
observability:
  metrics:
    authToken: "metrics-from-file"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	for _, key := range []string{
		"JWT_SECRET",
		"REDIS_RT_SHARD_0_PASSWORD",
		"REDIS_RT_SHARD_1_PASSWORD",
		"REDIS_RT_PRIMARY_PASSWORD",
		"REDIS_CACHE_PASSWORD",
		"OBJECT_STORAGE_ACCESS_KEY",
		"OBJECT_STORAGE_SECRET_KEY",
		"AGENT_LIVE_ANALYSIS_CALLBACK_API_KEY",
		"MCP_READ_API_KEY",
		"MCP_CONTROL_API_KEY",
		"OBSERVABILITY_METRICS_AUTH_TOKEN",
	} {
		t.Setenv(key, "")
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.JWT.Secret != "jwt-from-file" ||
		cfg.Redis.RT.Shards[0].Password != "rt-from-file" ||
		cfg.Redis.RT.Shards[1].Password != "rt2-from-file" ||
		cfg.Redis.Cache.Password != "cache-from-file" ||
		cfg.ObjectStorage.AccessKey != "ak-from-file" ||
		cfg.ObjectStorage.SecretKey != "sk-from-file" ||
		cfg.Agent.LiveAnalysisCallbackAPIKey != "callback-from-file" ||
		cfg.MCP.Read.APIKey != "read-from-file" ||
		cfg.MCP.Control.APIKey != "control-from-file" ||
		cfg.Observability.Metrics.AuthToken != "metrics-from-file" {
		t.Fatalf("empty secret env should not override yaml config: %+v", cfg)
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("jwt:\n  accessTokenTTL: bad\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestValidateRejectsObjectStorageBucketURLPointingToRedis(t *testing.T) {
	cfg := Default()
	cfg.Redis.RT.Shards[0].Addr = "redis-sy01vax76anstnm7a.redis.cn-guilin-boe.volces.com:6379"
	cfg.Redis.RT.Shards[1].Addr = "redis-sy01vax76anstnm7a.redis.cn-guilin-boe.volces.com:6380"
	cfg.Redis.Cache.Addr = "redis-cache.example.com:6379"
	cfg.ObjectStorage.Enabled = true
	cfg.ObjectStorage.Endpoint = "https://tos-cn-boe.volces.com"
	cfg.ObjectStorage.Region = "cn-guilin-boe"
	cfg.ObjectStorage.Bucket = "aieas"
	cfg.ObjectStorage.BucketURL = "https://redis-sy01vax76anstnm7a.redis.cn-guilin-boe.volces.com"
	cfg.ObjectStorage.AccessKey = "ak"
	cfg.ObjectStorage.SecretKey = "sk"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected object storage bucketURL pointing to redis to be rejected")
	}
}

func TestValidateRejectsPartialDoubaoTTSConfig(t *testing.T) {
	cfg := Default()
	cfg.DoubaoTTS.AppID = "doubao-appid"
	cfg.DoubaoTTS.AckToken = ""

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected partial doubao tts config to be rejected")
	}
}

func TestObservabilityDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Observability.Format != "text" {
		t.Fatalf("expected default observability.format=text, got %q", cfg.Observability.Format)
	}
	if cfg.Observability.SlowSQLThresholdMs != 200 {
		t.Fatalf("expected default observability.slowSQLThresholdMs=200, got %d", cfg.Observability.SlowSQLThresholdMs)
	}
}

func TestCORSDefaultsAllowAllOrigins(t *testing.T) {
	cfg := Default()
	if !cfg.Server.CORS.Enabled {
		t.Fatal("expected server.cors.enabled true by default")
	}
	if len(cfg.Server.CORS.AllowOrigins) != 1 || cfg.Server.CORS.AllowOrigins[0] != "*" {
		t.Fatalf("expected default cors origins to allow all, got %+v", cfg.Server.CORS.AllowOrigins)
	}
	if cfg.Server.CORS.AllowCredentials {
		t.Fatal("expected default cors credentials disabled with wildcard origin")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected default cors config to validate, got %v", err)
	}
}

func TestCORSValidateRejectsWildcardWithCredentials(t *testing.T) {
	cfg := Default()
	cfg.Server.CORS.AllowCredentials = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected wildcard origins with credentials to be rejected")
	}
}

func TestDefaultAgentCallbackURLsUseDefaultAPIPort(t *testing.T) {
	cfg := Default()
	if cfg.Server.Addr != ":8888" {
		t.Fatalf("expected default server addr :8888, got %q", cfg.Server.Addr)
	}
	if !strings.Contains(cfg.Agent.ProductAuditCallbackURL, "127.0.0.1:8888") {
		t.Fatalf("expected product audit callback on default api port, got %q", cfg.Agent.ProductAuditCallbackURL)
	}
	if !strings.Contains(cfg.Agent.LiveAnalysisCallbackURL, "127.0.0.1:8888") {
		t.Fatalf("expected live analysis callback on default api port, got %q", cfg.Agent.LiveAnalysisCallbackURL)
	}
}

func TestRepositoryConfigFilesLoad(t *testing.T) {
	for _, path := range []string{"configs/config.yaml", "configs/config.docker.yaml"} {
		t.Run(path, func(t *testing.T) {
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("load %s: %v", path, err)
			}
			if strings.TrimSpace(cfg.Server.Addr) == "" {
				t.Fatalf("%s loaded empty server addr", path)
			}
		})
	}
}

func TestObservabilityEnvOnlyOverridesMetricsAuthToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("jwt:\n  secret: \"local\"\n  accessTokenTTL: 30m\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("OBSERVABILITY_FORMAT", "json")
	t.Setenv("OBSERVABILITY_SLOW_SQL_THRESHOLD_MS", "500")
	t.Setenv("OBSERVABILITY_METRICS_AUTH_TOKEN", "metrics-secret")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Observability.Format != "text" {
		t.Fatalf("expected format from config/default, got %q", cfg.Observability.Format)
	}
	if cfg.Observability.SlowSQLThresholdMs != 200 {
		t.Fatalf("expected slowSQLThresholdMs from config/default, got %d", cfg.Observability.SlowSQLThresholdMs)
	}
	if cfg.Observability.Metrics.AuthToken != "metrics-secret" {
		t.Fatalf("expected metrics auth token from env, got %q", cfg.Observability.Metrics.AuthToken)
	}
}

func TestObservabilityValidateRejectsInvalidFormat(t *testing.T) {
	cfg := Default()
	cfg.Observability.Format = "xml"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid observability.format to be rejected")
	}
}

func TestObservabilityValidateRejectsNegativeSlowThreshold(t *testing.T) {
	cfg := Default()
	cfg.Observability.SlowSQLThresholdMs = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected negative observability.slowSQLThresholdMs to be rejected")
	}
}

func TestAppRoleDefaultConfigAndInvalid(t *testing.T) {
	cfg := Default()
	if cfg.App.Role != "all" {
		t.Fatalf("expected default app.role all, got %q", cfg.App.Role)
	}
	cfg.App.Role = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty role should normalize to all: %v", err)
	}
	if cfg.App.Role != "all" {
		t.Fatalf("expected empty role normalized to all, got %q", cfg.App.Role)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("jwt:\n  secret: local\n  accessTokenTTL: 30m\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("APP_ROLE", "ws-gateway")
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load config with ignored APP_ROLE: %v", err)
	}
	if loaded.App.Role != "all" {
		t.Fatalf("expected APP_ROLE env ignored and default all, got %q", loaded.App.Role)
	}

	cfg = Default()
	cfg.App.Role = "worker"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "app.role") {
		t.Fatalf("expected invalid app.role error, got %v", err)
	}
}

func TestWebSocketConfigBounds(t *testing.T) {
	cfg := Default()
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{name: "write timeout", mut: func(c *Config) { c.WebSocket.WriteTimeout = 0 }},
		{name: "negative handshake per ip", mut: func(c *Config) { c.WebSocket.HandshakeRateLimitPerIP = -1 }},
		{name: "negative handshake per user", mut: func(c *Config) { c.WebSocket.HandshakeRateLimitPerUser = -1 }},
		{name: "negative handshake per auction", mut: func(c *Config) { c.WebSocket.HandshakeRateLimitPerAuction = -1 }},
		{name: "drain timeout", mut: func(c *Config) { c.WebSocket.DrainTimeout = 0 }},
		{name: "close grace", mut: func(c *Config) { c.WebSocket.CloseGrace = 0 }},
		{name: "negative jitter", mut: func(c *Config) { c.WebSocket.PingJitter = Duration(-time.Second) }},
		{name: "large jitter", mut: func(c *Config) { c.WebSocket.PingJitter = Duration(6 * time.Second) }},
		{name: "replay too small", mut: func(c *Config) { c.WebSocket.ReplayLimit = 0 }},
		{name: "replay too large", mut: func(c *Config) { c.WebSocket.ReplayLimit = 4097 }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg
			tt.mut(&got)
			if err := got.Validate(); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestWebSocketConfigAllowsZeroHandshakeRateLimits(t *testing.T) {
	cfg := Default()
	cfg.WebSocket.HandshakeRateLimitPerIP = 0
	cfg.WebSocket.HandshakeRateLimitPerUser = 0
	cfg.WebSocket.HandshakeRateLimitPerAuction = 0

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected zero handshake rate limits to disable limiting, got %v", err)
	}
}

// TestObservabilityDefaultsTracing 锁定 tracing 默认值：
// 默认 disabled、sampler=parent_based_traceid_ratio、sampleRatio=0.1。
// 这是 G11 的契约：默认配置不应在导入项目时引入 OTel 依赖或采样全量。
func TestObservabilityDefaultsTracing(t *testing.T) {
	cfg := Default()
	if cfg.Observability.Tracing.Enabled {
		t.Fatalf("expected tracing disabled by default, got enabled=true")
	}
	if cfg.Observability.Tracing.Sampler != "parent_based_traceid_ratio" {
		t.Fatalf("expected default sampler parent_based_traceid_ratio, got %q", cfg.Observability.Tracing.Sampler)
	}
	if cfg.Observability.Tracing.SampleRatio != 0.1 {
		t.Fatalf("expected default sampleRatio=0.1, got %v", cfg.Observability.Tracing.SampleRatio)
	}
	if cfg.Observability.Tracing.ServiceName != "aieas-backend" {
		t.Fatalf("expected default serviceName=aieas-backend, got %q", cfg.Observability.Tracing.ServiceName)
	}
}

// TestObservabilityNormalizeFillsSampleRatio 验证 normalize 在 SampleRatio==0
// 时回填 0.1，避免 0 比例导致 trace 全部被 drop 而误以为采样工作正常。
func TestObservabilityNormalizeFillsSampleRatio(t *testing.T) {
	o := ObservabilityConfig{}
	o.normalize()
	if o.Tracing.SampleRatio != 0.1 {
		t.Fatalf("expected normalize to fill sampleRatio=0.1, got %v", o.Tracing.SampleRatio)
	}
}

// TestObservabilityValidateRequiresEndpointWhenTracingEnabled 验证 G11：
// 启用 tracing 且 exporter 是 otlphttp 时，endpoint 必填。
func TestObservabilityValidateRequiresEndpointWhenTracingEnabled(t *testing.T) {
	cfg := Default()
	cfg.Observability.Tracing.Enabled = true
	cfg.Observability.Tracing.Exporter = "otlphttp"
	cfg.Observability.Tracing.Endpoint = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validate to reject otlphttp tracing with empty endpoint")
	}
}

// TestObservabilityValidateAcceptsStdoutExporterWithoutEndpoint 验证
// stdout / noop exporter 不要求 endpoint（本地调试场景）。
func TestObservabilityValidateAcceptsStdoutExporterWithoutEndpoint(t *testing.T) {
	for _, exporter := range []string{"stdout", "noop"} {
		cfg := Default()
		cfg.Observability.Tracing.Enabled = true
		cfg.Observability.Tracing.Exporter = exporter
		cfg.Observability.Tracing.Endpoint = ""
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected %s exporter without endpoint to validate, got %v", exporter, err)
		}
	}
}

// TestObservabilityValidateRejectsInvalidExporter 验证未知 exporter 名拒绝。
func TestObservabilityValidateRejectsInvalidExporter(t *testing.T) {
	cfg := Default()
	cfg.Observability.Tracing.Enabled = true
	cfg.Observability.Tracing.Exporter = "jaeger"
	cfg.Observability.Tracing.Endpoint = "http://collector:4318"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown exporter to be rejected")
	}
}

// TestObservabilityValidateRejectsOutOfRangeSampleRatio 验证 ratio 必须 ∈ [0,1]。
func TestObservabilityValidateRejectsOutOfRangeSampleRatio(t *testing.T) {
	cfg := Default()
	cfg.Observability.Tracing.Enabled = true
	cfg.Observability.Tracing.Endpoint = "http://collector:4318"
	cfg.Observability.Tracing.SampleRatio = 1.5
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected sampleRatio>1 to be rejected")
	}
}

func TestValidateRejectsMissingMCPAPIKey(t *testing.T) {
	cfg := Default()
	cfg.MCP.Read.APIKey = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing mcp.read.apiKey to be rejected")
	}
}

func TestValidateRejectsInvalidMCPActorRole(t *testing.T) {
	cfg := Default()
	cfg.MCP.Control.ActorRole = "operator"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid mcp.control.actorRole to be rejected")
	}
}

func TestValidateRejectsInvalidAgentProductDescriptionURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.ProductDescriptionURL = "127.0.0.1:8000/api/v1/product-description"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.productDescriptionURL to be rejected")
	}
}

func TestValidateRejectsNonPositiveAgentProductDescriptionTimeout(t *testing.T) {
	cfg := Default()
	cfg.Agent.ProductDescriptionTimeout = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected non-positive agent.productDescriptionTimeout to be rejected")
	}
}

func TestValidateRejectsInvalidAgentLiveAnalysisURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.LiveAnalysisURL = "127.0.0.1:8000/api/v1/live-analysis"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.liveAnalysisURL to be rejected")
	}
}

func TestValidateRejectsInvalidAgentProductAuditCallbackURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.ProductAuditCallbackURL = "127.0.0.1:8080/api/v1/auctions/audit/callback"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.productAuditCallbackURL to be rejected")
	}
}

func TestValidateAllowsMissingProductAuditURLsWhenDisabled(t *testing.T) {
	cfg := Default()
	cfg.Agent.ProductAuditEnabled = false
	cfg.Agent.ProductAuditURL = ""
	cfg.Agent.ProductAuditCallbackURL = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected disabled product audit to allow missing URLs, got %v", err)
	}
}

func TestValidateRejectsInvalidAgentLiveAnalysisCallbackURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.LiveAnalysisCallbackURL = "127.0.0.1:8080/api/v1/live-analysis/callback"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.liveAnalysisCallbackURL to be rejected")
	}
}

func TestValidateRejectsMissingAgentLiveAnalysisCallbackAPIKey(t *testing.T) {
	cfg := Default()
	cfg.Agent.LiveAnalysisCallbackAPIKey = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing agent.liveAnalysisCallbackAPIKey to be rejected")
	}
}

func TestValidateRejectsInvalidAgentLiveAuctionHookURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.LiveAuctionHookURL = "127.0.0.1:8000/api/v1/live-auction-hook"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.liveAuctionHookURL to be rejected")
	}
}

func TestValidateRejectsEnabledKafkaWithEmptyBroker(t *testing.T) {
	cfg := Default()
	cfg.Kafka.Enabled = true
	cfg.Kafka.Brokers = []string{""}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected enabled kafka with empty broker to be rejected")
	}
}

// 路线 X：BidDecisionWorker 池 + commit 配置归一化。
func TestKafkaBidDecisionWorkerConfigDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Kafka.BidDecisionWorkerPoolSize != DefaultBidDecisionWorkerPoolSize {
		t.Fatalf("default pool size = %d, want %d", cfg.Kafka.BidDecisionWorkerPoolSize, DefaultBidDecisionWorkerPoolSize)
	}
	if cfg.Kafka.BidDecisionCommitMode != DefaultBidDecisionCommitMode {
		t.Fatalf("default commit mode = %q, want %q", cfg.Kafka.BidDecisionCommitMode, DefaultBidDecisionCommitMode)
	}
	if cfg.Kafka.BidDecisionCommitBatchSize != DefaultBidDecisionCommitBatchSize {
		t.Fatalf("default commit batch size = %d, want %d", cfg.Kafka.BidDecisionCommitBatchSize, DefaultBidDecisionCommitBatchSize)
	}
}

func TestKafkaBidDecisionWorkerConfigNormalize(t *testing.T) {
	cfg := Default()
	cfg.Kafka.BidDecisionWorkerPoolSize = 0
	cfg.Kafka.BidDecisionCommitMode = "garbage"
	cfg.Kafka.BidDecisionCommitBatchSize = -5
	cfg.Kafka.BidDecisionCommitMaxLatencyMs = 0
	cfg.Kafka.normalize()
	if cfg.Kafka.BidDecisionWorkerPoolSize != DefaultBidDecisionWorkerPoolSize {
		t.Fatalf("0 pool size should normalize to default, got %d", cfg.Kafka.BidDecisionWorkerPoolSize)
	}
	if cfg.Kafka.BidDecisionCommitMode != DefaultBidDecisionCommitMode {
		t.Fatalf("invalid commit mode should normalize to default, got %q", cfg.Kafka.BidDecisionCommitMode)
	}
	if cfg.Kafka.BidDecisionCommitBatchSize != DefaultBidDecisionCommitBatchSize {
		t.Fatalf("negative batch size should normalize to default, got %d", cfg.Kafka.BidDecisionCommitBatchSize)
	}
	if cfg.Kafka.BidDecisionCommitMaxLatencyMs != DefaultBidDecisionCommitMaxLatencyMs {
		t.Fatalf("0 max latency ms should normalize to default, got %d", cfg.Kafka.BidDecisionCommitMaxLatencyMs)
	}

	// 上下界 clamp。
	cfg.Kafka.BidDecisionWorkerPoolSize = 5 // 低于 Min=16
	cfg.Kafka.normalize()
	if cfg.Kafka.BidDecisionWorkerPoolSize != MinBidDecisionWorkerPoolSize {
		t.Fatalf("pool size below min should clamp to %d, got %d", MinBidDecisionWorkerPoolSize, cfg.Kafka.BidDecisionWorkerPoolSize)
	}
	cfg.Kafka.BidDecisionWorkerPoolSize = 9999
	cfg.Kafka.normalize()
	if cfg.Kafka.BidDecisionWorkerPoolSize != MaxBidDecisionWorkerPoolSize {
		t.Fatalf("pool size above max should clamp to %d, got %d", MaxBidDecisionWorkerPoolSize, cfg.Kafka.BidDecisionWorkerPoolSize)
	}

	// "single" 是合法值，应保留。
	cfg.Kafka.BidDecisionCommitMode = "SINGLE"
	cfg.Kafka.normalize()
	if cfg.Kafka.BidDecisionCommitMode != "single" {
		t.Fatalf("commit mode SINGLE should normalize to single, got %q", cfg.Kafka.BidDecisionCommitMode)
	}
}
