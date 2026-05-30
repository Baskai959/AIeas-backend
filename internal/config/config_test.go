package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadParsesYAMLAndAppliesEnvOverrides(t *testing.T) {
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
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("SERVER_ADDR", ":7070")
	t.Setenv("JWT_SECRET", "from-env")
	t.Setenv("REDIS_RT_SHARD_0_DB", "3")
	t.Setenv("IDEMPOTENCY_TTL", "30m")
	t.Setenv("OBJECT_STORAGE_ENABLED", "true")
	t.Setenv("OBJECT_STORAGE_ENDPOINT", "https://tos-cn-boe.volces.com")
	t.Setenv("OBJECT_STORAGE_REGION", "cn-guilin-boe")
	t.Setenv("OBJECT_STORAGE_BUCKET", "aieas")
	t.Setenv("OBJECT_STORAGE_BUCKET_URL", "https://aieas.tos-cn-boe.volces.com")
	t.Setenv("OBJECT_STORAGE_ACCESS_KEY", "ak")
	t.Setenv("OBJECT_STORAGE_SECRET_KEY", "sk")
	t.Setenv("AGENT_PRODUCT_DESCRIPTION_URL", "http://127.0.0.1:9000/api/v1/product-description")
	t.Setenv("AGENT_PRODUCT_AUDIT_URL", "http://127.0.0.1:9000/api/v1/product-audit")
	t.Setenv("AGENT_PRODUCT_AUDIT_CALLBACK_URL", "http://127.0.0.1:7070/api/v1/items/audit/callback")
	t.Setenv("AGENT_LIVE_ANALYSIS_URL", "http://127.0.0.1:9000/api/v1/live-analysis/async")
	t.Setenv("AGENT_LIVE_ANALYSIS_CALLBACK_URL", "http://127.0.0.1:7070/api/v1/live-analysis/callback")
	t.Setenv("AGENT_LIVE_ANALYSIS_CALLBACK_API_KEY", "callback-from-env")
	t.Setenv("AGENT_LIVE_AUCTION_HOOK_URL", "http://127.0.0.1:9000/api/v1/live-auction-hook")
	t.Setenv("AGENT_TIMEOUT", "5s")
	t.Setenv("MCP_READ_API_KEY", "mcp-read-from-env")
	t.Setenv("MCP_READ_ACTOR_ID", "u_9001")
	t.Setenv("MCP_READ_ACTOR_ROLE", "admin")
	t.Setenv("MCP_CONTROL_API_KEY", "mcp-control-from-env")
	t.Setenv("MCP_CONTROL_ACTOR_ID", "u_9001")
	t.Setenv("MCP_CONTROL_ACTOR_ROLE", "admin")
	t.Setenv("KAFKA_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092,127.0.0.1:9093")
	t.Setenv("KAFKA_BID_EVENTS_TOPIC", "test.bid.events")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Server.Addr != ":7070" {
		t.Fatalf("expected env server addr, got %q", cfg.Server.Addr)
	}
	if cfg.Server.ReadTimeout.Std() != 2*time.Second {
		t.Fatalf("expected read timeout 2s, got %s", cfg.Server.ReadTimeout.Std())
	}
	if cfg.MySQL.MaxOpenConns != 12 {
		t.Fatalf("expected mysql max open conns 12, got %d", cfg.MySQL.MaxOpenConns)
	}
	if cfg.Redis.RT.Shards[0].Addr != "127.0.0.1:6381" || cfg.Redis.RT.Shards[0].DB != 3 {
		t.Fatalf("unexpected redis rt shard[0] config: %+v", cfg.Redis.RT.Shards[0])
	}
	if len(cfg.Redis.RT.Shards) < 2 || cfg.Redis.RT.Shards[1].Addr != "127.0.0.1:6382" {
		t.Fatalf("unexpected redis rt shard[1] config: %+v", cfg.Redis.RT.Shards)
	}
	if cfg.Redis.Cache.Addr != "127.0.0.1:6380" || cfg.Redis.Cache.DB != 1 {
		t.Fatalf("unexpected redis cache config: %+v", cfg.Redis.Cache)
	}
	if cfg.JWT.Secret != "from-env" || cfg.JWT.AccessTokenTTL.Std() != 30*time.Minute {
		t.Fatalf("unexpected jwt config: %+v", cfg.JWT)
	}
	if cfg.Idempotency.TTL.Std() != 30*time.Minute {
		t.Fatalf("expected idempotency ttl env override, got %s", cfg.Idempotency.TTL.Std())
	}
	if !cfg.ObjectStorage.Enabled || cfg.ObjectStorage.Bucket != "aieas" || cfg.ObjectStorage.BucketURL != "https://aieas.tos-cn-boe.volces.com" {
		t.Fatalf("unexpected object storage config: %+v", cfg.ObjectStorage)
	}
	if cfg.Agent.ProductDescriptionURL != "http://127.0.0.1:9000/api/v1/product-description" ||
		cfg.Agent.ProductAuditURL != "http://127.0.0.1:9000/api/v1/product-audit" ||
		cfg.Agent.ProductAuditCallbackURL != "http://127.0.0.1:7070/api/v1/items/audit/callback" ||
		cfg.Agent.LiveAnalysisURL != "http://127.0.0.1:9000/api/v1/live-analysis/async" ||
		cfg.Agent.LiveAnalysisCallbackURL != "http://127.0.0.1:7070/api/v1/live-analysis/callback" ||
		cfg.Agent.LiveAnalysisCallbackAPIKey != "callback-from-env" ||
		cfg.Agent.LiveAuctionHookURL != "http://127.0.0.1:9000/api/v1/live-auction-hook" ||
		cfg.Agent.Timeout.Std() != 5*time.Second {
		t.Fatalf("unexpected agent config: %+v", cfg.Agent)
	}
	if cfg.MCP.Read.APIKey != "mcp-read-from-env" || cfg.MCP.Read.ActorID != "u_9001" || cfg.MCP.Read.ActorRole != "admin" ||
		cfg.MCP.Control.APIKey != "mcp-control-from-env" || cfg.MCP.Control.ActorID != "u_9001" || cfg.MCP.Control.ActorRole != "admin" {
		t.Fatalf("unexpected mcp config: %+v", cfg.MCP)
	}
	if !cfg.Kafka.Enabled || len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "127.0.0.1:9092" || cfg.Kafka.BidEventsTopic != "test.bid.events" {
		t.Fatalf("unexpected kafka config: %+v", cfg.Kafka)
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

func TestObservabilityDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Observability.Format != "text" {
		t.Fatalf("expected default observability.format=text, got %q", cfg.Observability.Format)
	}
	if cfg.Observability.SlowSQLThresholdMs != 200 {
		t.Fatalf("expected default observability.slowSQLThresholdMs=200, got %d", cfg.Observability.SlowSQLThresholdMs)
	}
}

func TestObservabilityEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("jwt:\n  secret: \"local\"\n  accessTokenTTL: 30m\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("OBSERVABILITY_FORMAT", "json")
	t.Setenv("OBSERVABILITY_SLOW_SQL_THRESHOLD_MS", "500")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Observability.Format != "json" {
		t.Fatalf("expected env override format=json, got %q", cfg.Observability.Format)
	}
	if cfg.Observability.SlowSQLThresholdMs != 500 {
		t.Fatalf("expected env override slowSQLThresholdMs=500, got %d", cfg.Observability.SlowSQLThresholdMs)
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

func TestValidateRejectsInvalidAgentLiveAnalysisURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.LiveAnalysisURL = "127.0.0.1:8000/api/v1/live-analysis"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.liveAnalysisURL to be rejected")
	}
}

func TestValidateRejectsInvalidAgentProductAuditCallbackURL(t *testing.T) {
	cfg := Default()
	cfg.Agent.ProductAuditCallbackURL = "127.0.0.1:8080/api/v1/items/audit/callback"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid agent.productAuditCallbackURL to be rejected")
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
