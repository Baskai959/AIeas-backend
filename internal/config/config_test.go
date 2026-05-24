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
  addr: "127.0.0.1:6379"
  db: 2
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
	t.Setenv("REDIS_DB", "3")
	t.Setenv("IDEMPOTENCY_TTL", "30m")
	t.Setenv("OBJECT_STORAGE_ENABLED", "true")
	t.Setenv("OBJECT_STORAGE_ENDPOINT", "https://tos-cn-boe.volces.com")
	t.Setenv("OBJECT_STORAGE_REGION", "cn-guilin-boe")
	t.Setenv("OBJECT_STORAGE_BUCKET", "aieas")
	t.Setenv("OBJECT_STORAGE_BUCKET_URL", "https://aieas.tos-cn-boe.volces.com")
	t.Setenv("OBJECT_STORAGE_ACCESS_KEY", "ak")
	t.Setenv("OBJECT_STORAGE_SECRET_KEY", "sk")

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
	if cfg.Redis.Addr != "127.0.0.1:6379" || cfg.Redis.DB != 3 {
		t.Fatalf("unexpected redis config: %+v", cfg.Redis)
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
}

func TestLoadAppliesDotEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
jwt:
  secret: "from-file"
  accessTokenTTL: 30m
redis:
  password: "from-file"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("JWT_SECRET=from-dotenv\nREDIS_PASSWORD=redis-from-dotenv\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	t.Setenv("JWT_SECRET", "")
	t.Setenv("REDIS_PASSWORD", "")
	os.Unsetenv("JWT_SECRET")
	os.Unsetenv("REDIS_PASSWORD")
	t.Cleanup(func() {
		os.Unsetenv("JWT_SECRET")
		os.Unsetenv("REDIS_PASSWORD")
	})

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.JWT.Secret != "from-dotenv" {
		t.Fatalf("expected JWT secret from .env, got %q", cfg.JWT.Secret)
	}
	if cfg.Redis.Password != "redis-from-dotenv" {
		t.Fatalf("expected Redis password from .env, got %q", cfg.Redis.Password)
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
	cfg.Redis.Addr = "redis-sy01vax76anstnm7a.redis.cn-guilin-boe.volces.com:6379"
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
