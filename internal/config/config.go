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
	WebSocket     WebSocketConfig     `yaml:"websocket"`
	ObjectStorage ObjectStorageConfig `yaml:"objectStorage"`
	Agent         AgentConfig         `yaml:"agent"`
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
	MinIncrementCent int64 `yaml:"minIncrementCent"`
	AntiSnipeMs      int64 `yaml:"antiSnipeMs"`
	ExtendMs         int64 `yaml:"extendMs"`
	MaxExtendCount   int   `yaml:"maxExtendCount"`
	FreqLimitCount   int   `yaml:"freqLimitCount"`
	FreqWindowMs     int64 `yaml:"freqWindowMs"`
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
	ProductDescriptionURL string   `yaml:"productDescriptionURL"`
	ProductAuditURL       string   `yaml:"productAuditURL"`
	Timeout               Duration `yaml:"timeout"`
}

type ObservabilityConfig struct {
	LogLevel           string `yaml:"logLevel"`
	MetricsPath        string `yaml:"metricsPath"`
	Format             string `yaml:"format"`
	SlowSQLThresholdMs int    `yaml:"slowSQLThresholdMs"`
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
			Addr:     "redis:6379",
			Username: "default",
			Password: "",
			DB:       0,
			PoolSize: 100,
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
			MinIncrementCent: 100,
			AntiSnipeMs:      30000,
			ExtendMs:         30000,
			MaxExtendCount:   20,
			FreqLimitCount:   10,
			FreqWindowMs:     1000,
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
			ProductDescriptionURL: "http://127.0.0.1:8000/api/v1/product-description",
			ProductAuditURL:       "http://127.0.0.1:8000/api/v1/product-audit",
			Timeout:               Duration(30 * time.Second),
		},
		Observability: ObservabilityConfig{
			LogLevel:           "info",
			MetricsPath:        "/metrics",
			Format:             "text",
			SlowSQLThresholdMs: 200,
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
	if c.Redis.DB < 0 {
		return fmt.Errorf("redis.db must be non-negative")
	}
	if c.Idempotency.TTL.Std() <= 0 {
		return fmt.Errorf("idempotency.ttl must be positive")
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
	if strings.TrimSpace(c.Agent.ProductDescriptionURL) == "" {
		return fmt.Errorf("agent.productDescriptionURL is required")
	}
	if err := validateHTTPURL(c.Agent.ProductDescriptionURL, "agent.productDescriptionURL"); err != nil {
		return err
	}
	if strings.TrimSpace(c.Agent.ProductAuditURL) == "" {
		return fmt.Errorf("agent.productAuditURL is required")
	}
	if err := validateHTTPURL(c.Agent.ProductAuditURL, "agent.productAuditURL"); err != nil {
		return err
	}
	if c.Agent.Timeout.Std() <= 0 {
		return fmt.Errorf("agent.timeout must be positive")
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
		if err := validateBucketURL(c.ObjectStorage.BucketURL, c.Redis.Addr); err != nil {
			return err
		}
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

	setString("REDIS_ADDR", &c.Redis.Addr)
	setString("REDIS_USERNAME", &c.Redis.Username)
	setString("REDIS_PASSWORD", &c.Redis.Password)
	if err := setInt("REDIS_DB", &c.Redis.DB); err != nil {
		return err
	}
	if err := setInt("REDIS_POOL_SIZE", &c.Redis.PoolSize); err != nil {
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
	setString("AGENT_PRODUCT_AUDIT_URL", &c.Agent.ProductAuditURL)
	if err := setDuration("AGENT_TIMEOUT", &c.Agent.Timeout); err != nil {
		return err
	}

	setString("OBSERVABILITY_LOG_LEVEL", &c.Observability.LogLevel)
	setString("OBSERVABILITY_METRICS_PATH", &c.Observability.MetricsPath)
	setString("OBSERVABILITY_FORMAT", &c.Observability.Format)
	if err := setInt("OBSERVABILITY_SLOW_SQL_THRESHOLD_MS", &c.Observability.SlowSQLThresholdMs); err != nil {
		return err
	}
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
