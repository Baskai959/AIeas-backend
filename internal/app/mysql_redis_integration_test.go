package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	redisinfra "aieas_backend/internal/infra/redis"
)

func TestMySQLRedisIntegrationSmoke(t *testing.T) {
	if os.Getenv("AIEAS_INTEGRATION") != "1" {
		t.Skip("set AIEAS_INTEGRATION=1 with MYSQL_DSN and REDIS_RT_SHARD_ADDRS to run real MySQL/Redis integration smoke")
	}
	ctx := context.Background()
	cfg := appconfig.Default()
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		cfg.MySQL.DSN = dsn
	}
	// 兼容旧环境变量 REDIS_ADDR：若未单独提供 RT/Cache 地址，则把它作为兜底注入。
	// RT 是 sharded：测试场景可以通过 REDIS_RT_SHARD_ADDRS 提供逗号分隔的多 shard 地址，
	// 也可以走单 shard（首个 shard 的 addr）的 REDIS_RT_SHARD0_ADDR。
	legacyAddr := os.Getenv("REDIS_ADDR")
	if shardAddrs := os.Getenv("REDIS_RT_SHARD_ADDRS"); shardAddrs != "" {
		parts := strings.Split(shardAddrs, ",")
		shards := make([]appconfig.RedisInstanceConfig, 0, len(parts))
		for _, p := range parts {
			addr := strings.TrimSpace(p)
			if addr == "" {
				continue
			}
			shards = append(shards, appconfig.RedisInstanceConfig{Addr: addr, Username: "default", PoolSize: 100})
		}
		if len(shards) > 0 {
			cfg.Redis.RT.Shards = shards
		}
	} else if addr := os.Getenv("REDIS_RT_SHARD0_ADDR"); addr != "" {
		cfg.Redis.RT.Shards = []appconfig.RedisInstanceConfig{{Addr: addr, Username: "default", PoolSize: 100}}
	} else if legacyAddr != "" {
		cfg.Redis.RT.Shards = []appconfig.RedisInstanceConfig{{Addr: legacyAddr, Username: "default", PoolSize: 100}}
	}
	if addr := os.Getenv("REDIS_CACHE_ADDR"); addr != "" {
		cfg.Redis.Cache.Addr = addr
	} else if legacyAddr != "" {
		cfg.Redis.Cache.Addr = legacyAddr
	}
	if len(cfg.Redis.RT.Shards) > 0 {
		if u := os.Getenv("REDIS_RT_USERNAME"); u != "" {
			for i := range cfg.Redis.RT.Shards {
				cfg.Redis.RT.Shards[i].Username = u
			}
		}
		if p := os.Getenv("REDIS_RT_PASSWORD"); p != "" {
			for i := range cfg.Redis.RT.Shards {
				cfg.Redis.RT.Shards[i].Password = p
			}
		}
	}
	if cfg.MySQL.DSN == "" || len(cfg.Redis.RT.Shards) == 0 || cfg.Redis.RT.Shards[0].Addr == "" {
		t.Fatal("MYSQL_DSN and REDIS_RT_SHARD_ADDRS / REDIS_RT_SHARD0_ADDR (or REDIS_ADDR) are required when AIEAS_INTEGRATION=1")
	}

	db, err := mysqlinfra.Open(ctx, cfg.MySQL, nil)
	if err != nil {
		t.Fatalf("open real mysql: %v", err)
	}
	defer mysqlinfra.Close(db)
	var one int
	if err := db.WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil || one != 1 {
		t.Fatalf("mysql SELECT 1 got one=%d err=%v", one, err)
	}

	shardedRT, err := redisinfra.NewShardedRTClient(ctx, cfg.Redis.RT.Shards)
	if err != nil {
		t.Fatalf("open real sharded redis: %v", err)
	}
	defer shardedRT.Close()
	scripts := redisinfra.NewShardedScriptRegistry(shardedRT, redisinfra.DefaultScripts())
	if err := scripts.LoadAll(ctx); err != nil {
		t.Fatalf("load redis scripts: %v", err)
	}
	prefix := fmt.Sprintf("it:%d", time.Now().UTC().UnixNano())
	store := redisinfra.NewAuctionRealtimeStore(shardedRT, scripts, redisinfra.NewKeyBuilder(prefix))
	now := time.Now().UTC().Truncate(time.Millisecond)
	auction := domain.AuctionLot{AuctionID: 910001, Status: domain.AuctionStatusRunning, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000, IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`), StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Hour)}
	if _, err := store.InitAuction(ctx, auction, 100); err != nil {
		t.Fatalf("init redis auction: %v", err)
	}
	for _, userID := range []string{"u_1001", "u_1002"} {
		if err := store.MarkEnrollment(ctx, auction.AuctionID, userID); err != nil {
			t.Fatalf("mark enrollment %s: %v", userID, err)
		}
	}
	first, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "it-bid-1", AuctionID: auction.AuctionID, BidderID: "u_1001", Price: 1100, Now: now, MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !first.Accepted {
		t.Fatalf("first redis bid result=%+v err=%v", first, err)
	}
	second, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "it-bid-2", AuctionID: auction.AuctionID, BidderID: "u_1002", Price: 1200, Now: now.Add(time.Millisecond), MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !second.Accepted || second.LeaderBidderID != "u_1002" {
		t.Fatalf("second redis bid should become leader, result=%+v err=%v", second, err)
	}
	top, err := store.TopN(ctx, auction.AuctionID, 2)
	if err != nil {
		t.Fatalf("redis topn: %v", err)
	}
	if len(top) != 2 || top[0].BidderID != "u_1002" || top[1].BidderID != "u_1001" {
		t.Fatalf("expected higher bidder first, top=%+v", top)
	}
}
