package app

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	redisinfra "aieas_backend/internal/infra/redis"
)

func TestMySQLRedisIntegrationSmoke(t *testing.T) {
	if os.Getenv("AIEAS_INTEGRATION") != "1" {
		t.Skip("set AIEAS_INTEGRATION=1 with MYSQL_DSN and REDIS_ADDR to run real MySQL/Redis integration smoke")
	}
	ctx := context.Background()
	cfg := appconfig.Default()
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		cfg.MySQL.DSN = dsn
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.Redis.Addr = addr
	}
	cfg.Redis.Username = os.Getenv("REDIS_USERNAME")
	cfg.Redis.Password = os.Getenv("REDIS_PASSWORD")
	if cfg.MySQL.DSN == "" || cfg.Redis.Addr == "" {
		t.Fatal("MYSQL_DSN and REDIS_ADDR are required when AIEAS_INTEGRATION=1")
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

	rdb, err := redisinfra.Open(ctx, cfg.Redis)
	if err != nil {
		t.Fatalf("open real redis: %v", err)
	}
	defer rdb.Close()
	scripts := redisinfra.NewScriptRegistry(rdb, redisinfra.DefaultScripts())
	if err := scripts.LoadAll(ctx); err != nil {
		t.Fatalf("load redis scripts: %v", err)
	}
	prefix := fmt.Sprintf("it:%d", time.Now().UTC().UnixNano())
	store := redisinfra.NewAuctionRealtimeStore(rdb, scripts, redisinfra.NewKeyBuilder(prefix))
	now := time.Now().UTC().Truncate(time.Millisecond)
	auction := domain.AuctionLot{AuctionID: 910001, Status: domain.AuctionStatusRunning, StartPrice: 1000, StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Hour)}
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
	second, err := store.PlaceBid(ctx, domain.BidInput{RequestID: "it-bid-2", AuctionID: auction.AuctionID, BidderID: "u_1002", Price: 1100, Now: now.Add(time.Millisecond), MinIncrement: 100, IdempotencyTTL: time.Hour})
	if err != nil || !second.Accepted || second.LeaderBidderID != "u_1001" {
		t.Fatalf("equal-price redis bid should keep first leader, result=%+v err=%v", second, err)
	}
	top, err := store.TopN(ctx, auction.AuctionID, 2)
	if err != nil {
		t.Fatalf("redis topn: %v", err)
	}
	if len(top) != 2 || top[0].BidderID != "u_1001" || top[1].BidderID != "u_1002" {
		t.Fatalf("expected first equal-price bidder before later bidder, top=%+v", top)
	}
}
