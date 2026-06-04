package redis

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

func TestOnlineCounterTouchHeartbeatIndexJanitorAndFallback(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys := NewKeyBuilder("test")
	rt := &RedisRTClient{Client: client}
	sharded := NewShardedRTClientFromShards([]*RedisRTClient{rt})
	counter := NewOnlineCounter(sharded, keys, 100*time.Millisecond)

	count, err := counter.Touch(ctx, 10001, "conn-1", "u_1001")
	if err != nil || count != 1 {
		t.Fatalf("touch count=%d err=%v", count, err)
	}
	member := counter.InstanceID() + ":conn-1"
	if ok, err := client.SIsMember(ctx, keys.WSInstances(), counter.InstanceID()).Result(); err != nil || !ok {
		t.Fatalf("instance heartbeat index missing ok=%v err=%v", ok, err)
	}
	if exists, err := client.Exists(ctx, keys.WSInstanceHeartbeat(counter.InstanceID())).Result(); err != nil || exists != 1 {
		t.Fatalf("heartbeat missing exists=%d err=%v", exists, err)
	}
	userID := normalizeOnlineUserID("u_1001", member)
	if ok, err := client.SIsMember(ctx, keys.OnlineInstanceConns(counter.InstanceID()), onlineIndexMember(10001, member, userID)).Result(); err != nil || !ok {
		t.Fatalf("instance connection index missing ok=%v err=%v", ok, err)
	}
	firstTTL := mr.TTL(keys.WSInstanceHeartbeat(counter.InstanceID()))
	mr.FastForward(50 * time.Millisecond)
	if _, err := counter.Touch(ctx, 10001, member, "u_1001"); err != nil {
		t.Fatalf("refresh touch: %v", err)
	}
	if refreshedTTL := mr.TTL(keys.WSInstanceHeartbeat(counter.InstanceID())); refreshedTTL <= firstTTL/2 {
		t.Fatalf("expected heartbeat ttl refresh, first=%s refreshed=%s", firstTTL, refreshedTTL)
	}

	mr.Del(keys.WSInstanceHeartbeat(counter.InstanceID()))
	if err := counter.CleanupDeadInstances(ctx); err != nil {
		t.Fatalf("cleanup dead instance: %v", err)
	}
	if count, err := counter.Count(ctx, 10001); err != nil || count != 0 {
		t.Fatalf("dead instance connections should be removed, count=%d err=%v", count, err)
	}

	_ = client.Close()
	if _, err := counter.Touch(ctx, 10001, "conn-fallback", "u_1001"); err == nil {
		t.Fatal("expected redis failure to surface so hub can fall back to local count")
	}
}

func TestOnlineCounterCountsDistinctUsers(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys := NewKeyBuilder("test")
	rt := &RedisRTClient{Client: client}
	sharded := NewShardedRTClientFromShards([]*RedisRTClient{rt})
	counter := NewOnlineCounter(sharded, keys, time.Minute)

	if count, err := counter.Join(ctx, 10001, "conn-1", "u_1001"); err != nil || count != 1 {
		t.Fatalf("join first count=%d err=%v", count, err)
	}
	if count, err := counter.Join(ctx, 10001, "conn-2", "u_1001"); err != nil || count != 1 {
		t.Fatalf("same user should still count once, count=%d err=%v", count, err)
	}
	if count, err := counter.Join(ctx, 10001, "conn-3", "u_1002"); err != nil || count != 2 {
		t.Fatalf("second user count=%d err=%v", count, err)
	}
	if count, err := counter.Leave(ctx, 10001, counter.InstanceID()+":conn-1", "u_1001"); err != nil || count != 2 {
		t.Fatalf("leaving one duplicate connection should keep user online, count=%d err=%v", count, err)
	}
	if count, err := counter.Leave(ctx, 10001, counter.InstanceID()+":conn-2", "u_1001"); err != nil || count != 1 {
		t.Fatalf("leaving last duplicate connection should remove user, count=%d err=%v", count, err)
	}
}

func TestOnlineCounterCanUseRedisCache(t *testing.T) {
	ctx := context.Background()
	rtMR := miniredis.RunT(t)
	rtClient := redisgo.NewClient(&redisgo.Options{Addr: rtMR.Addr()})
	t.Cleanup(func() { _ = rtClient.Close() })
	cacheMR := miniredis.RunT(t)
	cacheClient := redisgo.NewClient(&redisgo.Options{Addr: cacheMR.Addr()})
	t.Cleanup(func() { _ = cacheClient.Close() })
	keys := NewKeyBuilder("test")
	counter := NewOnlineCounterOnCache(&RedisCacheClient{Client: cacheClient}, keys, time.Minute)

	count, err := counter.Join(ctx, 10001, "conn-cache-1", "u_1001")
	if err != nil || count != 1 {
		t.Fatalf("join count=%d err=%v", count, err)
	}
	if exists, err := cacheClient.Exists(ctx, keys.OnlineAuctionUsers(10001)).Result(); err != nil || exists != 1 {
		t.Fatalf("expected online auction users in cache, exists=%d err=%v", exists, err)
	}
	if exists, err := rtClient.Exists(ctx, keys.OnlineAuctionUsers(10001)).Result(); err != nil || exists != 0 {
		t.Fatalf("online counter must not write RT in cache mode, exists=%d err=%v", exists, err)
	}
}

func TestOnlineCounterDefaultTTLIsShort(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys := NewKeyBuilder("test")
	rt := &RedisRTClient{Client: client}
	sharded := NewShardedRTClientFromShards([]*RedisRTClient{rt})
	counter := NewOnlineCounter(sharded, keys, 0)

	if _, err := counter.Touch(ctx, 10001, "conn-1", "u_1001"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	ttl := mr.TTL(keys.WSInstanceHeartbeat(counter.InstanceID()))
	if ttl <= time.Minute || ttl > DefaultOnlineCounterTTL {
		t.Fatalf("expected default heartbeat ttl in (1m,%s], got %s", DefaultOnlineCounterTTL, ttl)
	}
}
