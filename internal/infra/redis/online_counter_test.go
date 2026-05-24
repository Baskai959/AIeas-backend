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
	counter := NewOnlineCounter(client, keys, 100*time.Millisecond)

	count, err := counter.Touch(ctx, 10001, "conn-1")
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
	if ok, err := client.SIsMember(ctx, keys.OnlineInstanceConns(counter.InstanceID()), "10001|"+member).Result(); err != nil || !ok {
		t.Fatalf("instance connection index missing ok=%v err=%v", ok, err)
	}
	firstTTL := mr.TTL(keys.WSInstanceHeartbeat(counter.InstanceID()))
	mr.FastForward(50 * time.Millisecond)
	if _, err := counter.Touch(ctx, 10001, member); err != nil {
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
	if _, err := counter.Touch(ctx, 10001, "conn-fallback"); err == nil {
		t.Fatal("expected redis failure to surface so hub can fall back to local count")
	}
}
