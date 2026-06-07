package redis

import (
	"context"
	"testing"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

func liveSessionIDWithDifferentSessionAndRoomShards(t *testing.T, sharded *ShardedRTClient) uint64 {
	t.Helper()
	for id := uint64(1); id < 1000; id++ {
		if sharded.IndexSession(id) != sharded.IndexRoom(id) {
			return id
		}
	}
	t.Fatalf("could not find session id with different ForSession/ForRoom shards")
	return 0
}

func TestLiveSessionRealtimeActiveAuctionDoesNotTouchLockKeyOrTTL(t *testing.T) {
	ctx := context.Background()
	shards := []*RedisRTClient{
		newMiniredisShardClient(t),
		newMiniredisShardClient(t),
	}
	sharded := NewShardedRTClientFromShards(shards)
	keys := NewKeyBuilder("")
	sessionID := liveSessionIDWithDifferentSessionAndRoomShards(t, sharded)
	lock := NewLiveSessionLock(sharded, keys)
	store := NewLiveSessionRealtimeStore(sharded, keys)

	acquired, holder, err := lock.Acquire(ctx, sessionID, 1001, time.Minute)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	if !acquired || holder != 1001 {
		t.Fatalf("acquire lock got acquired=%v holder=%d", acquired, holder)
	}
	lockKey := keys.LiveSessionActiveLock(sessionID)
	activeKey := keys.LiveSessionActiveAuction(sessionID)
	lockClient := sharded.ForRoom(sessionID)
	beforeTTL, err := lockClient.TTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("read lock ttl: %v", err)
	}
	if beforeTTL <= 0 {
		t.Fatalf("lock ttl before active write=%v, want positive", beforeTTL)
	}

	if err := store.SetActiveAuction(ctx, sessionID, 2002); err != nil {
		t.Fatalf("set active auction: %v", err)
	}
	if got, err := lock.Current(ctx, sessionID); err != nil || got != 1001 {
		t.Fatalf("lock current after active write got=%d err=%v", got, err)
	}
	afterSetTTL, err := lockClient.TTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("read lock ttl after set: %v", err)
	}
	if afterSetTTL <= 0 {
		t.Fatalf("lock ttl after active write=%v, want positive", afterSetTTL)
	}
	if value, err := lockClient.Get(ctx, lockKey).Result(); err != nil || value != "1001" {
		t.Fatalf("lock key value after active write got=%q err=%v", value, err)
	}
	if value, err := sharded.ForSession(sessionID).Get(ctx, activeKey).Result(); err != nil || value != "2002" {
		t.Fatalf("active key on ForSession got=%q err=%v", value, err)
	}

	if err := store.ClearActiveAuction(ctx, sessionID); err != nil {
		t.Fatalf("clear active auction: %v", err)
	}
	if got, err := lock.Current(ctx, sessionID); err != nil || got != 1001 {
		t.Fatalf("lock current after active clear got=%d err=%v", got, err)
	}
	afterClearTTL, err := lockClient.TTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("read lock ttl after clear: %v", err)
	}
	if afterClearTTL <= 0 {
		t.Fatalf("lock ttl after active clear=%v, want positive", afterClearTTL)
	}
	if _, err := sharded.ForSession(sessionID).Get(ctx, activeKey).Result(); err != redisgo.Nil {
		t.Fatalf("active key after clear err=%v, want redis nil", err)
	}
}

func TestLiveSessionRealtimeActiveAuctionUsesForSessionShard(t *testing.T) {
	ctx := context.Background()
	shards := []*RedisRTClient{
		newMiniredisShardClient(t),
		newMiniredisShardClient(t),
	}
	sharded := NewShardedRTClientFromShards(shards)
	keys := NewKeyBuilder("")
	sessionID := liveSessionIDWithDifferentSessionAndRoomShards(t, sharded)
	store := NewLiveSessionRealtimeStore(sharded, keys)

	if err := store.SetActiveAuction(ctx, sessionID, 3003); err != nil {
		t.Fatalf("set active auction: %v", err)
	}
	activeKey := keys.LiveSessionActiveAuction(sessionID)
	sessionClient := sharded.ForSession(sessionID)
	roomClient := sharded.ForRoom(sessionID)
	if value, err := sessionClient.Get(ctx, activeKey).Result(); err != nil || value != "3003" {
		t.Fatalf("ForSession active value=%q err=%v", value, err)
	}
	if _, err := roomClient.Get(ctx, activeKey).Result(); err != redisgo.Nil {
		t.Fatalf("ForRoom active key err=%v, want redis nil", err)
	}
	activeID, ok, err := store.ActiveAuction(ctx, sessionID)
	if err != nil {
		t.Fatalf("ActiveAuction: %v", err)
	}
	if !ok || activeID != 3003 {
		t.Fatalf("ActiveAuction got id=%d ok=%v", activeID, ok)
	}
}
