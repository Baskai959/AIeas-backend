package redis

import (
	"hash/fnv"
	"strconv"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

// newMiniredisShardClient 启一个独立的 miniredis 进程内实例，用于组装多 shard
// ShardedRTClient。每个 shard 一份独立 miniredis，模拟真实跨进程的 RT 拓扑。
func newMiniredisShardClient(t *testing.T) *RedisRTClient {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return &RedisRTClient{Client: c}
}

// TestShardedRTClientRoutingFnv32 锁定路由约定：
//   - Index(hashKey) == fnv32a(hashKey) % len(shards)
//   - ForAuction/ForSession/ForRoom 分别使用 "auction:<id>" / "session:<id>" / "room:<id>" 作为 hash key
//   - ForGlobal 固定指向 shard 0
//
// 这个测试是 v2 sharding 拓扑的契约：路由公式一旦修改，已经在 shard 0/shard 1
// 上的状态就会错位，必须显式锁定。
func TestShardedRTClientRoutingFnv32(t *testing.T) {
	shards := []*RedisRTClient{
		newMiniredisShardClient(t),
		newMiniredisShardClient(t),
		newMiniredisShardClient(t),
	}
	s := NewShardedRTClientFromShards(shards)
	if s.Len() != 3 {
		t.Fatalf("len got=%d want=3", s.Len())
	}
	expectIdx := func(hashKey string) int {
		h := fnv.New32a()
		_, _ = h.Write([]byte(hashKey))
		return int(h.Sum32() % 3)
	}
	for _, id := range []uint64{1, 2, 3, 100, 999, 1<<63 - 1} {
		want := expectIdx("auction:" + strconv.FormatUint(id, 10))
		if got := s.IndexAuction(id); got != want {
			t.Fatalf("IndexAuction(%d) got=%d want=%d", id, got, want)
		}
		if s.ForAuction(id) != shards[want] {
			t.Fatalf("ForAuction(%d) returned wrong shard pointer", id)
		}

		want = expectIdx("session:" + strconv.FormatUint(id, 10))
		if got := s.IndexSession(id); got != want {
			t.Fatalf("IndexSession(%d) got=%d want=%d", id, got, want)
		}
		if s.ForSession(id) != shards[want] {
			t.Fatalf("ForSession(%d) returned wrong shard pointer", id)
		}

		want = expectIdx("room:" + strconv.FormatUint(id, 10))
		if got := s.IndexRoom(id); got != want {
			t.Fatalf("IndexRoom(%d) got=%d want=%d", id, got, want)
		}
		if s.ForRoom(id) != shards[want] {
			t.Fatalf("ForRoom(%d) returned wrong shard pointer", id)
		}
	}
	if s.IndexGlobal() != 0 {
		t.Fatalf("IndexGlobal got=%d want=0", s.IndexGlobal())
	}
	if s.ForGlobal() != shards[0] {
		t.Fatalf("ForGlobal must return shard 0")
	}
}

// TestShardedRTClientSingleShardRoutesAllToZero 验证退化场景：单 shard 时
// fnv 不参与路由，所有 ForXxx 都落到 shard 0。这是 miniredis 单进程测试的
// 兼容路径。
func TestShardedRTClientSingleShardRoutesAllToZero(t *testing.T) {
	c := newMiniredisShardClient(t)
	s := NewShardedRTClientFromShards([]*RedisRTClient{c})
	for _, id := range []uint64{1, 7, 42, 12345} {
		if s.IndexAuction(id) != 0 || s.IndexSession(id) != 0 || s.IndexRoom(id) != 0 {
			t.Fatalf("single-shard must always return 0, id=%d", id)
		}
		if s.ForAuction(id) != c || s.ForSession(id) != c || s.ForRoom(id) != c {
			t.Fatalf("single-shard ForXxx must return the only shard, id=%d", id)
		}
	}
	if s.ForGlobal() != c {
		t.Fatalf("ForGlobal must return the only shard")
	}
}

// TestShardedRTClientStableAcrossKeys 同一聚合根的 auction / session / room
// 三类 key 之间没有要求落同一 shard，但 **同一类** 的 ForXxx 与 IndexXxx 必须
// 同步：调用 ForAuction(id) 与 ForIndex(IndexAuction(id)) 必须指向同一 shard。
func TestShardedRTClientStableAcrossKeys(t *testing.T) {
	shards := []*RedisRTClient{
		newMiniredisShardClient(t),
		newMiniredisShardClient(t),
	}
	s := NewShardedRTClientFromShards(shards)
	for _, id := range []uint64{1, 2, 3, 5, 8, 13, 21, 34} {
		if s.ForAuction(id) != s.ForIndex(s.IndexAuction(id)) {
			t.Fatalf("ForAuction inconsistent with IndexAuction id=%d", id)
		}
		if s.ForSession(id) != s.ForIndex(s.IndexSession(id)) {
			t.Fatalf("ForSession inconsistent with IndexSession id=%d", id)
		}
		if s.ForRoom(id) != s.ForIndex(s.IndexRoom(id)) {
			t.Fatalf("ForRoom inconsistent with IndexRoom id=%d", id)
		}
	}
}

// TestShardedRTClientCloseShardsAllReturnsFirstError 验证 Close 把第一条错误
// 返回，并且所有 shard 的 Close 都会被尝试调用。
func TestShardedRTClientCloseAll(t *testing.T) {
	shards := []*RedisRTClient{
		newMiniredisShardClient(t),
		newMiniredisShardClient(t),
	}
	s := NewShardedRTClientFromShards(shards)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 关闭后再 Ping 任一 shard 应失败（go-redis 关闭后 Ping 返回 ErrClosed）。
	if err := shards[0].Ping(t.Context()).Err(); err == nil {
		t.Fatalf("expected error after Close on shard 0")
	}
}
