// Package cache 中的 cache_test.go 用 miniredis 把 L2 替换为内存 Redis，
// 覆盖 LayeredCache 的关键路径：
//   - L1 命中（不打 L2）。
//   - L2 命中后回填 L1。
//   - 完全 miss 触发 loader 并写入两层。
//   - loader found=false 写入负缓存（防穿透）。
//   - 并发回源被 singleflight 合并（防击穿）。
//   - Invalidate 清空 L1+L2。
package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"

	redisinfra "aieas_backend/internal/infra/redis"
)

type sample struct {
	ID   uint64
	Name string
}

func newTestCache(t *testing.T) (*LayeredCache[sample], *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	l2 := &redisinfra.RedisCacheClient{Client: rdb}
	c := New[sample](l2, JSONCodec[sample]{}, Options{
		Name:        "test",
		L1Capacity:  16,
		TTL:         time.Minute,
		L1TTL:       time.Minute,
		NegativeTTL: 30 * time.Second,
		// Rand 固定为 0.5（即 jitter=0），让断言可重复。
		Rand: func() float64 { return 0.5 },
	})
	return c, mr
}

func TestLayeredCacheGetOrLoadHitsL1AfterFirstLoad(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	var calls atomic.Int32
	loader := func(ctx context.Context) (sample, bool, error) {
		calls.Add(1)
		return sample{ID: 1, Name: "alpha"}, true, nil
	}

	v, src, err := c.GetOrLoad(ctx, "1", loader)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	if v.Name != "alpha" || src != SourceLoader {
		t.Fatalf("expected loader src + alpha, got %+v %s", v, src)
	}

	// 第二次调用应当全部命中 L1，loader 不再被触发。
	v, src, err = c.GetOrLoad(ctx, "1", loader)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if src != SourceL1 {
		t.Fatalf("expected SourceL1, got %s", src)
	}
	if v.Name != "alpha" {
		t.Fatalf("unexpected value: %+v", v)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times, want 1", got)
	}
}

func TestLayeredCacheL2RefillsL1(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	loader := func(ctx context.Context) (sample, bool, error) {
		return sample{ID: 2, Name: "beta"}, true, nil
	}
	if _, _, err := c.GetOrLoad(ctx, "2", loader); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 把 L1 抹掉，模拟进程刚启动只剩 L2 的场景。
	c.l1.invalidate("2")

	v, src, err := c.Get(ctx, "2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if src != SourceL2 || v.Name != "beta" {
		t.Fatalf("expected SourceL2 beta, got %s %+v", src, v)
	}
	// 现在 L1 应当被回填了。
	if _, _, ok := c.l1.get("2"); !ok {
		t.Fatal("expected L1 refill after L2 hit")
	}
}

func TestLayeredCacheNegativeCachePreventsRepeatedLoaderForMissingKey(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	var calls atomic.Int32
	loader := func(ctx context.Context) (sample, bool, error) {
		calls.Add(1)
		return sample{}, false, nil
	}

	if _, _, err := c.GetOrLoad(ctx, "missing", loader); err != ErrNegativeHit {
		t.Fatalf("expected ErrNegativeHit, got %v", err)
	}
	if _, _, err := c.GetOrLoad(ctx, "missing", loader); err != ErrNegativeHit {
		t.Fatalf("expected ErrNegativeHit on second call, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times, want 1 (negative cache should absorb)", got)
	}
}

func TestLayeredCacheSingleflightCoalescesConcurrentLoads(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	var calls atomic.Int32
	gate := make(chan struct{})
	loader := func(ctx context.Context) (sample, bool, error) {
		calls.Add(1)
		<-gate
		return sample{ID: 3, Name: "gamma"}, true, nil
	}

	const concurrent = 16
	var wg sync.WaitGroup
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func() {
			defer wg.Done()
			if _, _, err := c.GetOrLoad(ctx, "3", loader); err != nil {
				t.Errorf("load: %v", err)
			}
		}()
	}
	// 给所有 goroutine 充足时间进入 singleflight 队列。
	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times under concurrency, want 1 (singleflight)", got)
	}
}

func TestLayeredCacheInvalidateClearsBothLayers(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	loader := func(ctx context.Context) (sample, bool, error) {
		return sample{ID: 4, Name: "delta"}, true, nil
	}
	if _, _, err := c.GetOrLoad(ctx, "4", loader); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := c.Invalidate(ctx, "4"); err != nil {
		t.Fatalf("invalidate: %v", err)
	}

	if _, _, ok := c.l1.get("4"); ok {
		t.Fatal("expected L1 cleared")
	}
	if _, _, err := c.Get(ctx, "4"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after invalidate, got %v", err)
	}
}

func TestL1CacheEvictsOldestWhenOverCapacity(t *testing.T) {
	l1 := newL1Cache(2)
	l1.set("a", []byte("1"), false, time.Minute)
	l1.set("b", []byte("2"), false, time.Minute)
	l1.set("c", []byte("3"), false, time.Minute) // 应当淘汰 "a"

	if _, _, ok := l1.get("a"); ok {
		t.Fatal("expected 'a' evicted")
	}
	if _, _, ok := l1.get("b"); !ok {
		t.Fatal("expected 'b' present")
	}
	if _, _, ok := l1.get("c"); !ok {
		t.Fatal("expected 'c' present")
	}
}

func TestL1CacheRespectsTTL(t *testing.T) {
	l1 := newL1Cache(8)
	l1.set("k", []byte("v"), false, 10*time.Millisecond)
	if _, _, ok := l1.get("k"); !ok {
		t.Fatal("expected hit before expiry")
	}
	time.Sleep(20 * time.Millisecond)
	if _, _, ok := l1.get("k"); ok {
		t.Fatal("expected miss after expiry")
	}
}
