// Package cache 中的 layered.go 实现 L1（进程内）+ L2（Redis）层叠缓存，
// 内置三道防护：
//
//  1. 防穿透 —— 数据库也不存在的 key 写入负缓存（占位 + 短 TTL），L1/L2 同步保留。
//  2. 防击穿 —— 通过 singleflight 合并对同一 key 的并发回源；只有一个 goroutine 真的打 DB。
//  3. 防雪崩 —— 所有写入 L1/L2 的 TTL 都叠加 ±jitter（默认 ±10%），让批量同时写入的
//                key 的过期时间错开，避免雪崩回源。
//
// 调用方拿到的是 Cache[T] 接口；本文件提供唯一实现 LayeredCache[T] 与构造方法 New。
package cache

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	redisgo "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	redisinfra "aieas_backend/internal/infra/redis"
)

// negativePayload 是写入 L2 的负缓存占位字节串。选择 "\x00" 仅占 1 字节、
// 不会与任何业务 codec 输出冲撞（业务 codec 输出 JSON / proto 等结构化字节）。
var negativePayload = []byte{0x00}

// Options 是 LayeredCache 的构造参数。
//
// 任何字段留零值时使用稳健默认值，便于业务调用 New 时只配最关心的项。
type Options struct {
	// Name 是缓存命名空间，用于 L2 key 前缀（"<Name>:<key>"）和 Observer 标签。
	// 推荐使用领域名（"item" / "live_room"）。空值会被替换为 "cache"。
	Name string

	// L1Capacity 是 L1 LRU 容量；<=0 时使用 1024。
	L1Capacity int

	// TTL 是正向缓存的基准 TTL；<=0 时使用 5 分钟。
	TTL time.Duration

	// L1TTL 是 L1 内存层正向缓存的 TTL；0 时取 min(TTL, 30s) 以避免 L1 与 L2 长期不一致。
	L1TTL time.Duration

	// NegativeTTL 是负缓存 TTL；<=0 时使用 30 秒。短 TTL 是有意为之，避免长时间错误地
	// 屏蔽掉新插入的数据。
	NegativeTTL time.Duration

	// JitterRatio 是 TTL 抖动比例（±jitter）；建议 0~0.5；<=0 或 >=1 时使用 0.1。
	JitterRatio float64

	// Observer 可选；nil 时使用 nopObserver。
	Observer Observer

	// Now 注入当前时间，便于测试；默认 time.Now。
	Now func() time.Time

	// Rand 注入随机源，便于测试；默认 rand.New(rand.NewSource(seed))，并发安全包装。
	Rand func() float64
}

// LayeredCache 是 Cache[T] 的具体实现：L1 内存 + L2 Redis + singleflight + 抖动 TTL。
type LayeredCache[T any] struct {
	name        string
	l1          *l1Cache
	l2          *redisinfra.RedisCacheClient
	codec       Codec[T]
	ttl         time.Duration
	l1TTL       time.Duration
	negativeTTL time.Duration
	jitterRatio float64
	observer    Observer
	now         func() time.Time
	rand        func() float64
	group       singleflight.Group

	// randMu 保护内部默认 rand 实例（math/rand 的 *Rand 不并发安全）。
	randMu  sync.Mutex
	randSrc *rand.Rand
}

// New 构造一个 LayeredCache[T]。l2 与 codec 必须非 nil；其余从 opts 取值或落到默认值。
func New[T any](l2 *redisinfra.RedisCacheClient, codec Codec[T], opts Options) *LayeredCache[T] {
	if l2 == nil {
		panic("cache.New: l2 RedisCacheClient is required")
	}
	if codec == nil {
		panic("cache.New: codec is required")
	}
	name := opts.Name
	if name == "" {
		name = "cache"
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	l1TTL := opts.L1TTL
	if l1TTL <= 0 {
		l1TTL = ttl
		if l1TTL > 30*time.Second {
			l1TTL = 30 * time.Second
		}
	}
	negTTL := opts.NegativeTTL
	if negTTL <= 0 {
		negTTL = 30 * time.Second
	}
	jitter := opts.JitterRatio
	if jitter <= 0 || jitter >= 1 {
		jitter = 0.1
	}
	observer := opts.Observer
	if observer == nil {
		observer = nopObserver{}
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	c := &LayeredCache[T]{
		name:        name,
		l1:          newL1Cache(opts.L1Capacity),
		l2:          l2,
		codec:       codec,
		ttl:         ttl,
		l1TTL:       l1TTL,
		negativeTTL: negTTL,
		jitterRatio: jitter,
		observer:    observer,
		now:         now,
	}
	if opts.Rand != nil {
		c.rand = opts.Rand
	} else {
		c.randSrc = rand.New(rand.NewSource(now().UnixNano()))
		c.rand = c.defaultRand
	}
	return c
}

func (c *LayeredCache[T]) defaultRand() float64 {
	c.randMu.Lock()
	defer c.randMu.Unlock()
	return c.randSrc.Float64()
}

// l2Key 拼接 namespace 前缀，避免不同业务的 key 在共用 Cache 实例时碰撞。
func (c *LayeredCache[T]) l2Key(key string) string {
	return c.name + ":" + key
}

// jittered 在 base 上叠加 ±ratio 抖动，并把负值兜底为 base/2，避免出现 0 或负 TTL。
func (c *LayeredCache[T]) jittered(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}
	delta := (c.rand()*2 - 1) * c.jitterRatio // ∈ (-ratio, ratio)
	d := time.Duration(float64(base) * (1 + delta))
	if d <= 0 {
		d = base / 2
	}
	return d
}

// Get 仅读缓存。
func (c *LayeredCache[T]) Get(ctx context.Context, key string) (T, Source, error) {
	start := c.now()
	value, source, err := c.lookup(ctx, key)
	c.observer.ObserveGet(c.name, source, c.now().Sub(start))
	return value, source, err
}

// lookup 是 Get / GetOrLoad 共用的两层查询逻辑。命中负缓存时返回 ErrNegativeHit，
// 完全 miss 时返回 ErrNotFound。
func (c *LayeredCache[T]) lookup(ctx context.Context, key string) (T, Source, error) {
	var zero T
	if raw, negative, ok := c.l1.get(key); ok {
		if negative {
			return zero, SourceL1, ErrNegativeHit
		}
		v, err := c.codec.Decode(raw)
		if err != nil {
			// L1 解码失败（不应发生）→ 主动失效，让上层走 loader。
			c.l1.invalidate(key)
			return zero, SourceL1, err
		}
		return v, SourceL1, nil
	}
	raw, err := c.l2.Get(ctx, c.l2Key(key)).Bytes()
	switch {
	case err == nil:
		if isNegativePayload(raw) {
			// 把负缓存回填到 L1，让后续命中走更便宜的内存路径。
			c.l1.set(key, nil, true, c.jittered(c.l1NegativeTTL()))
			return zero, SourceL2, ErrNegativeHit
		}
		v, decErr := c.codec.Decode(raw)
		if decErr != nil {
			// L2 内容损坏 → 主动失效（避免长期返回错误数据）。
			_ = c.l2.Del(ctx, c.l2Key(key)).Err()
			return zero, SourceL2, decErr
		}
		c.l1.set(key, raw, false, c.jittered(c.l1TTL))
		return v, SourceL2, nil
	case errors.Is(err, redisgo.Nil):
		return zero, SourceL2, ErrNotFound
	default:
		return zero, SourceL2, err
	}
}

// l1NegativeTTL 决定负缓存写入 L1 的 TTL：取 min(negativeTTL, l1TTL) 防止
// L1 上的负条目活得比 L2 还久。
func (c *LayeredCache[T]) l1NegativeTTL() time.Duration {
	if c.l1TTL > 0 && c.l1TTL < c.negativeTTL {
		return c.l1TTL
	}
	return c.negativeTTL
}

// GetOrLoad 缓存全 miss 时通过 loader 回源；同一 key 的并发回源被 singleflight 合并。
func (c *LayeredCache[T]) GetOrLoad(ctx context.Context, key string, loader Loader[T]) (T, Source, error) {
	if loader == nil {
		var zero T
		return zero, "", errors.New("cache: loader is nil")
	}
	start := c.now()
	value, source, err := c.lookup(ctx, key)
	switch {
	case err == nil:
		c.observer.ObserveGet(c.name, source, c.now().Sub(start))
		return value, source, nil
	case errors.Is(err, ErrNegativeHit):
		c.observer.ObserveGet(c.name, SourceNegative, c.now().Sub(start))
		var zero T
		return zero, SourceNegative, ErrNegativeHit
	case errors.Is(err, ErrNotFound):
		// 走回源；下面 singleflight 处理。
	default:
		// 缓存层故障（Redis 不可达 / 解码错）：直接打 DB，不写缓存，避免错误传播。
		c.observer.ObserveGet(c.name, source, c.now().Sub(start))
		v, found, ldErr := loader(ctx)
		if ldErr != nil {
			var zero T
			return zero, SourceLoader, ldErr
		}
		if !found {
			var zero T
			return zero, SourceLoader, ErrNegativeHit
		}
		return v, SourceLoader, nil
	}

	// 缓存确实 miss → singleflight 合并相同 key 的并发回源。
	type loadResult struct {
		value    T
		found    bool
		negative bool
	}
	res, err, _ := c.group.Do(key, func() (any, error) {
		// 在 group 内再次查 L1：如果在等待期间另一个 goroutine 已经把结果回填，
		// 直接复用。这是 singleflight 内常见的二次查表优化。
		if raw, negative, ok := c.l1.get(key); ok {
			if negative {
				return loadResult{negative: true}, nil
			}
			v, decErr := c.codec.Decode(raw)
			if decErr == nil {
				return loadResult{value: v, found: true}, nil
			}
		}
		v, found, ldErr := loader(ctx)
		if ldErr != nil {
			return loadResult{}, ldErr
		}
		if !found {
			c.writeNegative(ctx, key)
			return loadResult{negative: true}, nil
		}
		c.writePositive(ctx, key, v)
		return loadResult{value: v, found: true}, nil
	})
	c.observer.ObserveGet(c.name, SourceLoader, c.now().Sub(start))
	if err != nil {
		var zero T
		return zero, SourceLoader, err
	}
	out := res.(loadResult)
	if out.negative {
		var zero T
		return zero, SourceLoader, ErrNegativeHit
	}
	return out.value, SourceLoader, nil
}

// writePositive 把成功回源的值写入 L1 + L2，TTL 各自独立抖动。
func (c *LayeredCache[T]) writePositive(ctx context.Context, key string, value T) {
	start := c.now()
	raw, err := c.codec.Encode(value)
	if err != nil {
		c.observer.ObserveSet(c.name, c.now().Sub(start), err)
		return
	}
	c.l1.set(key, raw, false, c.jittered(c.l1TTL))
	setErr := c.l2.Set(ctx, c.l2Key(key), raw, c.jittered(c.ttl)).Err()
	c.observer.ObserveSet(c.name, c.now().Sub(start), setErr)
}

// writeNegative 把 "数据库无此 key" 写入两层负缓存。
func (c *LayeredCache[T]) writeNegative(ctx context.Context, key string) {
	start := c.now()
	c.l1.set(key, nil, true, c.jittered(c.l1NegativeTTL()))
	setErr := c.l2.Set(ctx, c.l2Key(key), negativePayload, c.jittered(c.negativeTTL)).Err()
	c.observer.ObserveSet(c.name, c.now().Sub(start), setErr)
}

// Set 显式写入正向缓存；ttl<=0 时使用默认 TTL。
func (c *LayeredCache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	start := c.now()
	raw, err := c.codec.Encode(value)
	if err != nil {
		c.observer.ObserveSet(c.name, c.now().Sub(start), err)
		return err
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	c.l1.set(key, raw, false, c.jittered(c.l1TTL))
	setErr := c.l2.Set(ctx, c.l2Key(key), raw, c.jittered(ttl)).Err()
	c.observer.ObserveSet(c.name, c.now().Sub(start), setErr)
	return setErr
}

// Invalidate 同步清掉两层缓存；先清 L1（即时生效），再清 L2（持久层）。
func (c *LayeredCache[T]) Invalidate(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	start := c.now()
	c.l1.invalidate(keys...)
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = c.l2Key(k)
	}
	err := c.l2.Del(ctx, full...).Err()
	c.observer.ObserveInvalidate(c.name, c.now().Sub(start), err)
	if errors.Is(err, redisgo.Nil) {
		return nil
	}
	return err
}

// isNegativePayload 判断 raw 是否是 1 字节的 negativePayload 占位。
func isNegativePayload(raw []byte) bool {
	return len(raw) == 1 && raw[0] == negativePayload[0]
}
