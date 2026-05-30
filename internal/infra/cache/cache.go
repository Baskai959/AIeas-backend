// Package cache 提供按 "L1 进程内 + L2 Redis" 两层组合的查询缓存能力。
//
// 设计要点：
//
//  1. 强职责隔离：本包只做 "短 TTL、可丢失、可重建" 的查询缓存，不参与 RT 实时
//     状态（拍卖/出价/锁/计数）。L2 实例由 Redis 拆分后的 RedisCacheClient 注入，
//     与 RT 实例物理隔离。
//
//  2. 三道防护（穿透 / 击穿 / 雪崩）：
//     - 穿透：用 negative cache（短 TTL 的占位值）吸收 "数据库也没有" 的 key。
//     - 击穿：基于 singleflight 合并对同一 key 的并发回源，避免热 key 把 DB 打挂。
//     - 雪崩：所有 TTL 在写入前叠加 ±jitter（默认 ±10%），让批量同时写入的缓存
//     过期时间散开，避免同一时刻雪崩回源。
//
//  3. 业务可拼装：暴露的是泛型 Cache[T any] 接口（通过 []byte 序列化层），上层
//     业务（ItemService 等）持有自己的具体缓存包装（如 ItemCache），通过 GetOrLoad
//     的 loader 闭包定义如何回源 + 如何序列化。
//
//  4. 观测可插拔：通过 Observer 接口而不是直接依赖 *metrics.Registry，避免 cache
//     包反向依赖 observability 包；server.go 把 Registry 适配进 Observer 后注入。
package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound 表示 key 在缓存中不存在（既不在 L1 也不在 L2）。
// 调用方据此触发回源；GetOrLoad 内部会自动消化此错误。
var ErrNotFound = errors.New("cache: not found")

// ErrNegativeHit 表示命中负缓存（已知数据库也没有该 key）。
// 调用方应直接把它视为 "未找到"，不要再回源。
var ErrNegativeHit = errors.New("cache: negative hit")

// Source 标识一次查询命中来自哪一层，用于指标 / 调试。
type Source string

const (
	SourceL1       Source = "l1"       // 命中 L1（进程内）
	SourceL2       Source = "l2"       // 命中 L2（Redis）
	SourceLoader   Source = "loader"   // 缓存全部 miss，调用 loader 回源
	SourceNegative Source = "negative" // 命中负缓存（占位 nil）
)

// Loader 是 GetOrLoad 在缓存全 miss 时回源使用的函数。
//
// 返回 (value, true, nil) 表示数据库找到该 key；(nil, false, nil) 表示数据库
// 也不存在（会写入负缓存以吸收穿透）；(_, _, err) 表示回源失败，不写缓存。
type Loader[T any] func(ctx context.Context) (value T, found bool, err error)

// Cache 是面向业务的层叠缓存接口（语义层，不直接序列化）。
//
// 实现类（LayeredCache[T]）负责把 T 与 []byte 之间的序列化下沉到 codec，并维护
// L1/L2 的一致性写入与失效。
type Cache[T any] interface {
	// Get 仅查询缓存，不回源；未命中返回 ErrNotFound，命中负缓存返回 ErrNegativeHit。
	Get(ctx context.Context, key string) (T, Source, error)

	// GetOrLoad 在缓存全 miss 时调用 loader 回源，并写入两层缓存：
	//   - found=true → 写入正向缓存（TTL+jitter）。
	//   - found=false → 写入负缓存（NegativeTTL+jitter）。
	GetOrLoad(ctx context.Context, key string, loader Loader[T]) (T, Source, error)

	// Set 显式写入正向缓存；ttl<=0 时使用默认 TTL。
	Set(ctx context.Context, key string, value T, ttl time.Duration) error

	// Invalidate 同时清掉两层；失败返回 nil 也是允许的（缓存可丢失），
	// 但实现应至少尝试清 L1 再尝试清 L2 再返回首个错误。
	Invalidate(ctx context.Context, keys ...string) error
}

// Codec 把业务类型 T 与缓存载荷之间互相转换。
//
// Encode 必须是确定性的；Decode 收到 nil 视为 "负缓存命中"（由调用方决定语义）。
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(raw []byte) (T, error)
}

// Observer 是一个可选的指标钩子；nil 实现时所有方法 no-op。
//
// 通过接口而不是直接依赖 metrics 包，让 cache 包对 observability 反向解耦。
type Observer interface {
	// ObserveGet 记录一次缓存读取的来源与耗时。
	ObserveGet(name string, source Source, d time.Duration)
	// ObserveSet 记录一次缓存写入的耗时与是否失败。
	ObserveSet(name string, d time.Duration, err error)
	// ObserveInvalidate 记录一次失效操作的耗时与是否失败。
	ObserveInvalidate(name string, d time.Duration, err error)
}

// nopObserver 在未注入 Observer 时使用，所有方法 no-op。
type nopObserver struct{}

func (nopObserver) ObserveGet(string, Source, time.Duration)       {}
func (nopObserver) ObserveSet(string, time.Duration, error)        {}
func (nopObserver) ObserveInvalidate(string, time.Duration, error) {}
