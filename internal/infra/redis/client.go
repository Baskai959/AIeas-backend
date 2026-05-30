// Package redis 中的 client.go 定义按职责拆分的 Redis 客户端封装：
//
//   - RedisRTClient 用于实时路径（拍卖状态 / 出价 / Stream / 锁 / 在线计数等强一致 + 低延迟操作）。
//   - RedisCacheClient 用于查询缓存（L2 缓存层、幂等记录等可丢失/可重建数据）。
//
// 两者均通过具名结构 + 嵌入 *redisgo.Client 实现：
//   - 方法集等价于 *redisgo.Client，可以直接传给依赖原始客户端接口的组件（如 ScriptClient）。
//   - 类型不可互换，避免 RT 与 Cache 误用同一个连接池 / 实例。
//
// RT 路径在 v2 引入 ShardedRTClient：把 RT 实例拆成多个 shard，按聚合根
// （auctionID / sessionID / roomID）的 fnv32 哈希路由。同一聚合根的所有 key
// 落到同一 shard，保证 Lua EVAL 与 multi-key 命令成立。
package redis

import (
	"context"
	"errors"
	"hash/fnv"
	"strconv"

	"aieas_backend/internal/config"

	redisgo "github.com/redis/go-redis/v9"
)

// RedisRTClient 是实时路径专用的 Redis 客户端封装（单 shard 视图）。
type RedisRTClient struct {
	*redisgo.Client
}

// RedisCacheClient 是查询缓存路径专用的 Redis 客户端封装。
type RedisCacheClient struct {
	*redisgo.Client
}

// OpenRT 打开 RT 实例的连接（基于 RedisInstanceConfig）。
func OpenRT(ctx context.Context, cfg config.RedisInstanceConfig) (*RedisRTClient, error) {
	client, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &RedisRTClient{Client: client}, nil
}

// OpenCache 打开 Cache 实例的连接（基于 RedisInstanceConfig）。
func OpenCache(ctx context.Context, cfg config.RedisInstanceConfig) (*RedisCacheClient, error) {
	client, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &RedisCacheClient{Client: client}, nil
}

// ShardedRTClient 持有多个 RT 实例（shard），并按聚合根的 fnv32 哈希路由。
//
// 路由约定：
//   - auction:<id>:* → ForAuction(id)
//   - live_session:<id>:* → ForSession(id)
//   - live_session:<id>:* → ForRoom(id)
//   - 全局 key（ws:instances / ws:instance:<id> / online:instance:<id> / DLQ /
//     auction:active_streams） → ForGlobal()，固定 shard 0；active_streams 是
//     例外：每 shard 一份，由 EventLog 的 per-shard 实例分别维护。
//
// 同一聚合根的所有 key 必须落到同一 shard，否则 Lua EVAL 与 multi-key 命令会被
// Redis 拒绝（在 cluster 模式下）/ 在裸分片模式下导致逻辑错乱。
type ShardedRTClient struct {
	shards []*RedisRTClient
}

// NewShardedRTClient 基于配置构造一个 ShardedRTClient。
//
// 至少 1 个 shard；逐个 OpenRT，任一失败则把已开的连接关掉再返回错误。
func NewShardedRTClient(ctx context.Context, shardCfgs []config.RedisInstanceConfig) (*ShardedRTClient, error) {
	if len(shardCfgs) == 0 {
		return nil, errors.New("redis sharded rt: at least one shard required")
	}
	shards := make([]*RedisRTClient, 0, len(shardCfgs))
	for i, cfg := range shardCfgs {
		client, err := OpenRT(ctx, cfg)
		if err != nil {
			for _, c := range shards {
				_ = c.Close()
			}
			return nil, err
		}
		_ = i
		shards = append(shards, client)
	}
	return &ShardedRTClient{shards: shards}, nil
}

// NewShardedRTClientFromShards 直接基于已经打开的 RT 客户端组装 ShardedRTClient，
// 主要给单元测试 / miniredis 场景使用。
func NewShardedRTClientFromShards(shards []*RedisRTClient) *ShardedRTClient {
	if len(shards) == 0 {
		return nil
	}
	dup := make([]*RedisRTClient, len(shards))
	copy(dup, shards)
	return &ShardedRTClient{shards: dup}
}

// Len 返回 shard 数量。
func (s *ShardedRTClient) Len() int {
	if s == nil {
		return 0
	}
	return len(s.shards)
}

// Shards 返回 shard 列表的浅拷贝。调用方禁止修改返回切片元素。
func (s *ShardedRTClient) Shards() []*RedisRTClient {
	if s == nil {
		return nil
	}
	out := make([]*RedisRTClient, len(s.shards))
	copy(out, s.shards)
	return out
}

// Index 返回给定 hashKey 落到的 shard 索引（fnv32 % len）。
func (s *ShardedRTClient) Index(hashKey string) int {
	if s == nil || len(s.shards) == 0 {
		return 0
	}
	if len(s.shards) == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(hashKey))
	return int(h.Sum32() % uint32(len(s.shards)))
}

// ForIndex 返回指定 index 的 shard 客户端；越界时 panic（调用方应通过 Index/Len 校验）。
func (s *ShardedRTClient) ForIndex(idx int) *RedisRTClient {
	if s == nil || idx < 0 || idx >= len(s.shards) {
		return nil
	}
	return s.shards[idx]
}

// ForAuction 返回该 auctionID 应落到的 shard。
func (s *ShardedRTClient) ForAuction(auctionID uint64) *RedisRTClient {
	return s.ForIndex(s.IndexAuction(auctionID))
}

// IndexAuction 返回该 auctionID 应落到的 shard 索引。
func (s *ShardedRTClient) IndexAuction(auctionID uint64) int {
	return s.Index("auction:" + strconv.FormatUint(auctionID, 10))
}

// ForSession 返回该 sessionID 应落到的 shard。
func (s *ShardedRTClient) ForSession(sessionID uint64) *RedisRTClient {
	return s.ForIndex(s.IndexSession(sessionID))
}

// IndexSession 返回该 sessionID 应落到的 shard 索引。
func (s *ShardedRTClient) IndexSession(sessionID uint64) int {
	return s.Index("session:" + strconv.FormatUint(sessionID, 10))
}

// ForRoom 返回该 roomID 应落到的 shard。
func (s *ShardedRTClient) ForRoom(roomID uint64) *RedisRTClient {
	return s.ForIndex(s.IndexRoom(roomID))
}

// IndexRoom 返回该 roomID 应落到的 shard 索引。
func (s *ShardedRTClient) IndexRoom(roomID uint64) int {
	return s.Index("room:" + strconv.FormatUint(roomID, 10))
}

// ForGlobal 返回承载全局 key 的 shard，固定 shard 0。
//
// 所有"找不到聚合根"的 key（ws:instances / online:instance:* / bid_record:dlq 等）
// 走这里；同一进程内可观测可枚举。
func (s *ShardedRTClient) ForGlobal() *RedisRTClient {
	return s.ForIndex(0)
}

// IndexGlobal 返回全局 shard 索引（固定 0）。
func (s *ShardedRTClient) IndexGlobal() int { return 0 }

// Close 关闭所有 shard 的连接，把第一条错误返回。
func (s *ShardedRTClient) Close() error {
	if s == nil {
		return nil
	}
	var firstErr error
	for _, shard := range s.shards {
		if shard == nil {
			continue
		}
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
