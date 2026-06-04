package redis

import (
	"context"
	"fmt"
	"time"

	"aieas_backend/internal/infra/observability/metrics"
)

const redisPoolStatsInterval = 5 * time.Second

type RedisPoolStatsGroup struct {
	Prefix  string
	Sharded *ShardedRTClient
}

// StartPoolStatsCollector 启动一个后台 goroutine，周期性把每组 Redis pool 快照写入
// metrics。instance 命名与 metrics hook 保持一致：如 "rt-<idx>",
// "rt-worker-<idx>", "pubsub-<idx>", "ranking-<idx>", Cache 为 "cache"。
func StartPoolStatsCollector(ctx context.Context, registry *metrics.Registry, cache *RedisCacheClient, groups ...RedisPoolStatsGroup) {
	if registry == nil || !registry.Enabled() {
		return
	}
	collect := func() {
		for _, group := range groups {
			if group.Sharded == nil {
				continue
			}
			prefix := group.Prefix
			if prefix == "" {
				prefix = "rt"
			}
			for i, shard := range group.Sharded.Shards() {
				if shard == nil || shard.Client == nil {
					continue
				}
				registry.ObserveRedisPoolStats(fmt.Sprintf("%s-%d", prefix, i), shard.PoolStats())
			}
		}
		if cache != nil && cache.Client != nil {
			registry.ObserveRedisPoolStats("cache", cache.PoolStats())
		}
	}
	go func() {
		ticker := time.NewTicker(redisPoolStatsInterval)
		defer ticker.Stop()
		collect()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collect()
			}
		}
	}()
}
