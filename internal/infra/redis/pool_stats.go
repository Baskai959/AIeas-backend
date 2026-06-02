package redis

import (
	"context"
	"fmt"
	"time"

	"aieas_backend/internal/infra/observability/metrics"
)

const redisPoolStatsInterval = 5 * time.Second

// StartPoolStatsCollector 启动一个后台 goroutine，周期性把每个 RT shard 与 Cache
// 实例的连接池快照写入 metrics。instance 命名与 metrics hook 保持一致：RT 为
// "rt-<idx>"，Cache 为 "cache"。goroutine 随 ctx 取消而退出。
func StartPoolStatsCollector(ctx context.Context, registry *metrics.Registry, shardedRT *ShardedRTClient, cache *RedisCacheClient) {
	if registry == nil || !registry.Enabled() {
		return
	}
	collect := func() {
		if shardedRT != nil {
			for i, shard := range shardedRT.Shards() {
				if shard == nil || shard.Client == nil {
					continue
				}
				registry.ObserveRedisPoolStats(fmt.Sprintf("rt-%d", i), shard.PoolStats())
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
