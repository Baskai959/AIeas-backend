package redis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"
)

type DistributedRateLimiter struct {
	scripts *ScriptRegistry
	keys    KeyBuilder
}

func NewDistributedRateLimiter(scripts *ScriptRegistry, keys KeyBuilder) *DistributedRateLimiter {
	return &DistributedRateLimiter{scripts: scripts, keys: keys}
}

func (l *DistributedRateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration, cost int, now time.Time) (bool, error) {
	if l == nil || l.scripts == nil {
		return true, fmt.Errorf("redis rate limiter not configured")
	}
	if limit <= 0 {
		limit = 120
	}
	if window <= 0 {
		window = time.Minute
	}
	if cost <= 0 {
		cost = 1
	}
	if now.IsZero() {
		now = time.Now()
	}
	refillPerMs := float64(limit) / float64(window.Milliseconds())
	if refillPerMs <= 0 {
		refillPerMs = float64(limit) / float64(time.Minute.Milliseconds())
	}
	ttl := 2 * window.Milliseconds()
	if ttl <= 0 {
		ttl = int64(time.Minute / time.Millisecond)
	}
	result, err := l.scripts.Eval(ctx, ScriptRateLimit, []string{l.keys.RateLimitBucket(key)}, limit, refillPerMs, now.UnixMilli(), ttl, cost)
	if err != nil {
		return true, err
	}
	values, ok := result.([]interface{})
	if !ok || len(values) == 0 {
		return true, fmt.Errorf("redis rate limiter unexpected result %T", result)
	}
	allowed, ok := values[0].(int64)
	if !ok {
		return true, fmt.Errorf("redis rate limiter unexpected allowed type %T", values[0])
	}
	return allowed == 1, nil
}

func (b KeyBuilder) RateLimitBucket(key string) string {
	sum := sha1.Sum([]byte(key))
	return b.key("ratelimit:bucket:%s", hex.EncodeToString(sum[:]))
}
