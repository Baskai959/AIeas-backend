// Package redis 中的 metricsHook 把每条命令的耗时写入 metrics.Registry。
//
// 与 redisotel.InstrumentTracing 等仪器化 Hook 同链路注册，互不干扰：
// otel hook 负责 span/trace，metricsHook 负责 Prometheus 直方图。
//
// 设计要点：
//   - 用 redisgo.Hook 接口，避免侵入业务代码。
//   - op label 取自 cmd.Name() 并 ToUpper，保持低基数（GET/SET/HSET ...）。
//   - instance label 从外部注入，单 Redis 部署默认 "default"（C8 约定）。
//   - registry 为 nil 或 Disabled 时整链 no-op；不分配额外内存。
package redis

import (
	"context"
	"net"
	"strings"
	"time"

	"aieas_backend/internal/infra/observability/metrics"

	redisgo "github.com/redis/go-redis/v9"
)

// metricsHook 是 redisgo.Hook 的最小实现：仅观测 ProcessHook / ProcessPipelineHook。
type metricsHook struct {
	registry *metrics.Registry
	instance string
}

// NewMetricsHook 返回一个 redis 客户端可注册的 metrics hook。
// registry 为 nil 时返回的 hook 在所有方法上都是 no-op。
func NewMetricsHook(registry *metrics.Registry, instance string) redisgo.Hook {
	if strings.TrimSpace(instance) == "" {
		instance = "default"
	}
	return &metricsHook{registry: registry, instance: instance}
}

// DialHook 不参与命令耗时统计，直接透传。
func (h *metricsHook) DialHook(next redisgo.DialHook) redisgo.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook 记录单条命令的耗时与错误。
func (h *metricsHook) ProcessHook(next redisgo.ProcessHook) redisgo.ProcessHook {
	return func(ctx context.Context, cmd redisgo.Cmder) error {
		if h.registry == nil || !h.registry.Enabled() {
			return next(ctx, cmd)
		}
		start := time.Now()
		err := next(ctx, cmd)
		h.registry.ObserveRedisCommand(h.instance, cmdLabel(cmd), time.Since(start), redisCommandError(err))
		return err
	}
}

// ProcessPipelineHook 把整条 pipeline 视为一次 "PIPELINE" 操作记录。
// 单独记录每条命令会显著放大基数；用聚合视图够日常排障。
func (h *metricsHook) ProcessPipelineHook(next redisgo.ProcessPipelineHook) redisgo.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redisgo.Cmder) error {
		if h.registry == nil || !h.registry.Enabled() {
			return next(ctx, cmds)
		}
		start := time.Now()
		err := next(ctx, cmds)
		h.registry.ObserveRedisCommand(h.instance, "PIPELINE", time.Since(start), redisCommandError(err))
		return err
	}
}

// cmdLabel 返回命令名的大写形式，供 op label 使用。
func cmdLabel(cmd redisgo.Cmder) string {
	if cmd == nil {
		return "UNKNOWN"
	}
	name := strings.ToUpper(strings.TrimSpace(cmd.Name()))
	if name == "" {
		return "UNKNOWN"
	}
	return name
}

// redisCommandError 把 redis.Nil 视为业务级"未命中"，不计为错误，
// 避免 GET / HGET 大量 miss 把 errors_total 抬高。
func redisCommandError(err error) error {
	if err == nil {
		return nil
	}
	if err == redisgo.Nil {
		return nil
	}
	return err
}
