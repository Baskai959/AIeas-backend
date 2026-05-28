// Package http 中观测性相关的中间件：TracingMiddleware / MetricsMiddleware /
// MetricsAuth / 路径过滤辅助。
//
// 设计原则：
//   - **基数收敛**：metrics label 用 Hertz 路由模板（FullPath），无路由匹配时
//     回退到 path bucket，绝不出现 user_id / auction_id 等高基数维度。
//   - **顺序约定**：链路顺序固定为 Recovery → RequestID → Tracing → Metrics →
//     RateLimiter → Audit（C2）。Tracing 必须先于 Metrics，使 metric 的 label
//     可以从 span context 共享 trace_id（通过 trace 头透传，而非作为 label）。
//   - **健康检查跳过**：/metrics、/healthz、/readyz、/ping 不进入 Metrics /
//     RateLimiter / Audit，避免运维端探针抖高业务统计、亦避免被业务限流。
package http

import (
	"context"
	"crypto/subtle"
	"strings"
	"time"

	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	traceapi "go.opentelemetry.io/otel/trace"
)

// observabilitySkipPaths 是 metrics / 限流 / 审计统一跳过的低价值路径。
// 这些都是无用户身份、面向运维端的探针，强行参与会污染业务指标与限流桶。
var observabilitySkipPaths = map[string]struct{}{
	"/metrics": {},
	"/healthz": {},
	"/readyz":  {},
	"/ping":    {},
}

// IsObservabilitySkipPath 判断当前请求是否走观测性旁路。导出便于 RateLimiter /
// AuditMiddleware 在 middleware.go 中复用统一规则。
func IsObservabilitySkipPath(path string) bool {
	if path == "" {
		return false
	}
	_, ok := observabilitySkipPaths[path]
	return ok
}

// routeLabel 返回 metric 用的低基数 route label。
// 优先用 Hertz 的路由模板（如 "/api/v1/auctions/:id"），无匹配时回退到原始
// path 的第一段（避免把 64 位 ID 直接当 label）。
func routeLabel(c *app.RequestContext) string {
	if full := strings.TrimSpace(c.FullPath()); full != "" {
		return full
	}
	path := strings.TrimSpace(string(c.Path()))
	if path == "" {
		return "unknown"
	}
	return path
}

// TracingMiddleware 在每次请求开始时根据 propagator 抽取上游 trace context，
// 启动一个 Server kind 的 span，并把 ctx 透传给下游 handler。
//
// 当 provider 为 nil 或 disabled 时，仍会抽取 traceparent 头（otel 默认全局
// propagator 可用），但不会生成新 span：handler 拿到的 ctx 与 propagator 提取
// 出来的一致，方便上游已经在 trace 中的请求继续传播。
func TracingMiddleware(provider *tracing.Provider) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// 抽取上游 trace 头到 ctx
		ctx = extractTraceContext(ctx, c)
		if provider == nil || !provider.Enabled() {
			c.Next(ctx)
			return
		}
		tracer := provider.Tracer("aieas_backend.http")
		spanName := string(c.Method()) + " " + routeLabel(c)
		spanCtx, span := tracer.Start(ctx, spanName,
			traceapi.WithSpanKind(traceapi.SpanKindServer),
			traceapi.WithAttributes(
				semconv.HTTPRequestMethodKey.String(string(c.Method())),
				semconv.HTTPRoute(routeLabel(c)),
				semconv.URLPath(string(c.Path())),
				attribute.String("request_id", RequestID(c)),
			),
		)
		defer span.End()

		// 把 trace_id 写回响应头，方便客户端串联日志
		if sc := span.SpanContext(); sc.IsValid() {
			c.Response.Header.Set("X-Trace-Id", sc.TraceID().String())
		}

		c.Next(spanCtx)

		span.SetAttributes(
			semconv.HTTPResponseStatusCode(c.Response.StatusCode()),
		)
		if status := c.Response.StatusCode(); status >= 500 {
			span.SetStatus(codes.Error, "server error")
		}
	}
}

// extractTraceContext 用 hertz header 适配器把 traceparent / tracestate 提取出
// 来，写入到返回的 ctx 中。otel 全局 propagator 在 Setup 时已注册。
func extractTraceContext(ctx context.Context, c *app.RequestContext) context.Context {
	carrier := hertzHeaderCarrier{c: c}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// hertzHeaderCarrier 把 hertz RequestContext 适配成 otel 的 TextMapCarrier，
// 仅做请求侧 Get/Keys；Set 是无操作（注入由响应阶段的 Header.Set 完成）。
type hertzHeaderCarrier struct {
	c *app.RequestContext
}

func (h hertzHeaderCarrier) Get(key string) string {
	return string(h.c.GetHeader(key))
}

func (h hertzHeaderCarrier) Set(key, value string) {
	h.c.Request.Header.Set(key, value)
}

func (h hertzHeaderCarrier) Keys() []string {
	keys := make([]string, 0, 8)
	h.c.Request.Header.VisitAll(func(k, _ []byte) {
		keys = append(keys, string(k))
	})
	return keys
}

// 编译期断言：hertzHeaderCarrier 必须满足 propagation.TextMapCarrier 接口。
var _ propagation.TextMapCarrier = hertzHeaderCarrier{}

// MetricsMiddleware 在请求生命周期内：
//   - inflight gauge++ / 结束 --
//   - 结束后写 http_requests_total / http_request_duration_seconds
//   - 写入 request body / response body 体积直方图
//
// registry 为 nil 或 disabled 时整链 no-op；observabilitySkipPaths 中的请求
// 不进入统计（运维探针不应放大业务指标基数）。
func MetricsMiddleware(registry *metrics.Registry) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if registry == nil || !registry.Enabled() {
			c.Next(ctx)
			return
		}
		if IsObservabilitySkipPath(string(c.Path())) {
			c.Next(ctx)
			return
		}
		route := routeLabel(c)
		registry.IncHTTPInflight(route)
		start := time.Now()
		c.Next(ctx)
		elapsed := time.Since(start)
		registry.DecHTTPInflight(route)

		method := string(c.Method())
		status := c.Response.StatusCode()
		reqBytes := len(c.Request.Body())
		respBytes := len(c.Response.Body())
		registry.ObserveHTTP(method, route, status, elapsed, reqBytes, respBytes)
	}
}

// ReadinessProbe 是单个依赖的 readiness 检查闭包：返回 nil 表示就绪，否则
// /readyz 返回 503，并把 component → error.Error() 列入响应体。
type ReadinessProbe func(ctx context.Context) error

// ReadinessHandler 构造 /readyz 端点：按声明顺序串行执行所有 probe，任意一个
// 失败整体降级为 503。response payload 同时包含 status + 各依赖明细，方便运维
// 直接看出哪一个组件不健康。timeout<=0 时回退到 3s。
func ReadinessHandler(timeout time.Duration, probes map[string]ReadinessProbe) app.HandlerFunc {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	// 固定顺序：按 key 排序，避免 map 顺序在响应中漂移导致比较困难。
	names := make([]string, 0, len(probes))
	for name := range probes {
		names = append(names, name)
	}
	sortStrings(names)
	return func(ctx context.Context, c *app.RequestContext) {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		details := make(map[string]string, len(names))
		ready := true
		for _, name := range names {
			probe := probes[name]
			if probe == nil {
				details[name] = "ok"
				continue
			}
			if err := probe(probeCtx); err != nil {
				ready = false
				details[name] = err.Error()
				continue
			}
			details[name] = "ok"
		}
		status := consts.StatusOK
		statusText := "ok"
		if !ready {
			status = consts.StatusServiceUnavailable
			statusText = "not_ready"
		}
		c.JSON(status, map[string]any{
			"status":     statusText,
			"components": details,
		})
	}
}

// sortStrings 是一个最小化的 in-place 排序，避免引入 sort 包额外 import。
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// MetricsAuth 校验访问 /metrics 端点的 bearer token / X-Metrics-Token。
// token 为空时直接放行（与 Prometheus 同集群拉取的常见配置一致）。
func MetricsAuth(token string) app.HandlerFunc {
	expected := strings.TrimSpace(token)
	return func(ctx context.Context, c *app.RequestContext) {
		if expected == "" {
			c.Next(ctx)
			return
		}
		got := strings.TrimSpace(string(c.GetHeader("Authorization")))
		if strings.HasPrefix(got, "Bearer ") {
			got = strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
		} else {
			got = strings.TrimSpace(string(c.GetHeader("X-Metrics-Token")))
		}
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			c.AbortWithStatus(consts.StatusUnauthorized)
			return
		}
		c.Next(ctx)
	}
}
