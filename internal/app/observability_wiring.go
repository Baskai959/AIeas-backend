package app

import (
	"context"
	"net/http"
	"strings"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/infra/observability/metrics"
	httptransport "aieas_backend/internal/transport/http"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// registerObservabilityRoutes 注册 /metrics、/healthz、/readyz 三类运维端点。
//
// 这些路径已在 transport/http.IsObservabilitySkipPath 中登记，所以会自动跳过
// MetricsMiddleware / RateLimiter / AuditMiddleware（C2、C5 约定的运维旁路）。
//
//   - /metrics：Prometheus 文本格式，可选 Bearer token 鉴权（MetricsAuth）；
//     metrics 禁用时由 Registry.Handler() 自身返回 503。
//   - /healthz：纯 liveness 探针。进程存活即 200，不依赖任何下游。
//   - /readyz：依次执行 ReadinessProbes（mysql/redis/scripts 等）；任意失败 503。
func registerObservabilityRoutes(h *server.Hertz, cfg appconfig.ObservabilityConfig, registry *metrics.Registry, probes map[string]httptransport.ReadinessProbe) {
	livenessPath := strings.TrimSpace(cfg.Health.LivenessPath)
	if livenessPath == "" {
		livenessPath = "/healthz"
	}
	readinessPath := strings.TrimSpace(cfg.Health.ReadinessPath)
	if readinessPath == "" {
		readinessPath = "/readyz"
	}
	h.GET(livenessPath, func(ctx context.Context, c *hertzapp.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"status": "ok"})
	})
	h.GET(readinessPath, httptransport.ReadinessHandler(3*time.Second, probes))

	if !cfg.Metrics.Enabled {
		return
	}
	metricsPath := strings.TrimSpace(cfg.Metrics.Path)
	if metricsPath == "" {
		metricsPath = strings.TrimSpace(cfg.MetricsPath)
	}
	if metricsPath == "" {
		metricsPath = "/metrics"
	}
	handler := registry.Handler()
	auth := httptransport.MetricsAuth(cfg.Metrics.AuthToken)
	h.GET(metricsPath, auth, func(ctx context.Context, c *hertzapp.RequestContext) {
		// 用 net/http 适配桥接 promhttp 的 Handler：把 hertz 请求转写成 stdlib
		// http.Request → 调用 promhttp Handler → 再把响应写回 hertz Response。
		serveStdHTTP(c, handler)
	})
}

// serveStdHTTP 把一个 net/http.Handler 适配到 hertz RequestContext。
// 仅用于运维端点（/metrics）：拷贝必要的 method/path/header/body，调用 handler，
// 再把状态码 / header / body 写回 hertz 响应。
func serveStdHTTP(c *hertzapp.RequestContext, handler http.Handler) {
	req, err := http.NewRequest(string(c.Method()), string(c.Request.URI().FullURI()), nil)
	if err != nil {
		c.AbortWithStatus(consts.StatusInternalServerError)
		return
	}
	c.Request.Header.VisitAll(func(k, v []byte) {
		req.Header.Add(string(k), string(v))
	})
	rec := newResponseRecorder()
	handler.ServeHTTP(rec, req)
	for k, vs := range rec.Header() {
		for _, v := range vs {
			c.Response.Header.Add(k, v)
		}
	}
	c.Response.SetStatusCode(rec.statusCode)
	c.Response.SetBodyRaw(rec.body)
}

// responseRecorder 是 http.ResponseWriter 的最小化实现：仅缓存 status / header / body，
// 用于把 promhttp Handler 的输出转写到 hertz Response。
type responseRecorder struct {
	header     http.Header
	body       []byte
	statusCode int
	wroteHead  bool
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{header: make(http.Header), statusCode: consts.StatusOK}
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHead {
		r.WriteHeader(consts.StatusOK)
	}
	r.body = append(r.body, p...)
	return len(p), nil
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHead {
		return
	}
	r.statusCode = code
	r.wroteHead = true
}
