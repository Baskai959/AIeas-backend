package http

import (
	"context"
	"testing"

	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func TestIsObservabilitySkipPath(t *testing.T) {
	cases := map[string]bool{
		"/metrics":     true,
		"/healthz":     true,
		"/readyz":      true,
		"/ping":        true,
		"":             false,
		"/api/v1/foo":  false,
		"/metrics/sub": false,
	}
	for path, want := range cases {
		if got := IsObservabilitySkipPath(path); got != want {
			t.Fatalf("IsObservabilitySkipPath(%q)=%v want %v", path, got, want)
		}
	}
}

func TestMetricsAuthEmptyTokenAllowsRequest(t *testing.T) {
	h := server.Default()
	h.Use(MetricsAuth(""))
	h.GET("/metrics", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/metrics", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 when token unset, got %d", resp.Code)
	}
}

func TestMetricsAuthRejectsMissingToken(t *testing.T) {
	h := server.Default()
	h.Use(MetricsAuth("secret-token"))
	h.GET("/metrics", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/metrics", nil)
	if resp.Code != consts.StatusUnauthorized {
		t.Fatalf("expected 401 when no token, got %d", resp.Code)
	}
}

func TestMetricsAuthAcceptsBearerToken(t *testing.T) {
	h := server.Default()
	h.Use(MetricsAuth("secret-token"))
	h.GET("/metrics", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/metrics", nil,
		ut.Header{Key: "Authorization", Value: "Bearer secret-token"},
	)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 with Bearer token, got %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestMetricsAuthAcceptsXMetricsTokenHeader(t *testing.T) {
	h := server.Default()
	h.Use(MetricsAuth("secret-token"))
	h.GET("/metrics", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/metrics", nil,
		ut.Header{Key: "X-Metrics-Token", Value: "secret-token"},
	)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 with X-Metrics-Token, got %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestMetricsAuthRejectsBadToken(t *testing.T) {
	h := server.Default()
	h.Use(MetricsAuth("secret-token"))
	h.GET("/metrics", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/metrics", nil,
		ut.Header{Key: "Authorization", Value: "Bearer wrong"},
	)
	if resp.Code != consts.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", resp.Code)
	}
}

func TestMetricsMiddlewareNoopWhenDisabled(t *testing.T) {
	h := server.Default()
	h.Use(MetricsMiddleware(metrics.NewNoop()))
	h.GET("/api/v1/x", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/api/v1/x", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 with noop registry, got %d", resp.Code)
	}
}

func TestMetricsMiddlewareSkipsObservabilityPaths(t *testing.T) {
	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "t"})
	h := server.Default()
	h.Use(MetricsMiddleware(reg))
	h.GET("/metrics", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/metrics", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 from /metrics passthrough, got %d", resp.Code)
	}
	// Should not have produced any HTTP metric for the skipped path.
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() == "t_http_requests_total" && len(fam.GetMetric()) > 0 {
			t.Fatalf("expected no http_requests_total samples for skip path, got %v", fam.GetMetric())
		}
	}
}

func TestMetricsMiddlewareRecordsForBusinessRoute(t *testing.T) {
	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "t"})
	h := server.Default()
	h.Use(MetricsMiddleware(reg))
	h.GET("/api/v1/auctions/:id", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/api/v1/auctions/42", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, fam := range families {
		if fam.GetName() != "t_http_requests_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			// route label should be the Hertz template, not the concrete /42.
			if labels["route"] == "/api/v1/auctions/:id" && labels["method"] == "GET" && labels["status"] == "2xx" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("expected http_requests_total sample with route template, families=%v", families)
	}
}

func TestTracingMiddlewareNoopProviderWritesNoTraceHeader(t *testing.T) {
	provider := tracing.NewNoop()
	h := server.Default()
	h.Use(RequestIDMiddleware(), TracingMiddleware(provider))
	h.GET("/api/v1/x", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/api/v1/x", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	if got := string(resp.Header().Get("X-Trace-Id")); got != "" {
		t.Fatalf("expected no X-Trace-Id with noop provider, got %q", got)
	}
}
