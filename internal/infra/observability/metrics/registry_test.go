package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewNoopAndDefaultAreDisabled(t *testing.T) {
	if NewNoop().Enabled() {
		t.Fatalf("NewNoop should be disabled")
	}
	if Default().Enabled() {
		t.Fatalf("Default should be disabled")
	}
	// nil receiver safety
	var nilReg *Registry
	if nilReg.Enabled() {
		t.Fatalf("nil registry must report disabled")
	}
	nilReg.ObserveHTTP("GET", "/x", 200, time.Millisecond, 0, 0) // must not panic
	nilReg.ObserveBidStage("state", "ok", time.Millisecond)
	nilReg.IncBidRoute("lua_enter", "attempt")
	nilReg.IncBidDuplicate()
	nilReg.IncWSConnect()
	nilReg.ObserveRedisLua("foo", time.Millisecond, "")
}

func TestNewEnabledRegistersCollectors(t *testing.T) {
	r := New(Options{Enabled: true, Namespace: "test"})
	if !r.Enabled() {
		t.Fatalf("expected enabled registry")
	}
	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(families) == 0 {
		t.Fatalf("expected registered metric families, got 0")
	}
}

func TestRegistryHandlerServesMetricsWhenEnabled(t *testing.T) {
	r := New(Options{Enabled: true, Namespace: "ns"})
	r.ObserveHTTP("GET", "/api/x", 200, 5*time.Millisecond, 12, 34)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "ns_http_requests_total") {
		t.Fatalf("missing namespaced metric in response: %s", body)
	}
}

func TestRegistryHandlerReturns503WhenDisabled(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	NewNoop().Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for disabled handler, got %d", w.Code)
	}
}

func TestObserveHTTPIncrementsCounterWithStatusBucket(t *testing.T) {
	r := New(Options{Enabled: true})
	r.ObserveHTTP("GET", "/a", 201, time.Millisecond, 0, 0)
	r.ObserveHTTP("GET", "/a", 500, time.Millisecond, 0, 0)
	if v := counterVecValue(t, r.httpRequestsTotal, "GET", "/a", "2xx"); v != 1 {
		t.Fatalf("2xx expected 1, got %v", v)
	}
	if v := counterVecValue(t, r.httpRequestsTotal, "GET", "/a", "5xx"); v != 1 {
		t.Fatalf("5xx expected 1, got %v", v)
	}
}

func TestHTTPInflightGaugeIncDec(t *testing.T) {
	r := New(Options{Enabled: true})
	r.IncHTTPInflight("/a")
	r.IncHTTPInflight("/a")
	r.DecHTTPInflight("/a")
	if v := gaugeVecValue(t, r.httpInflight, "/a"); v != 1 {
		t.Fatalf("expected inflight=1, got %v", v)
	}
}

func TestObserveBidAndDuplicate(t *testing.T) {
	r := New(Options{Enabled: true})
	r.ObserveBid("accepted", "ok", 2*time.Millisecond)
	r.ObserveBidStage("lua_place_bid", "accepted", 2*time.Millisecond)
	r.IncBidRoute("lua_enter", "attempt")
	r.IncBidDuplicate()
	if v := counterVecValue(t, r.bidTotal, "accepted", "ok"); v != 1 {
		t.Fatalf("bidTotal accepted=ok expected 1, got %v", v)
	}
	if v := histogramVecCount(t, r.bidStageDuration, "lua_place_bid", "accepted"); v != 1 {
		t.Fatalf("bidStageDuration expected 1, got %v", v)
	}
	if v := counterVecValue(t, r.bidRouteTotal, "lua_enter", "attempt"); v != 1 {
		t.Fatalf("bidRouteTotal expected 1, got %v", v)
	}
	if v := counterValue(t, r.bidDuplicateTotal); v != 1 {
		t.Fatalf("bidDuplicateTotal expected 1, got %v", v)
	}
}

func TestObserveRedisCommandRecordsErrorsOnly(t *testing.T) {
	r := New(Options{Enabled: true})
	r.ObserveRedisCommand("default", "HGET", time.Millisecond, nil)
	r.ObserveRedisCommand("default", "HGET", time.Millisecond, errors.New("boom"))
	if v := counterVecValue(t, r.redisCommandErrors, "default", "HGET"); v != 1 {
		t.Fatalf("redisCommandErrors expected 1, got %v", v)
	}
}

func TestObserveWSBroadcastAndConnect(t *testing.T) {
	r := New(Options{Enabled: true})
	r.IncWSConnect()
	r.IncWSConnect()
	r.IncWSDisconnect("normal")
	r.ObserveWSBroadcast(time.Millisecond, 7)
	if v := gaugeValue(t, r.wsConnections); v != 1 {
		t.Fatalf("wsConnections expected 1, got %v", v)
	}
	if v := counterValue(t, r.wsBroadcastFanoutTotal); v != 7 {
		t.Fatalf("wsBroadcastFanoutTotal expected 7, got %v", v)
	}
}

func TestWSHandshakeRejectReasonNormalization(t *testing.T) {
	r := New(Options{Enabled: true})
	for _, reason := range []string{"rate_limit_ip", "rate_limit_user", "rate_limit_auction", "draining", "auth", "bad_request"} {
		r.IncWSHandshakeReject(reason)
		if v := counterVecValue(t, r.wsHandshakeRejectTotal, reason); v != 1 {
			t.Fatalf("reason %s expected 1, got %v", reason, v)
		}
	}
	r.IncWSHandshakeReject("per_user_123")
	if v := counterVecValue(t, r.wsHandshakeRejectTotal, "unknown"); v != 1 {
		t.Fatalf("unknown normalized reason expected 1, got %v", v)
	}
	r.IncWSDraining()
	if v := counterValue(t, r.wsDrainingTotal); v != 1 {
		t.Fatalf("wsDrainingTotal expected 1, got %v", v)
	}
}

func TestStatusBucketBoundaries(t *testing.T) {
	cases := map[int]string{
		99: "unknown", 100: "1xx", 199: "1xx",
		200: "2xx", 299: "2xx",
		300: "3xx", 399: "3xx",
		400: "4xx", 499: "4xx",
		500: "5xx", 599: "5xx",
	}
	for code, want := range cases {
		if got := HTTPStatusLabel(code); got != want {
			t.Fatalf("HTTPStatusLabel(%d)=%q want %q", code, got, want)
		}
	}
}

// ---- helpers ----------------------------------------------------------------

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.GetCounter().GetValue()
}

func counterVecValue(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	c, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	return counterValue(t, c)
}

func histogramVecCount(t *testing.T, vec *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	h, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	metric, ok := h.(prometheus.Metric)
	if !ok {
		t.Fatalf("histogram does not implement prometheus.Metric")
	}
	var m dto.Metric
	if err := metric.Write(&m); err != nil {
		t.Fatalf("histogram write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("gauge write: %v", err)
	}
	return m.GetGauge().GetValue()
}

func gaugeVecValue(t *testing.T, vec *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	g, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	return gaugeValue(t, g)
}
