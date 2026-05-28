// health_handler_test.go 覆盖 /readyz 的 ReadinessHandler 行为：
//   - 所有 probe 都 nil → 200，body.status="ok"。
//   - 任一 probe 失败 → 503，body.status="not_ready"，components 含具体错误。
//   - probe 顺序稳定（按 key 排序），保证响应可比较。
//   - 空 probe map 时仍能返回 200，便于本地 dev 模式只挂 liveness。
package http

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func TestReadinessHandlerAllProbesOK(t *testing.T) {
	h := server.Default()
	h.GET("/readyz", ReadinessHandler(0, map[string]ReadinessProbe{
		"mysql": func(ctx context.Context) error { return nil },
		"redis": func(ctx context.Context) error { return nil },
	}))
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	body := decodeReadinessBody(t, resp.Body.Bytes())
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", body["status"])
	}
	comps, ok := body["components"].(map[string]any)
	if !ok {
		t.Fatalf("expected components map, got %v", body["components"])
	}
	if comps["mysql"] != "ok" || comps["redis"] != "ok" {
		t.Fatalf("expected ok components, got %v", comps)
	}
}

func TestReadinessHandlerFailureReturns503(t *testing.T) {
	h := server.Default()
	h.GET("/readyz", ReadinessHandler(0, map[string]ReadinessProbe{
		"mysql":   func(ctx context.Context) error { return nil },
		"redis":   func(ctx context.Context) error { return errors.New("connection refused") },
		"scripts": func(ctx context.Context) error { return nil },
	}))
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
	if resp.Code != consts.StatusServiceUnavailable {
		t.Fatalf("expected 503 when redis probe fails, got %d body=%s", resp.Code, resp.Body.String())
	}
	body := decodeReadinessBody(t, resp.Body.Bytes())
	if body["status"] != "not_ready" {
		t.Fatalf("expected status=not_ready, got %v", body["status"])
	}
	comps, _ := body["components"].(map[string]any)
	if comps["mysql"] != "ok" {
		t.Fatalf("expected mysql ok, got %v", comps["mysql"])
	}
	if comps["redis"] != "connection refused" {
		t.Fatalf("expected redis to surface the failure message, got %v", comps["redis"])
	}
	if comps["scripts"] != "ok" {
		t.Fatalf("expected scripts ok, got %v", comps["scripts"])
	}
}

func TestReadinessHandlerEmptyProbesIsReady(t *testing.T) {
	h := server.Default()
	h.GET("/readyz", ReadinessHandler(0, nil))
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 with no probes registered, got %d", resp.Code)
	}
	body := decodeReadinessBody(t, resp.Body.Bytes())
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok with no probes, got %v", body["status"])
	}
}

func TestReadinessHandlerNilProbeTreatedAsOK(t *testing.T) {
	h := server.Default()
	h.GET("/readyz", ReadinessHandler(0, map[string]ReadinessProbe{
		"placeholder": nil,
	}))
	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/readyz", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200 when probe is nil, got %d", resp.Code)
	}
}

func decodeReadinessBody(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("decode response %q: %v", string(data), err)
	}
	return body
}
