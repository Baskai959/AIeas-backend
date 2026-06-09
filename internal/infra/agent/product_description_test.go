package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	auctionports "aieas_backend/internal/modules/auction/ports"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
)

func TestLiveAnalysisClientRequestsAsyncReport(t *testing.T) {
	var gotReq struct {
		Prompt          string                 `json:"prompt"`
		CallbackURL     string                 `json:"callback_url"`
		CallbackHeaders map[string]string      `json:"callback_headers"`
		CallbackContext map[string]interface{} `json:"callback_context"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"request_id":"agent-1","status":"ACCEPTED","message":"直播总结任务已受理，完成后将通过 callback_url 回调。"}`))
	}))
	defer server.Close()

	client := NewLiveAnalysisClient(appconfig.AgentConfig{LiveAnalysisURL: server.URL, Timeout: appconfig.Duration(time.Second)})
	got, err := client.RequestLiveAnalysis(t.Context(), liveanalysisports.AsyncRequestInput{
		Prompt:      "帮我总结商家id为u_2001最近一场直播情况。",
		CallbackURL: "http://backend/api/v1/live-analysis/callback",
		CallbackHeaders: map[string]string{
			"X-Callback-Key": "callback-key",
		},
		CallbackContext: map[string]interface{}{
			"taskId": "lar_1",
		},
	})
	if err != nil {
		t.Fatalf("request live analysis: %v", err)
	}
	if got.RequestID != "agent-1" || got.Status != "ACCEPTED" {
		t.Fatalf("unexpected accepted result: %+v", got)
	}
	if gotReq.Prompt != "帮我总结商家id为u_2001最近一场直播情况。" ||
		gotReq.CallbackURL != "http://backend/api/v1/live-analysis/callback" ||
		gotReq.CallbackHeaders["X-Callback-Key"] != "callback-key" ||
		gotReq.CallbackContext["taskId"] != "lar_1" {
		t.Fatalf("unexpected request payload: %+v", gotReq)
	}
}

func TestNewProductDescriptionClientUsesDedicatedTimeout(t *testing.T) {
	client := NewProductDescriptionClient(appconfig.AgentConfig{
		ProductDescriptionURL:     "http://127.0.0.1:8000/api/v1/product-description",
		ProductDescriptionTimeout: appconfig.Duration(2 * time.Minute),
		Timeout:                   appconfig.Duration(time.Second),
	})
	if client.client.Timeout != 2*time.Minute {
		t.Fatalf("expected product description timeout 2m, got %s", client.client.Timeout)
	}
}

func TestLiveAnalysisClientReturnsAsyncAgentErrorMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":false,"request_id":"","status":"FAILED","message":"模型服务超时"}`))
	}))
	defer server.Close()

	client := NewLiveAnalysisClient(appconfig.AgentConfig{LiveAnalysisURL: server.URL, Timeout: appconfig.Duration(time.Second)})
	if _, err := client.RequestLiveAnalysis(t.Context(), liveanalysisports.AsyncRequestInput{Prompt: "prompt", CallbackURL: "http://backend/callback"}); err == nil || !strings.Contains(err.Error(), "模型服务超时") {
		t.Fatal("expected agent error")
	}
}

func TestProductAuditClientUsesConfiguredCallbackURL(t *testing.T) {
	var gotCallbackURL string
	var gotCallbackHeaders string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if err := r.ParseMultipartForm(2 << 20); err != nil {
			t.Fatalf("parse multipart form: %v", err)
		}
		gotCallbackURL = r.FormValue("callback_url")
		gotCallbackHeaders = r.FormValue("callback_headers")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"requestId":"audit-1","status":"ACCEPTED"}`))
	}))
	defer server.Close()

	cfg := appconfig.Default().Agent
	cfg.ProductAuditURL = server.URL
	cfg.Timeout = appconfig.Duration(time.Second)
	client := NewProductAuditClient(cfg)
	if _, err := client.AuditProduct(t.Context(), auctionports.ProductAuditInput{ProductText: "商品标题：茶盏"}); err != nil {
		t.Fatalf("request product audit: %v", err)
	}
	if gotCallbackURL != "http://127.0.0.1:8888/api/v1/auctions/audit/callback" {
		t.Fatalf("unexpected product audit callback url: %q", gotCallbackURL)
	}
	if gotCallbackHeaders != "" {
		t.Fatalf("expected no default callback headers, got %q", gotCallbackHeaders)
	}
}

func TestLiveAuctionHookClientPostsQuestion(t *testing.T) {
	var gotReq struct {
		SessionID string `json:"session_id"`
		Question  string `json:"question"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode hook request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	client := NewLiveAuctionHookClient(appconfig.AgentConfig{
		LiveAuctionHookURL: server.URL,
		Timeout:            appconfig.Duration(time.Second),
	})
	if err := client.InvokeLiveAgentHook(t.Context(), "70001", "直播间70001开播了"); err != nil {
		t.Fatalf("invoke live auction hook: %v", err)
	}
	if gotReq.SessionID != "70001" || gotReq.Question != "直播间70001开播了" {
		t.Fatalf("unexpected hook request: %+v", gotReq)
	}
}
