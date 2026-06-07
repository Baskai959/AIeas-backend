package http

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"aieas_backend/internal/domain"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	redisgo "github.com/redis/go-redis/v9"
)

func TestRequestIDMiddlewareSetsTraceCompatibleResponse(t *testing.T) {
	h := server.Default()
	h.Use(RequestIDMiddleware())
	h.GET("/ok", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, map[string]string{"requestId": RequestID(c)})
	})

	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/ok", nil, ut.Header{Key: "X-Request-Id", Value: "req-test"})
	if resp.Code != 200 {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	if got := string(resp.Header().Get("X-Request-Id")); got != "req-test" {
		t.Fatalf("expected response request id header, got %q", got)
	}
	var body Response
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.TraceID != "req-test" {
		t.Fatalf("expected trace_id to use request id, got %q", body.TraceID)
	}
}

func TestRoleAuthAndIdempotencyMiddleware(t *testing.T) {
	h := server.Default()
	h.Use(RequestIDMiddleware())
	h.POST("/merchant", func(ctx context.Context, c *app.RequestContext) {
		c.Set(contextRole, string(domain.RoleMerchant))
		c.Next(ctx)
	}, RoleAuth(domain.RoleMerchant), RequireIdempotencyKey(), func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, map[string]string{"idem": IdempotencyKey(c)})
	})

	missing := ut.PerformRequest(h.Engine, consts.MethodPost, "/merchant", nil)
	if missing.Code != 400 || !strings.Contains(missing.Body.String(), "20011") {
		t.Fatalf("expected missing idempotency response, got status=%d body=%s", missing.Code, missing.Body.String())
	}

	ok := ut.PerformRequest(h.Engine, consts.MethodPost, "/merchant", nil, ut.Header{Key: "Idempotency-Key", Value: "idem-1"})
	if ok.Code != 200 {
		t.Fatalf("expected 200, got %d body=%s", ok.Code, ok.Body.String())
	}
}

func TestIdempotencyMiddlewareCachesSameBodyAndRejectsConflict(t *testing.T) {
	h := server.Default()
	h.Use(RequestIDMiddleware())
	store := NewMemoryIdempotencyStore(time.Hour)
	called := 0
	h.POST("/merchant", func(ctx context.Context, c *app.RequestContext) {
		c.Set(contextUserID, "u_2001")
		c.Set(contextRole, string(domain.RoleMerchant))
		c.Next(ctx)
	}, RoleAuth(domain.RoleMerchant), WithIdempotency(store, time.Hour, func(ctx context.Context, c *app.RequestContext) {
		called++
		var req struct {
			Name string `json:"name"`
		}
		if err := c.BindJSON(&req); err != nil {
			WriteError(c, 400, 20001, "参数不合法", nil)
			return
		}
		WriteSuccess(c, map[string]interface{}{"name": req.Name, "called": called, "idem": IdempotencyKey(c)})
	}))

	first := ut.PerformRequest(h.Engine, consts.MethodPost, "/merchant", &ut.Body{Body: strings.NewReader(`{"name":"same"}`), Len: len(`{"name":"same"}`)}, ut.Header{Key: "Idempotency-Key", Value: "idem-cache"}, ut.Header{Key: "Content-Type", Value: "application/json"})
	if first.Code != 200 || called != 1 {
		t.Fatalf("expected first request to execute once, status=%d called=%d body=%s", first.Code, called, first.Body.String())
	}
	second := ut.PerformRequest(h.Engine, consts.MethodPost, "/merchant", &ut.Body{Body: strings.NewReader(`{"name":"same"}`), Len: len(`{"name":"same"}`)}, ut.Header{Key: "Idempotency-Key", Value: "idem-cache"}, ut.Header{Key: "Content-Type", Value: "application/json"})
	if second.Code != 200 || called != 1 || second.Body.String() != first.Body.String() {
		t.Fatalf("expected cached response, first=%s second=%s called=%d", first.Body.String(), second.Body.String(), called)
	}
	conflict := ut.PerformRequest(h.Engine, consts.MethodPost, "/merchant", &ut.Body{Body: strings.NewReader(`{"name":"different"}`), Len: len(`{"name":"different"}`)}, ut.Header{Key: "Idempotency-Key", Value: "idem-cache"}, ut.Header{Key: "Content-Type", Value: "application/json"})
	if conflict.Code != 409 || !strings.Contains(conflict.Body.String(), "20012") || called != 1 {
		t.Fatalf("expected conflict without handler execution, status=%d called=%d body=%s", conflict.Code, called, conflict.Body.String())
	}
}

func TestAuditMiddlewareSkipsUnauthorizedWithoutOperator(t *testing.T) {
	h := server.Default()
	sink := &captureAuditSink{}
	h.Use(RequestIDMiddleware(), AuditMiddleware(sink, nil))
	h.POST("/api/v1/auth/refresh", func(ctx context.Context, c *app.RequestContext) {
		AbortError(c, consts.StatusUnauthorized, 10002, "访问令牌无效或已过期", nil)
	})

	resp := ut.PerformRequest(h.Engine, consts.MethodPost, "/api/v1/auth/refresh", nil)
	if resp.Code != consts.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", resp.Code, resp.Body.String())
	}
	if len(sink.logs) != 0 {
		t.Fatalf("expected no audit log for unauthorized request, got %+v", sink.logs)
	}
}

func TestAuditMiddlewareWritesAuthenticatedMutation(t *testing.T) {
	h := server.Default()
	sink := &captureAuditSink{}
	h.Use(RequestIDMiddleware(), AuditMiddleware(sink, nil))
	h.POST("/api/v1/items", func(ctx context.Context, c *app.RequestContext) {
		c.Set(contextUserID, "2")
		c.Set(contextRole, string(domain.RoleMerchant))
		WriteSuccess(c, map[string]string{"ok": "true"})
	})

	resp := ut.PerformRequest(h.Engine, consts.MethodPost, "/api/v1/items", nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	if len(sink.logs) != 1 {
		t.Fatalf("expected one audit log, got %d", len(sink.logs))
	}
	log := sink.logs[0]
	if log.OperatorID != "2" || log.OperatorRole != domain.RoleMerchant || log.Action != "POST /api/v1/items" {
		t.Fatalf("unexpected audit log: %+v", log)
	}
}

func TestAuditMiddlewareUsesRouteTemplateForLongPath(t *testing.T) {
	h := server.Default()
	sink := &captureAuditSink{}
	h.Use(RequestIDMiddleware(), AuditMiddleware(sink, nil))
	h.POST("/api/v1/ai-assistant/approvals/:requestID/decision", func(ctx context.Context, c *app.RequestContext) {
		c.Set(contextUserID, "2001")
		c.Set(contextRole, string(domain.RoleMerchant))
		WriteSuccess(c, map[string]string{"ok": "true"})
	})

	rawPath := "/api/v1/ai-assistant/approvals/ai-approval-90000021-1780768457282415000/decision"
	resp := ut.PerformRequest(h.Engine, consts.MethodPost, rawPath, nil)
	if resp.Code != consts.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	if len(sink.logs) != 1 {
		t.Fatalf("expected one audit log, got %d", len(sink.logs))
	}
	log := sink.logs[0]
	if log.Action != "POST /api/v1/ai-assistant/approvals/:requestID/decision" {
		t.Fatalf("unexpected audit action: %q", log.Action)
	}
	if len(log.Action) > 64 {
		t.Fatalf("audit action should fit schema, len=%d action=%q", len(log.Action), log.Action)
	}
	if log.TargetID != "/api/v1/ai-assistant/approvals/:requestID/decision" {
		t.Fatalf("unexpected audit target id: %q", log.TargetID)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(log.Payload, &payload); err != nil {
		t.Fatalf("unmarshal audit payload: %v", err)
	}
	if payload["path"] != rawPath {
		t.Fatalf("payload should keep raw path, got %+v", payload)
	}
}

type captureAuditSink struct {
	logs []domain.AuditLog
}

func (s *captureAuditSink) Create(ctx context.Context, log *domain.AuditLog) error {
	_ = ctx
	s.logs = append(s.logs, *log)
	return nil
}

func TestRedisIdempotencyStoreCachesWithTTL(t *testing.T) {
	client := newFakeRedisIdempotencyClient()
	store := NewRedisIdempotencyStore(client, "idem:test")
	record := IdempotencyRecord{Fingerprint: "fp", StatusCode: 200, ContentType: "application/json", Body: []byte(`{"code":0}`)}
	if err := store.Set(context.Background(), "u1|POST|/pay|k1", record, 50*time.Millisecond); err != nil {
		t.Fatalf("set redis idempotency: %v", err)
	}
	cached, ok, err := store.Get(context.Background(), "u1|POST|/pay|k1")
	if err != nil || !ok {
		t.Fatalf("expected cached record, ok=%v err=%v", ok, err)
	}
	if cached.Fingerprint != "fp" || string(cached.Body) != `{"code":0}` {
		t.Fatalf("unexpected cached record: %+v body=%s", cached, string(cached.Body))
	}
	if client.lastTTL <= 0 || client.lastTTL > time.Second {
		t.Fatalf("expected ttl propagated to redis set, got %s", client.lastTTL)
	}
	time.Sleep(70 * time.Millisecond)
	_, ok, err = store.Get(context.Background(), "u1|POST|/pay|k1")
	if err != nil || ok {
		t.Fatalf("expected expired record miss, ok=%v err=%v", ok, err)
	}
}

type fakeRedisIdempotencyClient struct {
	mu      sync.Mutex
	values  map[string][]byte
	expires map[string]time.Time
	lastTTL time.Duration
}

func newFakeRedisIdempotencyClient() *fakeRedisIdempotencyClient {
	return &fakeRedisIdempotencyClient{values: make(map[string][]byte), expires: make(map[string]time.Time)}
}

func (f *fakeRedisIdempotencyClient) Get(ctx context.Context, key string) *redisgo.StringCmd {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()
	if expiresAt, ok := f.expires[key]; ok && time.Now().After(expiresAt) {
		delete(f.values, key)
		delete(f.expires, key)
	}
	value, ok := f.values[key]
	if !ok {
		return redisgo.NewStringResult("", redisgo.Nil)
	}
	return redisgo.NewStringResult(string(value), nil)
}

func (f *fakeRedisIdempotencyClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redisgo.StatusCmd {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTTL = expiration
	switch typed := value.(type) {
	case []byte:
		f.values[key] = append([]byte(nil), typed...)
	case string:
		f.values[key] = []byte(typed)
	default:
		encoded, _ := json.Marshal(typed)
		f.values[key] = encoded
	}
	if expiration > 0 {
		f.expires[key] = time.Now().Add(expiration)
	}
	return redisgo.NewStatusResult("OK", nil)
}

func TestRateLimiterRejectsAfterLimit(t *testing.T) {
	h := server.Default()
	h.Use(RequestIDMiddleware(), NewRateLimiter(1, time.Minute).Middleware())
	h.GET("/limited", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})

	first := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.1"})
	if first.Code != 200 {
		t.Fatalf("expected first request success, got %d", first.Code)
	}
	second := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.1"})
	if second.Code != 429 {
		t.Fatalf("expected second request rate limited, got %d body=%s", second.Code, second.Body.String())
	}
}

func TestRateLimiterSkipsWebSocketPaths(t *testing.T) {
	h := server.Default()
	h.Use(RequestIDMiddleware(), NewRateLimiter(1, time.Minute).Middleware())
	h.GET("/ws/live-rooms/:room_id", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})

	first := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/live-rooms/1", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.9"})
	second := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/live-rooms/1", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.9"})
	if first.Code != 200 || second.Code != 200 {
		t.Fatalf("expected websocket paths to skip generic rate limiter, first=%d second=%d", first.Code, second.Code)
	}
}

func TestRateLimiterCanBeDisabledBySwitch(t *testing.T) {
	h := server.Default()
	limiter := NewRateLimiter(1, time.Minute)
	enabled := false
	limiter.SetEnabledFunc(func(ctx context.Context) bool {
		_ = ctx
		return enabled
	})
	h.Use(RequestIDMiddleware(), limiter.Middleware())
	h.GET("/limited", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})

	first := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.2"})
	second := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.2"})
	if first.Code != 200 || second.Code != 200 {
		t.Fatalf("expected disabled limiter to allow both requests, first=%d second=%d", first.Code, second.Code)
	}

	enabled = true
	third := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.3"})
	fourth := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.3"})
	if third.Code != 200 || fourth.Code != 429 {
		t.Fatalf("expected enabled limiter to reject second request, third=%d fourth=%d", third.Code, fourth.Code)
	}
}

type failingDistributedRateLimitStore struct{}

func (f failingDistributedRateLimitStore) Allow(ctx context.Context, key string, limit int, window time.Duration, cost int, now time.Time) (bool, error) {
	return false, errors.New("redis unavailable")
}

type denyingDistributedRateLimitStore struct{}

func (d denyingDistributedRateLimitStore) Allow(ctx context.Context, key string, limit int, window time.Duration, cost int, now time.Time) (bool, error) {
	return false, nil
}

func TestRateLimiterL2FailOpen(t *testing.T) {
	h := server.Default()
	limiter := NewRateLimiter(10, time.Minute)
	limiter.SetDistributedStore(failingDistributedRateLimitStore{})
	h.Use(RequestIDMiddleware(), limiter.Middleware())
	h.GET("/limited", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})

	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.4"})
	if resp.Code != 200 {
		t.Fatalf("expected L2 failure to fail-open, got %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestRateLimiterL2CanRejectWhenEnabled(t *testing.T) {
	h := server.Default()
	limiter := NewRateLimiter(10, time.Minute)
	limiter.SetDistributedStore(denyingDistributedRateLimitStore{})
	limiter.SetDistributedEnabledFunc(func(ctx context.Context) bool { return true })
	h.Use(RequestIDMiddleware(), limiter.Middleware())
	h.GET("/limited", func(ctx context.Context, c *app.RequestContext) {
		WriteSuccess(c, "ok")
	})

	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/limited", nil, ut.Header{Key: "X-Real-IP", Value: "10.0.0.5"})
	if resp.Code != 429 {
		t.Fatalf("expected L2 denial, got %d body=%s", resp.Code, resp.Body.String())
	}
}
