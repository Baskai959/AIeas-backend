package http

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

const (
	contextRequestID      = "request_id"
	contextIdempotencyKey = "idempotency_key"
)

type AuditSink interface {
	Create(ctx context.Context, log *domain.AuditLog) error
}

func RequestIDMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		requestID := strings.TrimSpace(string(c.GetHeader("X-Request-Id")))
		if requestID == "" {
			requestID = strings.TrimSpace(string(c.GetHeader("X-Trace-Id")))
		}
		if requestID == "" {
			requestID = fmt.Sprintf("req_%d", time.Now().UnixNano())
		}
		c.Set(contextRequestID, requestID)
		c.Set("trace_id", requestID)
		c.Response.Header.Set("X-Request-Id", requestID)
		c.Next(ctx)
	}
}

func RecoveryMiddleware(logger *slog.Logger) app.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic recovered",
					"request_id", RequestID(c),
					"method", string(c.Method()),
					"path", string(c.Path()),
					"panic", recovered,
				)
				AbortError(c, consts.StatusInternalServerError, 90001, "系统内部错误", nil)
			}
		}()
		c.Next(ctx)
	}
}

func RoleAuth(roles ...domain.Role) app.HandlerFunc {
	allowed := make(map[domain.Role]struct{}, len(roles))
	for _, role := range roles {
		allowed[role] = struct{}{}
	}
	return func(ctx context.Context, c *app.RequestContext) {
		role := AuthRole(c)
		if _, ok := allowed[role]; !ok {
			AbortError(c, consts.StatusForbidden, 10003, "无访问权限", nil)
			return
		}
		c.Next(ctx)
	}
}

func RequireIdempotencyKey() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		key := strings.TrimSpace(string(c.GetHeader("Idempotency-Key")))
		if key == "" {
			AbortError(c, consts.StatusBadRequest, 20011, "缺少幂等键", nil)
			return
		}
		c.Set(contextIdempotencyKey, key)
		c.Next(ctx)
	}
}

type IdempotencyStore interface {
	Get(ctx context.Context, key string) (IdempotencyRecord, bool, error)
	Set(ctx context.Context, key string, record IdempotencyRecord, ttl time.Duration) error
}

type IdempotencyRecord struct {
	Fingerprint string
	StatusCode  int
	ContentType string
	Body        []byte
	CreatedAt   time.Time
}

type MemoryIdempotencyStore struct {
	mu      sync.RWMutex
	ttl     time.Duration
	records map[string]memoryIdempotencyRecord
}

type memoryIdempotencyRecord struct {
	record    IdempotencyRecord
	expiresAt time.Time
}

func NewMemoryIdempotencyStore(ttl time.Duration) *MemoryIdempotencyStore {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &MemoryIdempotencyStore{ttl: ttl, records: make(map[string]memoryIdempotencyRecord)}
}

func (s *MemoryIdempotencyStore) Get(ctx context.Context, key string) (IdempotencyRecord, bool, error) {
	_ = ctx
	now := time.Now().UTC()
	s.mu.RLock()
	cached, ok := s.records[key]
	s.mu.RUnlock()
	if !ok {
		return IdempotencyRecord{}, false, nil
	}
	if now.After(cached.expiresAt) {
		s.mu.Lock()
		if current, exists := s.records[key]; exists && now.After(current.expiresAt) {
			delete(s.records, key)
		}
		s.mu.Unlock()
		return IdempotencyRecord{}, false, nil
	}
	record := cached.record
	record.Body = append([]byte(nil), record.Body...)
	return record, true, nil
}

func (s *MemoryIdempotencyStore) Set(ctx context.Context, key string, record IdempotencyRecord, ttl time.Duration) error {
	_ = ctx
	if ttl <= 0 {
		ttl = s.ttl
	}
	record.Body = append([]byte(nil), record.Body...)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key] = memoryIdempotencyRecord{record: record, expiresAt: record.CreatedAt.Add(ttl)}
	s.cleanupIdempotencyLocked(record.CreatedAt)
	return nil
}

func (s *MemoryIdempotencyStore) cleanupIdempotencyLocked(now time.Time) {
	for key, cached := range s.records {
		if now.After(cached.expiresAt) {
			delete(s.records, key)
		}
	}
}

func IdempotencyMiddleware(store IdempotencyStore, ttl time.Duration) app.HandlerFunc {
	if store == nil {
		store = NewMemoryIdempotencyStore(ttl)
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return func(ctx context.Context, c *app.RequestContext) {
		processIdempotency(ctx, c, store, ttl, func(ctx context.Context, c *app.RequestContext) {
			c.Next(ctx)
		})
	}
}

func WithIdempotency(store IdempotencyStore, ttl time.Duration, next app.HandlerFunc) app.HandlerFunc {
	if store == nil {
		store = NewMemoryIdempotencyStore(ttl)
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return func(ctx context.Context, c *app.RequestContext) {
		processIdempotency(ctx, c, store, ttl, next)
	}
}

func processIdempotency(ctx context.Context, c *app.RequestContext, store IdempotencyStore, ttl time.Duration, next app.HandlerFunc) {
	key := strings.TrimSpace(string(c.GetHeader("Idempotency-Key")))
	if key == "" {
		AbortError(c, consts.StatusBadRequest, 20011, "缺少幂等键", nil)
		return
	}
	c.Set(contextIdempotencyKey, key)
	fingerprint := idempotencyFingerprint(c, key)
	cacheKey := idempotencyCacheKey(c, key)
	if cached, ok, err := store.Get(ctx, cacheKey); err != nil {
		AbortError(c, consts.StatusInternalServerError, 90001, "系统内部错误", nil)
		return
	} else if ok {
		if cached.Fingerprint != fingerprint {
			AbortError(c, consts.StatusConflict, 20012, "幂等键请求内容冲突", nil)
			return
		}
		c.Response.SetStatusCode(cached.StatusCode)
		if cached.ContentType != "" {
			c.Response.Header.Set("Content-Type", cached.ContentType)
		}
		c.Response.SetBodyRaw(append([]byte(nil), cached.Body...))
		return
	}

	next(ctx, c)
	if !shouldCacheIdempotencyResponse(c) {
		return
	}
	contentType := string(c.Response.Header.ContentType())
	_ = store.Set(ctx, cacheKey, IdempotencyRecord{
		Fingerprint: fingerprint,
		StatusCode:  c.Response.StatusCode(),
		ContentType: contentType,
		Body:        append([]byte(nil), c.Response.Body()...),
		CreatedAt:   time.Now().UTC(),
	}, ttl)
}

func idempotencyCacheKey(c *app.RequestContext, key string) string {
	return strings.Join([]string{AuthUserID(c), strings.ToUpper(string(c.Method())), string(c.Path()), key}, "|")
}

func idempotencyFingerprint(c *app.RequestContext, key string) string {
	h := sha256.New()
	h.Write([]byte(strings.ToUpper(string(c.Method()))))
	h.Write([]byte{'\n'})
	h.Write(c.Path())
	h.Write([]byte{'\n'})
	h.Write([]byte(key))
	h.Write([]byte{'\n'})
	h.Write(c.Request.Body())
	return hex.EncodeToString(h.Sum(nil))
}

func shouldCacheIdempotencyResponse(c *app.RequestContext) bool {
	status := c.Response.StatusCode()
	if status < consts.StatusOK || status >= consts.StatusMultipleChoices {
		return false
	}
	var response struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(c.Response.Body(), &response); err != nil {
		return false
	}
	return response.Code == 0
}

type RateLimiter struct {
	mu                     sync.Mutex
	limit                  int
	window                 time.Duration
	buckets                map[string]*rateBucket
	enabledFunc            func(context.Context) bool
	distributed            DistributedRateLimitStore
	distributedEnabledFunc func(context.Context) bool
}

type DistributedRateLimitStore interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration, cost int, now time.Time) (bool, error)
}

type rateBucket struct {
	windowStart time.Time
	count       int
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	if limit <= 0 {
		limit = 120
	}
	if window <= 0 {
		window = time.Minute
	}
	return &RateLimiter{limit: limit, window: window, buckets: make(map[string]*rateBucket)}
}

func (l *RateLimiter) SetEnabledFunc(fn func(context.Context) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabledFunc = fn
}

func (l *RateLimiter) SetDistributedStore(store DistributedRateLimitStore) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.distributed = store
}

func (l *RateLimiter) SetDistributedEnabledFunc(fn func(context.Context) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.distributedEnabledFunc = fn
}

func (l *RateLimiter) Middleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.Path())
		if IsObservabilitySkipPath(path) || isWebSocketPath(path) {
			c.Next(ctx)
			return
		}
		if !l.enabled(ctx) {
			c.Next(ctx)
			return
		}
		key := clientIP(c) + ":" + string(c.Method()) + ":" + string(c.Path())
		now := time.Now()
		if !l.allow(key, now) {
			AbortError(c, consts.StatusTooManyRequests, 20029, "请求过于频繁", nil)
			return
		}
		if !l.allowDistributed(ctx, key, now) {
			AbortError(c, consts.StatusTooManyRequests, 20029, "请求过于频繁", nil)
			return
		}
		c.Next(ctx)
	}
}

func isWebSocketPath(path string) bool {
	return path == "/ws" || strings.HasPrefix(path, "/ws/")
}

func (l *RateLimiter) enabled(ctx context.Context) bool {
	l.mu.Lock()
	fn := l.enabledFunc
	l.mu.Unlock()
	if fn == nil {
		return true
	}
	return fn(ctx)
}

func (l *RateLimiter) allowDistributed(ctx context.Context, key string, now time.Time) bool {
	l.mu.Lock()
	store := l.distributed
	fn := l.distributedEnabledFunc
	limit := l.limit
	window := l.window
	l.mu.Unlock()
	if store == nil {
		return true
	}
	if fn != nil && !fn(ctx) {
		return true
	}
	allowed, err := store.Allow(ctx, key, limit, window, 1, now)
	if err != nil {
		// Redis L2 限流按 fail-open 处理：L1 已经保护单机，L2 故障不阻塞主链路。
		return true
	}
	return allowed
}

func (l *RateLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	bucket, ok := l.buckets[key]
	if !ok || now.Sub(bucket.windowStart) >= l.window {
		l.buckets[key] = &rateBucket{windowStart: now, count: 1}
		l.cleanupLocked(now)
		return true
	}
	if bucket.count >= l.limit {
		return false
	}
	bucket.count++
	return true
}

func (l *RateLimiter) cleanupLocked(now time.Time) {
	for key, bucket := range l.buckets {
		if now.Sub(bucket.windowStart) >= 2*l.window {
			delete(l.buckets, key)
		}
	}
}

func AuditMiddleware(sink AuditSink, logger *slog.Logger) app.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, c *app.RequestContext) {
		c.Next(ctx)
		if sink == nil || shouldSkipAudit(c) {
			return
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"method":     string(c.Method()),
			"path":       string(c.Path()),
			"status":     c.Response.StatusCode(),
			"request_id": RequestID(c),
		})
		log := &domain.AuditLog{
			OperatorID:   AuthUserID(c),
			OperatorRole: AuthRole(c),
			Action:       auditAction(c),
			TargetType:   "HTTP",
			TargetID:     auditTargetID(c),
			Payload:      payload,
			IP:           clientIP(c),
			UserAgent:    string(c.GetHeader("User-Agent")),
			CreatedAt:    time.Now().UTC(),
		}
		if err := sink.Create(ctx, log); err != nil {
			logger.Warn("write audit log failed", "request_id", RequestID(c), "error", err)
		}
	}
}

func shouldSkipAudit(c *app.RequestContext) bool {
	method := string(c.Method())
	if method == consts.MethodGet || method == consts.MethodHead || method == consts.MethodOptions {
		return true
	}
	if c.Response.StatusCode() == consts.StatusUnauthorized {
		return true
	}
	if strings.TrimSpace(AuthUserID(c)) == "" || strings.TrimSpace(string(AuthRole(c))) == "" {
		return true
	}
	path := string(c.Path())
	if IsObservabilitySkipPath(path) {
		return true
	}
	return path == "/ping"
}

func auditAction(c *app.RequestContext) string {
	return strings.ToUpper(string(c.Method())) + " " + auditRoutePath(c)
}

func auditTargetID(c *app.RequestContext) string {
	return auditRoutePath(c)
}

func auditRoutePath(c *app.RequestContext) string {
	if full := strings.TrimSpace(c.FullPath()); full != "" {
		return full
	}
	return string(c.Path())
}

func AuthUserID(c *app.RequestContext) string {
	return c.GetString(contextUserID)
}

func AuthRole(c *app.RequestContext) domain.Role {
	return domain.Role(c.GetString(contextRole))
}

func RequestID(c *app.RequestContext) string {
	if v, ok := c.Get(contextRequestID); ok {
		if requestID, ok := v.(string); ok && requestID != "" {
			return requestID
		}
	}
	return TraceID(c)
}

func IdempotencyKey(c *app.RequestContext) string {
	if v, ok := c.Get(contextIdempotencyKey); ok {
		if key, ok := v.(string); ok {
			return key
		}
	}
	return ""
}

func clientIP(c *app.RequestContext) string {
	if v := strings.TrimSpace(string(c.GetHeader("X-Forwarded-For"))); v != "" {
		if idx := strings.IndexByte(v, ','); idx >= 0 {
			return strings.TrimSpace(v[:idx])
		}
		return v
	}
	if v := strings.TrimSpace(string(c.GetHeader("X-Real-IP"))); v != "" {
		return v
	}
	return c.RemoteAddr().String()
}
