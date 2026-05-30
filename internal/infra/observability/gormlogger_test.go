package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	gormlogger "gorm.io/gorm/logger"
)

func newTestGormLogger(t *testing.T) (gormlogger.Interface, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	base := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger := NewGormLogger(base, 200*time.Millisecond, true).LogMode(gormlogger.Info)
	return logger, buf
}

func decodeOnly(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	if buf.Len() == 0 {
		t.Fatalf("expected log output, got empty buffer")
	}
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("decode log line %q: %v", buf.String(), err)
	}
	return got
}

func TestGormSlogLogger_TraceNormalEmitsDebug(t *testing.T) {
	logger, buf := newTestGormLogger(t)
	begin := time.Now().Add(-50 * time.Millisecond)
	logger.Trace(context.Background(), begin,
		func() (string, int64) { return "SELECT 1", 1 },
		nil,
	)

	got := decodeOnly(t, buf)
	if got["level"] != "DEBUG" {
		t.Fatalf("expected DEBUG level, got %v", got["level"])
	}
	if got["sql"] != "SELECT 1" {
		t.Fatalf("expected sql field, got %v", got["sql"])
	}
	if rows, ok := got["rows"].(float64); !ok || rows != 1 {
		t.Fatalf("expected rows=1, got %v", got["rows"])
	}
	if _, ok := got["elapsed_ms"].(float64); !ok {
		t.Fatalf("expected elapsed_ms field, got %v", got["elapsed_ms"])
	}
	if _, ok := got["slow"]; ok {
		t.Fatalf("normal trace should not include slow flag, got %v", got["slow"])
	}
}

func TestGormSlogLogger_TraceSlowEmitsWarn(t *testing.T) {
	logger, buf := newTestGormLogger(t)
	begin := time.Now().Add(-300 * time.Millisecond)
	logger.Trace(context.Background(), begin,
		func() (string, int64) { return "SELECT * FROM live_session", 7 },
		nil,
	)

	got := decodeOnly(t, buf)
	if got["level"] != "WARN" {
		t.Fatalf("expected WARN level for slow sql, got %v", got["level"])
	}
	if got["msg"] != "gorm slow sql" {
		t.Fatalf("expected msg=gorm slow sql, got %v", got["msg"])
	}
	if slow, ok := got["slow"].(bool); !ok || !slow {
		t.Fatalf("expected slow=true, got %v", got["slow"])
	}
	if rows, ok := got["rows"].(float64); !ok || rows != 7 {
		t.Fatalf("expected rows=7, got %v", got["rows"])
	}
	if got["sql"] != "SELECT * FROM live_session" {
		t.Fatalf("expected sql preserved, got %v", got["sql"])
	}
	if elapsed, ok := got["elapsed_ms"].(float64); !ok || elapsed < 200 {
		t.Fatalf("expected elapsed_ms >= 200, got %v", got["elapsed_ms"])
	}
}

func TestGormSlogLogger_TraceErrorEmitsError(t *testing.T) {
	logger, buf := newTestGormLogger(t)
	begin := time.Now().Add(-10 * time.Millisecond)
	boom := errors.New("connection refused")
	logger.Trace(context.Background(), begin,
		func() (string, int64) { return "INSERT INTO bid VALUES (?)", 0 },
		boom,
	)

	got := decodeOnly(t, buf)
	if got["level"] != "ERROR" {
		t.Fatalf("expected ERROR level, got %v", got["level"])
	}
	if got["error"] != "connection refused" {
		t.Fatalf("expected error field, got %v", got["error"])
	}
	if got["sql"] != "INSERT INTO bid VALUES (?)" {
		t.Fatalf("expected sql preserved, got %v", got["sql"])
	}
	if rows, ok := got["rows"].(float64); !ok || rows != 0 {
		t.Fatalf("expected rows=0, got %v", got["rows"])
	}
}

func TestGormSlogLogger_TraceIgnoresRecordNotFoundWhenConfigured(t *testing.T) {
	buf := &bytes.Buffer{}
	base := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger := NewGormLogger(base, 200*time.Millisecond, true).LogMode(gormlogger.Info)

	begin := time.Now().Add(-10 * time.Millisecond)
	logger.Trace(context.Background(), begin,
		func() (string, int64) { return "SELECT * FROM users WHERE id=?", 0 },
		gormlogger.ErrRecordNotFound,
	)

	got := decodeOnly(t, buf)
	if got["level"] != "DEBUG" {
		t.Fatalf("expected DEBUG (record not found ignored), got %v", got["level"])
	}
	if _, ok := got["error"]; ok {
		t.Fatalf("ignored record-not-found should not surface error field, got %v", got["error"])
	}
}

func TestGormSlogLogger_LogModeSilentSuppresses(t *testing.T) {
	buf := &bytes.Buffer{}
	base := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger := NewGormLogger(base, 200*time.Millisecond, true).LogMode(gormlogger.Silent)

	logger.Trace(context.Background(), time.Now().Add(-time.Second),
		func() (string, int64) { return "SELECT 1", 1 },
		errors.New("boom"),
	)
	if buf.Len() != 0 {
		t.Fatalf("silent mode should suppress output, got %q", buf.String())
	}
}

// TestGormSlogLogger_TraceContextInjectsTraceID 验证 G7：当 ctx 中带有 active
// span 时，gormlogger 输出（经过 WithTraceContext 装饰）会注入 trace_id /
// span_id 字段，方便日志与 trace 串联。
func TestGormSlogLogger_TraceContextInjectsTraceID(t *testing.T) {
	buf := &bytes.Buffer{}
	jsonHandler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	base := slog.New(WithTraceContext(jsonHandler))
	logger := NewGormLogger(base, 200*time.Millisecond, true).LogMode(gormlogger.Info)

	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("aieas_backend.test").Start(context.Background(), "select-test")
	defer span.End()

	logger.Trace(ctx, time.Now().Add(-5*time.Millisecond),
		func() (string, int64) { return "SELECT 1", 1 },
		nil,
	)

	got := decodeOnly(t, buf)
	traceID, ok := got["trace_id"].(string)
	if !ok || traceID == "" {
		t.Fatalf("expected trace_id in JSON output, got %v", got["trace_id"])
	}
	if traceID != span.SpanContext().TraceID().String() {
		t.Fatalf("trace_id mismatch: got %q want %q", traceID, span.SpanContext().TraceID().String())
	}
	spanID, ok := got["span_id"].(string)
	if !ok || spanID == "" {
		t.Fatalf("expected span_id in JSON output, got %v", got["span_id"])
	}
}

// TestGormSlogLogger_TraceContextOmitsTraceIDWithoutSpan 验证无 span 时不写入
// 空 trace_id（避免污染日志）。
func TestGormSlogLogger_TraceContextOmitsTraceIDWithoutSpan(t *testing.T) {
	buf := &bytes.Buffer{}
	jsonHandler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	base := slog.New(WithTraceContext(jsonHandler))
	logger := NewGormLogger(base, 200*time.Millisecond, true).LogMode(gormlogger.Info)

	logger.Trace(context.Background(), time.Now().Add(-5*time.Millisecond),
		func() (string, int64) { return "SELECT 1", 1 },
		nil,
	)
	got := decodeOnly(t, buf)
	if _, ok := got["trace_id"]; ok {
		t.Fatalf("expected no trace_id without active span, got %v", got["trace_id"])
	}
}
