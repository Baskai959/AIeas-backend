package observability

import (
	"context"
	"log/slog"

	"aieas_backend/internal/infra/observability/tracing"
)

// traceContextHandler 是一个 slog.Handler 装饰器：在 Handle 时若 ctx 中存在
// active span，自动把 trace_id / span_id 注入到 record。
//
// 仅在 JSON 模式下使用，避免 dev/text 模式下的彩色输出被两个额外字段挤占成
// 多行难读日志（C11）。
type traceContextHandler struct {
	inner slog.Handler
}

// WithTraceContext 在底层 handler 之上叠加 trace 字段注入。
// 在 text 模式 / 禁用 trace 模式下也是安全的：tracing.TraceIDFromContext 在没有
// active span 时返回空字符串，handler 不会写入空字段。
func WithTraceContext(inner slog.Handler) slog.Handler {
	if inner == nil {
		return nil
	}
	return &traceContextHandler{inner: inner}
}

func (h *traceContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if traceID := tracing.TraceIDFromContext(ctx); traceID != "" {
		record.AddAttrs(slog.String("trace_id", traceID))
		if spanID := tracing.SpanIDFromContext(ctx); spanID != "" {
			record.AddAttrs(slog.String("span_id", spanID))
		}
	}
	return h.inner.Handle(ctx, record)
}

func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithGroup(name)}
}
