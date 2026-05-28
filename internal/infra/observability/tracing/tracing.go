// Package tracing 封装 OpenTelemetry SDK 的初始化与生命周期管理。
//
// 默认 enabled=false：返回 noop TracerProvider 与 propagator，所有 StartSpan
// 调用走 SDK 内置 noop 路径，不产生网络流量、不引入额外延迟，适合在禁用 trace
// 的部署形态（本地、单测、CI）下完全旁路。
//
// 启用时使用 OTLP HTTP exporter，对外参数（endpoint / insecure / sampler）从
// ObservabilityConfig.Tracing 读入。
package tracing

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	traceapi "go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Config 是 tracing 子系统所需的最小配置面。从 ObservabilityConfig.Tracing
// 1:1 映射，避免该包反向依赖 internal/config。
type Config struct {
	Enabled     bool
	Exporter    string // 当前仅支持 "otlphttp"
	Endpoint    string // host[:port]，不要带 scheme
	Insecure    bool   // true 时使用 http；false 时使用 https
	ServiceName string
	Sampler     string  // "always_on" | "always_off" | "parent_based_traceid_ratio"
	SampleRatio float64 // 仅 ratio 采样器使用，[0,1]
}

// Provider 同时承载 TracerProvider 与 Shutdown，便于 server.go 在关闭时统一释放。
type Provider struct {
	enabled  bool
	tp       traceapi.TracerProvider
	shutdown func(context.Context) error
}

// Tracer 返回名为 name 的 Tracer。返回值在 enabled=false 时是 noop tracer。
func (p *Provider) Tracer(name string) traceapi.Tracer {
	if p == nil || p.tp == nil {
		return tracenoop.NewTracerProvider().Tracer(name)
	}
	return p.tp.Tracer(name)
}

// Enabled 报告 trace 是否启用。
func (p *Provider) Enabled() bool {
	return p != nil && p.enabled
}

// Shutdown 关闭底层 exporter。enabled=false 时是 no-op。
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

// NewNoop 返回禁用状态的 Provider。所有方法都安全 no-op。
func NewNoop() *Provider {
	return &Provider{enabled: false, tp: tracenoop.NewTracerProvider()}
}

// Setup 根据 Config 构建 Provider，并把它注册为全局 TracerProvider 与
// Propagator（W3C TraceContext + Baggage），使任何使用 otel.Tracer(name) 的
// 第三方库（gorm、redis 中间件、http client 仪器化）都能拿到正确的 tracer。
//
// 失败时返回 noop Provider 与原始错误：调用方可以选择降级运行而非阻塞启动。
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	if !cfg.Enabled {
		p := NewNoop()
		// 即便禁用也设置 propagator，便于跨服务透传（无 active span 时仍可解析 traceparent）。
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		))
		otel.SetTracerProvider(p.tp)
		return p, nil
	}
	exporter, err := buildExporter(ctx, cfg)
	if err != nil {
		return NewNoop(), fmt.Errorf("build trace exporter: %w", err)
	}
	res, err := buildResource(ctx, cfg)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return NewNoop(), fmt.Errorf("build trace resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithSampler(buildSampler(cfg)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return &Provider{
		enabled:  true,
		tp:       tp,
		shutdown: tp.Shutdown,
	}, nil
}

// Init 是 composition root 友好的入口：根据 cfg.Enabled 决定走 Setup 或直接
// 返回 noop Provider，并提供一个统一的 shutdown 闭包。
//
// 该函数永不返回 nil Provider；当启用 trace 但 exporter 初始化失败时会回退
// 到 noop Provider，并把原始错误一并返回，方便调用方记录降级日志。
func Init(ctx context.Context, cfg Config) (*Provider, func(context.Context) error, error) {
	provider, err := Setup(ctx, cfg)
	if provider == nil {
		provider = NewNoop()
	}
	shutdown := func(sctx context.Context) error {
		if provider == nil {
			return nil
		}
		return provider.Shutdown(sctx)
	}
	return provider, shutdown, err
}

func buildExporter(ctx context.Context, cfg Config) (*otlptrace.Exporter, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Exporter)) {
	case "", "otlphttp":
		opts := []otlptracehttp.Option{}
		if endpoint := strings.TrimSpace(cfg.Endpoint); endpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		client := otlptracehttp.NewClient(opts...)
		return otlptrace.New(ctx, client)
	default:
		return nil, fmt.Errorf("unsupported tracing.exporter %q", cfg.Exporter)
	}
}

func buildResource(ctx context.Context, cfg Config) (*sdkresource.Resource, error) {
	service := strings.TrimSpace(cfg.ServiceName)
	if service == "" {
		service = "aieas-backend"
	}
	return sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceName(service),
		),
		sdkresource.WithProcess(),
		sdkresource.WithHost(),
	)
}

func buildSampler(cfg Config) sdktrace.Sampler {
	switch strings.ToLower(strings.TrimSpace(cfg.Sampler)) {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "parent_based_traceid_ratio", "":
		ratio := cfg.SampleRatio
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		if ratio == 0 {
			// 没指定比例时默认全采样，避免“开了 trace 却看不到任何 span”的迷惑场景。
			ratio = 1
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1))
	}
}

// StartSpan 是便捷函数：基于全局 TracerProvider 创建一个 span。
// 调用方负责 span.End()。
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, traceapi.Span) {
	tracer := otel.Tracer("aieas_backend")
	opts := []traceapi.SpanStartOption{}
	if len(attrs) > 0 {
		opts = append(opts, traceapi.WithAttributes(attrs...))
	}
	return tracer.Start(ctx, name, opts...)
}

// TraceIDFromContext 在 active span 存在时返回 trace_id 字符串，否则返回空。
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	span := traceapi.SpanFromContext(ctx)
	sc := span.SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanIDFromContext 在 active span 存在时返回 span_id 字符串，否则返回空。
func SpanIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	span := traceapi.SpanFromContext(ctx)
	sc := span.SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}

// InjectHTTP 把 ctx 中的 trace context 通过全局 propagator 注入到 HTTP header。
// 用于出站 HTTP 客户端（agent、callback 等）手工传播 trace。
func InjectHTTP(ctx context.Context, header http.Header) {
	if ctx == nil || header == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
}

// ExtractHTTP 从 HTTP header 抽取 trace context 并返回新 ctx。
// 用于入站 HTTP/Webhook 入口手工恢复 trace。
func ExtractHTTP(ctx context.Context, header http.Header) context.Context {
	if header == nil {
		if ctx == nil {
			return context.Background()
		}
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(header))
}

// InjectMap 把 ctx 中的 trace context 注入到一个 map[string]string 载体里，
// 适配 Redis Stream / Kafka / 自定义事件总线等无 HTTP header 的场景。
//
// 写入前会跳过 nil ctx 与 nil map；调用方在 m 为 nil 时需自行新建。
func InjectMap(ctx context.Context, m map[string]string) {
	if ctx == nil || m == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, mapCarrier(m))
}

// ExtractMap 从 map[string]string 中抽取 trace context 并返回新 ctx。
// 与 InjectMap 配对使用，是 Redis Stream / Kafka 等异步消费端续上 trace 的入口。
func ExtractMap(ctx context.Context, m map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(m) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, mapCarrier(m))
}

// mapCarrier 把 map[string]string 适配到 otel propagation.TextMapCarrier。
type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string { return m[key] }

func (m mapCarrier) Set(key, value string) { m[key] = value }

func (m mapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = mapCarrier(nil)
