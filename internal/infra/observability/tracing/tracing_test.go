package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	traceapi "go.opentelemetry.io/otel/trace"
)

func TestNewNoopProviderIsDisabled(t *testing.T) {
	p := NewNoop()
	if p.Enabled() {
		t.Fatalf("noop provider should be disabled")
	}
	if p.Tracer("x") == nil {
		t.Fatalf("Tracer must return a non-nil tracer even when disabled")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown noop: %v", err)
	}
	// nil safety
	var nilP *Provider
	if nilP.Enabled() {
		t.Fatalf("nil provider must report disabled")
	}
	if nilP.Tracer("x") == nil {
		t.Fatalf("nil Tracer must fall back to noop")
	}
	if err := nilP.Shutdown(context.Background()); err != nil {
		t.Fatalf("nil shutdown: %v", err)
	}
}

func TestSetupDisabledRegistersPropagator(t *testing.T) {
	p, err := Setup(context.Background(), Config{Enabled: false})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if p.Enabled() {
		t.Fatalf("disabled config must produce disabled provider")
	}
	// composite propagator (TraceContext+Baggage) should now be installed.
	prop := otel.GetTextMapPropagator()
	if prop == nil {
		t.Fatalf("expected propagator to be set")
	}
	fields := prop.Fields()
	if !contains(fields, "traceparent") {
		t.Fatalf("expected traceparent in propagator fields, got %v", fields)
	}
}

func TestStartSpanReturnsValidContext(t *testing.T) {
	// Even when disabled, StartSpan should return non-nil ctx and span.
	ctx, span := StartSpan(context.Background(), "test-span")
	if ctx == nil {
		t.Fatalf("StartSpan returned nil ctx")
	}
	if span == nil {
		t.Fatalf("StartSpan returned nil span")
	}
	span.End()
}

func TestTraceIDFromContextEmptyForNoSpan(t *testing.T) {
	if got := TraceIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty trace_id without active span, got %q", got)
	}
	if got := SpanIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty span_id without active span, got %q", got)
	}
	if got := TraceIDFromContext(nil); got != "" {
		t.Fatalf("nil ctx must produce empty trace_id, got %q", got)
	}
}

func TestSetupUnsupportedExporterFallsBackToNoop(t *testing.T) {
	p, err := Setup(context.Background(), Config{Enabled: true, Exporter: "stdout"})
	if err == nil {
		t.Fatalf("expected error for unsupported exporter")
	}
	if p.Enabled() {
		t.Fatalf("provider must be disabled noop on exporter error")
	}
}

func TestBuildSamplerFallback(t *testing.T) {
	// Empty sampler -> parent_based_traceid_ratio with default ratio=1.
	s := buildSampler(Config{Sampler: "", SampleRatio: 0})
	if s == nil {
		t.Fatalf("expected non-nil sampler")
	}
	if got := s.Description(); got == "" {
		t.Fatalf("sampler description empty")
	}
	// always_on / always_off / unknown all produce non-nil samplers.
	for _, name := range []string{"always_on", "always_off", "garbage"} {
		if buildSampler(Config{Sampler: name, SampleRatio: 0.5}) == nil {
			t.Fatalf("sampler %q returned nil", name)
		}
	}
}

func TestProviderTracerProducesSpan(t *testing.T) {
	p := NewNoop()
	tracer := p.Tracer("aieas_backend.test")
	_, span := tracer.Start(context.Background(), "op",
		traceapi.WithSpanKind(traceapi.SpanKindInternal),
	)
	defer span.End()
	if span == nil {
		t.Fatalf("expected span, got nil")
	}
}

func contains(items []string, target string) bool {
	for _, s := range items {
		if s == target {
			return true
		}
	}
	return false
}
