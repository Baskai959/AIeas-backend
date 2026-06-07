package service

import (
	"context"
	"fmt"

	"aieas_backend/internal/infra/observability/tracing"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	wstransport "aieas_backend/internal/transport/ws"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	traceapi "go.opentelemetry.io/otel/trace"
)

type EventPublisher interface {
	Broadcast(auctionID uint64, env wstransport.Envelope) int
}

type auctionEventPublisherAdapter struct {
	publisher EventPublisher
}

func (a auctionEventPublisherAdapter) Broadcast(auctionID uint64, env auctionports.EventEnvelope) int {
	if a.publisher == nil {
		return 0
	}
	return a.publisher.Broadcast(auctionID, wstransport.Envelope{
		Type:      env.Type,
		RequestID: env.RequestID,
		Seq:       env.Seq,
		Payload:   env.Payload,
	})
}

type auctionTracerAdapter struct {
	provider *tracing.Provider
}

func (a auctionTracerAdapter) Start(ctx context.Context, name string, attrs ...auctionapp.AuctionAttr) (context.Context, auctionapp.AuctionSpan) {
	if ctx == nil {
		ctx = context.Background()
	}
	provider := a.provider
	if provider == nil {
		provider = tracing.NewNoop()
	}
	tracer := provider.Tracer("aieas_backend")
	startOpts := []traceapi.SpanStartOption{}
	if len(attrs) > 0 {
		startOpts = append(startOpts, traceapi.WithAttributes(toOTelAttrs(attrs)...))
	}
	nextCtx, span := tracer.Start(ctx, name, startOpts...)
	return nextCtx, auctionTraceSpanAdapter{inner: span}
}

type auctionTraceSpanAdapter struct {
	inner traceapi.Span
}

func (a auctionTraceSpanAdapter) End() {
	if a.inner != nil {
		a.inner.End()
	}
}

func (a auctionTraceSpanAdapter) SetAttributes(attrs ...auctionapp.AuctionAttr) {
	if a.inner != nil {
		a.inner.SetAttributes(toOTelAttrs(attrs)...)
	}
}

func (a auctionTraceSpanAdapter) RecordError(err error) {
	if a.inner != nil {
		a.inner.RecordError(err)
	}
}

func (a auctionTraceSpanAdapter) SetStatus(code auctionapp.AuctionStatusCode, description string) {
	if a.inner != nil {
		statusCode := codes.Unset
		if code == auctionapp.AuctionStatusError {
			statusCode = codes.Error
		}
		a.inner.SetStatus(statusCode, description)
	}
}

func toOTelAttrs(attrs []auctionapp.AuctionAttr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		key := attr.Key
		if key == "" {
			continue
		}
		switch value := attr.Value.(type) {
		case string:
			out = append(out, attribute.String(key, value))
		case int:
			out = append(out, attribute.Int(key, value))
		case int64:
			out = append(out, attribute.Int64(key, value))
		case bool:
			out = append(out, attribute.Bool(key, value))
		default:
			out = append(out, attribute.String(key, fmt.Sprint(value)))
		}
	}
	return out
}
