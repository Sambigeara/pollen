package traces

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const traceIDSize = 16

type Provider struct {
	provider trace.TracerProvider
	shutdown func(context.Context) error
}

func NewProvider() *Provider {
	sdk := sdktrace.NewTracerProvider()
	return &Provider{
		provider: sdk,
		shutdown: sdk.Shutdown,
	}
}

func NewNoopProvider() *Provider {
	return &Provider{
		provider: tracenoop.NewTracerProvider(),
		shutdown: func(context.Context) error { return nil },
	}
}

func (p *Provider) Tracer() trace.TracerProvider {
	return p.provider
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

func SpanContextFromTraceID(traceID []byte) trace.SpanContext {
	if len(traceID) != traceIDSize {
		return trace.SpanContext{}
	}

	var tid trace.TraceID
	copy(tid[:], traceID)

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}
