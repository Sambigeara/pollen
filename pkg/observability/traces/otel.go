package traces

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type Providers struct {
	provider *sdktrace.TracerProvider
}

func NewProviders() *Providers {
	return &Providers{
		provider: sdktrace.NewTracerProvider(),
	}
}

func NewNoopProviders() *Providers {
	return &Providers{}
}

func (p *Providers) Tracer() trace.TracerProvider {
	if p.provider != nil {
		return p.provider
	}
	return tracenoop.NewTracerProvider()
}

func (p *Providers) Shutdown(ctx context.Context) error {
	if p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}

func SpanContextFromTraceID(traceID []byte) trace.SpanContext {
	if len(traceID) != 16 { //nolint:mnd
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
