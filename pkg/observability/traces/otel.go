package traces

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// Providers holds the OTel TracerProvider.
type Providers struct {
	Tracer   trace.TracerProvider
	shutdown func(context.Context) error
}

// NewProviders creates a real TracerProvider with batch span export to zap.
func NewProviders(log *zap.SugaredLogger) *Providers {
	exp := &zapSpanExporter{log: log}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	return &Providers{
		Tracer:   tp,
		shutdown: tp.Shutdown,
	}
}

// NewNoopProviders returns a zero-cost noop TracerProvider.
func NewNoopProviders() *Providers {
	return &Providers{
		Tracer:   tracenoop.NewTracerProvider(),
		shutdown: func(context.Context) error { return nil },
	}
}

// Shutdown flushes and shuts down the TracerProvider.
func (p *Providers) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

// SpanContextFromTraceID builds a remote SpanContext from raw trace ID bytes.
// Returns an empty SpanContext if traceID is not exactly 16 bytes.
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

// zapSpanExporter writes completed spans to a zap logger at Debug level.
type zapSpanExporter struct {
	log *zap.SugaredLogger
}

func (e *zapSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, s := range spans {
		fields := []any{
			"trace_id", s.SpanContext().TraceID().String(),
			"span_id", s.SpanContext().SpanID().String(),
			"duration", s.EndTime().Sub(s.StartTime()),
		}
		if s.Parent().IsValid() {
			fields = append(fields, "parent_span_id", s.Parent().SpanID().String())
		}
		for _, attr := range s.Attributes() {
			fields = append(fields, string(attr.Key), attr.Value.Emit())
		}
		e.log.Debugw(s.Name(), fields...)
	}
	return nil
}

func (e *zapSpanExporter) Shutdown(context.Context) error { return nil }
