package traces

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestSpanContextFromTraceID_Valid(t *testing.T) {
	tid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sc := SpanContextFromTraceID(tid[:])
	require.True(t, sc.HasTraceID())
	require.True(t, sc.IsRemote())
	require.Equal(t, trace.TraceID(tid), sc.TraceID())
}

func TestSpanContextFromTraceID_InvalidLength(t *testing.T) {
	sc := SpanContextFromTraceID([]byte{1, 2, 3})
	require.False(t, sc.HasTraceID())
}

func TestSpanContextFromTraceID_Nil(t *testing.T) {
	sc := SpanContextFromTraceID(nil)
	require.False(t, sc.HasTraceID())
}

func TestZapSpanExporter_RecordsSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	tracer := tp.Tracer("test")
	_, span := tracer.Start(context.Background(), "test.op")
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "test.op", spans[0].Name())
}

func TestNoopProvider_NoSpans(t *testing.T) {
	p := NewNoopProviders()
	tracer := p.Tracer.Tracer("test")
	_, span := tracer.Start(context.Background(), "noop.op")
	span.End()
}
