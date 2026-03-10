package traces

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingSink struct {
	mu    sync.Mutex
	spans []ReadOnlySpan
}

func (s *recordingSink) Export(spans []ReadOnlySpan) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, spans...)
}

func (s *recordingSink) get() []ReadOnlySpan {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ReadOnlySpan, len(s.spans))
	copy(out, s.spans)
	return out
}

func TestNilTracerIsNoOp(t *testing.T) {
	var tr *Tracer
	span := tr.Start("test")
	require.Nil(t, span)

	span.End()
	span.SetAttr("k", "v")
	require.Nil(t, span.TraceIDBytes())
}

func TestSpanLifecycle(t *testing.T) {
	sink := &recordingSink{}
	tr := NewTracer(sink)

	span := tr.Start("root")
	require.NotNil(t, span)
	require.False(t, span.traceID.IsZero())
	require.False(t, span.spanID.IsZero())

	span.SetAttr("peer", "abc")
	span.End()

	spans := sink.get()
	require.Len(t, spans, 1)
	require.Equal(t, "root", spans[0].Name)
	require.False(t, spans[0].EndTime.IsZero())
	require.Len(t, spans[0].Attributes, 1)
	require.Equal(t, "peer", spans[0].Attributes[0].Key)
}

func TestStartFromRemote(t *testing.T) {
	sink := &recordingSink{}
	tr := NewTracer(sink)

	traceID := NewTraceID()
	span := tr.StartFromRemote("remote-op", traceID.Bytes())
	require.Equal(t, traceID, span.traceID)

	span.End()
	spans := sink.get()
	require.Len(t, spans, 1)
	require.Equal(t, traceID.String(), spans[0].TraceID)
}

func TestStartFromRemoteEmptyTraceID(t *testing.T) {
	sink := &recordingSink{}
	tr := NewTracer(sink)

	span := tr.StartFromRemote("remote-op", nil)
	require.False(t, span.traceID.IsZero())

	span.End()
}

func TestDoubleEndIsNoOp(t *testing.T) {
	sink := &recordingSink{}
	tr := NewTracer(sink)

	span := tr.Start("test")
	span.End()
	span.End()

	require.Len(t, sink.get(), 1)
}

func TestTraceIDFromBytesWrongLength(t *testing.T) {
	id := TraceIDFromBytes([]byte{1, 2, 3})
	require.True(t, id.IsZero())
}

func TestNewTracerNilSinkTracesWithoutExport(t *testing.T) {
	tr := NewTracer(nil)
	require.NotNil(t, tr)
	s := tr.Start("test")
	require.NotNil(t, s)
	s.SetAttr("k", "v")
	s.End()
}
