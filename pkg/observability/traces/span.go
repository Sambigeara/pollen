package traces

import "time"

// Tracer creates and exports spans. A nil *Tracer is safe to use; all methods
// return nil spans that are themselves no-ops.
type Tracer struct {
	sink Sink
}

// NewTracer creates a Tracer that exports completed spans to sink. If sink is
// nil the tracer still creates spans but never exports them.
func NewTracer(sink Sink) *Tracer {
	return &Tracer{sink: sink}
}

// Start begins a new root span with the given name.
func (t *Tracer) Start(name string) *Span {
	if t == nil {
		return nil
	}
	return &Span{
		tracer:    t,
		traceID:   NewTraceID(),
		spanID:    NewSpanID(),
		name:      name,
		startTime: time.Now(),
	}
}

// StartFromRemote continues a trace received from a remote envelope.
// If traceID is empty or invalid, a new trace is started.
func (t *Tracer) StartFromRemote(name string, traceID []byte) *Span {
	if t == nil {
		return nil
	}
	tid := TraceIDFromBytes(traceID)
	if tid.IsZero() {
		tid = NewTraceID()
	}
	return &Span{
		tracer:    t,
		traceID:   tid,
		spanID:    NewSpanID(),
		name:      name,
		startTime: time.Now(),
	}
}

// Attribute is a key-value pair attached to a span.
type Attribute struct {
	Key   string
	Value string
}

// Span represents a unit of work within a trace. A nil *Span is safe to use;
// all methods are no-ops.
type Span struct {
	startTime  time.Time
	endTime    time.Time
	tracer     *Tracer
	name       string
	attributes []Attribute
	traceID    TraceID
	spanID     SpanID
}

// End records the end time and exports the span. Calling End on a nil or
// already-ended span is a no-op.
func (s *Span) End() {
	if s == nil || !s.endTime.IsZero() {
		return
	}
	s.endTime = time.Now()
	if s.tracer.sink != nil {
		s.tracer.sink.Export([]ReadOnlySpan{s.readOnly()})
	}
}

// SetAttr appends a key-value attribute to the span.
func (s *Span) SetAttr(key, value string) {
	if s == nil {
		return
	}
	s.attributes = append(s.attributes, Attribute{Key: key, Value: value})
}

// TraceIDBytes returns the raw trace ID bytes for embedding in Envelope messages.
func (s *Span) TraceIDBytes() []byte {
	if s == nil {
		return nil
	}
	return s.traceID[:]
}
