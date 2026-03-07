package traces

import "time"

// Tracer creates and exports spans. A nil *Tracer is safe to use; all methods
// return nil spans that are themselves no-ops.
type Tracer struct {
	sink Sink
}

// NewTracer creates a Tracer that exports completed spans to sink.
func NewTracer(sink Sink) *Tracer {
	if sink == nil {
		return nil
	}
	return &Tracer{sink: sink}
}

// Start begins a new root span with the given name.
func (t *Tracer) Start(name string) *Span {
	if t == nil {
		return nil
	}
	return &Span{
		tracer:    t,
		TraceID:   NewTraceID(),
		SpanID:    NewSpanID(),
		Name:      name,
		StartTime: time.Now(),
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
		TraceID:   tid,
		SpanID:    NewSpanID(),
		Name:      name,
		StartTime: time.Now(),
	}
}

// Attribute is a key-value pair attached to a span.
type Attribute struct {
	Key   string
	Value string
}

// SpanStatus records the outcome of a span.
type SpanStatus int

const (
	StatusUnset SpanStatus = iota
	StatusOK
	StatusError
)

// Span represents a unit of work within a trace. A nil *Span is safe to use;
// all methods are no-ops.
type Span struct {
	StartTime    time.Time
	EndTime      time.Time
	tracer       *Tracer
	Name         string
	Attributes   []Attribute
	Status       SpanStatus
	TraceID      TraceID
	SpanID       SpanID
	ParentSpanID SpanID
}

// End records the end time and exports the span. Calling End on a nil or
// already-ended span is a no-op.
func (s *Span) End() {
	if s == nil || !s.EndTime.IsZero() {
		return
	}
	s.EndTime = time.Now()
	s.tracer.sink.Export([]ReadOnlySpan{s.readOnly()})
}

// SetAttr appends a key-value attribute to the span.
func (s *Span) SetAttr(key, value string) {
	if s == nil {
		return
	}
	s.Attributes = append(s.Attributes, Attribute{Key: key, Value: value})
}

// SetStatus sets the span status.
func (s *Span) SetStatus(status SpanStatus) {
	if s == nil {
		return
	}
	s.Status = status
}

// Child creates a child span sharing the same TraceID.
func (s *Span) Child(name string) *Span {
	if s == nil {
		return nil
	}
	return &Span{
		tracer:       s.tracer,
		TraceID:      s.TraceID,
		SpanID:       NewSpanID(),
		ParentSpanID: s.SpanID,
		Name:         name,
		StartTime:    time.Now(),
	}
}

// TraceIDBytes returns the raw trace ID bytes for embedding in Envelope messages.
func (s *Span) TraceIDBytes() []byte {
	if s == nil {
		return nil
	}
	return s.TraceID.Bytes()
}
