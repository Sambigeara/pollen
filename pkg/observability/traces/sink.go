package traces

import (
	"time"

	"go.uber.org/zap"
)

// ReadOnlySpan is a snapshot of a completed span for export.
type ReadOnlySpan struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	StartTime    time.Time
	EndTime      time.Time
	Attributes   []Attribute
	Status       SpanStatus
}

func (s *Span) readOnly() ReadOnlySpan {
	parentID := ""
	if !s.ParentSpanID.IsZero() {
		parentID = s.ParentSpanID.String()
	}
	return ReadOnlySpan{
		TraceID:      s.TraceID.String(),
		SpanID:       s.SpanID.String(),
		ParentSpanID: parentID,
		Name:         s.Name,
		StartTime:    s.StartTime,
		EndTime:      s.EndTime,
		Attributes:   s.Attributes,
		Status:       s.Status,
	}
}

// Sink receives completed spans for export.
type Sink interface {
	Export(spans []ReadOnlySpan)
}

// LogSink writes spans to a zap logger at Debug level.
type LogSink struct {
	log *zap.SugaredLogger
}

// NewLogSink creates a sink that logs spans.
func NewLogSink(log *zap.SugaredLogger) *LogSink {
	return &LogSink{log: log}
}

func (s *LogSink) Export(spans []ReadOnlySpan) {
	for _, span := range spans {
		fields := []any{
			"trace_id", span.TraceID,
			"span_id", span.SpanID,
			"duration", span.EndTime.Sub(span.StartTime),
		}
		if span.ParentSpanID != "" {
			fields = append(fields, "parent_span_id", span.ParentSpanID)
		}
		for _, attr := range span.Attributes {
			fields = append(fields, attr.Key, attr.Value)
		}
		s.log.Debugw(span.Name, fields...)
	}
}
