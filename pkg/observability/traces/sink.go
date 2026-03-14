package traces

import (
	"time"

	"go.uber.org/zap"
)

// ReadOnlySpan is a snapshot of a completed span for export.
type ReadOnlySpan struct {
	TraceID    string
	SpanID     string
	Name       string
	StartTime  time.Time
	EndTime    time.Time
	Attributes []Attribute
}

func (s *Span) readOnly() ReadOnlySpan {
	return ReadOnlySpan{
		TraceID:    s.traceID.String(),
		SpanID:     s.spanID.String(),
		Name:       s.name,
		StartTime:  s.startTime,
		EndTime:    s.endTime,
		Attributes: s.attributes,
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
		for _, attr := range span.Attributes {
			fields = append(fields, attr.Key, attr.Value)
		}
		s.log.Debugw(span.Name, fields...)
	}
}
