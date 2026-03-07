// Package metrics provides lightweight, opt-in application metrics.
//
// When disabled (the default), all operations are no-ops with zero allocation.
// When enabled, counters and gauges are tracked in-memory using atomic operations
// and flushed to a Sink at configurable thresholds.
package metrics

import "time"

// Sink receives periodic metric snapshots. Implementations may push to OTLP,
// write to a log, expose via HTTP, etc.
type Sink interface {
	// Flush receives a batch of metric snapshots. Implementations must not
	// retain the slice past the call.
	Flush(snapshots []Snapshot)
}

// Snapshot is a point-in-time reading of a single metric.
type Snapshot struct {
	Name   string
	Labels Labels
	Kind   Kind
	Value  float64
}

// Kind distinguishes metric types for downstream interpretation.
type Kind int

const (
	KindCounter Kind = iota
	KindGauge
)

// Labels is an ordered set of key-value pairs attached to a metric.
type Labels [maxLabels]Label

// Label is a single key-value dimension.
type Label struct{ Key, Value string }

const maxLabels = 4

// MakeLabels builds a Labels array from key-value pairs.
func MakeLabels(kvs ...string) Labels {
	var l Labels
	for i := 0; i+1 < len(kvs) && i/2 < maxLabels; i += 2 {
		l[i/2] = Label{Key: kvs[i], Value: kvs[i+1]}
	}
	return l
}

// Config controls the collector's flush behavior.
type Config struct {
	// FlushInterval is the maximum time between flushes. Default: 10s.
	FlushInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.FlushInterval <= 0 {
		c.FlushInterval = 10 * time.Second //nolint:mnd
	}
	return c
}
