package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// Providers holds the OTel MeterProvider and an optional ManualReader for
// on-demand metric collection.
type Providers struct {
	Meter    metric.MeterProvider
	reader   *sdkmetric.ManualReader
	shutdown func(context.Context) error
}

// NewProviders creates a real MeterProvider with periodic log export and a
// ManualReader for on-demand metric collection (used by the control service).
func NewProviders(log *zap.SugaredLogger) *Providers {
	manual := sdkmetric.NewManualReader()
	periodic := sdkmetric.NewPeriodicReader(
		&zapExporter{log: log},
		sdkmetric.WithInterval(10*time.Second), //nolint:mnd
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(manual),
		sdkmetric.WithReader(periodic),
	)
	return &Providers{
		Meter:    mp,
		reader:   manual,
		shutdown: mp.Shutdown,
	}
}

// NewNoopProviders returns a zero-cost noop MeterProvider.
func NewNoopProviders() *Providers {
	return &Providers{
		Meter:    noop.NewMeterProvider(),
		shutdown: func(context.Context) error { return nil },
	}
}

// Shutdown flushes and shuts down the MeterProvider.
func (p *Providers) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

// MetricSnapshot holds a point-in-time collection of named metric values,
// extracted from the OTel pipeline via ManualReader.
type MetricSnapshot struct {
	Int64s   map[string]int64
	Float64s map[string]float64
}

// CollectSnapshot collects current metric values from the ManualReader.
// Returns an empty snapshot if the provider has no ManualReader (noop mode).
func (p *Providers) CollectSnapshot(ctx context.Context) MetricSnapshot {
	snap := MetricSnapshot{
		Int64s:   make(map[string]int64),
		Float64s: make(map[string]float64),
	}
	if p.reader == nil {
		return snap
	}
	var rm metricdata.ResourceMetrics
	_ = p.reader.Collect(ctx, &rm)
	for _, sm := range rm.ScopeMetrics {
		for _, met := range sm.Metrics {
			switch data := met.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					snap.Int64s[met.Name] += dp.Value
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					snap.Float64s[met.Name] = dp.Value
				}
			}
		}
	}
	return snap
}

// zapExporter writes metric snapshots to a zap logger at Debug level.
type zapExporter struct {
	log *zap.SugaredLogger
}

func (e *zapExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

func (e *zapExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

func (e *zapExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			default:
				e.log.Debugw(m.Name, "data", fmt.Sprintf("%T", m.Data))
			}
		}
	}
	return nil
}

func (e *zapExporter) ForceFlush(context.Context) error { return nil }
func (e *zapExporter) Shutdown(context.Context) error   { return nil }
