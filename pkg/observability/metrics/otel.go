package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type Providers struct {
	provider *sdkmetric.MeterProvider
	reader   *sdkmetric.ManualReader
}

func NewProviders() *Providers {
	manual := sdkmetric.NewManualReader()
	return &Providers{
		provider: sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(manual),
		),
		reader: manual,
	}
}

func NewNoopProviders() *Providers {
	return &Providers{}
}

func (p *Providers) Meter() metric.MeterProvider {
	if p.provider != nil {
		return p.provider
	}
	return noop.NewMeterProvider()
}

func (p *Providers) Shutdown(ctx context.Context) error {
	if p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}

type MetricSnapshot struct {
	CertExpirySeconds  float64
	CertRenewals       int64
	CertRenewalsFailed int64
	PunchAttempts      int64
	PunchFailures      int64
}

// CollectSnapshot collects current metric values from the ManualReader.
// Returns a zero snapshot in noop mode (reader is nil).
func (p *Providers) CollectSnapshot(ctx context.Context) (MetricSnapshot, error) {
	var snap MetricSnapshot
	if p.reader == nil {
		return snap, nil
	}
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return snap, fmt.Errorf("collect metrics: %w", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, met := range sm.Metrics {
			switch data := met.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					switch met.Name {
					case nameCertRenewals:
						snap.CertRenewals += dp.Value
					case nameCertRenewalsFailed:
						snap.CertRenewalsFailed += dp.Value
					case namePunchAttempts:
						snap.PunchAttempts += dp.Value
					case namePunchFailures:
						snap.PunchFailures += dp.Value
					}
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					if met.Name == nameCertExpirySeconds {
						snap.CertExpirySeconds = dp.Value
					}
				}
			}
		}
	}
	return snap, nil
}
