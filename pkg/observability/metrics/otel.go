package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	nameCertExpirySeconds  = "pollen.node.cert.expiry.seconds"
	nameCertRenewals       = "pollen.node.cert.renewals"
	nameCertRenewalsFailed = "pollen.node.cert.renewals.failed"
	namePunchAttempts      = "pollen.node.punch.attempts"
	namePunchFailures      = "pollen.node.punch.failures"
)

// metricReader abstracts snapshot collection to allow zero-allocation noops.
type metricReader interface {
	Collect(context.Context, *metricdata.ResourceMetrics) error
}

type noopReader struct{}

func (noopReader) Collect(context.Context, *metricdata.ResourceMetrics) error { return nil }

type Provider struct {
	provider metric.MeterProvider
	reader   metricReader
	shutdown func(context.Context) error
}

func NewProvider() *Provider {
	manual := sdkmetric.NewManualReader()
	sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(manual))

	return &Provider{
		provider: sdk,
		reader:   manual,
		shutdown: sdk.Shutdown,
	}
}

func NewNoopProvider() *Provider {
	return &Provider{
		provider: noop.NewMeterProvider(),
		reader:   noopReader{},
		shutdown: func(context.Context) error { return nil },
	}
}

func (p *Provider) Meter() metric.MeterProvider {
	return p.provider
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

type MetricSnapshot struct {
	CertExpirySeconds  float64
	CertRenewals       int64
	CertRenewalsFailed int64
	PunchAttempts      int64
	PunchFailures      int64
}

// CollectSnapshot collects current metric values from the ManualReader.
func (p *Provider) CollectSnapshot(ctx context.Context) (MetricSnapshot, error) {
	var snap MetricSnapshot
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
