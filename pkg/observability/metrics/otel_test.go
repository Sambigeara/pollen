package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMeshMetrics_CountersIncrement(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	mm := NewMeshMetrics(mp)
	ctx := context.Background()

	mm.DatagramsSent.Add(ctx, 5)
	mm.DatagramErrors.Add(ctx, 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	values := extractInt64Sums(rm)
	require.Equal(t, int64(5), values["pollen.mesh.datagrams.sent"])
	require.Equal(t, int64(1), values["pollen.mesh.datagram.errors"])
}

func TestPeerMetrics_EnabledReflectsProvider(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	pm := NewPeerMetrics(mp)
	require.True(t, pm.Enabled(context.Background()))
}

func TestPeerMetrics_NoopDisabled(t *testing.T) {
	pm := NewPeerMetrics(NewNoopProviders().Meter)
	require.False(t, pm.Enabled(context.Background()))
}

func TestGossipMetrics_EWMAExposedViaObservableGauge(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	gm := NewGossipMetrics(mp)
	gm.StaleRatio.Update(1.0)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	values := extractFloat64Gauges(rm)
	require.InDelta(t, 0.01, values["pollen.gossip.stale.ratio"], 0.001)
}

// extractInt64Sums returns metric name -> cumulative value for all Int64 Sum metrics.
func extractInt64Sums(rm metricdata.ResourceMetrics) map[string]int64 {
	out := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					out[m.Name] += dp.Value
				}
			}
		}
	}
	return out
}

// extractFloat64Gauges returns metric name -> value for all Float64 Gauge metrics.
func extractFloat64Gauges(rm metricdata.ResourceMetrics) map[string]float64 {
	out := make(map[string]float64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok {
				for _, dp := range g.DataPoints {
					out[m.Name] = dp.Value
				}
			}
		}
	}
	return out
}
