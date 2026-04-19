// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestStateCollector_GateWaitClearsEmitsZero guards the regression path
// where a seed's gate wait transitions nonzero → zero while other
// SeedMetrics fields remain active. The collector must emit
// pollen_workload_gate_wait_ms=0 so Grafana reflects the clear rather
// than pinning the series at the last scrape with nonzero data.
func TestStateCollector_GateWaitClearsEmitsZero(t *testing.T) {
	hash := strings.Repeat("a", 64)
	node := types.PeerKeyFromBytes([]byte("01234567890123456789012345678901"))

	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			node: {
				Name: "node-a",
				SeedMetrics: map[string]state.SeedMetrics{
					hash: {
						ServedRate:    12.5,
						ComputeCostMs: 40,
						GateWaitMs:    0, // cleared, but entry still present
					},
				},
			},
		},
		Specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "api", MinReplicas: 1},
				Publisher: node,
			},
		},
		Claims: map[string]map[types.PeerKey]struct{}{
			hash: {node: struct{}{}},
		},
	}

	c := newStateCollector(func() state.Snapshot { return snap })
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	gateWait := findMetric(mfs, "pollen_workload_gate_wait_ms")
	require.NotNil(t, gateWait, "pollen_workload_gate_wait_ms must be emitted")
	require.Len(t, gateWait.Metric, 1)
	require.InDelta(t, 0.0, gateWait.Metric[0].Gauge.GetValue(), 0.0001,
		"gate wait must be zero when SeedMetrics entry exists with GateWaitMs=0")

	load := findMetric(mfs, "pollen_workload_load")
	require.NotNil(t, load, "sibling load metric must still render")
	require.Len(t, load.Metric, 1)
	require.InDelta(t, 12.5, load.Metric[0].Gauge.GetValue(), 0.0001)
}

func findMetric(mfs []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}
