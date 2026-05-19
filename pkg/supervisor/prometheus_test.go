// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// TestWorkloadInfo_PerPublisher is the multi-tenant accounting
// regression: two authorities publishing byte-identical content under
// the same logical name must surface as two distinct workloadInfo
// series, not collapse to one deduped content-hash entry and not panic
// on a duplicate label set.
func TestWorkloadInfo_PerPublisher(t *testing.T) {
	pkA := types.PeerKey{0xaa}
	pkB := types.PeerKey{0xbb}
	hash := "aaaaaaaa" + "00000000000000000000000000000000000000000000000000000000"

	snap := state.Snapshot{
		SpecsAll: []state.WorkloadSpecView{
			{Spec: state.WorkloadSpec{Hash: hash, Name: "echo"}, Publisher: pkA},
			{Spec: state.WorkloadSpec{Hash: hash, Name: "echo"}, Publisher: pkB},
		},
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(newStateCollector(func() state.Snapshot { return snap }))
	families, err := reg.Gather()
	require.NoError(t, err)

	var info *dto.MetricFamily
	for _, f := range families {
		if f.GetName() == "pollen_workload_info" {
			info = f
			break
		}
	}
	require.NotNil(t, info, "pollen_workload_info collected")
	require.Len(t, info.GetMetric(), 2, "one series per (authority, name), not collapsed by content hash")

	publishers := make([]string, 0, 2)
	for _, m := range info.GetMetric() {
		labels := map[string]string{}
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		require.Equal(t, "echo", labels["name"])
		require.Equal(t, hash[:8], labels["hash"])
		publishers = append(publishers, labels["publisher"])
	}
	require.ElementsMatch(t, []string{pkA.Short(), pkB.Short()}, publishers,
		"each tenant attributed to its own authority")
}
