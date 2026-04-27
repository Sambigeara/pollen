// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/stretchr/testify/require"
)

func TestWorkloadSpecRoundTrip(t *testing.T) {
	t.Run("populated spec survives proto round-trip", func(t *testing.T) {
		in := WorkloadSpec{
			Hash:        "abc123",
			Name:        "api",
			MinReplicas: 3,
			MemoryBytes: 64 << 20,
			Timeout:     5 * time.Second,
			Spread:      0.25,
			LatencySLO:  250 * time.Millisecond,
		}
		pb := workloadSpecToProto(in)
		out := workloadSpecFromProto(pb)
		require.Equal(t, in, out)
	})

	t.Run("nil proto decodes to zero value", func(t *testing.T) {
		require.Equal(t, WorkloadSpec{}, workloadSpecFromProto(nil))
	})

	t.Run("zero spec survives round-trip", func(t *testing.T) {
		in := WorkloadSpec{}
		require.Equal(t, in, workloadSpecFromProto(workloadSpecToProto(in)))
	})
}

func TestSeedMetricsRoundTrip(t *testing.T) {
	t.Run("non-empty map survives proto round-trip", func(t *testing.T) {
		in := map[string]SeedMetrics{
			"hot": {
				ServedRate:       12.5,
				ComputeCostMs:    48,
				SLOSatisfiedRate: 10,
				SLOBurnedRate:    2.5,
				ParkedMs:         15,
				OriginRateFast:   20,
				OriginRateSlow:   10,
				RejectRate:       1,
			},
			"cold": {ServedRate: 0.1},
		}
		pb := seedMetricsToProto(in)
		out := seedMetricsFromProto(pb)
		require.Equal(t, in, out)
	})

	t.Run("empty map yields empty proto and empty decode", func(t *testing.T) {
		pb := seedMetricsToProto(map[string]SeedMetrics{})
		require.NotNil(t, pb)
		require.Empty(t, pb.Seeds)
		require.Empty(t, seedMetricsFromProto(pb))
	})

	t.Run("nil proto decodes to empty map", func(t *testing.T) {
		out := seedMetricsFromProto(nil)
		require.NotNil(t, out)
		require.Empty(t, out)
	})

	t.Run("nil entry inside proto is skipped", func(t *testing.T) {
		pb := &statev1.SeedMetricsChange{
			Seeds: map[string]*statev1.SeedMetrics{
				"good": {ServedRate: 1},
				"nil":  nil,
			},
		}
		out := seedMetricsFromProto(pb)
		require.Contains(t, out, "good")
		require.NotContains(t, out, "nil")
	})
}

func TestSeedMetricsIsZero(t *testing.T) {
	require.True(t, SeedMetrics{}.IsZero())
	require.False(t, SeedMetrics{ServedRate: 1}.IsZero())
	require.False(t, SeedMetrics{ComputeCostMs: 0.01}.IsZero())
	require.False(t, SeedMetrics{SLOSatisfiedRate: 0.5}.IsZero())
	require.False(t, SeedMetrics{SLOBurnedRate: 0.5}.IsZero())
	require.False(t, SeedMetrics{ParkedMs: 0.5}.IsZero())
	require.False(t, SeedMetrics{OriginRateFast: 0.5}.IsZero())
	require.False(t, SeedMetrics{OriginRateSlow: 0.5}.IsZero())
	require.False(t, SeedMetrics{RejectRate: 0.5}.IsZero())
}
