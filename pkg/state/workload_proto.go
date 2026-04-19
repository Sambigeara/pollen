// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

// workloadSpecToProto builds the gossip-side change message owned by
// the store. Sub-millisecond and overflow durations truncate cleanly:
// callers are expected to reject those at the API boundary rather
// than rely on silent coercion.
func workloadSpecToProto(spec WorkloadSpec) *statev1.WorkloadSpecChange {
	return &statev1.WorkloadSpecChange{
		Hash:         spec.Hash,
		Name:         spec.Name,
		MinReplicas:  spec.MinReplicas,
		MemoryBytes:  spec.MemoryBytes,
		TimeoutMs:    uint32(spec.Timeout / time.Millisecond),
		Spread:       spec.Spread,
		LatencySloMs: uint32(spec.LatencySLO / time.Millisecond),
	}
}

// workloadSpecFromProto is the read-side counterpart used by snapshot
// construction. A nil input yields the zero value so callers don't
// need to nil-guard.
func workloadSpecFromProto(pb *statev1.WorkloadSpecChange) WorkloadSpec {
	if pb == nil {
		return WorkloadSpec{}
	}
	return WorkloadSpec{
		Hash:        pb.Hash,
		Name:        pb.Name,
		MinReplicas: pb.MinReplicas,
		MemoryBytes: pb.MemoryBytes,
		Timeout:     time.Duration(pb.TimeoutMs) * time.Millisecond,
		Spread:      pb.Spread,
		LatencySLO:  time.Duration(pb.LatencySloMs) * time.Millisecond,
	}
}

// seedMetricsToProto builds the gossip-side change message. Caller
// owns the source map; the returned proto is fresh and owned by
// the store.
func seedMetricsToProto(in map[string]SeedMetrics) *statev1.SeedMetricsChange {
	seeds := make(map[string]*statev1.SeedMetrics, len(in))
	for hash, m := range in {
		seeds[hash] = &statev1.SeedMetrics{
			ServedRate:       m.ServedRate,
			ComputeCostMs:    m.ComputeCostMs,
			SloSatisfiedRate: m.SLOSatisfiedRate,
			SloBurnedRate:    m.SLOBurnedRate,
			GateWaitMs:       m.GateWaitMs,
			ParkedMs:         m.ParkedMs,
		}
	}
	return &statev1.SeedMetricsChange{Seeds: seeds}
}

// seedMetricsFromProto is the read-side counterpart used by snapshot
// construction. A nil or empty input yields an empty (non-nil) map so
// callers can range over the result without nil-guarding.
func seedMetricsFromProto(in *statev1.SeedMetricsChange) map[string]SeedMetrics {
	if in == nil {
		return map[string]SeedMetrics{}
	}
	out := make(map[string]SeedMetrics, len(in.Seeds))
	for hash, m := range in.Seeds {
		if m == nil {
			continue
		}
		out[hash] = SeedMetrics{
			ServedRate:       m.ServedRate,
			ComputeCostMs:    m.ComputeCostMs,
			SLOSatisfiedRate: m.SloSatisfiedRate,
			SLOBurnedRate:    m.SloBurnedRate,
			GateWaitMs:       m.GateWaitMs,
			ParkedMs:         m.ParkedMs,
		}
	}
	return out
}
