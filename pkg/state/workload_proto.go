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
		Hash:        spec.Hash,
		Name:        spec.Name,
		MinReplicas: spec.MinReplicas,
		MemoryBytes: spec.MemoryBytes,
		TimeoutMs:   uint32(spec.Timeout / time.Millisecond),
		Spread:      spec.Spread,
	}
}

// workloadSpecFromProto returns a zero-valued WorkloadSpec when pb is nil
// so callers don't need to nil-guard.
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
	}
}
