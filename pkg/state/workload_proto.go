// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

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
