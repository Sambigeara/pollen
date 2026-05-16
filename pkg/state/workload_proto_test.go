// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"testing"
	"time"

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

	// Pins the daemon-side conversion seam: the CLI builds the wire body
	// directly while the daemon builds it via workloadSpecToProto, so a
	// field added to WorkloadSpec but left unmapped here would silently
	// drop out of the signed body and break presigned publishes. Every
	// proto field must be non-zero when every struct field is.
	t.Run("every field is mapped to the proto", func(t *testing.T) {
		full := WorkloadSpec{
			Hash:        "ff",
			Name:        "svc",
			MinReplicas: 2,
			MemoryBytes: 1,
			Timeout:     time.Millisecond,
			Spread:      1,
		}
		pb := workloadSpecToProto(full)
		require.NotEmpty(t, pb.GetHash())
		require.NotEmpty(t, pb.GetName())
		require.NotZero(t, pb.GetMinReplicas())
		require.NotZero(t, pb.GetMemoryBytes())
		require.NotZero(t, pb.GetTimeoutMs())
		require.NotZero(t, pb.GetSpread())
	})
}
