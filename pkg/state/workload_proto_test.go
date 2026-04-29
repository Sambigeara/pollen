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
}
