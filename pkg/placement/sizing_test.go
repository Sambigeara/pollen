// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDesiredGateSize_ColdStart: no invocations yet → conservative ramp.
func TestDesiredGateSize_ColdStart(t *testing.T) {
	require.Equal(t, 2*gateInitialMultiplier, desiredGateSize(2, 0, 0))
}

// TestDesiredGateSize_Leaf: observed invocations but no outbound dials.
// Cap at host CPU count — pure CPU throughput, more instances thrash.
func TestDesiredGateSize_Leaf(t *testing.T) {
	require.Equal(t, 2, desiredGateSize(2, 0, 100))
}

// TestDesiredGateSize_Chain: chain holder with one outbound dial per
// invocation. Sizer expands the gate to absorb downstream latency.
// NumCPU × (1 + 4×1) = 10.
func TestDesiredGateSize_Chain(t *testing.T) {
	require.Equal(t, 10, desiredGateSize(2, 50, 50))
}

// TestDesiredGateSize_DeeperChain: two outbound dials per invocation,
// bigger concurrency budget for the parked callers.
func TestDesiredGateSize_DeeperChain(t *testing.T) {
	require.Equal(t, 18, desiredGateSize(2, 100, 50))
}
