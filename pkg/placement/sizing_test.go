// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDesiredGateSize_ColdStart: no compute observation yet — conservative
// ramp keeps instance count bounded until telemetry lands.
func TestDesiredGateSize_ColdStart(t *testing.T) {
	require.Equal(t, 2*gateInitialMultiplier, desiredGateSize(2, 0, 0))
}

// TestDesiredGateSize_Leaf: invocations observed, none of which park inside
// pollen_request (leaf seed). Active fraction = 1 → cap collapses to the
// initial-multiplier floor.
func TestDesiredGateSize_Leaf(t *testing.T) {
	require.Equal(t, 2*gateInitialMultiplier, desiredGateSize(2, 50, 0))
}

// TestDesiredGateSize_Chain: chain holder parks 80% of wall time inside
// pollen_request waiting for downstream. active = 0.2, cores=2 → cap =
// ceil(2/0.2) = 10. The whole point of this knob is that the chain-end
// host gets a wider gate than a CPU-bound leaf, so parked slots don't
// starve fresh ready work.
func TestDesiredGateSize_Chain(t *testing.T) {
	require.Equal(t, 10, desiredGateSize(2, 100, 80))
}

// TestDesiredGateSize_ParkedSpike: a momentary fully-parked sample would
// produce a zero active fraction. The floor clamps to a 10× cap so the
// gate stays bounded.
func TestDesiredGateSize_ParkedSpike(t *testing.T) {
	require.Equal(t, 20, desiredGateSize(2, 100, 100))
}
