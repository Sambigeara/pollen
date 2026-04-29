// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func peerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

func TestEvaluate_NonClaimantDoesNothing(t *testing.T) {
	local := peerKey(1)
	other := peerKey(2)
	actions := evaluate(evaluateInput{
		localID:  local,
		allPeers: []types.PeerKey{local, other},
		specs: map[string]spec{
			"h1": {MinReplicas: 1},
		},
		claims:    map[string]map[types.PeerKey]struct{}{},
		draining:  map[string]map[types.PeerKey]struct{}{},
		isRunning: func(string) bool { return false },
	})

	require.Empty(t, actions)
}

func TestEvaluate_RecoversWhenLocallyDropped(t *testing.T) {
	local := peerKey(1)
	actions := evaluate(evaluateInput{
		localID:  local,
		allPeers: []types.PeerKey{local},
		specs: map[string]spec{
			"h1": {MinReplicas: 1},
		},
		claims: map[string]map[types.PeerKey]struct{}{
			"h1": {local: {}},
		},
		draining:  map[string]map[types.PeerKey]struct{}{},
		isRunning: func(string) bool { return false },
	})

	require.Len(t, actions, 1)
	require.Equal(t, actionClaim, actions[0].Kind)
}

func TestEvaluate_ReleasesWhenSpecGone(t *testing.T) {
	local := peerKey(1)
	actions := evaluate(evaluateInput{
		localID:  local,
		allPeers: []types.PeerKey{local},
		specs:    map[string]spec{},
		claims: map[string]map[types.PeerKey]struct{}{
			"orphan": {local: {}},
		},
		draining:  map[string]map[types.PeerKey]struct{}{},
		isRunning: func(string) bool { return true },
	})

	require.Len(t, actions, 1)
	require.Equal(t, "orphan", actions[0].Hash)
	require.Equal(t, actionRelease, actions[0].Kind)
}

func TestEvaluate_DrainingClaimReleasesAtTarget(t *testing.T) {
	local := peerKey(1)
	other := peerKey(2)
	actions := evaluate(evaluateInput{
		localID:  local,
		allPeers: []types.PeerKey{local, other},
		specs: map[string]spec{
			"h1": {MinReplicas: 1},
		},
		claims: map[string]map[types.PeerKey]struct{}{
			"h1": {local: {}, other: {}},
		},
		draining: map[string]map[types.PeerKey]struct{}{
			"h1": {local: {}},
		},
		isRunning: func(string) bool { return true },
	})

	require.Len(t, actions, 1)
	require.Equal(t, actionRelease, actions[0].Kind)
}

func TestEvaluate_DrainingClaimUndrainsBelowTarget(t *testing.T) {
	local := peerKey(1)
	actions := evaluate(evaluateInput{
		localID:  local,
		allPeers: []types.PeerKey{local},
		specs: map[string]spec{
			"h1": {MinReplicas: 1},
		},
		claims: map[string]map[types.PeerKey]struct{}{
			"h1": {local: {}},
		},
		draining: map[string]map[types.PeerKey]struct{}{
			"h1": {local: {}},
		},
		isRunning: func(string) bool { return true },
	})

	require.Len(t, actions, 1)
	require.Equal(t, actionClaim, actions[0].Kind)
}

func TestEffectiveTarget(t *testing.T) {
	t.Run("Spread fills the cluster", func(t *testing.T) {
		require.Equal(t, uint32(4), effectiveTarget(2, 1.0, 4))
	})
	t.Run("MinReplicas caps at cluster size", func(t *testing.T) {
		require.Equal(t, uint32(2), effectiveTarget(2, 0, 4))
	})
	t.Run("MinReplicas larger than cluster floors at cluster", func(t *testing.T) {
		require.Equal(t, uint32(4), effectiveTarget(10, 0, 4))
	})
}
