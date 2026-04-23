// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestSubjectFromPeerKey_WithProps(t *testing.T) {
	peer := types.PeerKey{1}
	peerProps := func(pk types.PeerKey) map[string]any {
		require.Equal(t, peer, pk)
		return map[string]any{"role": "admin"}
	}
	got := evaluator.SubjectFromPeerKey(peer, peerProps)
	require.Equal(t, evaluator.Subject{
		Type:       "peer",
		ID:         peer.String(),
		Properties: map[string]any{"role": "admin"},
	}, got)
}

func TestSubjectFromPeerKey_NilLookup(t *testing.T) {
	peer := types.PeerKey{2}
	got := evaluator.SubjectFromPeerKey(peer, nil)
	require.Equal(t, evaluator.Subject{Type: "peer", ID: peer.String()}, got)
	require.Nil(t, got.Properties)
}

// ownedBy returns a seedOwnedBy lookup that accepts (hash, peer) only
// if both match the supplied pair; all other combinations are treated
// as non-owners. Tests that don't care about ownership pass alwaysOwned
// instead.
func ownedBy(owner types.PeerKey, seed string) func(string, types.PeerKey) bool {
	return func(hash string, peer types.PeerKey) bool {
		return hash == seed && peer == owner
	}
}

func alwaysOwned(string, types.PeerKey) bool { return true }

func TestSubjectFromCallerInfo_OnBehalfOfResolvesFromState(t *testing.T) {
	peer := types.PeerKey{1}
	info := evaluator.CallerInput{PeerKey: peer, OnBehalfOf: "deadbeef"}
	seedProps := func(hash string) (map[string]any, bool) {
		require.Equal(t, "deadbeef", hash)
		return map[string]any{"class": "frontend"}, true
	}
	got, err := evaluator.SubjectFromCallerInfo(info, nil, seedProps, ownedBy(peer, "deadbeef"))
	require.NoError(t, err)
	require.Equal(t, evaluator.Subject{
		Type:       "seed",
		ID:         "deadbeef",
		Properties: map[string]any{"class": "frontend"},
	}, got)
}

// Wire-borne properties must not influence Subject.Properties — the
// state lookup is the only trusted source.
func TestSubjectFromCallerInfo_IgnoresWireReportedProperties(t *testing.T) {
	peer := types.PeerKey{2}
	info := evaluator.CallerInput{
		PeerKey:    peer,
		OnBehalfOf: "deadbeef",
	}
	seedProps := func(string) (map[string]any, bool) {
		return map[string]any{"class": "frontend"}, true
	}
	got, err := evaluator.SubjectFromCallerInfo(info, nil, seedProps, alwaysOwned)
	require.NoError(t, err)
	require.Equal(t, "frontend", got.Properties["class"])
}

func TestSubjectFromCallerInfo_FallsBackToPeer(t *testing.T) {
	peer := types.PeerKey{3}
	info := evaluator.CallerInput{PeerKey: peer}
	peerProps := func(pk types.PeerKey) map[string]any {
		require.Equal(t, peer, pk)
		return map[string]any{"role": "admin"}
	}
	got, err := evaluator.SubjectFromCallerInfo(info, peerProps, nil, nil)
	require.NoError(t, err)
	require.Equal(t, evaluator.Subject{
		Type:       "peer",
		ID:         peer.String(),
		Properties: map[string]any{"role": "admin"},
	}, got)
}

// A forged OnBehalfOf hash — one that doesn't resolve in cluster state —
// must return ErrSeedUnknown. Silently attributing to the host peer or
// producing a seed-typed Subject with empty properties would pass most
// permissive policies.
func TestSubjectFromCallerInfo_UnknownSeedIsRejected(t *testing.T) {
	info := evaluator.CallerInput{PeerKey: types.PeerKey{4}, OnBehalfOf: "forgedhash"}
	seedProps := func(string) (map[string]any, bool) { return nil, false }
	_, err := evaluator.SubjectFromCallerInfo(info, nil, seedProps, alwaysOwned)
	require.Error(t, err)
	require.True(t, errors.Is(err, evaluator.ErrSeedUnknown))
}

// seedProps or seedOwnedBy left nil alongside an OnBehalfOf envelope is
// a wiring bug — fail closed rather than attributing to the host peer.
func TestSubjectFromCallerInfo_MissingOnBehalfOfLookup(t *testing.T) {
	info := evaluator.CallerInput{PeerKey: types.PeerKey{5}, OnBehalfOf: "deadbeef"}
	_, err := evaluator.SubjectFromCallerInfo(info, nil, nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, evaluator.ErrSeedUnknown))
}

// A seed that exists in state but isn't claimed by the transport-
// authenticated peer must be rejected with ErrSeedNotClaimedByPeer —
// otherwise any member peer could forge OnBehalfOf to inherit any
// seed's policy context.
func TestSubjectFromCallerInfo_SeedNotClaimedByPeer(t *testing.T) {
	runner := types.PeerKey{6}
	attacker := types.PeerKey{7}
	info := evaluator.CallerInput{PeerKey: attacker, OnBehalfOf: "deadbeef"}
	seedProps := func(string) (map[string]any, bool) {
		return map[string]any{"class": "frontend"}, true
	}
	_, err := evaluator.SubjectFromCallerInfo(info, nil, seedProps, ownedBy(runner, "deadbeef"))
	require.Error(t, err)
	require.True(t, errors.Is(err, evaluator.ErrSeedNotClaimedByPeer))
}
