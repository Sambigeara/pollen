// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

func newServiceForUnseedTests(localID types.PeerKey, store WorkloadState) (*Service, *mockBlobs) {
	blobs := &mockBlobs{}
	return &Service{
		localID:     localID,
		store:       store,
		utilisation: newUtilisationTracker(),
		manager:     newManager(nil, nil),
		blobs:       blobs,
		gates:       newGateRegistry(1),
	}, blobs
}

func TestNew_NormalisesBudgetDefaults(t *testing.T) {
	// Without normalisation, an unset memBudget admits at 80% locally
	// (admission.go default) but gossips a raw 0 — and remote scoring
	// reads 0 as 100% (no constraint), letting a saturated peer still
	// win claims. Pin both budgets to their effective defaults so the
	// gossiped, locally-enforced, and admissionState views agree.
	s := New(peerKey(1), nil, &mockBlobs{}, nil)
	require.Equal(t, defaultMemBudgetPercent, s.memBudgetPercent)
	require.Equal(t, defaultCPUBudgetPercent, s.cpuBudgetPercent)

	// Explicit non-zero budgets pass through unchanged.
	s2 := New(peerKey(1), nil, &mockBlobs{}, nil, WithResourceBudget(50, 60))
	require.Equal(t, uint32(50), s2.cpuBudgetPercent)
	require.Equal(t, uint32(60), s2.memBudgetPercent)
}

// TestService_Unseed_WhenNotLocallyClaimed pins the bug where `pln unseed`
// on the publisher failed because the local manager wasn't running the
// workload — e.g. placement had evicted the claim to another node. The
// authoritative tear-down is the spec deletion (gossip propagates to
// claimants), not the local manager's runtime state.
func TestService_Unseed_WhenNotLocallyClaimed(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload-xyz"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "foo", MinReplicas: 1},
				Publisher: local,
			},
		},
		claims: map[string]map[types.PeerKey]struct{}{hash: {peer2: {}}},
	}
	s, _ := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed(hash))

	store.mu.Lock()
	_, specStillThere := store.specs[hash]
	store.mu.Unlock()
	require.False(t, specStillThere, "spec should be deleted so claimants release on reconcile")
}

// TestService_Unseed_EvictsWasmBlob pins the invariant that unseed
// evicts the workload's wasm bytes from the local CAS — otherwise every
// unseed leaves an orphan behind.
func TestService_Unseed_EvictsWasmBlob(t *testing.T) {
	local := peerKey(1)
	hash := "workload-xyz"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "foo", MinReplicas: 1},
				Publisher: local,
			},
		},
	}
	s, blobs := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed(hash))
	require.Equal(t, []string{hash}, blobs.removed)
}

// TestService_Unseed_RejectsNonOwner guards against a silent no-op when
// unseed runs against a node that didn't publish the spec:
// DeleteWorkloadSpec only tombstones the local log, so a non-owner's call
// would do nothing. The handler must resolve the name globally — an admin
// that didn't publish the seed still needs to see the ownership error
// rather than a useless "workload not running: <name>".
func TestService_Unseed_RejectsNonOwner(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload-xyz"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "foo", MinReplicas: 1},
				Publisher: peer2,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)

	err := s.Unseed("foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), peer2.Short(), "error should point the operator at the publisher")
	require.NotContains(t, err.Error(), ErrNotRunning.Error(), "must not fall through to the generic not-running branch")
}

// TestService_Unseed_LocalNameWins verifies the local-first bias in name
// resolution: if two peers publish specs under the same name, the local
// publisher's spec is selected so operators don't accidentally delete (or
// in this case, fail to delete) their own seed just because a remote
// namesake exists.
func TestService_Unseed_LocalNameWins(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	localHash := "local-hash"
	remoteHash := "remote-hash"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			localHash: {
				Spec:      state.WorkloadSpec{Hash: localHash, Name: "foo", MinReplicas: 1},
				Publisher: local,
			},
			remoteHash: {
				Spec:      state.WorkloadSpec{Hash: remoteHash, Name: "foo", MinReplicas: 1},
				Publisher: peer2,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed("foo"))

	store.mu.Lock()
	_, localStillThere := store.specs[localHash]
	_, remoteStillThere := store.specs[remoteHash]
	store.mu.Unlock()
	require.False(t, localStillThere, "local spec should be deleted")
	require.True(t, remoteStillThere, "remote spec must remain untouched")
}

// TestService_Unseed_UnknownHash ensures an obviously bogus identifier
// still errors — after the fix, Unseed no longer gates on local manager
// state, so a dedicated "nothing to unseed" branch replaces the previous
// ErrNotRunning path.
func TestService_Unseed_UnknownHash(t *testing.T) {
	s, _ := newServiceForUnseedTests(peerKey(1), &mockStore{})

	require.ErrorIs(t, s.Unseed("deadbeef"), ErrNotRunning)
}

// TestService_Call_UnknownTarget pins the distinction between "URI names a
// target that simply doesn't exist" (caller bug) and "spec exists but
// nobody is currently claiming it" (transient placement state). The first
// must surface wasm.ErrTargetNotFound, never ErrNotRunning.
func TestService_Call_UnknownTarget(t *testing.T) {
	s, _ := newServiceForUnseedTests(peerKey(1), &mockStore{})

	_, err := s.Call(context.Background(), "no-such-name", "handle", nil)
	require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	require.False(t, errors.Is(err, ErrNotRunning), "must not collapse into ErrNotRunning")
}

// TestService_Call_KnownSpecNoClaimants is the inverse: the spec resolves
// fine but no node currently claims it. This is transient and must keep
// returning ErrNotRunning.
func TestService_Call_KnownSpecNoClaimants(t *testing.T) {
	const (
		seedName = "sink"
		seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	)
	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: seedName, MinReplicas: 1},
				Publisher: peerKey(2),
			},
		},
	}
	s, _ := newServiceForUnseedTests(peerKey(1), store)

	_, err := s.Call(context.Background(), seedName, "handle", nil)
	require.ErrorIs(t, err, ErrNotRunning)
	require.False(t, errors.Is(err, wasm.ErrTargetNotFound), "resolved spec must not surface as not-found")
}
