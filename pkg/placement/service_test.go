// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

func newServiceForDialTests(store WorkloadState) *Service {
	return &Service{
		store:       store,
		utilisation: newUtilisationTracker(),
		manager:     newManager(nil, nil),
	}
}

func dialDrain(s *Service, dials int, caller, target string) map[string]map[string]float64 {
	for range dials {
		s.RecordDial(caller, "handle", target)
	}
	s.utilisation.tick(time.Second)
	return s.utilisation.DialRates()
}

func TestService_RecordDial_ResolvesSeedNameToHash(t *testing.T) {
	// pln://seed/<name> dials arrive with the friendly name; the dial graph
	// must store the resolved hash so it joins up with claim-keyed lookups
	// during placement scoring.
	const (
		seedName = "sink"
		seedHash = "deadbeef"
		caller   = "ingest-hash"
	)
	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: seedName, MinReplicas: 1},
				Publisher: peerKey(1),
			},
		},
	}
	s := newServiceForDialTests(store)

	got := dialDrain(s, 50, caller, "seed:"+seedName) //nolint:mnd
	require.Contains(t, got, caller)
	require.Contains(t, got[caller], "seed:"+seedHash, "name should be resolved to hash")
	require.NotContains(t, got[caller], "seed:"+seedName, "raw friendly-name target should not be stored")
}

func TestService_RecordDial_PassesServiceTargetUntouched(t *testing.T) {
	// Service targets are name-keyed end-to-end (matches snap.Services()),
	// so no rewrite happens.
	s := newServiceForDialTests(&mockStore{})
	got := dialDrain(s, 50, "caller", "service:store") //nolint:mnd
	require.Contains(t, got["caller"], "service:store")
}

func TestService_RecordDial_UnknownSeedFallsBackToPrefix(t *testing.T) {
	// resolveGlobal returns the raw identifier when no spec matches, so the
	// dial still gets recorded under the raw key — placement scoring will
	// see hosts(d) as empty and the affinity term simply contributes zero
	// RTT.
	s := newServiceForDialTests(&mockStore{})
	got := dialDrain(s, 50, "caller", "seed:no-such-name") //nolint:mnd
	require.Contains(t, got["caller"], "seed:no-such-name")
}

func newServiceForUnseedTests(localID types.PeerKey, store WorkloadState) *Service {
	return &Service{
		localID:     localID,
		store:       store,
		utilisation: newUtilisationTracker(),
		manager:     newManager(nil, nil),
		gates:       newGateRegistry(1),
	}
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
	s := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed(hash))

	store.mu.Lock()
	_, specStillThere := store.specs[hash]
	store.mu.Unlock()
	require.False(t, specStillThere, "spec should be deleted so claimants release on reconcile")
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
	s := newServiceForUnseedTests(local, store)

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
	s := newServiceForUnseedTests(local, store)

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
	s := newServiceForUnseedTests(peerKey(1), &mockStore{})

	require.ErrorIs(t, s.Unseed("deadbeef"), ErrNotRunning)
}

// TestService_Call_UnknownTarget pins the distinction between "URI names a
// target that simply doesn't exist" (caller bug) and "spec exists but
// nobody is currently claiming it" (transient placement state). The first
// must surface wasm.ErrTargetNotFound, never ErrNotRunning.
func TestService_Call_UnknownTarget(t *testing.T) {
	s := newServiceForUnseedTests(peerKey(1), &mockStore{})

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
	s := newServiceForUnseedTests(peerKey(1), store)

	_, err := s.Call(context.Background(), seedName, "handle", nil)
	require.ErrorIs(t, err, ErrNotRunning)
	require.False(t, errors.Is(err, wasm.ErrTargetNotFound), "resolved spec must not surface as not-found")
}
