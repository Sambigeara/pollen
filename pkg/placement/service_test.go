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
	return New(localID, store, blobs, nil), blobs
}

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

func TestService_Unseed_UnknownHash(t *testing.T) {
	s, _ := newServiceForUnseedTests(peerKey(1), &mockStore{})

	require.ErrorIs(t, s.Unseed("deadbeef"), ErrNotRunning)
}

func TestService_Call_UnknownTarget(t *testing.T) {
	s, _ := newServiceForUnseedTests(peerKey(1), &mockStore{})

	_, err := s.Call(context.Background(), "no-such-name", "handle", nil)
	require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	require.False(t, errors.Is(err, ErrNotRunning), "must not collapse into ErrNotRunning")
}

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

func TestService_callLocal_RefusedWhenBudgetExhausted(t *testing.T) {
	local := peerKey(1)
	const seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: "sink", MinReplicas: 1},
				Publisher: local,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)

	const totalBytes = int64(1 << 20)
	s.budget = newBudget(totalBytes)
	require.True(t, s.budget.Reserve("filler", totalBytes))

	_, err := s.callLocal(context.Background(), seedHash, "handle", nil)

	var ovl *OverloadError
	require.ErrorAs(t, err, &ovl)
	require.ErrorIs(t, err, ErrOverloaded)
	require.Equal(t, "node memory budget exhausted", ovl.Reason)
	require.True(t, s.backoff.IsLocallyOverloaded(), "refusal must arm backoff so peers' dispatchers divert future calls")
}

func TestService_callLocal_ReleasesAdmissionOnReturn(t *testing.T) {
	local := peerKey(1)
	const seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: "sink", MinReplicas: 1},
				Publisher: local,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)
	s.budget = newBudget(replicaMemoryBytes(0) * 2)

	_, err := s.callLocal(context.Background(), seedHash, "handle", nil)
	require.ErrorIs(t, err, ErrNotRunning)
	require.Zero(t, s.budget.reserved.Load(), "release must run regardless of how the call exits")
}
