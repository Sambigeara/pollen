// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/blobs"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestDenyOneDoesNotAffectOther is the multi-tenant isolation
// acceptance: two distinct authorities publish byte-identical content
// under the same logical name; denying one must not touch the other,
// exercised end to end through the real admission pipeline.
func TestDenyOneDoesNotAffectOther(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	aStore, pA, _, hash := publisherStore(t, rootPriv, rootPub)
	bStore, pB, _, _ := publisherStore(t, rootPriv, rootPub)

	// The observer is the cluster root: only an authoritative (root or
	// admin) deny poisons a subtree, so the denier must hold root.
	obs := validatedStore(t, types.PeerKeyFromBytes(rootPub), rootPub)
	_, _, err := obs.ApplyDelta(aStore.EncodeFull())
	require.NoError(t, err)
	_, _, err = obs.ApplyDelta(bStore.EncodeFull())
	require.NoError(t, err)

	snap := obs.Snapshot()
	_, _, okA := snap.SpecByName("echo", pA)
	_, _, okB := snap.SpecByName("echo", pB)
	require.True(t, okA, "A's echo present")
	require.True(t, okB, "B's byte-identical echo present under its own authority")

	obs.DenyPeer(pA)

	snap = obs.Snapshot()
	_, _, okA = snap.SpecByName("echo", pA)
	_, _, okB = snap.SpecByName("echo", pB)
	require.False(t, okA, "denied publisher's spec is suppressed")
	require.True(t, okB, "co-publisher of identical bytes is unaffected by the other's deny")
	require.True(t, snap.IsDenied(pA))
	require.False(t, snap.IsDenied(pB))
	require.False(t, snap.LocalPublishesWorkload(hash, pA), "A no longer pins the shared hash")
	require.True(t, snap.LocalPublishesWorkload(hash, pB), "B still pins the shared hash")
}

// TestSharedBytesLifetimeFollowsLastOwner proves the content-lifetime
// guarantee end to end through DeleteWorkloadSpec: shared bytes stay in
// the keep set until the last co-owner unseeds. This exercises the
// tombstone -> SpecsAll -> KeepSet composition on real (authority, name)
// registers, which neither the synthetic KeepSet test nor the
// single-publisher tombstone tests cover.
func TestSharedBytesLifetimeFollowsLastOwner(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	aStore, pA, _, hash := publisherStore(t, rootPriv, rootPub)
	bStore, _, _, _ := publisherStore(t, rootPriv, rootPub)

	// Any observer suffices here: replaying tombstones needs no
	// authority, unlike the deny test which must observe from root.
	obs := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	_, _, err := obs.ApplyDelta(aStore.EncodeFull())
	require.NoError(t, err)
	_, _, err = obs.ApplyDelta(bStore.EncodeFull())
	require.NoError(t, err)

	require.Contains(t, blobs.KeepSet(obs.Snapshot()), hash, "both owners pin the shared bytes")

	_, err = aStore.DeleteWorkloadSpec(hash)
	require.NoError(t, err)
	_, _, err = obs.ApplyDelta(aStore.EncodeFull())
	require.NoError(t, err)

	snap := obs.Snapshot()
	_, _, okA := snap.SpecByName("echo", pA)
	require.False(t, okA, "A's spec tombstoned")
	require.Contains(t, blobs.KeepSet(snap), hash, "co-owner B still pins the shared bytes after A unseeds")

	_, err = bStore.DeleteWorkloadSpec(hash)
	require.NoError(t, err)
	_, _, err = obs.ApplyDelta(bStore.EncodeFull())
	require.NoError(t, err)

	require.NotContains(t, blobs.KeepSet(obs.Snapshot()), hash, "bytes unpinned only after the last owner unseeds")
}
