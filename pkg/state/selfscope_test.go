// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// TestDeleteSpecSelfScopesToLocalAuthority locks the store property the
// control ownership-chokepoint consolidation depends on:
// Delete{Workload,Static,Blob}Spec only tombstones the local node's own
// (authority, key) register. A non-publisher deleting another
// authority's spec is rejected with ErrUnseedNotAuthored and mints no
// events; the handler maps this to NotFound so the CLI no longer prints
// "unseeded" on a foreign publication. The redundant per-call-site
// ownership guards were removed on the strength of this backstop; if it
// regresses, that removal becomes unsafe.
func TestDeleteSpecSelfScopesToLocalAuthority(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	aStore, pA, _, hash := publisherStore(t, rootPriv, rootPub)

	const staticName, blobName = "site", "data"
	manifest := hex.EncodeToString(bytes.Repeat([]byte{0xbb}, 32))
	digest := hex.EncodeToString(bytes.Repeat([]byte{0xcc}, 32))
	_, err := aStore.SetStaticSpec(state.StaticSpec{Name: staticName, ManifestDigest: manifest}, nil)
	require.NoError(t, err)
	_, err = aStore.SetBlobSpec(state.BlobSpec{Name: blobName, Digest: digest}, nil)
	require.NoError(t, err)

	// Node B holds a different authority and never published any of these.
	bKey := types.PeerKeyFromBytes([]byte{0x09})
	bStore := validatedStore(t, bKey, rootPub)
	_, _, err = bStore.ApplyDelta(aStore.EncodeFull())
	require.NoError(t, err)

	pre := bStore.Snapshot()
	require.True(t, pre.LocalPublishesWorkload(hash, pA), "A is the workload publisher")
	require.True(t, pre.LocalPublishesStatic(staticName, pA), "A is the static publisher")
	require.True(t, pre.LocalPublishesBlob(digest, pA), "A is the blob publisher")
	require.False(t, pre.LocalPublishesWorkload(hash, bKey), "B never published the workload")

	wev, err := bStore.DeleteWorkloadSpec(hash)
	require.ErrorIs(t, err, state.ErrUnseedNotAuthored)
	require.Empty(t, wev, "non-publisher delete mints no events")
	sev, err := bStore.DeleteStaticSpec(staticName)
	require.ErrorIs(t, err, state.ErrUnseedNotAuthored)
	require.Empty(t, sev, "non-publisher delete mints no events")
	bev, err := bStore.DeleteBlobSpec(digest)
	require.ErrorIs(t, err, state.ErrUnseedNotAuthored)
	require.Empty(t, bev, "non-publisher delete mints no events")

	post := bStore.Snapshot()
	require.True(t, post.LocalPublishesWorkload(hash, pA), "A's workload untouched by B's delete")
	require.True(t, post.LocalPublishesStatic(staticName, pA), "A's static untouched by B's delete")
	require.True(t, post.LocalPublishesBlob(digest, pA), "A's blob untouched by B's delete")
}
