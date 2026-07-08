// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestKeepSet_UnionsSpecsBlobSpecsAndExtras(t *testing.T) {
	snap := state.Snapshot{
		SpecsAll: []state.WorkloadSpecView{
			{Spec: state.WorkloadSpec{Hash: "wasm1", Name: "fn1"}},
			{Spec: state.WorkloadSpec{Hash: "wasm2", Name: "fn2"}},
		},
		BlobSpecsAll: []state.BlobSpecView{
			{Spec: state.BlobSpec{Digest: "named1", Name: "b1"}},
		},
	}
	staticBlobs := map[string]struct{}{
		"manifest1": {},
		"file1":     {},
	}

	keep := KeepSet(snap, staticBlobs)
	require.ElementsMatch(t,
		[]string{"wasm1", "wasm2", "named1", "manifest1", "file1"},
		slices.Collect(maps.Keys(keep)))
}

func TestKeepSet_NoExtras(t *testing.T) {
	snap := state.Snapshot{SpecsAll: []state.WorkloadSpecView{{Spec: state.WorkloadSpec{Hash: "w", Name: "fn"}}}}
	require.ElementsMatch(t, []string{"w"}, slices.Collect(maps.Keys(KeepSet(snap))))
}

// A content hash is pinned while any owner's (authority, name) spec
// references it. KeepSet reads the un-deduped sources, so a tie-break
// loser's reference still pins bytes the co-owner serves; the hash only
// drops once every owner has tombstoned.
func TestKeepSet_MultiOwnerCoOwnerStillPins(t *testing.T) {
	ownerA := state.WorkloadSpecView{Spec: state.WorkloadSpec{Hash: "wasmX", Name: "a-fn"}, Publisher: types.PeerKey{0xaa}}
	ownerB := state.WorkloadSpecView{Spec: state.WorkloadSpec{Hash: "wasmX", Name: "b-fn"}, Publisher: types.PeerKey{0xbb}}
	blobA := state.BlobSpecView{Spec: state.BlobSpec{Digest: "blobX", Name: "a-b"}, Publisher: types.PeerKey{0xaa}}
	blobB := state.BlobSpecView{Spec: state.BlobSpec{Digest: "blobX", Name: "b-b"}, Publisher: types.PeerKey{0xbb}}

	both := KeepSet(state.Snapshot{
		SpecsAll:     []state.WorkloadSpecView{ownerA, ownerB},
		BlobSpecsAll: []state.BlobSpecView{blobA, blobB},
	})
	require.Contains(t, both, "wasmX")
	require.Contains(t, both, "blobX")

	onlyB := KeepSet(state.Snapshot{
		SpecsAll:     []state.WorkloadSpecView{ownerB},
		BlobSpecsAll: []state.BlobSpecView{blobB},
	})
	require.Contains(t, onlyB, "wasmX")
	require.Contains(t, onlyB, "blobX")

	none := KeepSet(state.Snapshot{})
	require.NotContains(t, none, "wasmX")
	require.NotContains(t, none, "blobX")
}

func TestPrune_EvictsOrphansKeepsReferenced(t *testing.T) {
	dir := t.TempDir()
	store, err := cas.New(dir)
	require.NoError(t, err)

	keepHash := putAged(t, store, dir, "keep-me")
	orphanHash := putAged(t, store, dir, "orphan")

	svc := &Service{store: store, local: map[string]struct{}{keepHash: {}, orphanHash: {}}}

	keep := map[string]struct{}{keepHash: {}}
	removed, err := svc.Prune(keep, 5*time.Minute)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{orphanHash}, removed)
	require.True(t, store.Has(keepHash))
	require.False(t, store.Has(orphanHash))
}

func TestPrune_GracePeriodProtectsRecentBlobs(t *testing.T) {
	dir := t.TempDir()
	store, err := cas.New(dir)
	require.NoError(t, err)

	hash, err := store.Put(strings.NewReader("fresh"), testDEK(t))
	require.NoError(t, err)

	svc := &Service{store: store, local: map[string]struct{}{hash: {}}}

	removed, err := svc.Prune(map[string]struct{}{}, 5*time.Minute)
	require.NoError(t, err)
	require.Empty(t, removed)
	require.True(t, store.Has(hash))
}

func TestPrune_ZeroGraceEvictsImmediately(t *testing.T) {
	dir := t.TempDir()
	store, err := cas.New(dir)
	require.NoError(t, err)

	hash, err := store.Put(strings.NewReader("payload"), testDEK(t))
	require.NoError(t, err)

	svc := &Service{store: store, local: map[string]struct{}{hash: {}}}

	removed, err := svc.Prune(map[string]struct{}{}, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{hash}, removed)
	require.False(t, store.Has(hash))
}

// putAged stamps the file mtime an hour in the past so prune's grace
// window can't protect it. Callers always want this so the parameter is
// implicit.
func putAged(t *testing.T, store *cas.Store, dir, content string) string {
	t.Helper()
	hash, err := store.Put(strings.NewReader(content), testDEK(t))
	require.NoError(t, err)
	path := filepath.Join(dir, "cas", hash[:2], hash)
	when := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(path, when, when))
	return hash
}

func testDEK(t *testing.T) []byte {
	t.Helper()
	dek, err := cas.GenerateDEK()
	require.NoError(t, err)
	return dek
}
