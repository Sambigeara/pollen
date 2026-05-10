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
	"github.com/stretchr/testify/require"
)

func TestKeepSet_UnionsSpecsBlobSpecsAndExtras(t *testing.T) {
	snap := state.Snapshot{
		Specs: map[string]state.WorkloadSpecView{
			"wasm1": {},
			"wasm2": {},
		},
		BlobSpecs: map[string]state.BlobSpecView{
			"named1": {},
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
	snap := state.Snapshot{Specs: map[string]state.WorkloadSpecView{"w": {}}}
	require.ElementsMatch(t, []string{"w"}, slices.Collect(maps.Keys(KeepSet(snap))))
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
