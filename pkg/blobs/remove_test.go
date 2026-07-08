// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"strings"
	"testing"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/stretchr/testify/require"
)

// Remove tombstones the spec and leaves the bytes for the keep-set
// janitor: a digest may still be referenced by another owner's live
// spec, so synchronous erasure here would strand a co-owner. Byte
// reclamation is proved in the Prune tests.
func TestRemove_LeavesBytesForJanitor(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	hash, err := store.Put(strings.NewReader("payload"), testDEK(t))
	require.NoError(t, err)

	svc := &Service{store: store, local: map[string]struct{}{hash: {}}}
	require.NoError(t, svc.Remove(hash))
	require.True(t, store.Has(hash), "Remove must defer byte eviction to the janitor")
}
