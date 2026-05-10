// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"errors"
	"strings"
	"testing"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/stretchr/testify/require"
)

func TestRemove_NotLocal_ReturnsErrNotLocal(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	svc := &Service{store: store, local: map[string]struct{}{}}
	err = svc.Remove(strings.Repeat("ab", 32))
	require.True(t, errors.Is(err, ErrNotLocal), "expected ErrNotLocal, got %v", err)
}

func TestRemove_Local_Succeeds(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	hash, err := store.Put(strings.NewReader("payload"), testDEK(t))
	require.NoError(t, err)

	svc := &Service{store: store, local: map[string]struct{}{hash: {}}}
	require.NoError(t, svc.Remove(hash))
	require.False(t, store.Has(hash))
}
