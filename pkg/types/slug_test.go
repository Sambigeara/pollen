// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublisherSlug_DeterministicAndShaped(t *testing.T) {
	pk := PeerKey{1, 2, 3, 4}
	got := PublisherSlug(pk[:])
	require.Equal(t, SlugLen, len(got))
	require.Equal(t, got, PublisherSlug(pk[:]), "must be deterministic")
	require.True(t, IsValidSlug(got))
}

func TestPublisherSlug_DistinctKeysDistinctSlugs(t *testing.T) {
	a := PeerKey{1}.Slug()
	b := PeerKey{2}.Slug()
	require.NotEqual(t, a, b)
}

func TestPublisherSlug_NeverReturnsReservedSentinel(t *testing.T) {
	for i := range 1000 {
		pk := PeerKey{byte(i), byte(i >> 8)}
		require.NotEqual(t, ReservedBearerSlug, pk.Slug())
		require.False(t, strings.Contains(pk.Slug(), ReservedBearerSlug))
	}
}

func TestIsValidSlug(t *testing.T) {
	require.True(t, IsValidSlug(PeerKey{1}.Slug()))
	require.False(t, IsValidSlug(""))
	require.False(t, IsValidSlug(ReservedBearerSlug))
	require.False(t, IsValidSlug("toolong-toolong-toolong"))
	require.False(t, IsValidSlug("UPPERCASE123"))
	require.False(t, IsValidSlug("abci0lou12345"[:SlugLen]), "must reject excluded letters i,l,o,u")
}
