// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package view_test

import (
	"testing"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/view"
	"github.com/stretchr/testify/require"
)

func key(b byte) types.PeerKey {
	raw := make([]byte, 32)
	raw[0] = b
	return types.PeerKeyFromBytes(raw)
}

func grant(subject types.PeerKey, admin bool) *identityv1.Grant {
	return &identityv1.Grant{Claims: &identityv1.GrantClaims{
		SubjectPub:   subject.Bytes(),
		Capabilities: &identityv1.Capabilities{CanAdmit: admin},
	}}
}

func TestLensPermits(t *testing.T) {
	tenant := key(1)
	other := key(2)

	t.Run("nil grant denies everything", func(t *testing.T) {
		l := view.LensFor(nil)
		require.False(t, l.Admin())
		require.False(t, l.Permits(tenant))
		require.False(t, l.Permits(other))
	})

	t.Run("tenant sees only itself", func(t *testing.T) {
		l := view.LensFor(grant(tenant, false))
		require.False(t, l.Admin())
		require.Equal(t, tenant, l.Subject())
		require.True(t, l.Permits(tenant))
		require.False(t, l.Permits(other))
	})

	t.Run("admin sees all", func(t *testing.T) {
		l := view.LensFor(grant(key(9), true))
		require.True(t, l.Admin())
		require.True(t, l.Permits(tenant))
		require.True(t, l.Permits(other))
	})
}

func TestProjectScopesResourcesByAuthority(t *testing.T) {
	tenant := key(1)
	other := key(2)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			key(10): {}, key(11): {}, key(12): {},
		},
		Specs: map[string]state.WorkloadSpecView{
			"wmine":  {Publisher: tenant},
			"wother": {Publisher: other},
		},
		StaticSpecs: map[string]state.StaticSpecView{
			"smine":  {Publisher: tenant},
			"sother": {Publisher: other},
		},
		BlobSpecs: map[string]state.BlobSpecView{
			"bmine":  {Publisher: tenant},
			"bother": {Publisher: other},
		},
		WorkloadStoringPeers: map[string]map[types.PeerKey]struct{}{
			"wmine": {key(10): {}},
		},
		Claims: map[string]map[types.PeerKey]struct{}{
			"wmine": {key(11): {}},
		},
		StaticStoringPeers: map[string]map[types.PeerKey]struct{}{
			"smine": {key(12): {}},
		},
		BlobStoringPeers: map[string]map[types.PeerKey]struct{}{
			"bother": {key(10): {}},
		},
	}

	t.Run("admin gets the unfiltered cluster", func(t *testing.T) {
		sv := view.Project(snap, view.LensFor(grant(key(9), true)))
		require.Len(t, sv.Workloads, 2)
		require.Len(t, sv.Statics, 2)
		require.Len(t, sv.Blobs, 2)
		require.Len(t, sv.Nodes, 3)
	})

	t.Run("tenant sees only its own facts and their holders", func(t *testing.T) {
		sv := view.Project(snap, view.LensFor(grant(tenant, false)))
		require.Equal(t, []string{"wmine"}, keysOf(sv.Workloads))
		require.Equal(t, []string{"smine"}, keysOf(sv.Statics))
		require.Equal(t, []string{"bmine"}, keysOf(sv.Blobs))

		// Nodes = storers + claimants of the tenant's own facts only:
		// key(10) stores wmine, key(11) claims wmine, key(12) stores
		// smine. key(10) also stores bother, but that is another
		// tenant's blob and must not widen this tenant's node view
		// beyond what it already sees via wmine.
		require.ElementsMatch(t, []types.PeerKey{key(10), key(11), key(12)}, nodeKeys(sv.Nodes))
	})

	t.Run("fresh tenant with no facts sees no nodes", func(t *testing.T) {
		sv := view.Project(snap, view.LensFor(grant(key(7), false)))
		require.Empty(t, sv.Workloads)
		require.Empty(t, sv.Statics)
		require.Empty(t, sv.Blobs)
		require.Empty(t, sv.Nodes)
	})

	t.Run("holder absent from the node set is dropped", func(t *testing.T) {
		s2 := snap
		s2.Claims = map[string]map[types.PeerKey]struct{}{
			"wmine": {key(99): {}}, // key(99) has no NodeView
		}
		sv := view.Project(s2, view.LensFor(grant(tenant, false)))
		require.NotContains(t, sv.Nodes, key(99))
		require.Contains(t, sv.Nodes, key(10)) // still a storer of wmine
	})
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func nodeKeys(m map[types.PeerKey]state.NodeView) []types.PeerKey {
	out := make([]types.PeerKey, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
