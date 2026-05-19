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
		SpecsAll: []state.WorkloadSpecView{
			{Spec: state.WorkloadSpec{Hash: "wmine", Name: "fnmine"}, Publisher: tenant},
			{Spec: state.WorkloadSpec{Hash: "wother", Name: "fnother"}, Publisher: other},
		},
		StaticSpecsAll: []state.StaticSpecView{
			{Spec: state.StaticSpec{Name: "smine"}, Publisher: tenant},
			{Spec: state.StaticSpec{Name: "sother"}, Publisher: other},
		},
		BlobSpecsAll: []state.BlobSpecView{
			{Spec: state.BlobSpec{Name: "blmine", Digest: "bmine"}, Publisher: tenant},
			{Spec: state.BlobSpec{Name: "blother", Digest: "bother"}, Publisher: other},
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
		require.Equal(t, []string{"wmine"}, pluck(sv.Workloads, func(v state.WorkloadSpecView) string { return v.Spec.Hash }))
		require.Equal(t, []string{"smine"}, pluck(sv.Statics, func(v state.StaticSpecView) string { return v.Spec.Name }))
		require.Equal(t, []string{"bmine"}, pluck(sv.Blobs, func(v state.BlobSpecView) string { return v.Spec.Digest }))

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

// TestProjectSurfacesCollidingAuthorities proves the P2 reshape: when
// two principals publish byte-identical content, the listing projection
// no longer collapses to a single lowest-publisher winner. An admin
// sees BOTH (authority, name) publications, order-independently, and a
// tenant still sees only its own under the shared content hash.
func TestProjectSurfacesCollidingAuthorities(t *testing.T) {
	lo, hi := key(1), key(2)
	require.True(t, lo.Compare(hi) < 0)
	forward := []state.WorkloadSpecView{
		{Spec: state.WorkloadSpec{Hash: "shared", Name: "a"}, Publisher: lo},
		{Spec: state.WorkloadSpec{Hash: "shared", Name: "b"}, Publisher: hi},
	}
	reversed := []state.WorkloadSpecView{forward[1], forward[0]}

	for _, order := range [][]state.WorkloadSpecView{forward, reversed} {
		snap := state.Snapshot{SpecsAll: order}

		admin := view.Project(snap, view.LensFor(grant(key(9), true)))
		require.Len(t, admin.Workloads, 2)
		got := map[types.PeerKey]string{}
		for _, w := range admin.Workloads {
			got[w.Publisher] = w.Spec.Name
		}
		require.Equal(t, map[types.PeerKey]string{lo: "a", hi: "b"}, got)

		hiOwn := view.Project(snap, view.LensFor(grant(hi, false)))
		require.Len(t, hiOwn.Workloads, 1)
		require.Equal(t, hi, hiOwn.Workloads[0].Publisher)
		require.Equal(t, "b", hiOwn.Workloads[0].Spec.Name)
	}
}

func pluck[T any](xs []T, key func(T) string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, key(x))
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
