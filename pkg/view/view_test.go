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

func infraGrant(subject types.PeerKey) *identityv1.Grant {
	return &identityv1.Grant{Claims: &identityv1.GrantClaims{
		SubjectPub:   subject.Bytes(),
		Capabilities: &identityv1.Capabilities{IsInfrastructure: true},
	}}
}

// wsGrant builds a workspace-admin grant with the given subject and
// chain. Chain entries are leaf-to-root: chain[0] is the immediate
// parent.
func wsGrant(subject types.PeerKey, ws bool, chain []*identityv1.Grant) *identityv1.Grant {
	return &identityv1.Grant{
		Claims: &identityv1.GrantClaims{
			SubjectPub:   subject.Bytes(),
			Capabilities: &identityv1.Capabilities{IsWorkspaceAdmin: ws},
		},
		Chain: chain,
	}
}

func snapWith(grants map[types.PeerKey]*identityv1.Grant) state.Snapshot {
	nodes := make(map[types.PeerKey]state.NodeView, len(grants))
	for k, g := range grants {
		nodes[k] = state.NodeView{Grant: g}
	}
	return state.Snapshot{Nodes: nodes}
}

// TestPermits pins the cluster visibility rule across the four shapes
// it composes: admin shortcut, self, own subtree, plus the role-specific
// extras (workspace-admin's chain ancestors; publisher's workspace
// peers).
func TestPermits(t *testing.T) {
	root := key(0xFF)
	rootGrant := wsGrant(root, false, nil)
	wsAdminA := key(0x10)
	wsAdminAGrant := wsGrant(wsAdminA, true, []*identityv1.Grant{rootGrant})
	wsAdminB := key(0x20)
	wsAdminBGrant := wsGrant(wsAdminB, true, []*identityv1.Grant{rootGrant})
	alice := key(0xa1)
	aliceGrant := wsGrant(alice, false, []*identityv1.Grant{wsAdminAGrant, rootGrant})
	bob := key(0xb0)
	bobGrant := wsGrant(bob, false, []*identityv1.Grant{wsAdminAGrant, rootGrant})
	user := key(0xc0)
	userGrant := wsGrant(user, false, []*identityv1.Grant{wsAdminBGrant, rootGrant})
	nested := key(0x30)
	nestedGrant := wsGrant(nested, true, []*identityv1.Grant{aliceGrant, wsAdminAGrant, rootGrant})
	eve := key(0xe1)
	eveGrant := wsGrant(eve, false, []*identityv1.Grant{nestedGrant, aliceGrant, wsAdminAGrant, rootGrant})

	snap := snapWith(map[types.PeerKey]*identityv1.Grant{
		wsAdminA: wsAdminAGrant,
		wsAdminB: wsAdminBGrant,
		alice:    aliceGrant,
		bob:      bobGrant,
		user:     userGrant,
		nested:   nestedGrant,
		eve:      eveGrant,
	})

	t.Run("admin sees everyone", func(t *testing.T) {
		l := view.LensFor(grant(key(0x01), true))
		for _, pub := range []types.PeerKey{wsAdminA, wsAdminB, alice, bob, user, nested, eve} {
			require.True(t, view.Permits(l, pub, snap))
		}
	})

	t.Run("zero lens sees nothing", func(t *testing.T) {
		require.False(t, view.Permits(view.LensFor(nil), alice, snap))
	})

	t.Run("publisher sees own and workspace peers, not other workspaces", func(t *testing.T) {
		l := view.LensFor(aliceGrant)
		require.True(t, view.Permits(l, alice, snap), "alice sees herself")
		require.True(t, view.Permits(l, wsAdminA, snap), "alice sees her workspace-admin")
		require.True(t, view.Permits(l, bob, snap), "alice sees her workspace peer")
		require.False(t, view.Permits(l, wsAdminB, snap), "alice does not see another workspace-admin")
		require.False(t, view.Permits(l, user, snap), "alice does not see another workspace's publisher")
	})

	t.Run("publisher sees own subtree across nested workspace", func(t *testing.T) {
		l := view.LensFor(aliceGrant)
		require.True(t, view.Permits(l, nested, snap), "alice sees the sub-workspace she founded")
		require.True(t, view.Permits(l, eve, snap), "alice sees inside her own nested workspace")
	})

	t.Run("sibling publisher does not see into a workspace nested under another publisher", func(t *testing.T) {
		l := view.LensFor(bobGrant)
		require.True(t, view.Permits(l, alice, snap), "bob sees alice as a workspace peer")
		require.False(t, view.Permits(l, nested, snap), "bob does not see alice's sub-workspace")
		require.False(t, view.Permits(l, eve, snap), "bob does not see inside alice's sub-workspace")
	})

	t.Run("workspace-admin sees chain ancestors and full subtree", func(t *testing.T) {
		l := view.LensFor(wsAdminAGrant)
		require.True(t, view.Permits(l, alice, snap), "ws-admin sees its subtree")
		require.True(t, view.Permits(l, eve, snap), "ws-admin sees through nested boundaries")
		require.True(t, view.Permits(l, nested, snap), "ws-admin sees the nested workspace's root")
		require.False(t, view.Permits(l, wsAdminB, snap), "ws-admin does not see sibling workspaces")
		require.False(t, view.Permits(l, user, snap), "ws-admin does not see other tenants")
	})

	t.Run("inner workspace-admin sees its ancestor chain", func(t *testing.T) {
		l := view.LensFor(nestedGrant)
		require.True(t, view.Permits(l, alice, snap), "nested sees its alice parent in chain")
		require.True(t, view.Permits(l, wsAdminA, snap), "nested sees the outer workspace-admin")
		require.True(t, view.Permits(l, eve, snap), "nested sees its own subtree")
		require.False(t, view.Permits(l, bob, snap), "nested does not see alice's other peers")
		require.False(t, view.Permits(l, user, snap), "nested does not see other tenants")
	})

	t.Run("publisher in nested workspace is sealed from the outer workspace", func(t *testing.T) {
		l := view.LensFor(eveGrant)
		require.True(t, view.Permits(l, nested, snap), "eve sees her workspace-admin")
		require.False(t, view.Permits(l, alice, snap), "eve does not see her workspace's outer parent")
		require.False(t, view.Permits(l, bob, snap), "eve does not see outer workspace peers")
		require.False(t, view.Permits(l, wsAdminA, snap), "eve does not reach beyond her own workspace")
	})

	t.Run("unworkspaced peers stay isolated by default", func(t *testing.T) {
		lonely1 := key(0xfa)
		lonely2 := key(0xfb)
		bareSnap := snapWith(map[types.PeerKey]*identityv1.Grant{
			lonely1: wsGrant(lonely1, false, []*identityv1.Grant{rootGrant}),
			lonely2: wsGrant(lonely2, false, []*identityv1.Grant{rootGrant}),
		})
		l := view.LensFor(wsGrant(lonely1, false, []*identityv1.Grant{rootGrant}))
		require.False(t, view.Permits(l, lonely2, bareSnap),
			"two grants with no workspace-admin in chain do not share a workspace")
	})
}

func TestProjectScopesResourcesByAuthority(t *testing.T) {
	tenant := key(1)
	other := key(2)
	snap := state.Snapshot{
		// Holders of the tenant's resources are shared infrastructure, so
		// they surface in the tenant's view (reduced).
		Nodes: map[types.PeerKey]state.NodeView{
			key(10): {Grant: infraGrant(key(10))},
			key(11): {Grant: infraGrant(key(11))},
			key(12): {Grant: infraGrant(key(12))},
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

// TestProjectSurfacesCollidingAuthorities proves that when
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

// A sibling tenant's node that merely stores the same content hash must
// not enter the view through the resource-host union.
func TestProjectExcludesSiblingHashCollision(t *testing.T) {
	tenant, sibling, siblingNode := key(1), key(2), key(20)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			siblingNode: {Grant: grant(siblingNode, false)}, // a plain tenant node, not infra
		},
		SpecsAll: []state.WorkloadSpecView{
			{Spec: state.WorkloadSpec{Hash: "shared", Name: "mine"}, Publisher: tenant},
			{Spec: state.WorkloadSpec{Hash: "shared", Name: "theirs"}, Publisher: sibling},
		},
		WorkloadStoringPeers: map[string]map[types.PeerKey]struct{}{
			"shared": {siblingNode: {}},
		},
	}

	sv := view.Project(snap, view.LensFor(grant(tenant, false)))
	require.Len(t, sv.Workloads, 1, "tenant sees only its own publication of the shared hash")
	require.NotContains(t, sv.Nodes, siblingNode,
		"a sibling node storing the same content hash must not leak into the view")
}

// An infrastructure host of the tenant's resource is shown, but reduced
// to identity and location: topology, load and the grant chain are gone.
func TestProjectReducesInfraHost(t *testing.T) {
	tenant, infraHost := key(1), key(20)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			infraHost: {
				Grant:         infraGrant(infraHost),
				Name:          "relay-eu",
				LastAddr:      "198.51.100.7:9000",
				ControlAddr:   "198.51.100.7:7000",
				GatewayDomain: ".staging.pln.sh",
				Reachable:     map[types.PeerKey]struct{}{key(99): {}},
				MemTotalBytes: 1 << 30,
				CPUPercent:    42,
			},
		},
		SpecsAll: []state.WorkloadSpecView{
			{Spec: state.WorkloadSpec{Hash: "h", Name: "mine"}, Publisher: tenant},
		},
		Claims: map[string]map[types.PeerKey]struct{}{"h": {infraHost: {}}},
	}

	sv := view.Project(snap, view.LensFor(grant(tenant, false)))
	got, ok := sv.Nodes[infraHost]
	require.True(t, ok, "tenant sees the infrastructure its workload runs on")
	require.Equal(t, "relay-eu", got.Name)
	require.Equal(t, "198.51.100.7:9000", got.LastAddr)
	require.Zero(t, got.ControlAddr, "control endpoint stripped")
	require.Zero(t, got.GatewayDomain, "gateway domain stripped")
	require.Nil(t, got.Reachable, "mesh adjacency stripped")
	require.Zero(t, got.MemTotalBytes, "capacity stripped")
	require.Zero(t, got.CPUPercent, "load stripped")
	require.Nil(t, got.Grant, "delegation chain stripped")
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
