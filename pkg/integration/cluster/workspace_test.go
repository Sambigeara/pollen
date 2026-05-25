// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cluster

import (
	"context"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/view"
	"github.com/stretchr/testify/require"
)

// TestPublicMesh_WorkspaceIsolation exercises the workspace visibility
// rule against a converged real cluster: admin demotes foo and bar to
// the --workspace role (workspace-admin, no admit) via UpgradePeer; foo
// and bar each publish a workload; then the cluster snapshot is
// projected through each lens to assert the multi-tenant isolation
// invariant. foo sees its chain (admin) and its own subtree only; bar
// likewise; admin sees both; foo does not see bar's workload and bar
// does not see foo's.
//
// This is the smallest shape that proves the four propagation legs the
// rule depends on: a publisher's grant gossiped into snap.GrantFor, the
// publisher's workload gossiped into SpecsAll, the WorkspaceOf check
// against the publisher's grant, and the AncestorIn check against the
// lens's chain.
func TestPublicMesh_WorkspaceIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	b := New(t).
		SetDefaultLatency(5 * time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15)                   //nolint:mnd
	b.AddNode("admin", Public)
	b.AddNode("foo", Public)
	b.AddNode("bar", Public)
	b.MakeRoot("admin")
	b.Introduce("admin", "foo")
	b.Introduce("admin", "bar")
	c := b.Start(ctx)
	c.RequireConverged(t)
	c.RequireHealthy(t)

	admin := c.Node("admin")
	foo := c.Node("foo")
	bar := c.Node("bar")

	wsCaps := func() *identityv1.Capabilities {
		caps := identity.WorkspaceCapabilities()
		caps.MaxDepth = 0 // ceiling-only; no need to delegate further in this test
		return caps
	}

	adminCtx := auth.WithCaller(ctx, identity.PrincipalFromGrant(admin.Node().Credentials().Grant()))
	for _, n := range []*TestNode{foo, bar} {
		resp, err := admin.Node().ControlService().UpgradePeer(adminCtx, &controlv1.UpgradePeerRequest{
			PeerPub:      n.PeerKey().Bytes(),
			Capabilities: wsCaps(),
		})
		require.NoError(t, err)
		require.True(t, resp.GetDelivered(), "admin must reach %s over the mesh", n.Name())
	}

	c.RequireEventually(t, func() bool {
		fc := foo.Node().Credentials().Grant().GetClaims().GetCapabilities()
		bc := bar.Node().Credentials().Grant().GetClaims().GetCapabilities()
		return fc.GetIsWorkspaceAdmin() && !fc.GetCanAdmit() && bc.GetIsWorkspaceAdmin() && !bc.GetCanAdmit()
	}, assertTimeout, "foo and bar must adopt their workspace-admin grants")

	const fooWorkload = "f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f00f"
	const barWorkload = "ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4b"
	_, err := foo.Store().PublishWorkload(state.WorkloadSpec{Hash: fooWorkload, Name: "foo-svc", MinReplicas: 1}, nil)
	require.NoError(t, err)
	_, err = bar.Store().PublishWorkload(state.WorkloadSpec{Hash: barWorkload, Name: "bar-svc", MinReplicas: 1}, nil)
	require.NoError(t, err)

	c.RequireEventually(t, func() bool {
		snap := admin.Store().Snapshot()
		_, gotFoo := snap.Specs[fooWorkload]
		_, gotBar := snap.Specs[barWorkload]
		return gotFoo && gotBar
	}, assertTimeout, "admin must observe both tenants' workloads via gossip")

	hasHash := func(views []state.WorkloadSpecView, hash string) bool {
		for _, w := range views {
			if w.Spec.Hash == hash {
				return true
			}
		}
		return false
	}

	t.Run("admin sees both tenants' workloads", func(t *testing.T) {
		lens := view.LensFor(admin.Node().Credentials().Grant())
		scoped := view.Project(admin.Store().Snapshot(), lens)
		require.True(t, hasHash(scoped.Workloads, fooWorkload))
		require.True(t, hasHash(scoped.Workloads, barWorkload))
	})

	t.Run("foo sees its own workload and not bar's", func(t *testing.T) {
		lens := view.LensFor(foo.Node().Credentials().Grant())
		scoped := view.Project(foo.Store().Snapshot(), lens)
		require.True(t, hasHash(scoped.Workloads, fooWorkload), "foo sees its own")
		require.False(t, hasHash(scoped.Workloads, barWorkload), "foo does not see bar's workload")
	})

	t.Run("bar sees its own workload and not foo's", func(t *testing.T) {
		lens := view.LensFor(bar.Node().Credentials().Grant())
		scoped := view.Project(bar.Store().Snapshot(), lens)
		require.True(t, hasHash(scoped.Workloads, barWorkload), "bar sees its own")
		require.False(t, hasHash(scoped.Workloads, fooWorkload), "bar does not see foo's workload")
	})

	t.Run("workspace-admin sees its chain ancestor", func(t *testing.T) {
		lens := view.LensFor(foo.Node().Credentials().Grant())
		snap := foo.Store().Snapshot()
		require.True(t, view.Permits(lens, admin.PeerKey(), snap),
			"foo (workspace-admin) sees admin (chain ancestor)")
		require.False(t, view.Permits(lens, bar.PeerKey(), snap),
			"foo does not see bar (sibling workspace)")
	})
}

// TestPublicMesh_WorkspacePeerVisibility proves the brief's headline
// use-case end-to-end: two publishers in the same workspace see each
// other's workloads over the real mesh, while a sibling tenant in a
// different workspace remains opaque to them. The shape is admin (root)
// with two workspace-admins, foo and bar, born beneath it; alice and bob
// are publishers born beneath foo, as a real invite would place them, and
// bar publishes a workload standing in for any sibling-workspace resource.
func TestPublicMesh_WorkspacePeerVisibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	b := New(t).
		SetDefaultLatency(5 * time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15)                   //nolint:mnd
	b.AddNode("admin", Public)
	b.MakeRoot("admin")
	b.AddMember("foo", Public, "admin", identity.WorkspaceCapabilities())
	b.AddMember("bar", Public, "admin", identity.WorkspaceCapabilities())
	b.AddMember("alice", Public, "foo", identity.PublisherCapabilities())
	b.AddMember("bob", Public, "foo", identity.PublisherCapabilities())
	b.Introduce("admin", "foo")
	b.Introduce("admin", "bar")
	b.Introduce("admin", "alice")
	b.Introduce("admin", "bob")
	b.Introduce("foo", "alice")
	b.Introduce("foo", "bob")
	c := b.Start(ctx)
	c.RequireConverged(t)
	c.RequireHealthy(t)

	foo := c.Node("foo")
	bar := c.Node("bar")
	alice := c.Node("alice")
	bob := c.Node("bob")

	const aliceWorkload = "a11ce0000000000000000000000000000000000000000000000000000000000a"
	const bobWorkload = "b0b0000000000000000000000000000000000000000000000000000000000b0b"
	const barWorkload = "ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4ba4b"
	_, err := alice.Store().PublishWorkload(state.WorkloadSpec{Hash: aliceWorkload, Name: "alice-svc", MinReplicas: 1}, nil)
	require.NoError(t, err)
	_, err = bob.Store().PublishWorkload(state.WorkloadSpec{Hash: bobWorkload, Name: "bob-svc", MinReplicas: 1}, nil)
	require.NoError(t, err)
	_, err = bar.Store().PublishWorkload(state.WorkloadSpec{Hash: barWorkload, Name: "bar-svc", MinReplicas: 1}, nil)
	require.NoError(t, err)

	c.RequireEventually(t, func() bool {
		snap := alice.Store().Snapshot()
		_, gotAlice := snap.Specs[aliceWorkload]
		_, gotBob := snap.Specs[bobWorkload]
		_, gotBar := snap.Specs[barWorkload]
		return gotAlice && gotBob && gotBar
	}, assertTimeout, "alice's local store must observe all three workloads via gossip before projection")

	hasHash := func(views []state.WorkloadSpecView, hash string) bool {
		for _, w := range views {
			if w.Spec.Hash == hash {
				return true
			}
		}
		return false
	}

	t.Run("publishers under the same workspace see each other's workloads", func(t *testing.T) {
		aliceLens := view.LensFor(alice.Node().Credentials().Grant())
		scoped := view.Project(alice.Store().Snapshot(), aliceLens)
		require.True(t, hasHash(scoped.Workloads, aliceWorkload), "alice sees her own workload")
		require.True(t, hasHash(scoped.Workloads, bobWorkload), "alice sees bob's workload (same workspace under foo)")

		bobLens := view.LensFor(bob.Node().Credentials().Grant())
		scoped = view.Project(bob.Store().Snapshot(), bobLens)
		require.True(t, hasHash(scoped.Workloads, aliceWorkload), "bob sees alice's workload (same workspace under foo)")
		require.True(t, hasHash(scoped.Workloads, bobWorkload), "bob sees his own workload")
	})

	t.Run("publisher does not see sibling-workspace resources", func(t *testing.T) {
		aliceLens := view.LensFor(alice.Node().Credentials().Grant())
		scoped := view.Project(alice.Store().Snapshot(), aliceLens)
		require.False(t, hasHash(scoped.Workloads, barWorkload), "alice does not see bar's workload (different workspace)")
		require.False(t, view.Permits(aliceLens, bar.PeerKey(), alice.Store().Snapshot()),
			"alice does not see bar as a node either")
	})

	t.Run("sibling workspace-admin does not see publishers from another workspace", func(t *testing.T) {
		barLens := view.LensFor(bar.Node().Credentials().Grant())
		scoped := view.Project(bar.Store().Snapshot(), barLens)
		require.False(t, hasHash(scoped.Workloads, aliceWorkload), "bar does not see alice's workload")
		require.False(t, hasHash(scoped.Workloads, bobWorkload), "bar does not see bob's workload")
	})

	t.Run("workspace-admin sees its publishers' subtree", func(t *testing.T) {
		fooLens := view.LensFor(foo.Node().Credentials().Grant())
		scoped := view.Project(foo.Store().Snapshot(), fooLens)
		require.True(t, hasHash(scoped.Workloads, aliceWorkload), "foo sees alice's workload (own subtree)")
		require.True(t, hasHash(scoped.Workloads, bobWorkload), "foo sees bob's workload (own subtree)")
		require.False(t, hasHash(scoped.Workloads, barWorkload), "foo does not see bar's workload (sibling)")
	})
}
