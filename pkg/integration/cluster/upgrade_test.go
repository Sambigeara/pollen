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
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestPublicMesh_AdminInitiatedUpgrade exercises the full admin push
// pipeline against a converged real cluster: admin (node-0, the root)
// mints a new grant for foo (node-1) via the UpgradePeer control RPC
// and dispatches it over the existing mesh transport; foo's daemon
// validates, swaps it into local credentials, runs RevokeOwnSpecs for
// any dropped publish kinds, and gossips the new Principal entry. Both
// nodes' snapshots must converge on the new caps; any pre-existing
// Facts published under the dropped kinds must be tombstoned on both
// sides.
//
// The two-node shape exercises cap-shrink tombstones and the
// wire-mode fallback's pre-condition (codes.Unavailable when no
// live mesh). A three-node shape would only retest gossip
// propagation that TestPublicMesh_GossipConvergence already covers.
func TestPublicMesh_AdminInitiatedUpgrade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	b := New(t).
		SetDefaultLatency(5 * time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15)                   //nolint:mnd
	b.AddNode("admin", Public)
	b.AddNode("foo", Public)
	b.MakeRoot("admin")
	b.Introduce("admin", "foo")
	c := b.Start(ctx)
	c.RequireConverged(t)
	c.RequireHealthy(t)

	admin := c.Node("admin")
	foo := c.Node("foo")
	fooKey := foo.PeerKey()

	// Seed foo with a workload under its initial (full) publish caps
	// so the downgrade has something to tombstone.
	const fooWorkloadHash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	_, err := foo.Store().PublishWorkload(state.WorkloadSpec{Hash: fooWorkloadHash, Name: "echo", MinReplicas: 1}, nil)
	require.NoError(t, err)
	c.RequireEventually(t, func() bool {
		_, ok := admin.Store().Snapshot().Specs[fooWorkloadHash]
		return ok
	}, assertTimeout, "admin must observe foo's workload before the upgrade")

	t.Run("LeafToFullDelegationConverges", func(t *testing.T) {
		// Mint and dispatch a grant that adds CanDelegate to foo while
		// retaining its publish caps. This is the classic admin
		// promotion path.
		newCaps := &identityv1.Capabilities{
			CanDelegate: true,
			Publish:     &identityv1.PublishCapability{Functions: true, Blobs: true, Sites: true, Services: true},
		}
		ctx := auth.WithCaller(ctx, identity.PrincipalFromGrant(admin.Node().Credentials().Grant()))
		resp, err := admin.Node().ControlService().UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      fooKey.Bytes(),
			Capabilities: newCaps,
		})
		require.NoError(t, err)
		require.True(t, resp.GetDelivered(), "admin upgrade must deliver to a connected peer")

		c.RequireEventually(t, func() bool {
			return foo.Node().Credentials().Grant().GetClaims().GetCapabilities().GetCanDelegate()
		}, assertTimeout, "foo's in-memory grant must reflect the upgrade")
		c.RequireEventually(t, func() bool {
			nv, ok := admin.Store().Snapshot().Nodes[fooKey]
			return ok && nv.Grant.GetClaims().GetCapabilities().GetCanDelegate()
		}, assertTimeout, "admin's snapshot must observe foo's new caps via gossip")
	})

	t.Run("CapShrinkTombstonesAcrossCluster", func(t *testing.T) {
		// Drop publish:functions: foo's pre-existing workload must be
		// tombstoned in foo's own store and in admin's store via
		// gossip. authorise()'s tombstone exemption is what lets the
		// second-pass tombstones admit on remote peers once the new
		// (shrunken) grant has displaced the old one.
		shrunken := &identityv1.Capabilities{
			Publish: &identityv1.PublishCapability{Sites: true},
		}
		ctx := auth.WithCaller(ctx, identity.PrincipalFromGrant(admin.Node().Credentials().Grant()))
		resp, err := admin.Node().ControlService().UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      fooKey.Bytes(),
			Capabilities: shrunken,
		})
		require.NoError(t, err)
		require.True(t, resp.GetDelivered())

		c.RequireEventually(t, func() bool {
			_, present := foo.Store().Snapshot().Specs[fooWorkloadHash]
			return !present
		}, assertTimeout, "foo's own workload must be tombstoned locally on cap-shrink")
		c.RequireEventually(t, func() bool {
			_, present := admin.Store().Snapshot().Specs[fooWorkloadHash]
			return !present
		}, assertTimeout, "admin must observe the tombstone via gossip")

		// Admission must now reject a fresh seed attempt under the lost
		// kind: the publisher's gossiped grant no longer carries
		// publish:functions, so a non-tombstone workload Fact fails at
		// the authorise stage.
		const reseedHash = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
		_, reseedErr := foo.Store().PublishWorkload(state.WorkloadSpec{Hash: reseedHash, Name: "echo-2", MinReplicas: 1}, nil)
		require.Error(t, reseedErr, "post-shrink seed must be rejected at admission")
	})

	t.Run("OfflinePeerSurfacesUnavailable", func(t *testing.T) {
		// A peer that is not connected to admin's mesh surfaces as a
		// gRPC Unavailable, which the CLI uses as the discriminator
		// for falling back to a subject-pinned invite ticket.
		stranger := types.PeerKeyFromBytes([]byte("strangeer-32byte-pubkey--padded!"))
		ctx := auth.WithCaller(ctx, identity.PrincipalFromGrant(admin.Node().Credentials().Grant()))
		_, err := admin.Node().ControlService().UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      stranger.Bytes(),
			Capabilities: identity.LeafCapabilities(),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "peer has no live mesh daemon",
			"offline peer must produce the codes.Unavailable reason string used by the CLI")
	})
}
