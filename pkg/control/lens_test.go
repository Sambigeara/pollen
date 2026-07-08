// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/view"
	"github.com/stretchr/testify/require"
)

func pk(b byte) types.PeerKey {
	raw := make([]byte, 32)
	raw[0] = b
	return types.PeerKeyFromBytes(raw)
}

func lensFor(subject types.PeerKey, admin bool) view.Lens {
	return view.LensFor(&identityv1.Grant{Claims: &identityv1.GrantClaims{
		SubjectPub:   subject.Bytes(),
		Capabilities: &identityv1.Capabilities{CanAdmit: admin},
	}})
}

// A tenant's status header must be the caller's own identity, never the
// serving daemon's node, and must carry none of the host's telemetry.
func TestBuildSelfSummary(t *testing.T) {
	local := pk(1)
	tenant := pk(2)
	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {Name: "host", CPUPercent: 50, MemPercent: 60, NumCPU: 8},
		},
	}
	s := &Service{}

	t.Run("operator sees the serving node", func(t *testing.T) {
		self := s.buildSelfSummary(snap, lensFor(pk(9), true), true, nil)
		require.Equal(t, local.Bytes(), self.GetNode().GetPeerPub())
		require.Equal(t, "host", self.GetName())
		require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, self.GetStatus())
		require.EqualValues(t, 50, self.GetCpuPercent())
	})

	t.Run("daemon-self sees the serving node", func(t *testing.T) {
		self := s.buildSelfSummary(snap, lensFor(local, false), true, nil)
		require.Equal(t, local.Bytes(), self.GetNode().GetPeerPub())
		require.Equal(t, "host", self.GetName())
	})

	t.Run("wire tenant sees itself, not the serving node", func(t *testing.T) {
		self := s.buildSelfSummary(snap, lensFor(tenant, false), false, nil)
		require.Equal(t, tenant.Bytes(), self.GetNode().GetPeerPub())
		require.NotEqual(t, local.Bytes(), self.GetNode().GetPeerPub())
		require.Equal(t, controlv1.NodeStatus_NODE_STATUS_OFFLINE, self.GetStatus())
		require.Empty(t, self.GetName())
		require.Zero(t, self.GetCpuPercent())
		require.Zero(t, self.GetMemPercent())
	})
}

// The node shown as Self must never also appear as a peer: a wire
// tenant drops its own identity (the duplicate would force the CLI to
// render its full hash), while keeping the serving node it borrows.
func TestBuildNodeSummariesDropsSelf(t *testing.T) {
	local := pk(1)
	tenant := pk(2)
	other := pk(3)
	snap := state.Snapshot{LocalID: local}
	scoped := view.ScopedView{Nodes: map[types.PeerKey]state.NodeView{
		local:  {Name: "host"},
		tenant: {Name: "tenant"},
		other:  {Name: "other"},
	}}
	s := &Service{}

	keys := func(out []*controlv1.NodeSummary) []types.PeerKey {
		ks := make([]types.PeerKey, 0, len(out))
		for _, ns := range out {
			ks = append(ks, types.PeerKeyFromBytes(ns.GetNode().GetPeerPub()))
		}
		return ks
	}

	t.Run("wire tenant drops itself, keeps the serving node", func(t *testing.T) {
		ks := keys(s.buildNodeSummaries(snap, scoped, lensFor(tenant, false), false, nil, time.Now()))
		require.NotContains(t, ks, tenant)
		require.Contains(t, ks, local)
		require.Contains(t, ks, other)
	})

	t.Run("operator drops the serving node", func(t *testing.T) {
		ks := keys(s.buildNodeSummaries(snap, scoped, lensFor(pk(9), true), true, nil, time.Now()))
		require.NotContains(t, ks, local)
		require.Contains(t, ks, tenant)
		require.Contains(t, ks, other)
	})
}

// A peer whose non-renewable grant has passed is gone for good. Status
// must drop it from the cluster overview rather than render it as
// "offline", because no further heartbeat can resurrect it.
func TestBuildNodeSummariesDropsTerminallyExpiredPeers(t *testing.T) {
	local := pk(1)
	live := pk(2)
	dead := pk(3)
	now := time.Now()
	mkGrant := func(deadlineUnix int64, nonRenewable bool) *identityv1.Grant {
		return &identityv1.Grant{Claims: &identityv1.GrantClaims{
			GrantDeadlineUnix: deadlineUnix,
			NonRenewable:      nonRenewable,
		}}
	}
	snap := state.Snapshot{LocalID: local}
	scoped := view.ScopedView{Nodes: map[types.PeerKey]state.NodeView{
		local: {Name: "host"},
		live:  {Name: "live", Grant: mkGrant(now.Add(time.Hour).Unix(), true)},
		dead:  {Name: "dead", Grant: mkGrant(now.Add(-time.Hour).Unix(), true)},
	}}
	s := &Service{}

	keys := func(out []*controlv1.NodeSummary) []types.PeerKey {
		ks := make([]types.PeerKey, 0, len(out))
		for _, ns := range out {
			ks = append(ks, types.PeerKeyFromBytes(ns.GetNode().GetPeerPub()))
		}
		return ks
	}

	ks := keys(s.buildNodeSummaries(snap, scoped, lensFor(pk(9), true), true, nil, now))
	require.Contains(t, ks, live)
	require.NotContains(t, ks, dead)
}

func TestRedactNodeTelemetry(t *testing.T) {
	ns := &controlv1.NodeSummary{
		Node:               &controlv1.NodeRef{PeerPub: pk(3).Bytes()},
		Name:               "n",
		Status:             controlv1.NodeStatus_NODE_STATUS_ONLINE,
		Addr:               "1.2.3.4:9",
		PubliclyAccessible: true,
		CpuPercent:         42,
		MemPercent:         42,
		NumCpu:             4,
		TrafficRateIn:      99,
		TrafficRateOut:     99,
		LatencyMs:          7,
		TunnelCount:        3,
	}
	redactNodeTelemetry(ns)

	require.Zero(t, ns.GetCpuPercent())
	require.Zero(t, ns.GetMemPercent())
	require.Zero(t, ns.GetNumCpu())
	require.Zero(t, ns.GetTrafficRateIn())
	require.Zero(t, ns.GetTrafficRateOut())
	require.Zero(t, ns.GetLatencyMs())
	require.Zero(t, ns.GetTunnelCount())

	// Identity and reachability survive: a tenant still learns WHERE its
	// fact runs, just not the host's load.
	require.Equal(t, "n", ns.GetName())
	require.Equal(t, "1.2.3.4:9", ns.GetAddr())
	require.True(t, ns.GetPubliclyAccessible())
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, ns.GetStatus())
}

// Inspecting a shared holder must never enumerate another tenant's
// facts; an admin still sees everything the node published.
func TestFillPublishedResources(t *testing.T) {
	nodeA := pk(1)
	tenantB := pk(2)
	svcA := &state.Service{Name: "svcA", Fact: &factv1.Fact{AuthorityPub: nodeA.Bytes()}}
	snap := state.Snapshot{
		SpecsAll: []state.WorkloadSpecView{
			{Publisher: nodeA, Spec: state.WorkloadSpec{Name: "wA", Hash: "wAh"}},
		},
		StaticSpecsAll: []state.StaticSpecView{
			{Publisher: nodeA, Spec: state.StaticSpec{Name: "sA"}},
		},
		BlobSpecsAll: []state.BlobSpecView{
			{Publisher: nodeA, Spec: state.BlobSpec{Name: "bA", Digest: "bAd"}},
		},
	}
	nv := state.NodeView{Services: map[string]*state.Service{"svcA": svcA}}

	t.Run("admin sees the node's published resources", func(t *testing.T) {
		d := &controlv1.NodeDetail{}
		lens := lensFor(pk(9), true)
		fillPublishedResources(snap, d, view.Project(snap, lens), nv, nodeA, lens)
		require.Equal(t, []string{"svcA"}, d.GetPublishedServices())
		require.Equal(t, []string{"wA"}, d.GetPublishedWorkloads())
		require.Equal(t, []string{"sA"}, d.GetPublishedStatics())
		require.Equal(t, []string{"bA"}, d.GetPublishedBlobs())
	})

	t.Run("other tenant sees none of them", func(t *testing.T) {
		d := &controlv1.NodeDetail{}
		lens := lensFor(tenantB, false)
		fillPublishedResources(snap, d, view.Project(snap, lens), nv, nodeA, lens)
		require.Empty(t, d.GetPublishedServices())
		require.Empty(t, d.GetPublishedWorkloads())
		require.Empty(t, d.GetPublishedStatics())
		require.Empty(t, d.GetPublishedBlobs())
	})

	t.Run("owning tenant sees its own", func(t *testing.T) {
		d := &controlv1.NodeDetail{}
		lens := lensFor(nodeA, false)
		fillPublishedResources(snap, d, view.Project(snap, lens), nv, nodeA, lens)
		require.Equal(t, []string{"svcA"}, d.GetPublishedServices())
		require.Equal(t, []string{"wA"}, d.GetPublishedWorkloads())
	})
}
