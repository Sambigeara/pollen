// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/stretchr/testify/require"
)

func nodeRef(id string) *controlv1.NodeRef {
	b := make([]byte, 32)
	copy(b, id)
	return &controlv1.NodeRef{PeerPub: b}
}

func TestNodeNameLabels_NoNames(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a")}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b")}}

	labels := nodeNameLabels(self, peers, false)
	require.Empty(t, labels)
}

func TestStatusContextLabel(t *testing.T) {
	cases := []struct {
		name, dir string
		t         transportSelection
		want      string
	}{
		{"", "/x", transportSelection{kind: transportLocal}, ""},
		{"default", "/x", transportSelection{kind: transportLocal}, "default"},
		{"default", "/x", transportSelection{kind: transportWire, wireAddr: "edge:7443"}, "default (pln://edge:7443)"},
		{"cloud", "/x", transportSelection{kind: transportWire, wireAddr: "edge:7443"}, "cloud (pln://edge:7443)"},
		{"cloud", "/x", transportSelection{kind: transportLocal}, "cloud (/x)"},
		{"prod", "/x", transportSelection{kind: transportSSHBridge, sshHost: "root@prod"}, "prod (root@prod)"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			require.Equal(t, tc.want, statusContextLabel(tc.name, tc.dir, tc.t))
		})
	}
}

func TestNodeNameLabels_UniqueNames(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "laptop"}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b"), Name: "server"}}

	labels := nodeNameLabels(self, peers, false)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())
	peerPK := peerKeyString(peers[0].GetNode().GetPeerPub())

	require.Contains(t, labels[selfPK], "laptop [")
	require.Contains(t, labels[peerPK], "server [")
}

func TestNodeNameLabels_DuplicateNames(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "node"}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b"), Name: "node"}}

	labels := nodeNameLabels(self, peers, false)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())
	peerPK := peerKeyString(peers[0].GetNode().GetPeerPub())

	require.Contains(t, labels[selfPK], "node [")
	require.Contains(t, labels[peerPK], "node [")
	require.NotEqual(t, labels[selfPK], labels[peerPK])
}

func TestNodeNameLabels_Wide(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "laptop"}
	labels := nodeNameLabels(self, nil, true)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())

	require.Contains(t, labels[selfPK], "laptop (")
	require.Contains(t, labels[selfPK], selfPK)
}

func TestNodeNameLabels_MixedNamedAndUnnamed(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "laptop"}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b")}}

	labels := nodeNameLabels(self, peers, false)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())
	peerPK := peerKeyString(peers[0].GetNode().GetPeerPub())

	require.Contains(t, labels[selfPK], "laptop [")
	require.Empty(t, labels[peerPK])
}

func TestCertExpiryFooter(t *testing.T) {
	resp := func(certs ...*controlv1.CertInfo) *controlv1.GetStatusResponse {
		return &controlv1.GetStatusResponse{Certificates: certs}
	}
	future := time.Now().Add(23 * 24 * time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	t.Run("no certificates", func(t *testing.T) {
		require.Empty(t, certExpiryFooter(resp()))
	})

	t.Run("admin/root grant with no deadline has no footer", func(t *testing.T) {
		// GrantDeadlineUnix==0 is time.Unix(0,0) (1970), not the Go zero
		// time; before the fix this rendered a spurious "membership
		// expired" line on a healthy admin node.
		got := certExpiryFooter(resp(&controlv1.CertInfo{
			CanAdmit: true, CanDelegate: true,
			Health: controlv1.CertHealth_CERT_HEALTH_OK,
		}))
		require.Empty(t, got)
	})

	t.Run("delegated grant within deadline", func(t *testing.T) {
		got := certExpiryFooter(resp(&controlv1.CertInfo{
			GrantDeadlineUnix: future,
			Health:            controlv1.CertHealth_CERT_HEALTH_OK,
		}))
		require.Contains(t, got, "temporary access expires in")
		require.NotContains(t, got, "expired")
		require.NotContains(t, got, "membership")
	})

	t.Run("delegated grant past its deadline", func(t *testing.T) {
		got := certExpiryFooter(resp(&controlv1.CertInfo{
			GrantDeadlineUnix: past,
			Health:            controlv1.CertHealth_CERT_HEALTH_EXPIRED,
		}))
		require.Contains(t, got, "temporary access expired")
	})

	t.Run("expiring soon prompts a rejoin", func(t *testing.T) {
		got := certExpiryFooter(resp(&controlv1.CertInfo{
			GrantDeadlineUnix: future,
			Health:            controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON,
		}))
		require.Contains(t, got, "temporary access expires in")
		require.Contains(t, got, "rejoin")
	})

	t.Run("admin grant alongside a delegated one uses the deadline", func(t *testing.T) {
		got := certExpiryFooter(resp(
			&controlv1.CertInfo{CanAdmit: true, Health: controlv1.CertHealth_CERT_HEALTH_OK},
			&controlv1.CertInfo{GrantDeadlineUnix: future, Health: controlv1.CertHealth_CERT_HEALTH_OK},
		))
		require.Contains(t, got, "temporary access expires in")
	})
}

func TestMatchBlobArg_Name(t *testing.T) {
	blobs := []*controlv1.BlobSummary{
		{Hash: "abc123", Name: "config", Publisher: nodeRef("a")},
		{Hash: "def456", Name: "model", Publisher: nodeRef("b")},
	}
	got, err := matchBlobArg(blobs, "config")
	require.NoError(t, err)
	require.Equal(t, "abc123", got)
}

func TestMatchBlobArg_NameCollision(t *testing.T) {
	blobs := []*controlv1.BlobSummary{
		{Hash: "abc", Name: "config", Publisher: nodeRef("a")},
		{Hash: "def", Name: "config", Publisher: nodeRef("b")},
	}
	_, err := matchBlobArg(blobs, "config")
	require.Error(t, err)
	require.Contains(t, err.Error(), "multiple blobs match")
}

func TestMatchBlobArg_SuffixDisambiguates(t *testing.T) {
	pkA := peerKeyString(nodeRef("aardvark").GetPeerPub())
	pkB := peerKeyString(nodeRef("badger").GetPeerPub())
	blobs := []*controlv1.BlobSummary{
		{Hash: "hashA", Name: "config", Publisher: nodeRef("aardvark")},
		{Hash: "hashB", Name: "config", Publisher: nodeRef("badger")},
	}
	got, err := matchBlobArg(blobs, "config-"+pkA[:2])
	require.NoError(t, err)
	require.Equal(t, "hashA", got)

	got, err = matchBlobArg(blobs, "config-"+pkB[:2])
	require.NoError(t, err)
	require.Equal(t, "hashB", got)
}

func TestMatchBlobArg_NotFound(t *testing.T) {
	blobs := []*controlv1.BlobSummary{{Hash: "abc", Name: "config", Publisher: nodeRef("a")}}
	_, err := matchBlobArg(blobs, "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no blob matching")
}

func TestMatchBlobArg_IgnoresAnonymous(t *testing.T) {
	blobs := []*controlv1.BlobSummary{
		{Hash: "abc", Publisher: nodeRef("a")},
		{Hash: "def", Name: "config", Publisher: nodeRef("b")},
	}
	got, err := matchBlobArg(blobs, "config")
	require.NoError(t, err)
	require.Equal(t, "def", got)
}

func TestMatchBlobArg_UniqueHashPrefix(t *testing.T) {
	blobs := []*controlv1.BlobSummary{
		{Hash: "abc123def", Publisher: nodeRef("a")},
		{Hash: "def456abc", Publisher: nodeRef("b")},
	}
	got, err := matchBlobArg(blobs, "abc")
	require.NoError(t, err)
	require.Equal(t, "abc123def", got)

	got, err = matchBlobArg(blobs, "a")
	require.NoError(t, err)
	require.Equal(t, "abc123def", got)
}

func TestMatchBlobArg_AmbiguousHashPrefix(t *testing.T) {
	blobs := []*controlv1.BlobSummary{
		{Hash: "abc123", Publisher: nodeRef("a")},
		{Hash: "abc456", Publisher: nodeRef("b")},
	}
	_, err := matchBlobArg(blobs, "abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "matches multiple blobs")
}

func TestMatchBlobArg_NamePreferredOverPrefix(t *testing.T) {
	blobs := []*controlv1.BlobSummary{
		{Hash: "beefcafe", Name: "beef", Publisher: nodeRef("a")},
		{Hash: "beef1234", Publisher: nodeRef("b")},
	}
	got, err := matchBlobArg(blobs, "beef")
	require.NoError(t, err)
	require.Equal(t, "beefcafe", got)
}

func TestLocalTier(t *testing.T) {
	admin := &controlv1.CertInfo{CanAdmit: true, CanDelegate: true, CanPublish: true, IsWorkspaceAdmin: true}
	workspace := &controlv1.CertInfo{IsWorkspaceAdmin: true, CanDelegate: true, CanPublish: true}
	publisher := &controlv1.CertInfo{CanPublish: true}
	leaf := &controlv1.CertInfo{}

	cases := []struct {
		name  string
		certs []*controlv1.CertInfo
		want  string
	}{
		{"no certs", nil, ""},
		{"leaf only", []*controlv1.CertInfo{leaf}, "leaf"},
		{"publisher only", []*controlv1.CertInfo{publisher}, "publisher"},
		{"workspace only", []*controlv1.CertInfo{workspace}, "workspace"},
		{"admin only", []*controlv1.CertInfo{admin}, "admin"},
		{"publisher beats leaf", []*controlv1.CertInfo{leaf, publisher}, "publisher"},
		{"workspace beats publisher", []*controlv1.CertInfo{publisher, workspace}, "workspace"},
		{"admin beats workspace", []*controlv1.CertInfo{workspace, admin}, "admin"},
		{"admin beats all, order-insensitive", []*controlv1.CertInfo{leaf, admin, workspace, publisher}, "admin"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, localTier(tc.certs))
		})
	}
}

func TestCollectStaticSection_ReplicasDisplay(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a")}
	cases := []struct {
		name      string
		claimants int
		capacity  uint32
		want      string
	}{
		{"no capable peers", 0, 0, "0/0"},
		{"partial coverage", 1, 3, "1/3"},
		{"full coverage", 3, 3, "3/3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claimants := make([]*controlv1.NodeRef, tc.claimants)
			for i := range claimants {
				claimants[i] = nodeRef("a")
			}
			resp := &controlv1.GetStatusResponse{
				Self: self,
				Sites: []*controlv1.StaticSummary{{
					Name:            "home.local",
					ManifestDigest:  make([]byte, 32),
					ServingCapacity: tc.capacity,
					Claimants:       claimants,
					Publisher:       self.GetNode(),
				}},
			}
			sec := collectStaticSection(resp, statusViewOpts{})
			require.Len(t, sec.rows, 1)
			require.Equal(t, tc.want, sec.rows[0][2])
		})
	}
}

func TestCollectBlobsSection_OrphanLabelling(t *testing.T) {
	resp := &controlv1.GetStatusResponse{
		Blobs: []*controlv1.BlobSummary{
			{Hash: "aaaa", Orphan: true, Local: true, Replicas: 1},
			{Hash: "bbbb", Orphan: true, Local: false, Replicas: 1},
			{Hash: "cccc", Name: "config", Publisher: nodeRef("a"), Local: true, Replicas: 1},
		},
	}

	t.Run("default hides remote orphans, labels local orphans", func(t *testing.T) {
		sec := collectBlobsSection(resp, statusViewOpts{})
		// Remote orphan filtered out; local orphan + named blob shown.
		require.Len(t, sec.rows, 2)
		// Rows are emitted in the order they pass the filter.
		require.Equal(t, "(orphaned)", sec.rows[0][0])
		require.Equal(t, "config", sec.rows[1][0])
		require.Equal(t, "1 orphaned blobs hidden (use --include-offline)", sec.footer)
	})

	t.Run("includeAll shows and labels every orphan", func(t *testing.T) {
		sec := collectBlobsSection(resp, statusViewOpts{includeAll: true})
		require.Len(t, sec.rows, 3)
		require.Equal(t, "(orphaned)", sec.rows[0][0])
		require.Equal(t, "(orphaned)", sec.rows[1][0])
		require.Equal(t, "config", sec.rows[2][0])
		require.Empty(t, sec.footer)
	})
}
