// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

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

func TestNodeNameLabels_UniqueNames(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "laptop"}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b"), Name: "server"}}

	labels := nodeNameLabels(self, peers, false)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())
	peerPK := peerKeyString(peers[0].GetNode().GetPeerPub())

	// Condensed mode shows name [prefix].
	require.Contains(t, labels[selfPK], "laptop [")
	require.Contains(t, labels[peerPK], "server [")
}

func TestNodeNameLabels_DuplicateNames(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "node"}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b"), Name: "node"}}

	labels := nodeNameLabels(self, peers, false)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())
	peerPK := peerKeyString(peers[0].GetNode().GetPeerPub())

	// Both share the name but have distinct prefixes in brackets.
	require.Contains(t, labels[selfPK], "node [")
	require.Contains(t, labels[peerPK], "node [")
	require.NotEqual(t, labels[selfPK], labels[peerPK])
}

func TestNodeNameLabels_Wide(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "laptop"}
	labels := nodeNameLabels(self, nil, true)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())

	// Wide mode includes the full peer ID in parentheses.
	require.Contains(t, labels[selfPK], "laptop (")
	require.Contains(t, labels[selfPK], selfPK)
}

func TestNodeNameLabels_MixedNamedAndUnnamed(t *testing.T) {
	self := &controlv1.NodeSummary{Node: nodeRef("a"), Name: "laptop"}
	peers := []*controlv1.NodeSummary{{Node: nodeRef("b")}}

	labels := nodeNameLabels(self, peers, false)
	selfPK := peerKeyString(self.GetNode().GetPeerPub())
	peerPK := peerKeyString(peers[0].GetNode().GetPeerPub())

	// Named node gets a label with prefix; unnamed node gets no label.
	require.Contains(t, labels[selfPK], "laptop [")
	require.Empty(t, labels[peerPK])
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
