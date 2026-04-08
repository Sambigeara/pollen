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
