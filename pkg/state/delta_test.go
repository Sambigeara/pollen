package state

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestEncodeDelta_RoundTrip(t *testing.T) {
	pubA := genPub(t)
	storeA := newTestStore(pubA)

	storeA.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	storeA.upsertLocalService(8080, "web")

	snapBefore := storeA.Snapshot()
	clkBefore := snapBefore.Clock()

	storeA.setExternalPort(45000)

	delta := storeA.EncodeDelta(clkBefore)
	require.NotEmpty(t, delta)

	pubB := genPub(t)
	storeB := newTestStore(pubB)

	from := types.PeerKeyFromBytes(pubA)
	events, _, err := storeB.ApplyDelta(from, delta)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	snapB := storeB.Snapshot()
	peerA, ok := snapB.Peer(from)
	require.True(t, ok)
	require.Equal(t, uint32(45000), peerA.ExternalPort)
}

func TestFullState_ContainsEverything(t *testing.T) {
	pubA := genPub(t)
	storeA := newTestStore(pubA)

	storeA.setLocalNetwork([]string{"10.0.0.1", "10.0.0.2"}, 9000)
	storeA.upsertLocalService(8080, "web")
	storeA.upsertLocalService(9090, "api")
	storeA.setExternalPort(45000)
	storeA.setLocalVivaldiCoord(coords.Coord{X: 1.0, Y: 2.0, Height: coords.MinHeight})

	full := storeA.FullState()
	require.NotEmpty(t, full)

	pubB := genPub(t)
	storeB := newTestStore(pubB)

	from := types.PeerKeyFromBytes(pubA)
	_, _, err := storeB.ApplyDelta(from, full)
	require.NoError(t, err)

	snapB := storeB.Snapshot()
	peerA, ok := snapB.Peer(from)
	require.True(t, ok)
	require.Equal(t, uint32(45000), peerA.ExternalPort)
	require.Equal(t, uint32(9000), peerA.LocalPort)
	require.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, peerA.IPs)
	require.Contains(t, peerA.Services, "web")
	require.Contains(t, peerA.Services, "api")
	require.Equal(t, uint32(8080), peerA.Services["web"].GetPort())
	require.Equal(t, uint32(9090), peerA.Services["api"].GetPort())
}

func TestApplyDelta_ReturnsDomainEvents(t *testing.T) {
	pubA := genPub(t)
	storeA := newTestStore(pubA)

	storeA.upsertLocalService(8080, "web")
	storeA.setLocalVivaldiCoord(coords.Coord{X: 1.0, Y: 2.0, Height: coords.MinHeight})

	full := storeA.FullState()

	pubB := genPub(t)
	storeB := newTestStore(pubB)

	from := types.PeerKeyFromBytes(pubA)
	events, _, err := storeB.ApplyDelta(from, full)
	require.NoError(t, err)

	var (
		hasGossipApplied bool
		hasPeerJoined    bool
		hasService       bool
		hasTopology      bool
	)
	for _, ev := range events {
		switch ev.(type) {
		case GossipApplied:
			hasGossipApplied = true
		case PeerJoined:
			hasPeerJoined = true
		case ServiceChanged:
			hasService = true
		case TopologyChanged:
			hasTopology = true
		}
	}

	require.True(t, hasGossipApplied, "expected GossipApplied event")
	require.True(t, hasPeerJoined, "expected PeerJoined event")
	require.True(t, hasService, "expected ServiceChanged event")
	require.True(t, hasTopology, "expected TopologyChanged event for Vivaldi")
}
