package state

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/stretchr/testify/require"
)

func TestEncodeDelta_RoundTrip(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(pkA)

	storeA.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	storeA.upsertLocalService(8080, "web")

	snapBefore := storeA.Snapshot()
	digestBefore := snapBefore.Digest()

	storeA.setObservedAddress("", 45000)

	delta := storeA.EncodeDelta(digestBefore)
	require.NotEmpty(t, delta)

	pkB := genKey(t)
	storeB := newTestStore(pkB)

	events, _, err := storeB.ApplyDelta(pkA, delta)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	snapB := storeB.Snapshot()
	peerA, ok := snapB.Peer(pkA)
	require.True(t, ok)
	require.Equal(t, uint32(45000), peerA.ExternalPort)
}

func TestEncodeFull_ContainsEverything(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(pkA)

	storeA.setLocalNetwork([]string{"10.0.0.1", "10.0.0.2"}, 9000)
	storeA.upsertLocalService(8080, "web")
	storeA.upsertLocalService(9090, "api")
	storeA.setObservedAddress("", 45000)
	storeA.setLocalVivaldiCoord(coords.Coord{X: 1.0, Y: 2.0, Height: coords.MinHeight}, 0.5)

	full := storeA.EncodeFull()
	require.NotEmpty(t, full)

	pkB := genKey(t)
	storeB := newTestStore(pkB)

	_, _, err := storeB.ApplyDelta(pkA, full)
	require.NoError(t, err)

	snapB := storeB.Snapshot()
	peerA, ok := snapB.Peer(pkA)
	require.True(t, ok)
	require.Equal(t, uint32(45000), peerA.ExternalPort)
	require.Equal(t, uint32(9000), peerA.LocalPort)
	require.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, peerA.IPs)
	require.Contains(t, peerA.Services, "web")
	require.Contains(t, peerA.Services, "api")
	require.Equal(t, uint32(8080), peerA.Services["web"].Port)
	require.Equal(t, uint32(9090), peerA.Services["api"].Port)
}

func TestApplyDelta_ReturnsDomainEvents(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(pkA)

	storeA.upsertLocalService(8080, "web")
	storeA.setLocalVivaldiCoord(coords.Coord{X: 1.0, Y: 2.0, Height: coords.MinHeight}, 0.5)

	full := storeA.EncodeFull()

	pkB := genKey(t)
	storeB := newTestStore(pkB)

	events, _, err := storeB.ApplyDelta(pkA, full)
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
