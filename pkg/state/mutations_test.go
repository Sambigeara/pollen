package state

import (
	"net/netip"
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/stretchr/testify/require"
)

func TestDenyPeer_ReturnsPeerDeniedEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	targetKey, _ := peerKey(0x42)

	events := s.DenyPeer(targetKey)
	require.Len(t, events, 1)
	require.Equal(t, PeerDenied{Key: targetKey}, events[0])

	// Duplicate deny returns no events.
	events = s.DenyPeer(targetKey)
	require.Nil(t, events)
}

func TestSetService_ReturnsServiceChangedEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.SetService(8080, "web")
	require.Len(t, events, 1)
	require.Equal(t, ServiceChanged{Peer: s.localID, Name: "web"}, events[0])

	// Idempotent: same port+name returns no events.
	events = s.SetService(8080, "web")
	require.Nil(t, events)
}

func TestSetLocalCoord_ReturnsTopologyChangedEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	c := coords.Coord{X: 1.0, Y: 2.0, Height: 0.5}
	events := s.SetLocalCoord(c, 0.5)
	require.Len(t, events, 1)
	require.Equal(t, TopologyChanged{Peer: s.localID}, events[0])
}

func TestClaimWorkload_ReturnsWorkloadChangedEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.ClaimWorkload("abc123")
	require.Len(t, events, 1)
	require.Equal(t, WorkloadChanged{Hash: "abc123"}, events[0])

	// Release also returns WorkloadChanged.
	events = s.ReleaseWorkload("abc123")
	require.Len(t, events, 1)
	require.Equal(t, WorkloadChanged{Hash: "abc123"}, events[0])

	// Releasing again is a no-op.
	events = s.ReleaseWorkload("abc123")
	require.Nil(t, events)
}

func TestSetLocalAddresses_ReturnsTopologyChangedEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	addrs := []netip.AddrPort{
		netip.MustParseAddrPort("10.0.0.1:9000"),
		netip.MustParseAddrPort("10.0.0.2:9000"),
	}
	events := s.SetLocalAddresses(addrs)
	require.Len(t, events, 1)
	require.Equal(t, TopologyChanged{Peer: s.localID}, events[0])

	// Same addresses returns no events.
	events = s.SetLocalAddresses(addrs)
	require.Nil(t, events)
}
