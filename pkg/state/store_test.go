package state

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/netip"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func genKey(t *testing.T) types.PeerKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return types.PeerKeyFromBytes(pub)
}

func newTestStore(t *testing.T, pk types.PeerKey) *store {
	t.Helper()
	return New(pk).(*store)
}

func applyTestEvent(t *testing.T, s StateStore, ev *statev1.GossipEvent) []Event {
	t.Helper()
	batch := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{ev}}
	data, err := batch.MarshalVT()
	require.NoError(t, err)
	events, _, err := s.ApplyDelta(s.Snapshot().LocalID, data)
	require.NoError(t, err)
	return events
}

func TestStore_SnapshotIsImmutable(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	s.SetService(8080, "web")

	snap1 := s.Snapshot()

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.99:7777")})
	s.SetService(9090, "api")
	s.RemoveService("web")

	snap2 := s.Snapshot()

	local1 := snap1.Nodes[snap1.LocalID]
	require.Equal(t, []string{"10.0.0.1"}, local1.IPs)
	require.Equal(t, uint32(9000), local1.LocalPort)
	require.Contains(t, local1.Services, "web")
	require.NotContains(t, local1.Services, "api")

	local2 := snap2.Nodes[snap2.LocalID]
	require.Equal(t, []string{"10.0.0.99"}, local2.IPs)
	require.Equal(t, uint32(7777), local2.LocalPort)
	require.Contains(t, local2.Services, "api")
	require.NotContains(t, local2.Services, "web")
}

func TestStore_MutationsReturnEvents(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	events := s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	require.Len(t, events, 1)
	require.IsType(t, TopologyChanged{}, events[0])

	events = s.SetService(8080, "web")
	require.Len(t, events, 1)
	require.Equal(t, ServiceChanged{Peer: s.localID, Name: "web"}, events[0])

	events = s.SetLocalCoord(coords.Coord{X: 1.0, Y: 2.0, Height: 0.5}, 0.5)
	require.Len(t, events, 1)
	require.IsType(t, TopologyChanged{}, events[0])

	events = s.ClaimWorkload("abc123")
	require.Len(t, events, 1)
	require.Equal(t, WorkloadChanged{Hash: "abc123"}, events[0])

	targetKey := genKey(t)
	events = s.DenyPeer(targetKey)
	require.Len(t, events, 1)
	require.Equal(t, PeerDenied{Key: targetKey}, events[0])
}

func TestStore_ApplyDeltaReturnsEvents(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(t, pkA)
	storeA.SetService(8080, "web")
	fullData := storeA.EncodeFull()

	pkB := genKey(t)
	storeB := newTestStore(t, pkB)

	events, rebroadcast, err := storeB.ApplyDelta(pkA, fullData)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.NotEmpty(t, rebroadcast)

	var hasGossipApplied, hasPeerJoined, hasService bool
	for _, ev := range events {
		switch ev.(type) {
		case GossipApplied:
			hasGossipApplied = true
		case PeerJoined:
			hasPeerJoined = true
		case ServiceChanged:
			hasService = true
		}
	}

	require.True(t, hasGossipApplied)
	require.True(t, hasPeerJoined)
	require.True(t, hasService)
}

func TestSnapshot_PeerKeysFiltering(t *testing.T) {
	localKey := genKey(t)
	s := newTestStore(t, localKey)

	peerA, peerB, peerC := genKey(t), genKey(t), genKey(t)

	// Peer A is valid
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})

	// Peer B is valid
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerB.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002},
		},
	})

	// Peer C is expired
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerC.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(-time.Hour).Unix()},
		},
	})

	s.SetLocalReachable([]types.PeerKey{peerA, peerB})

	snap := s.Snapshot()

	require.Contains(t, snap.PeerKeys, localKey)
	require.Contains(t, snap.PeerKeys, peerA)
	require.Contains(t, snap.PeerKeys, peerB)
	require.NotContains(t, snap.PeerKeys, peerC)

	// Deny peer B
	s.DenyPeer(peerB)
	snap2 := s.Snapshot()

	require.NotContains(t, snap2.PeerKeys, peerB)
	require.NotContains(t, snap2.Nodes, peerB)
}

func TestStore_WorkloadSpecConflict(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	// Local claims spec
	s.SetWorkloadSpec("contested", 3, 0, 0)

	// Remote (lower peer ID) claims spec
	winnerPK := genKey(t)
	if pk.Compare(winnerPK) < 0 {
		winnerPK, pk = pk, winnerPK // Ensure winnerPK is actually lower
		s = newTestStore(t, pk)
		s.SetWorkloadSpec("contested", 3, 0, 0)
	}

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  winnerPK.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2},
		},
	})

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	require.Equal(t, winnerPK, specs["contested"].Publisher)
}

func TestStore_TrafficHeatmapRoundTrip(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(t, pkA)

	pkB := genKey(t)
	storeB := newTestStore(t, pkB)

	peerC := genKey(t)

	storeA.SetLocalTraffic(peerC, 100, 200)

	fullState := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(pkA, fullState)
	require.NoError(t, err)

	rates := storeB.Snapshot().Nodes[pkA].TrafficRates
	require.Len(t, rates, 1)
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, rates[peerC])
}

func TestStore_EphemeralTombstones(t *testing.T) {
	pk := genKey(t)
	s1 := newTestStore(t, pk)
	s1.SetLocalObservedAddress("1.2.3.4", 45000)
	s1.SetBootstrapPublic()

	staleState := s1.EncodeFull()

	// Restart
	s2 := newTestStore(t, pk)
	require.Equal(t, uint32(0), s2.Snapshot().Nodes[s2.localID].ExternalPort)

	// Apply stale state - should trigger conflict and tombstone wins
	_, _, err := s2.ApplyDelta(pk, staleState)
	require.NoError(t, err)

	snap := s2.Snapshot()
	require.Equal(t, uint32(0), snap.Nodes[s2.localID].ExternalPort)
	require.False(t, snap.Nodes[s2.localID].PubliclyAccessible)
}

func TestExportLoadLastAddrs(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	peerA := genKey(t)
	peerB := genKey(t)

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9002},
		},
	})

	s.SetPeerLastAddr(peerA, "10.0.0.2:9002")
	s.SetPeerLastAddr(peerB, "10.0.0.3:9003") // peerB not yet fully known, but we map it

	addrs := s.ExportLastAddrs()
	require.Equal(t, "10.0.0.2:9002", addrs[peerA])

	s2 := newTestStore(t, genKey(t))
	s2.LoadLastAddrs(addrs)

	// LoadLastAddrs only loads into known peers to avoid reviving dead peers
	applyTestEvent(t, s2, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9002},
		},
	})
	s2.LoadLastAddrs(addrs)

	snap := s2.Snapshot()
	require.Equal(t, "10.0.0.2:9002", snap.Nodes[peerA].LastAddr)
}
