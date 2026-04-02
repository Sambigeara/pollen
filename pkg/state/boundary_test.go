package state_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/netip"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func newBoundaryStore(t *testing.T) (state.StateStore, ed25519.PublicKey) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return state.New(types.PeerKeyFromBytes(pub)), pub
}

func genBoundaryPub(t *testing.T) ed25519.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub
}

func TestStore_SnapshotIsImmutable(t *testing.T) {
	s, _ := newBoundaryStore(t)

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	s.SetService(8080, "web")

	snap1 := s.Snapshot()

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.99:7777")})
	s.SetService(9090, "api")
	s.RemoveService("web")

	snap2 := s.Snapshot()

	// snap1 must reflect the original state, not the mutations.
	local1, ok := snap1.Peer(snap1.Self())
	require.True(t, ok)
	require.Equal(t, []string{"10.0.0.1"}, local1.IPs)
	require.Equal(t, uint32(9000), local1.LocalPort)
	require.Contains(t, local1.Services, "web")
	require.NotContains(t, local1.Services, "api")

	// snap2 must reflect the mutated state.
	local2, ok := snap2.Peer(snap2.Self())
	require.True(t, ok)
	require.Equal(t, []string{"10.0.0.99"}, local2.IPs)
	require.Equal(t, uint32(7777), local2.LocalPort)
	require.Contains(t, local2.Services, "api")
	require.NotContains(t, local2.Services, "web")
}

func TestStore_ApplyDeltaReturnsEvents(t *testing.T) {
	storeA, pubA := newBoundaryStore(t)
	storeB, _ := newBoundaryStore(t)

	storeA.SetService(8080, "web")
	fullData := storeA.EncodeFull()
	require.NotEmpty(t, fullData)

	from := types.PeerKeyFromBytes(pubA)
	events, rebroadcast, err := storeB.ApplyDelta(from, fullData)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.NotEmpty(t, rebroadcast)
}

func TestStore_DenyPeerAppearsInSnapshot(t *testing.T) {
	s, _ := newBoundaryStore(t)

	targetPub := genBoundaryPub(t)
	targetKey := types.PeerKeyFromBytes(targetPub)

	s.DenyPeer(targetKey)

	snap := s.Snapshot()
	denied := snap.DeniedPeers()
	require.Len(t, denied, 1)
	require.Equal(t, targetKey, denied[0])
}

func TestStore_ServiceAppearsInSnapshot(t *testing.T) {
	s, _ := newBoundaryStore(t)

	s.SetService(8080, "web")

	snap := s.Snapshot()
	svcs := snap.Services()
	require.Len(t, svcs, 1)
	require.Equal(t, "web", svcs[0].Name)
	require.Equal(t, uint32(8080), svcs[0].Port)
	require.Equal(t, snap.Self(), svcs[0].Peer)
}

func TestStore_WorkloadSpecAppearsInSnapshot(t *testing.T) {
	s, _ := newBoundaryStore(t)

	s.SetWorkloadSpec("abc123", 1, 0, 0)

	snap := s.Snapshot()
	workloads := snap.Workloads()
	require.Len(t, workloads, 1)
	require.Equal(t, "abc123", workloads[0].Hash)
	require.Equal(t, snap.Self(), workloads[0].Spec.Publisher)
}

func TestStore_MutationReturnsEvents(t *testing.T) {
	s, _ := newBoundaryStore(t)

	events := s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	require.NotEmpty(t, events)
}

func TestSnapshot_ProjectionConsistency(t *testing.T) {
	s, pub := newBoundaryStore(t)
	localKey := types.PeerKeyFromBytes(pub)

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	s.SetService(8080, "web")
	s.SetService(9090, "api")
	s.SetWorkloadSpec("hash1", 1, 0, 0)

	// Add a remote peer via gossip and make it reachable.
	peerPub := genBoundaryPub(t)
	peerKey := types.PeerKeyFromBytes(peerPub)
	peerFullState := func() []byte {
		batch := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{
			{
				PeerId:  peerKey.String(),
				Counter: 1,
				Change: &statev1.GossipEvent_Network{
					Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
				},
			},
			{
				PeerId:  peerKey.String(),
				Counter: 2,
				Change: &statev1.GossipEvent_CertExpiry{
					CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()},
				},
			},
			{
				PeerId:  peerKey.String(),
				Counter: 3,
				Change: &statev1.GossipEvent_Service{
					Service: &statev1.ServiceChange{Name: "remote-svc", Port: 3000},
				},
			},
		}}
		data, _ := batch.MarshalVT()
		return data
	}()
	_, _, err := s.ApplyDelta(peerKey, peerFullState)
	require.NoError(t, err)
	s.SetLocalReachable([]types.PeerKey{peerKey})

	snap := s.Snapshot()

	// Peers() length matches PeerKeys length.
	require.Equal(t, len(snap.PeerKeys), len(snap.Peers()))

	// Self() matches the store's local key.
	require.Equal(t, localKey, snap.Self())

	// Each service in Services() corresponds to a real entry in a peer's NodeView.
	for _, svc := range snap.Services() {
		nv, ok := snap.Nodes[svc.Peer]
		require.True(t, ok, "service peer %s not found in Nodes", svc.Peer)
		require.Contains(t, nv.Services, svc.Name)
		require.Equal(t, svc.Port, nv.Services[svc.Name].Port)
	}

	// Each workload in Workloads() has a matching spec.
	for _, wl := range snap.Workloads() {
		require.NotNil(t, wl.Spec.Spec, "workload %s has nil spec", wl.Hash)
		require.Equal(t, wl.Hash, wl.Spec.Spec.GetHash())
	}
}
