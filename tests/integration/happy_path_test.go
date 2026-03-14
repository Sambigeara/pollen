package integration

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/scheduler"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/traffic"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func testPeerKey(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func testPub(b byte) []byte {
	pub := make([]byte, 32)
	pub[0] = b
	return pub
}

func loadTestStore(t *testing.T, pub []byte) *store.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Load(dir, pub)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// ---------------------------------------------------------------------------
// 1. Store CRDT convergence
// ---------------------------------------------------------------------------

func TestStoreCRDT_ApplyAndConverge(t *testing.T) {
	pubA := testPub(1)
	pubB := testPub(2)

	storeA := loadTestStore(t, pubA)
	storeB := loadTestStore(t, pubB)

	// Node A sets its network info.
	eventsA := storeA.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.Len(t, eventsA, 1)

	// Node B applies A's events.
	storeB.ApplyEvents(eventsA, true)

	// Node B sets its own network info.
	eventsB := storeB.SetLocalNetwork([]string{"10.0.0.2"}, 9001)
	require.Len(t, eventsB, 1)

	// Node A applies B's events.
	storeA.ApplyEvents(eventsB, true)

	// KnownPeers excludes the local node, so each store should see 1 remote peer.
	peersA := storeA.KnownPeers()
	peersB := storeB.KnownPeers()

	require.Len(t, peersA, 1, "store A should know about 1 remote peer")
	require.Len(t, peersB, 1, "store B should know about 1 remote peer")

	// Verify cross-visibility: A sees B, B sees A.
	require.Equal(t, storeB.LocalID(), peersA[0].PeerID)
	require.Equal(t, storeA.LocalID(), peersB[0].PeerID)
}

func TestStoreCRDT_IdempotentApply(t *testing.T) {
	pubA := testPub(1)
	pubB := testPub(2)

	storeA := loadTestStore(t, pubA)
	storeB := loadTestStore(t, pubB)

	events := storeA.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Apply the same events twice to B.
	storeB.ApplyEvents(events, true)
	storeB.ApplyEvents(events, true)

	// KnownPeers excludes local, so B sees 1 remote (A).
	peers := storeB.KnownPeers()
	require.Len(t, peers, 1, "duplicate apply should not create extra records")
	require.Equal(t, storeA.LocalID(), peers[0].PeerID)
}

func TestStoreCRDT_OutOfOrderEvents(t *testing.T) {
	pubA := testPub(1)
	pubB := testPub(2)

	storeA := loadTestStore(t, pubA)
	storeB := loadTestStore(t, pubB)

	// Generate two events from A in order.
	events1 := storeA.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	events2 := storeA.SetExternalPort(45000)

	// Apply out of order to B: event 2 first, then event 1.
	storeB.ApplyEvents(events2, true)
	storeB.ApplyEvents(events1, true)

	// B should see A as a known peer (with IPs and port).
	peers := storeB.KnownPeers()
	require.Len(t, peers, 1)
	require.Equal(t, storeA.LocalID(), peers[0].PeerID)
}

func TestStoreCRDT_WorkloadSpecAndClaim(t *testing.T) {
	pub := testPub(1)
	s := loadTestStore(t, pub)

	// Publish a workload spec.
	events, err := s.SetLocalWorkloadSpec("abc123", 2, 256, 5000)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	// Verify the spec is visible.
	specs := s.AllWorkloadSpecs()
	require.Contains(t, specs, "abc123")
	require.Equal(t, uint32(2), specs["abc123"].Spec.GetReplicas())

	// Claim the workload.
	claimEvents := s.SetLocalWorkloadClaim("abc123", true)
	require.NotEmpty(t, claimEvents)

	// Verify the claim is visible.
	claims := s.AllWorkloadClaims()
	require.Contains(t, claims, "abc123")
	require.Contains(t, claims["abc123"], s.LocalID())

	// Release the claim.
	releaseEvents := s.SetLocalWorkloadClaim("abc123", false)
	require.NotEmpty(t, releaseEvents)

	claims = s.AllWorkloadClaims()
	if claimants, ok := claims["abc123"]; ok {
		require.NotContains(t, claimants, s.LocalID())
	}
}

func TestStoreCRDT_ConnectedReachability(t *testing.T) {
	pub := testPub(1)
	peerPK := testPeerKey(2)
	s := loadTestStore(t, pub)

	// Connect to a peer.
	events := s.SetLocalConnected(peerPK, true)
	require.NotEmpty(t, events)

	// Disconnect.
	events = s.SetLocalConnected(peerPK, false)
	require.NotEmpty(t, events)
}

func TestStoreCRDT_ClockDigestRoundTrip(t *testing.T) {
	pubA := testPub(1)
	pubB := testPub(2)

	storeA := loadTestStore(t, pubA)
	storeB := loadTestStore(t, pubB)

	// A publishes state.
	storeA.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	storeA.SetExternalPort(45000)

	// B sends its digest to A; A computes what B is missing.
	digestB := storeB.EagerSyncClock()
	missingForB := storeA.MissingFor(digestB)

	// Apply A's missing events to B.
	require.NotEmpty(t, missingForB, "A should have events B is missing")
	storeB.ApplyEvents(missingForB, true)

	// Now B should see A as a known peer.
	peersB := storeB.KnownPeers()
	require.Len(t, peersB, 1)
	require.Equal(t, storeA.LocalID(), peersB[0].PeerID)
}

// ---------------------------------------------------------------------------
// 2. Peer state machine
// ---------------------------------------------------------------------------

func TestPeerStateMachine_DiscoveryToConnected(t *testing.T) {
	s := peer.NewStore()
	key := testPeerKey(1)
	now := time.Now()

	// Discover a peer.
	s.Step(now, peer.DiscoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	p, ok := s.Get(key)
	require.True(t, ok)
	require.Equal(t, peer.Discovered, p.State)

	// Tick transitions to Connecting via an AttemptConnect output.
	outputs := s.Step(now, peer.Tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(peer.AttemptConnect)
	require.True(t, isAttempt, "expected AttemptConnect output")

	p, _ = s.Get(key)
	require.Equal(t, peer.Connecting, p.State)

	// Simulate successful connect.
	s.Step(now.Add(time.Second), peer.ConnectPeer{
		PeerKey:      key,
		IP:           net.ParseIP("10.0.0.1"),
		ObservedPort: 9000,
	})

	p, _ = s.Get(key)
	require.Equal(t, peer.Connected, p.State)
}

func TestPeerStateMachine_DisconnectAndRediscover(t *testing.T) {
	s := peer.NewStore()
	key := testPeerKey(1)
	now := time.Now()

	// Full cycle: discover -> connect -> disconnect -> rediscover.
	s.Step(now, peer.DiscoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})
	s.Step(now, peer.Tick{})
	s.Step(now, peer.ConnectPeer{
		PeerKey:      key,
		IP:           net.ParseIP("10.0.0.1"),
		ObservedPort: 9000,
	})

	p, _ := s.Get(key)
	require.Equal(t, peer.Connected, p.State)

	// Disconnect.
	s.Step(now, peer.PeerDisconnected{
		PeerKey: key,
		Reason:  peer.DisconnectGraceful,
	})

	p, _ = s.Get(key)
	require.Equal(t, peer.Discovered, p.State)
}

func TestPeerStateMachine_ForgetRemovesPeer(t *testing.T) {
	s := peer.NewStore()
	key := testPeerKey(1)
	now := time.Now()

	s.Step(now, peer.DiscoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	_, ok := s.Get(key)
	require.True(t, ok)

	s.Step(now, peer.ForgetPeer{PeerKey: key})

	_, ok = s.Get(key)
	require.False(t, ok, "peer should be gone after ForgetPeer")
}

func TestPeerStateMachine_ConnectFailureEscalatesToUnreachable(t *testing.T) {
	s := peer.NewStore()
	key := testPeerKey(1)
	now := time.Now()

	s.Step(now, peer.DiscoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	// Exhaust direct attempts (2 failures).
	s.Step(now, peer.Tick{})
	s.Step(now, peer.ConnectFailed{PeerKey: key})
	s.Step(now.Add(2*time.Second), peer.Tick{})
	s.Step(now.Add(2*time.Second), peer.ConnectFailed{PeerKey: key})

	// Exhaust punch attempts (2 failures).
	s.Step(now.Add(4*time.Second), peer.Tick{})
	s.Step(now.Add(4*time.Second), peer.ConnectFailed{PeerKey: key})
	s.Step(now.Add(6*time.Second), peer.Tick{})
	s.Step(now.Add(6*time.Second), peer.ConnectFailed{PeerKey: key})

	p2, _ := s.Get(key)
	require.Equal(t, peer.Unreachable, p2.State)
}

// ---------------------------------------------------------------------------
// 3. Scheduler reconciler (with mocks)
// ---------------------------------------------------------------------------

// mockSchedulerStore implements scheduler.SchedulerStore.
type mockSchedulerStore struct {
	specs     map[string]store.WorkloadSpecView
	claims    map[string]map[types.PeerKey]struct{}
	allPeers  []types.PeerKey
	placement map[types.PeerKey]store.NodePlacementState
	claimed   map[string]bool
}

func (m *mockSchedulerStore) AllWorkloadSpecs() map[string]store.WorkloadSpecView {
	return m.specs
}

func (m *mockSchedulerStore) AllWorkloadClaims() map[string]map[types.PeerKey]struct{} {
	return m.claims
}

func (m *mockSchedulerStore) AllPeerKeys() []types.PeerKey {
	return m.allPeers
}

func (m *mockSchedulerStore) SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent {
	if m.claimed == nil {
		m.claimed = make(map[string]bool)
	}
	m.claimed[hash] = claimed
	return nil
}

func (m *mockSchedulerStore) AllNodePlacementStates() map[types.PeerKey]store.NodePlacementState {
	return m.placement
}

type mockWorkloadManager struct {
	running map[string]bool
}

func (m *mockWorkloadManager) SeedFromCAS(string, wasm.PluginConfig) error { return nil }
func (m *mockWorkloadManager) Unseed(string) error                         { return nil }
func (m *mockWorkloadManager) IsRunning(hash string) bool {
	if m.running == nil {
		return false
	}
	return m.running[hash]
}

type mockCAS struct{ has map[string]bool }

func (m *mockCAS) Has(hash string) bool {
	if m.has == nil {
		return true
	}
	return m.has[hash]
}

type mockFetcher struct{}

func (m *mockFetcher) Fetch(_ context.Context, _ string, _ []types.PeerKey) error { return nil }

func TestReconciler_RunAndSignal(t *testing.T) {
	local := testPeerKey(1)

	ms := &mockSchedulerStore{
		specs:    map[string]store.WorkloadSpecView{},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}

	var signalCount atomic.Int32
	r := scheduler.NewReconciler(
		local, ms, &mockWorkloadManager{}, &mockCAS{}, &mockFetcher{},
		func(_ []*statev1.GossipEvent) { signalCount.Add(1) },
		zap.NewNop().Sugar(),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Run reconciler in background.
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	// Signal and give it time to process.
	r.Signal()
	r.SignalTraffic()

	// Cancel and wait for clean shutdown.
	cancel()
	select {
	case <-done:
		// Clean shutdown.
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not shut down within 5s")
	}
}

// ---------------------------------------------------------------------------
// 4. Traffic recorder
// ---------------------------------------------------------------------------

func TestTrafficRecorder_RecordAndRotate(t *testing.T) {
	tr := traffic.New()
	pk := testPeerKey(1)

	tr.Record(pk, 100, 200)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Equal(t, traffic.PeerTraffic{BytesIn: 100, BytesOut: 200}, snap[pk])

	// Second rotation with more traffic accumulates in the window.
	tr.Record(pk, 50, 30)
	snap, changed = tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Equal(t, traffic.PeerTraffic{BytesIn: 150, BytesOut: 230}, snap[pk])
}

func TestTrafficRecorder_MultiPeer(t *testing.T) {
	tr := traffic.New()
	pk1 := testPeerKey(1)
	pk2 := testPeerKey(2)

	tr.Record(pk1, 100, 200)
	tr.Record(pk2, 300, 400)

	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Len(t, snap, 2)
	require.Equal(t, uint64(100), snap[pk1].BytesIn)
	require.Equal(t, uint64(300), snap[pk2].BytesIn)
}

// ---------------------------------------------------------------------------
// 5. Store + Scheduler integration: store-level claim lifecycle
// ---------------------------------------------------------------------------

func TestStoreAndScheduler_EndToEnd(t *testing.T) {
	pubA := testPub(1)
	pubB := testPub(2)

	storeA := loadTestStore(t, pubA)
	storeB := loadTestStore(t, pubB)

	// Make each store aware of the other by exchanging network events.
	netA := storeA.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	netB := storeB.SetLocalNetwork([]string{"10.0.0.2"}, 9001)
	storeB.ApplyEvents(netA, true)
	storeA.ApplyEvents(netB, true)

	// Mark both as connected so they appear in each other's live component.
	connA := storeA.SetLocalConnected(storeB.LocalID(), true)
	connB := storeB.SetLocalConnected(storeA.LocalID(), true)
	storeB.ApplyEvents(connA, true)
	storeA.ApplyEvents(connB, true)

	// A publishes a workload spec with replicas=2.
	specEvents, err := storeA.SetLocalWorkloadSpec("wl1", 2, 256, 5000)
	require.NoError(t, err)

	// B learns about the spec.
	storeB.ApplyEvents(specEvents, true)

	// Both stores see the spec.
	specsA := storeA.AllWorkloadSpecs()
	specsB := storeB.AllWorkloadSpecs()
	require.Contains(t, specsA, "wl1")
	require.Contains(t, specsB, "wl1")

	// Execute claims on both stores.
	claimEventsA := storeA.SetLocalWorkloadClaim("wl1", true)
	require.NotEmpty(t, claimEventsA)
	claimEventsB := storeB.SetLocalWorkloadClaim("wl1", true)
	require.NotEmpty(t, claimEventsB)

	// Cross-replicate claim events.
	storeB.ApplyEvents(claimEventsA, true)
	storeA.ApplyEvents(claimEventsB, true)

	// Both stores should see both claims.
	claimsA := storeA.AllWorkloadClaims()
	claimsB := storeB.AllWorkloadClaims()
	require.Len(t, claimsA["wl1"], 2)
	require.Len(t, claimsB["wl1"], 2)
}
