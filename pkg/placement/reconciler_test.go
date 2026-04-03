package placement

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockStore struct {
	mu       sync.Mutex
	specs    map[string]state.WorkloadSpecView
	claims   map[string]map[types.PeerKey]struct{}
	allPeers []types.PeerKey
	claimed  map[string]bool
	nodes    map[types.PeerKey]state.NodeView
}

func (m *mockStore) Snapshot() state.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	specs := make(map[string]state.WorkloadSpecView, len(m.specs))
	maps.Copy(specs, m.specs)

	claims := make(map[string]map[types.PeerKey]struct{}, len(m.claims))
	for k, v := range m.claims {
		inner := make(map[types.PeerKey]struct{}, len(v))
		for pk := range v {
			inner[pk] = struct{}{}
		}
		claims[k] = inner
	}

	peers := append([]types.PeerKey(nil), m.allPeers...)

	nodes := make(map[types.PeerKey]state.NodeView, len(m.nodes))
	maps.Copy(nodes, m.nodes)

	return state.Snapshot{
		Specs:    specs,
		Claims:   claims,
		PeerKeys: peers,
		Nodes:    nodes,
	}
}

func (m *mockStore) SetWorkloadSpec(string, uint32, uint32, uint32) []state.Event { return nil }
func (m *mockStore) SetLocalResources(float64, float64) []state.Event             { return nil }

func (m *mockStore) ClaimWorkload(hash string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed == nil {
		m.claimed = make(map[string]bool)
	}
	m.claimed[hash] = true
	return nil
}

func (m *mockStore) ReleaseWorkload(hash string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed == nil {
		m.claimed = make(map[string]bool)
	}
	m.claimed[hash] = false
	return nil
}

func (m *mockStore) wasClaimed(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.claimed[hash]
}

func (m *mockStore) removeSpec(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.specs, hash)
}

func (m *mockStore) addClaim(hash string, pk types.PeerKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claims[hash] == nil {
		m.claims[hash] = make(map[types.PeerKey]struct{})
	}
	m.claims[hash][pk] = struct{}{}
}

type mockWorkloads struct {
	seeded atomic.Bool
}

func (m *mockWorkloads) SeedFromCAS(context.Context, string, wasm.PluginConfig) error {
	m.seeded.Store(true)
	return nil
}
func (m *mockWorkloads) Unseed(string) error   { return nil }
func (m *mockWorkloads) IsRunning(string) bool { return false }

type runningWorkloads struct{ mockWorkloads }

func (m *runningWorkloads) IsRunning(string) bool { return true }

type mockCAS struct{}

func (m *mockCAS) Has(string) bool { return false }

type slowFetcher struct {
	midFlight func()
}

func (f *slowFetcher) Fetch(_ context.Context, _ string, _ []types.PeerKey) error {
	if f.midFlight != nil {
		f.midFlight()
	}
	return nil
}

func TestExecuteClaim_SpecRemovedDuringFetch(t *testing.T) {
	local := peerKey(1)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}
	wm := &mockWorkloads{}

	r := newReconciler(local, ms, wm, &mockCAS{}, &slowFetcher{midFlight: func() { ms.removeSpec(hash) }}, zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, []types.PeerKey{local})

	require.False(t, wm.seeded.Load(), "should not seed when spec was removed")
	require.False(t, ms.wasClaimed(hash), "should not set claim when spec was removed")
}

func TestExecuteClaim_NoLongerWinnerAfterFetch(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	peer3 := peerKey(3)
	hash := "workload2"

	allPeers := []types.PeerKey{local, peer2, peer3}
	actions := evaluate(local, allPeers, map[string]spec{hash: {Replicas: 1}}, nil, clusterState{}, func(string) bool { return false })
	require.True(t, hasAction(actions, hash, actionClaim), "precondition: local must be the winner for this hash")

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: allPeers,
	}

	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{midFlight: func() { ms.addClaim(hash, peer2) }}, zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, []types.PeerKey{local})

	require.False(t, ms.wasClaimed(hash), "should not set claim when no longer a winner")
}

func TestResidencyWindow_SuppressesEarlyRelease(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: peer2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{hash: {local: {}}},
		allPeers: []types.PeerKey{local, peer2},
		nodes: map[types.PeerKey]state.NodeView{
			local: {NumCPU: 2, CPUPercent: 90, MemTotalBytes: 2 << 30, MemPercent: 90},
			peer2: {NumCPU: 16, CPUPercent: 5, MemTotalBytes: 64 << 30, MemPercent: 5},
		},
	}

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.claimStartTime[hash] = time.Now()
	r.pendingRelease[hash] = time.Now().Add(-time.Second)

	r.reconcile(context.Background())

	ms.mu.Lock()
	_, claimWasSet := ms.claimed[hash]
	ms.mu.Unlock()
	require.False(t, claimWasSet, "should not release during residency window")
}

func TestResidencyWindow_AllowsReleaseAfterMaturity(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: peer2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{hash: {local: {}}},
		allPeers: []types.PeerKey{local, peer2},
		nodes: map[types.PeerKey]state.NodeView{
			local: {NumCPU: 2, CPUPercent: 90, MemTotalBytes: 2 << 30, MemPercent: 90},
			peer2: {NumCPU: 16, CPUPercent: 5, MemTotalBytes: 64 << 30, MemPercent: 5},
		},
	}

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.firstRun = false // skip cleanupStaleClaims — test starts mid-lifecycle
	r.claimStartTime[hash] = time.Now().Add(-minResidencyDuration - time.Second)
	r.pendingRelease[hash] = time.Now().Add(-time.Second)

	r.reconcile(context.Background())

	ms.mu.Lock()
	val, claimWasSet := ms.claimed[hash]
	ms.mu.Unlock()
	require.True(t, claimWasSet, "should release after residency window")
	require.False(t, val, "claim should be set to false (released)")
}

func TestReconcile_SignalCoalesces(t *testing.T) {
	local := peerKey(1)
	ms := &mockStore{
		specs:    map[string]state.WorkloadSpecView{},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}

	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{}, zap.NewNop().Sugar(), &sync.WaitGroup{})

	for range 100 {
		r.Signal()
	}

	require.Len(t, r.triggerCh, 1)
}

func TestResidencyWindow_SkippedForOverReplication(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: peer2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{hash: {local: {}, peer2: {}}},
		allPeers: []types.PeerKey{local, peer2},
	}

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.claimStartTime[hash] = time.Now()
	r.pendingRelease[hash] = time.Now().Add(-time.Second)

	r.reconcile(context.Background())

	ms.mu.Lock()
	val, claimWasSet := ms.claimed[hash]
	ms.mu.Unlock()

	if claimWasSet {
		require.False(t, val, "over-replicated release should bypass residency window")
	}
}
