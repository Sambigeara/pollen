package placement

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func (m *mockStore) SetWorkloadSpec(state.WorkloadSpec) []state.Event { return nil }
func (m *mockStore) DeleteWorkloadSpec(hash string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.specs, hash)
	return nil
}

func (m *mockStore) SetLocalResources(state.NodeResources) []state.Event { return nil }

func (m *mockStore) SetSeedMetrics(map[string]state.SeedMetrics) []state.Event { return nil }
func (m *mockStore) SetSeedDialRates(map[string]map[string]float32) []state.Event {
	return nil
}

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
				Spec:      state.WorkloadSpec{Hash: hash, MinReplicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}
	wm := &mockWorkloads{}

	r := newReconciler(local, ms, wm, &mockCAS{}, &slowFetcher{midFlight: func() { ms.removeSpec(hash) }}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, 1, []types.PeerKey{local})

	require.False(t, wm.seeded.Load(), "should not seed when spec was removed")
	require.False(t, ms.wasClaimed(hash), "should not set claim when spec was removed")
}

func TestExecuteClaim_LosesCapacityDuringFetch(t *testing.T) {
	// While the artifact is being fetched, local's capacity collapses (e.g.
	// CPU saturation). The post-fetch re-check should trip the hard
	// capacity gate and skip the claim — the seed will land on a healthier
	// peer next reconcile tick.
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload2"
	allPeers := []types.PeerKey{local, peer2}

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, MinReplicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: allPeers,
		nodes: map[types.PeerKey]state.NodeView{
			local: {NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, CPUPercent: 10},
			peer2: {NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, CPUPercent: 10},
		},
	}

	saturate := func() {
		ms.mu.Lock()
		nv := ms.nodes[local]
		nv.CPUPercent = 100
		ms.nodes[local] = nv
		ms.mu.Unlock()
	}

	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{midFlight: saturate}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, 1, []types.PeerKey{local})

	require.False(t, ms.wasClaimed(hash), "should not set claim when capacity gate fails post-fetch")
}

// TestExecuteClaim_HappyPath ensures the claim path actually flips the
// store's claimed bit to true. Paired with the failure-case tests above
// it pins the positive assertion: a mutation that inverts the claim
// decision (e.g. always-return-false) would fail here even if the
// negative tests still passed.
func TestExecuteClaim_HappyPath(t *testing.T) {
	local := peerKey(1)
	hash := "workload-happy"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, MinReplicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}
	wm := &mockWorkloads{}

	r := newReconciler(local, ms, wm, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, 1, []types.PeerKey{local})

	require.True(t, wm.seeded.Load(), "manager should seed on happy path")
	require.True(t, ms.wasClaimed(hash), "store should be marked claimed on happy path")
}

func TestResidencyWindow_SuppressesEarlyRelease(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, MinReplicas: 1},
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

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.claimStartTime[hash] = time.Now()
	r.pendingRelease[hash] = time.Now().Add(-time.Second)

	r.reconcile(context.Background())

	ms.mu.Lock()
	_, claimWasSet := ms.claimed[hash]
	ms.mu.Unlock()
	require.False(t, claimWasSet, "should not release during residency window")
}

// TestResidencyWindow_AllowsReleaseAfterMaturity verifies that an excess
// claimant (above target) releases after the residency window + idle timeout.
// At-target claimants never release — challengers join first via shouldChallenge.
func TestResidencyWindow_AllowsReleaseAfterMaturity(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload1"

	// Two claimants, min_replicas=1 → local is excess.
	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, MinReplicas: 1},
				Publisher: peer2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{hash: {local: {}, peer2: {}}},
		allPeers: []types.PeerKey{local, peer2},
		nodes: map[types.PeerKey]state.NodeView{
			local: {NumCPU: 2, CPUPercent: 90, MemTotalBytes: 2 << 30, MemPercent: 90},
			peer2: {NumCPU: 16, CPUPercent: 5, MemTotalBytes: 64 << 30, MemPercent: 5},
		},
	}

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.firstRun = false
	r.claimStartTime[hash] = time.Now().Add(-minResidencyDuration - time.Second)
	r.pendingRelease[hash] = time.Now().Add(-time.Second)

	r.reconcile(context.Background())

	ms.mu.Lock()
	val, claimWasSet := ms.claimed[hash]
	ms.mu.Unlock()
	require.True(t, claimWasSet, "should release excess claimant after residency window")
	require.False(t, val, "claim should be set to false (released)")
}

func TestReconcile_SignalCoalesces(t *testing.T) {
	local := peerKey(1)
	ms := &mockStore{
		specs:    map[string]state.WorkloadSpecView{},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}

	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})

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
				Spec:      state.WorkloadSpec{Hash: hash, MinReplicas: 1},
				Publisher: peer2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{hash: {local: {}, peer2: {}}},
		allPeers: []types.PeerKey{local, peer2},
	}

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &sync.WaitGroup{})
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

func TestReconcile_NameConflictFiltering(t *testing.T) {
	local := peerKey(1)
	publisher1 := peerKey(1) // lower PeerKey — winner
	publisher2 := peerKey(2) // higher PeerKey — loser

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			"hash-a": {
				Spec:      state.WorkloadSpec{Hash: "hash-a", Name: "myapp", MinReplicas: 1},
				Publisher: publisher1,
			},
			"hash-b": {
				Spec:      state.WorkloadSpec{Hash: "hash-b", Name: "myapp", MinReplicas: 1},
				Publisher: publisher2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}

	var wg sync.WaitGroup
	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), newGateRegistry(func(string) int { return 16 }), zap.NewNop().Sugar(), &wg)
	r.reconcile(context.Background())
	wg.Wait()

	require.True(t, ms.wasClaimed("hash-a"), "winning hash (lower PeerKey publisher) must be claimed")
	require.False(t, ms.wasClaimed("hash-b"), "losing hash (higher PeerKey publisher) must not be claimed")
}
