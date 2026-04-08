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

func (m *mockStore) SetWorkloadSpec(string, string, uint32, uint32, uint32, float32) []state.Event {
	return nil
}

func (m *mockStore) SetLocalResources(float64, float64, uint64, uint32, uint32, uint32) []state.Event {
	return nil
}

func (m *mockStore) SetSeedLoad(map[string]float32) []state.Event   { return nil }
func (m *mockStore) SetSeedDemand(map[string]float32) []state.Event { return nil }

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
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, MinReplicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}
	wm := &mockWorkloads{}

	r := newReconciler(local, ms, wm, &mockCAS{}, &slowFetcher{midFlight: func() { ms.removeSpec(hash) }}, newUtilisationTracker(), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, 1, []types.PeerKey{local})

	require.False(t, wm.seeded.Load(), "should not seed when spec was removed")
	require.False(t, ms.wasClaimed(hash), "should not set claim when spec was removed")
}

func TestExecuteClaim_NoLongerWinnerAfterFetch(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	peer3 := peerKey(3)
	hash := "workload2"

	allPeers := []types.PeerKey{local, peer2, peer3}
	actions := evaluate(evaluateInput{
		localID:        local,
		allPeers:       allPeers,
		specs:          map[string]spec{hash: {MinReplicas: 1}},
		claims:         nil,
		cluster:        clusterState{},
		isRunning:      func(string) bool { return false },
		demandRates:    map[string]float64{},
		idleDurations:  map[string]time.Duration{},
		dynamicTargets: map[string]uint32{hash: 1},
	})
	require.True(t, hasAction(actions, hash, actionClaim), "precondition: local must be the winner for this hash")

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, MinReplicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: allPeers,
	}

	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{midFlight: func() { ms.addClaim(hash, peer2) }}, newUtilisationTracker(), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, 1, []types.PeerKey{local})

	require.False(t, ms.wasClaimed(hash), "should not set claim when no longer a winner")
}

func TestResidencyWindow_SuppressesEarlyRelease(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, MinReplicas: 1},
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

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), zap.NewNop().Sugar(), &sync.WaitGroup{})
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
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, MinReplicas: 1},
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

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), zap.NewNop().Sugar(), &sync.WaitGroup{})
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

	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), zap.NewNop().Sugar(), &sync.WaitGroup{})

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
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, MinReplicas: 1},
				Publisher: peer2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{hash: {local: {}, peer2: {}}},
		allPeers: []types.PeerKey{local, peer2},
	}

	r := newReconciler(local, ms, &runningWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), zap.NewNop().Sugar(), &sync.WaitGroup{})
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
				Spec:      &statev1.WorkloadSpecChange{Hash: "hash-a", Name: "myapp", MinReplicas: 1},
				Publisher: publisher1,
			},
			"hash-b": {
				Spec:      &statev1.WorkloadSpecChange{Hash: "hash-b", Name: "myapp", MinReplicas: 1},
				Publisher: publisher2,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}

	var wg sync.WaitGroup
	r := newReconciler(local, ms, &mockWorkloads{}, &mockCAS{}, &slowFetcher{}, newUtilisationTracker(), zap.NewNop().Sugar(), &wg)
	r.reconcile(context.Background())
	wg.Wait()

	require.True(t, ms.wasClaimed("hash-a"), "winning hash (lower PeerKey publisher) must be claimed")
	require.False(t, ms.wasClaimed("hash-b"), "losing hash (higher PeerKey publisher) must not be claimed")
}
