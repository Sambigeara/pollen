// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"io"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testBackoff(t *testing.T) *backoff {
	t.Helper()
	return newBackoff(backoffConfig{ttl: time.Second}, func(time.Duration) {})
}

type mockStore struct {
	mu       sync.Mutex
	specs    map[string]state.WorkloadSpecView
	claims   map[string]map[types.PeerKey]struct{}
	allPeers []types.PeerKey
	claimed  map[string]bool
	draining map[string]bool
	nodes    map[types.PeerKey]state.NodeView
	localID  types.PeerKey
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

	drainingClaims := make(map[string]map[types.PeerKey]struct{})
	for hash, on := range m.draining {
		if !on {
			continue
		}
		if _, claimed := claims[hash][m.localID]; !claimed {
			continue
		}
		drainingClaims[hash] = map[types.PeerKey]struct{}{m.localID: {}}
	}

	peers := append([]types.PeerKey(nil), m.allPeers...)

	// Stamp every peer with a fresh LastEventAt so the staleness
	// filter doesn't drop them.
	now := time.Now()
	nodes := make(map[types.PeerKey]state.NodeView, len(m.nodes))
	for pk, nv := range m.nodes {
		if nv.LastEventAt.IsZero() {
			nv.LastEventAt = now
		}
		nodes[pk] = nv
	}

	return state.Snapshot{
		Specs:          specs,
		Claims:         claims,
		DrainingClaims: drainingClaims,
		PeerKeys:       peers,
		Nodes:          nodes,
		LocalID:        m.localID,
	}
}

func (m *mockStore) PublishWorkload(state.WorkloadSpec, *admissionv1.Predicate) ([]state.Event, error) {
	return nil, nil
}

func (m *mockStore) DeleteWorkloadSpec(hash string) ([]state.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.specs, hash)
	return nil, nil
}

func (m *mockStore) SetLocalResources(state.NodeResources) []state.Event { return nil }

func (m *mockStore) SetBackoffTTL(time.Time) []state.Event                { return nil }
func (m *mockStore) SetPerSeedCallCounts(map[string]uint64) []state.Event { return nil }

func (m *mockStore) ClaimWorkload(hash string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed == nil {
		m.claimed = make(map[string]bool)
	}
	m.claimed[hash] = true
	delete(m.draining, hash)
	return nil
}

func (m *mockStore) MarkWorkloadDraining(hash string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.draining == nil {
		m.draining = make(map[string]bool)
	}
	m.draining[hash] = true
	return nil
}

func (m *mockStore) ReleaseWorkload(hash string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed == nil {
		m.claimed = make(map[string]bool)
	}
	m.claimed[hash] = false
	delete(m.draining, hash)
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

type mockBlobs struct {
	midFlight func()
	removed   []string
}

func (m *mockBlobs) Put(io.Reader) (string, error)     { return "", nil }
func (m *mockBlobs) Get(string) (io.ReadCloser, error) { return nil, nil }
func (m *mockBlobs) Has(string) bool                   { return false }
func (m *mockBlobs) Remove(hash string) error          { m.removed = append(m.removed, hash); return nil }
func (m *mockBlobs) Fetch(_ context.Context, _ string, _ []types.PeerKey) error {
	if m.midFlight != nil {
		m.midFlight()
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

	r := newReconciler(local, ms, wm, &mockBlobs{midFlight: func() { ms.removeSpec(hash) }}, newBudget(0), testBackoff(t), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, []types.PeerKey{local})

	require.False(t, wm.seeded.Load(), "should not seed when spec was removed")
	require.False(t, ms.wasClaimed(hash), "should not set claim when spec was removed")
}

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

	r := newReconciler(local, ms, wm, &mockBlobs{}, newBudget(0), testBackoff(t), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.executeClaim(t.Context(), hash, []types.PeerKey{local})

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

	r := newReconciler(local, ms, &runningWorkloads{}, &mockBlobs{}, newBudget(0), testBackoff(t), zap.NewNop().Sugar(), &sync.WaitGroup{})
	r.claimStartTime[hash] = time.Now()
	r.pendingRelease[hash] = time.Now().Add(-time.Second)

	r.reconcile(context.Background())

	ms.mu.Lock()
	_, claimWasSet := ms.claimed[hash]
	ms.mu.Unlock()
	require.False(t, claimWasSet, "should not release during residency window")
}

func TestReconcile_SignalCoalesces(t *testing.T) {
	local := peerKey(1)
	ms := &mockStore{
		specs:    map[string]state.WorkloadSpecView{},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}

	r := newReconciler(local, ms, &mockWorkloads{}, &mockBlobs{}, newBudget(0), testBackoff(t), zap.NewNop().Sugar(), &sync.WaitGroup{})

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

	r := newReconciler(local, ms, &runningWorkloads{}, &mockBlobs{}, newBudget(0), testBackoff(t), zap.NewNop().Sugar(), &sync.WaitGroup{})
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
