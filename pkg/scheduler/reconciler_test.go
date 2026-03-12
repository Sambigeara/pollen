package scheduler

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- mock store ---

type mockStore struct {
	mu       sync.Mutex
	specs    map[string]store.WorkloadSpecView
	claims   map[string]map[types.PeerKey]struct{}
	allPeers []types.PeerKey
	claimed  map[string]bool // track SetLocalWorkloadClaim calls
}

func (m *mockStore) AllWorkloadSpecs() map[string]store.WorkloadSpecView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]store.WorkloadSpecView, len(m.specs))
	maps.Copy(out, m.specs)
	return out
}

func (m *mockStore) AllWorkloadClaims() map[string]map[types.PeerKey]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]map[types.PeerKey]struct{}, len(m.claims))
	for k, v := range m.claims {
		inner := make(map[types.PeerKey]struct{}, len(v))
		for pk := range v {
			inner[pk] = struct{}{}
		}
		out[k] = inner
	}
	return out
}

func (m *mockStore) AllPeerKeys() []types.PeerKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]types.PeerKey(nil), m.allPeers...)
}

func (m *mockStore) SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed == nil {
		m.claimed = make(map[string]bool)
	}
	m.claimed[hash] = claimed
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

// --- mock workload manager ---

type mockWorkloads struct {
	seeded atomic.Bool
}

func (m *mockWorkloads) SeedFromCAS(string) error { m.seeded.Store(true); return nil }
func (m *mockWorkloads) Unseed(string) error      { return nil }
func (m *mockWorkloads) IsRunning(string) bool    { return false }

// --- mock CAS ---

type mockCAS struct{}

func (m *mockCAS) Has(string) bool { return false }

// --- slow fetcher that calls a hook mid-flight ---

type slowFetcher struct {
	midFlight func() // called after "fetch starts" but before returning
}

func (f *slowFetcher) Fetch(_ context.Context, _ string, _ []types.PeerKey) error {
	if f.midFlight != nil {
		f.midFlight()
	}
	return nil
}

// TestExecuteClaim_SpecRemovedDuringFetch verifies that if a spec disappears
// while an artifact fetch is in flight, no claim is published.
func TestExecuteClaim_SpecRemovedDuringFetch(t *testing.T) {
	local := peerKey(1)
	hash := "workload1"

	ms := &mockStore{
		specs: map[string]store.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{local},
	}
	wm := &mockWorkloads{}

	fetcher := &slowFetcher{
		midFlight: func() {
			// Simulate spec removal during fetch.
			ms.removeSpec(hash)
		},
	}

	var published atomic.Bool
	r := &Reconciler{
		localID:   local,
		store:     ms,
		workloads: wm,
		cas:       &mockCAS{},
		fetcher:   fetcher,
		publish:   func(_ []*statev1.GossipEvent) { published.Store(true) },
		log:       zap.NewNop().Sugar(),
	}

	r.executeClaim(t.Context(), hash, []types.PeerKey{local})

	require.False(t, wm.seeded.Load(), "should not seed when spec was removed")
	require.False(t, published.Load(), "should not publish claim when spec was removed")
	require.False(t, ms.wasClaimed(hash), "should not set claim when spec was removed")
}

// TestExecuteClaim_NoLongerWinnerAfterFetch verifies that if another peer
// claims the only replica slot while fetch is in flight, the local node's
// post-fetch recheck prevents a stale claim.
func TestExecuteClaim_NoLongerWinnerAfterFetch(t *testing.T) {
	// peerKey(1) has the lowest placement score for "workload2" among these
	// three peers, so it's the winner and would normally claim.
	local := peerKey(1)
	peer2 := peerKey(2)
	peer3 := peerKey(3)
	hash := "workload2"

	// Verify precondition: local is the winner for replicas=1.
	allPeers := []types.PeerKey{local, peer2, peer3}
	actions := Evaluate(local, allPeers, map[string]Spec{hash: {Replicas: 1}}, nil, func(string) bool { return false })
	var localWouldClaim bool
	for _, a := range actions {
		if a.Hash == hash && a.Kind == ActionClaim {
			localWouldClaim = true
		}
	}
	require.True(t, localWouldClaim, "precondition: local must be the winner for this hash")

	ms := &mockStore{
		specs: map[string]store.WorkloadSpecView{
			hash: {
				Spec:      &statev1.WorkloadSpecChange{Hash: hash, Replicas: 1},
				Publisher: local,
			},
		},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: allPeers,
	}

	wm := &mockWorkloads{}
	fetcher := &slowFetcher{
		midFlight: func() {
			// Simulate peer2 claiming the only replica slot while fetch
			// is in flight. This saturates the slot (needed = 1-1 = 0),
			// so shouldClaim returns false for local on the post-fetch recheck.
			ms.addClaim(hash, peer2)
		},
	}

	var published atomic.Bool
	r := &Reconciler{
		localID:   local,
		store:     ms,
		workloads: wm,
		cas:       &mockCAS{},
		fetcher:   fetcher,
		publish:   func(_ []*statev1.GossipEvent) { published.Store(true) },
		log:       zap.NewNop().Sugar(),
	}

	// executeClaim is synchronous in this test — assertions are immediate.
	r.executeClaim(t.Context(), hash, []types.PeerKey{local})

	require.False(t, published.Load(), "should not publish claim when no longer a winner")
	require.False(t, ms.wasClaimed(hash), "should not set claim when no longer a winner")
}
