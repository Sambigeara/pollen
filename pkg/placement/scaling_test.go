// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func defaultReplicaCountCfg() replicaCountConfig {
	return replicaCountConfig{
		tick:               time.Second,
		scaleUpThreshold:   0.5,
		scaleDownThreshold: 0.1,
		scaleDownGrace:     time.Minute,
	}
}

func newReplicaCountHarness(t *testing.T, self types.PeerKey, cfg replicaCountConfig, store *mockStore, calls *callTracker) (*replicaCountLoop, *mockClock) {
	t.Helper()
	clock := &mockClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := newBackoff(defaultBackoffCfg(), func(time.Duration) {})
	r := newReplicaCountLoop(self, cfg, store, calls, b)
	r.now = clock.Now
	return r, clock
}

func TestSaturationOf(t *testing.T) {
	a, b, c := peerKey(1), peerKey(2), peerKey(3)
	require.Equal(t, 0.0, saturationOf(nil, nil))
	require.Equal(t, 0.0, saturationOf([]types.PeerKey{a}, map[types.PeerKey]struct{}{}))
	require.Equal(t, 1.0, saturationOf([]types.PeerKey{a}, map[types.PeerKey]struct{}{a: {}}))
	require.InDelta(t, 0.5, saturationOf([]types.PeerKey{a, b}, map[types.PeerKey]struct{}{a: {}}), 0.001)
	require.InDelta(t, 2.0/3.0, saturationOf([]types.PeerKey{a, b, c}, map[types.PeerKey]struct{}{a: {}, b: {}}), 0.001)
}

func TestHeaviestUnservedSource_PicksMostCalls(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	sources := []sourceCount{
		{caller: a, count: 5},
		{caller: b, count: 10},
	}
	heaviest, ok := heaviestUnservedSource(sources, 0, self, nil, nil)
	require.True(t, ok)
	require.Equal(t, b, heaviest)
}

func TestHeaviestUnservedSource_ExcludesReplicas(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	sources := []sourceCount{
		{caller: a, count: 100},
		{caller: b, count: 5},
	}
	heaviest, ok := heaviestUnservedSource(sources, 0, self, []types.PeerKey{a}, nil)
	require.True(t, ok)
	require.Equal(t, b, heaviest)
}

func TestHeaviestUnservedSource_LexMinTiebreak(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	sources := []sourceCount{
		{caller: b, count: 5},
		{caller: a, count: 5},
	}
	heaviest, ok := heaviestUnservedSource(sources, 0, self, nil, nil)
	require.True(t, ok)
	require.Equal(t, a, heaviest)
}

func TestHeaviestUnservedSource_NoCandidates(t *testing.T) {
	self, a := peerKey(1), peerKey(2)
	sources := []sourceCount{{caller: a, count: 5}}
	_, ok := heaviestUnservedSource(sources, 0, self, []types.PeerKey{a}, nil)
	require.False(t, ok)
}

func TestHeaviestUnservedSource_ExcludesBackedOff(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	sources := []sourceCount{
		{caller: a, count: 100},
		{caller: b, count: 5},
	}
	backed := map[types.PeerKey]struct{}{a: {}}
	heaviest, ok := heaviestUnservedSource(sources, 0, self, nil, backed)
	require.True(t, ok)
	require.Equal(t, b, heaviest, "heavier-but-backed-off peer must be skipped")
}

func TestHeaviestUnservedSource_ExcludesSelfWhenBackedOff(t *testing.T) {
	self, a := peerKey(1), peerKey(2)
	sources := []sourceCount{{caller: a, count: 1}}
	backed := map[types.PeerKey]struct{}{self: {}}
	heaviest, ok := heaviestUnservedSource(sources, 100, self, nil, backed)
	require.True(t, ok)
	require.Equal(t, a, heaviest, "self in backoff must not elect itself even when heaviest")
}

func TestReplicaCountLoop_ScalesUpWhenSelfIsHeaviest(t *testing.T) {
	self, a := peerKey(1), peerKey(2)
	now := time.Now()
	saturatedA := state.NodeView{BackoffExpiry: now.Add(time.Hour)}
	store := &mockStore{
		specs:  map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x"}}},
		claims: map[string]map[types.PeerKey]struct{}{"seed-x": {a: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self: {},
			a:    saturatedA,
		},
		localID: self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})
	calls.RecordCall("seed-x")
	calls.RecordCall("seed-x")
	calls.RecordCall("seed-x")

	r, _ := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)
	r.now = func() time.Time { return now }

	r.tick()

	require.True(t, store.claimed["seed-x"])
}

func TestReplicaCountLoop_DoesNotScaleUpWhenSelfNotHeaviest(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	now := time.Now()
	saturatedA := state.NodeView{BackoffExpiry: now.Add(time.Hour)}
	bWithDemand := state.NodeView{CallCounts: map[string]uint64{"seed-x": 100}}
	store := &mockStore{
		specs:  map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x"}}},
		claims: map[string]map[types.PeerKey]struct{}{"seed-x": {a: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self: {},
			a:    saturatedA,
			b:    bWithDemand,
		},
		localID: self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})
	calls.RecordCall("seed-x")

	r, _ := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)
	r.now = func() time.Time { return now }

	r.tick()

	require.False(t, store.claimed["seed-x"])
}

func TestReplicaCountLoop_FillsFloorWhenZeroReplicas(t *testing.T) {
	self := peerKey(1)
	store := &mockStore{
		specs:    map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x", MinReplicas: 1}}},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{self},
		localID:  self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	r, _ := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)

	r.tick()

	require.True(t, store.claimed["seed-x"])
}

func TestReplicaCountLoop_FillsFloorWithLexMinElection(t *testing.T) {
	a, b, c := peerKey(1), peerKey(2), peerKey(3)
	mkStore := func() *mockStore {
		return &mockStore{
			specs:    map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x", MinReplicas: 2}}},
			claims:   map[string]map[types.PeerKey]struct{}{"seed-x": {a: {}}},
			allPeers: []types.PeerKey{a, b, c},
		}
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	storeB := mkStore()
	storeB.localID = b
	rB, _ := newReplicaCountHarness(t, b, defaultReplicaCountCfg(), storeB, calls)
	rB.tick()
	require.True(t, storeB.claimed["seed-x"], "lex-min non-replica (b) should claim")

	storeC := mkStore()
	storeC.localID = c
	rC, _ := newReplicaCountHarness(t, c, defaultReplicaCountCfg(), storeC, calls)
	rC.tick()
	require.False(t, storeC.claimed["seed-x"], "non-lex-min peer must not claim")
}

func TestReplicaCountLoop_FloorSkipsBackedOffPeers(t *testing.T) {
	a, b, c := peerKey(1), peerKey(2), peerKey(3)
	now := time.Now()
	mkStore := func() *mockStore {
		return &mockStore{
			specs:    map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x", MinReplicas: 2}}},
			claims:   map[string]map[types.PeerKey]struct{}{"seed-x": {a: {}}},
			allPeers: []types.PeerKey{a, b, c},
			nodes:    map[types.PeerKey]state.NodeView{b: {BackoffExpiry: now.Add(time.Hour)}},
		}
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	storeB := mkStore()
	storeB.localID = b
	rB, _ := newReplicaCountHarness(t, b, defaultReplicaCountCfg(), storeB, calls)
	rB.now = func() time.Time { return now }
	rB.tick()
	require.False(t, storeB.claimed["seed-x"], "backed-off lex-min peer must not claim")

	storeC := mkStore()
	storeC.localID = c
	rC, _ := newReplicaCountHarness(t, c, defaultReplicaCountCfg(), storeC, calls)
	rC.now = func() time.Time { return now }
	rC.tick()
	require.True(t, storeC.claimed["seed-x"], "next-lex-min peer claims when lex-min is backed off")
}

func TestReplicaCountLoop_FloorSkipsLocallyOverloadedSelf(t *testing.T) {
	self, b := peerKey(1), peerKey(2)
	store := &mockStore{
		specs:    map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x", MinReplicas: 1}}},
		claims:   map[string]map[types.PeerKey]struct{}{},
		allPeers: []types.PeerKey{self, b},
		localID:  self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})
	r, _ := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)

	r.backoff.SignalRefusal()
	require.True(t, r.backoff.IsLocallyOverloaded())

	r.tick()
	require.False(t, store.claimed["seed-x"], "self must not claim while locally overloaded, even before gossip rolls in")
}

func TestReplicaCountLoop_DoesNotScaleBelowFloor(t *testing.T) {
	self, b := peerKey(1), peerKey(2)
	store := &mockStore{
		specs:    map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x", MinReplicas: 2}}},
		claims:   map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}, b: {}}},
		allPeers: []types.PeerKey{self, b},
		localID:  self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	r, clock := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)

	r.tick()
	clock.advance(2 * time.Minute)
	r.tick()

	require.False(t, store.draining["seed-x"], "scale-down must respect MinReplicas floor")
}

func TestReplicaCountLoop_ScalesDownAfterGrace(t *testing.T) {
	self, b := peerKey(1), peerKey(2)
	store := &mockStore{
		specs:   map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x"}}},
		claims:  map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}, b: {}}},
		localID: self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	r, clock := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)

	r.tick()
	require.False(t, store.draining["seed-x"])

	clock.advance(2 * time.Minute)
	r.tick()

	require.True(t, store.draining["seed-x"])
}

func TestReplicaCountLoop_DoesNotScaleDownWithinGrace(t *testing.T) {
	self, b := peerKey(1), peerKey(2)
	store := &mockStore{
		specs:   map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x"}}},
		claims:  map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}, b: {}}},
		localID: self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	r, clock := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)

	r.tick()
	clock.advance(30 * time.Second)
	r.tick()

	require.False(t, store.draining["seed-x"])
}

func TestReplicaCountLoop_DoesNotScaleDownWhenNotLexMin(t *testing.T) {
	self, b := peerKey(2), peerKey(1)
	store := &mockStore{
		specs:   map[string]state.WorkloadSpecView{"seed-x": {Spec: state.WorkloadSpec{Hash: "seed-x"}}},
		claims:  map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}, b: {}}},
		localID: self,
	}
	calls := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	r, clock := newReplicaCountHarness(t, self, defaultReplicaCountCfg(), store, calls)

	r.tick()
	clock.advance(2 * time.Minute)
	r.tick()

	require.False(t, store.draining["seed-x"])
}
