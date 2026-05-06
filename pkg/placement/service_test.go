// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

const (
	tailHashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tailHashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func newServiceForUnseedTests(localID types.PeerKey, store WorkloadState) (*Service, *mockBlobs) {
	blobs := &mockBlobs{}
	return New(localID, store, blobs, nil), blobs
}

func TestService_Unseed_WhenNotLocallyClaimed(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload-xyz"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "foo", MinReplicas: 1},
				Publisher: local,
			},
		},
		claims: map[string]map[types.PeerKey]struct{}{hash: {peer2: {}}},
	}
	s, _ := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed(hash))

	store.mu.Lock()
	_, specStillThere := store.specs[hash]
	store.mu.Unlock()
	require.False(t, specStillThere, "spec should be deleted so claimants release on reconcile")
}

func TestService_Unseed_EvictsWasmBlob(t *testing.T) {
	local := peerKey(1)
	hash := "workload-xyz"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "foo", MinReplicas: 1},
				Publisher: local,
			},
		},
	}
	s, blobs := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed(hash))
	require.Equal(t, []string{hash}, blobs.removed)
}

func TestService_Unseed_RejectsNonOwner(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	hash := "workload-xyz"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			hash: {
				Spec:      state.WorkloadSpec{Hash: hash, Name: "foo", MinReplicas: 1},
				Publisher: peer2,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)

	err := s.Unseed("foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), peer2.Short(), "error should point the operator at the publisher")
	require.NotContains(t, err.Error(), ErrNotRunning.Error(), "must not fall through to the generic not-running branch")
}

func TestService_Unseed_LocalNameWins(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	localHash := "local-hash"
	remoteHash := "remote-hash"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			localHash: {
				Spec:      state.WorkloadSpec{Hash: localHash, Name: "foo", MinReplicas: 1},
				Publisher: local,
			},
			remoteHash: {
				Spec:      state.WorkloadSpec{Hash: remoteHash, Name: "foo", MinReplicas: 1},
				Publisher: peer2,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)

	require.NoError(t, s.Unseed("foo"))

	store.mu.Lock()
	_, localStillThere := store.specs[localHash]
	_, remoteStillThere := store.specs[remoteHash]
	store.mu.Unlock()
	require.False(t, localStillThere, "local spec should be deleted")
	require.True(t, remoteStillThere, "remote spec must remain untouched")
}

func TestService_Unseed_UnknownHash(t *testing.T) {
	s, _ := newServiceForUnseedTests(peerKey(1), &mockStore{})

	require.ErrorIs(t, s.Unseed("deadbeef"), ErrNotRunning)
}

func TestService_Call_UnknownTarget(t *testing.T) {
	s, _ := newServiceForUnseedTests(peerKey(1), &mockStore{})

	_, err := s.Call(context.Background(), "no-such-name", "handle", nil)
	require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	require.False(t, errors.Is(err, ErrNotRunning), "must not collapse into ErrNotRunning")
}

func TestService_Call_KnownSpecNoClaimants(t *testing.T) {
	const (
		seedName = "sink"
		seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	)
	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: seedName, MinReplicas: 1},
				Publisher: peerKey(2),
			},
		},
	}
	s, _ := newServiceForUnseedTests(peerKey(1), store)

	_, err := s.Call(context.Background(), seedName, "handle", nil)
	require.ErrorIs(t, err, ErrNotRunning)
	require.False(t, errors.Is(err, wasm.ErrTargetNotFound), "resolved spec must not surface as not-found")
}

func TestService_Call_FollowsTailCallMarker(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", []byte("next")),
		tailHashB: []byte("done"),
	}}
	s := newTailCallService(local, rt)

	out, err := s.Call(context.Background(), "alpha", "start", []byte("first"))

	require.NoError(t, err)
	require.Equal(t, []byte("done"), out)
	require.Equal(t, []tailRuntimeCall{
		{hash: tailHashA, function: "start", input: []byte("first")},
		{hash: tailHashB, function: "run", input: []byte("next")},
	}, rt.callsSnapshot())
	require.Equal(t, uint64(1), s.calls.localCount(tailHashA))
	require.Equal(t, uint64(1), s.calls.localCount(tailHashB))
}

func TestService_Call_ReleasesTailCallerAdmissionBeforeNextHop(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", nil),
		tailHashB: []byte("done"),
	}}
	s := newTailCallService(local, rt)
	s.budget = newBudget(1)
	s.budget.caps[tailHashA] = 1
	s.budget.caps[tailHashB] = 1

	out, err := s.Call(context.Background(), "alpha", "start", nil)

	require.NoError(t, err)
	require.Equal(t, []byte("done"), out)
	require.Zero(t, s.budget.reserved.Load())
}

func TestService_Call_DetectsTailCallCycle(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", nil),
		tailHashB: tailCallMarker(t, "pln://seed/alpha/start", nil),
	}}
	s := newTailCallService(local, rt)

	_, err := s.Call(context.Background(), "alpha", "start", nil)

	require.ErrorIs(t, err, ErrCycle)
	require.Equal(t, []tailRuntimeCall{
		{hash: tailHashA, function: "start"},
		{hash: tailHashB, function: "run"},
	}, rt.callsSnapshot())
}

func TestService_Call_RejectsNonSeedTailCall(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://service/sink", nil),
	}}
	s := newTailCallService(local, rt)

	_, err := s.Call(context.Background(), "alpha", "start", nil)

	require.ErrorIs(t, err, ErrWorkloadFailed)
	require.False(t, errors.Is(err, wasm.ErrTargetNotFound))
}

func TestService_Call_RejectsMalformedTailCallMarker(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: []byte(`{"kind":"tail_call","uri":"pln://seed/beta/run","input":"not base64"}`),
	}}
	s := newTailCallService(local, rt)

	_, err := s.Call(context.Background(), "alpha", "start", nil)

	require.ErrorIs(t, err, ErrWorkloadFailed)
}

func TestService_Call_TailCallMissingTargetIsWorkloadFailure(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/missing/run", nil),
	}}
	s := newTailCallService(local, rt)

	_, err := s.Call(context.Background(), "alpha", "start", nil)

	require.ErrorIs(t, err, ErrWorkloadFailed)
	require.False(t, errors.Is(err, wasm.ErrTargetNotFound))
}

func TestService_Call_TailCallUnclaimedTargetIsWorkloadFailure(t *testing.T) {
	local := peerKey(1)
	rt := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", nil),
	}}
	store := tailCallStore(local, map[string]map[types.PeerKey]struct{}{
		tailHashA: {local: {}},
	})
	s := New(local, store, &mockBlobs{}, rt)
	s.budget = newBudget(0)
	registerWorkloads(s, tailHashA)

	_, err := s.Call(context.Background(), "alpha", "start", nil)

	require.ErrorIs(t, err, ErrWorkloadFailed)
	require.False(t, errors.Is(err, ErrNotRunning))
}

func TestService_Call_RemoteServeContinuesTailCallOnServingNode(t *testing.T) {
	ingress := peerKey(1)
	mid := peerKey(2)
	leaf := peerKey(3)

	ingressMesh := newRoutingWorkloadMesh(t, ingress)
	midMesh := newRoutingWorkloadMesh(t, mid)
	leafMesh := newRoutingWorkloadMesh(t, leaf)

	ingressRT := &tailCallRuntime{}
	midRT := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", []byte("next")),
	}}
	leafRT := &tailCallRuntime{outputs: map[string][]byte{
		tailHashB: []byte("done"),
	}}

	claims := map[string]map[types.PeerKey]struct{}{
		tailHashA: {mid: {}},
		tailHashB: {leaf: {}},
	}
	ingressSvc := New(ingress, tailCallStore(ingress, claims), &mockBlobs{}, ingressRT, WithMesh(ingressMesh))
	midSvc := New(mid, tailCallStore(mid, claims), &mockBlobs{}, midRT, WithMesh(midMesh))
	leafSvc := New(leaf, tailCallStore(leaf, claims), &mockBlobs{}, leafRT, WithMesh(leafMesh))
	midSvc.ctx = context.Background()
	leafSvc.ctx = context.Background()
	registerWorkloads(midSvc, tailHashA)
	registerWorkloads(leafSvc, tailHashB)
	ingressMesh.route(mid, midSvc)
	ingressMesh.route(leaf, leafSvc)
	midMesh.route(leaf, leafSvc)

	out, err := ingressSvc.Call(context.Background(), "alpha", "start", []byte("first"))

	require.NoError(t, err)
	require.Equal(t, []byte("done"), out)
	require.Equal(t, []tailRuntimeCall{{hash: tailHashA, function: "start", input: []byte("first")}}, midRT.callsSnapshot())
	require.Equal(t, []tailRuntimeCall{{hash: tailHashB, function: "run", input: []byte("next")}}, leafRT.callsSnapshot())
	require.Equal(t, 1, ingressMesh.openCount(mid))
	require.Zero(t, ingressMesh.openCount(leaf), "ingress must not bounce the tail marker back through its own loop")
	require.Equal(t, 1, midMesh.openCount(leaf))
}

func TestService_Call_RemoteServeReleasesAdmissionBeforeLocalTailHop(t *testing.T) {
	ingress := peerKey(1)
	remote := peerKey(2)
	ingressMesh := newRoutingWorkloadMesh(t, ingress)

	remoteRT := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", nil),
		tailHashB: []byte("done"),
	}}
	claims := map[string]map[types.PeerKey]struct{}{
		tailHashA: {remote: {}},
		tailHashB: {remote: {}},
	}
	ingressSvc := New(ingress, tailCallStore(ingress, claims), &mockBlobs{}, &tailCallRuntime{}, WithMesh(ingressMesh))
	remoteSvc := New(remote, tailCallStore(remote, claims), &mockBlobs{}, remoteRT)
	remoteSvc.ctx = context.Background()
	remoteSvc.budget = newBudget(1)
	remoteSvc.budget.caps[tailHashA] = 1
	remoteSvc.budget.caps[tailHashB] = 1
	registerWorkloads(remoteSvc, tailHashA, tailHashB)
	ingressMesh.route(remote, remoteSvc)

	out, err := ingressSvc.Call(context.Background(), "alpha", "start", nil)

	require.NoError(t, err)
	require.Equal(t, []byte("done"), out)
	require.Equal(t, []tailRuntimeCall{{hash: tailHashA, function: "start"}, {hash: tailHashB, function: "run"}}, remoteRT.callsSnapshot())
	require.Zero(t, remoteSvc.budget.reserved.Load())
	require.Equal(t, 1, ingressMesh.openCount(remote))
}

func TestService_Call_RemoteContinuationDetectsCycleAcrossForwardedTailHop(t *testing.T) {
	ingress := peerKey(1)
	mid := peerKey(2)
	leaf := peerKey(3)

	ingressMesh := newRoutingWorkloadMesh(t, ingress)
	midMesh := newRoutingWorkloadMesh(t, mid)
	leafMesh := newRoutingWorkloadMesh(t, leaf)

	midRT := &tailCallRuntime{outputs: map[string][]byte{
		tailHashA: tailCallMarker(t, "pln://seed/beta/run", nil),
	}}
	leafRT := &tailCallRuntime{outputs: map[string][]byte{
		tailHashB: tailCallMarker(t, "pln://seed/alpha/start", nil),
	}}
	claims := map[string]map[types.PeerKey]struct{}{
		tailHashA: {mid: {}},
		tailHashB: {leaf: {}},
	}
	ingressSvc := New(ingress, tailCallStore(ingress, claims), &mockBlobs{}, &tailCallRuntime{}, WithMesh(ingressMesh))
	midSvc := New(mid, tailCallStore(mid, claims), &mockBlobs{}, midRT, WithMesh(midMesh))
	leafSvc := New(leaf, tailCallStore(leaf, claims), &mockBlobs{}, leafRT, WithMesh(leafMesh))
	registerWorkloads(midSvc, tailHashA)
	registerWorkloads(leafSvc, tailHashB)
	ingressMesh.route(mid, midSvc)
	ingressMesh.route(leaf, leafSvc)
	midMesh.route(leaf, leafSvc)
	leafMesh.route(mid, midSvc)

	_, err := ingressSvc.Call(context.Background(), "alpha", "start", nil)

	require.ErrorIs(t, err, ErrCycle)
	require.Equal(t, 1, ingressMesh.openCount(mid))
	require.Zero(t, ingressMesh.openCount(leaf))
	require.Equal(t, 1, midMesh.openCount(leaf))
	require.Zero(t, leafMesh.openCount(mid), "leaf should detect the upstream alpha hop before forwarding back to it")
}

func TestService_callLocal_RefusedWhenBudgetExhausted(t *testing.T) {
	local := peerKey(1)
	const seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: "sink", MinReplicas: 1},
				Publisher: local,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)

	const totalBytes = int64(1 << 20)
	s.budget = newBudget(totalBytes)
	require.True(t, s.budget.Reserve("filler", totalBytes))

	_, err := s.callLocal(context.Background(), seedHash, "handle", nil)

	var ovl *OverloadError
	require.ErrorAs(t, err, &ovl)
	require.ErrorIs(t, err, ErrOverloaded)
	require.Equal(t, "node memory budget exhausted", ovl.Reason)
	require.True(t, s.backoff.IsLocallyOverloaded(), "refusal must arm backoff so peers' dispatchers divert future calls")
}

func TestService_callLocal_ReleasesAdmissionOnReturn(t *testing.T) {
	local := peerKey(1)
	const seedHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	store := &mockStore{
		specs: map[string]state.WorkloadSpecView{
			seedHash: {
				Spec:      state.WorkloadSpec{Hash: seedHash, Name: "sink", MinReplicas: 1},
				Publisher: local,
			},
		},
	}
	s, _ := newServiceForUnseedTests(local, store)
	s.budget = newBudget(replicaMemoryBytes(0) * 2)

	_, err := s.callLocal(context.Background(), seedHash, "handle", nil)
	require.ErrorIs(t, err, ErrNotRunning)
	require.Zero(t, s.budget.reserved.Load(), "release must run regardless of how the call exits")
}

type tailRuntimeCall struct {
	hash     string
	function string
	input    []byte
}

type tailCallRuntime struct {
	mu      sync.Mutex
	outputs map[string][]byte
	errs    map[string]error
	calls   []tailRuntimeCall
}

func (r *tailCallRuntime) Compile(context.Context, []byte, string, wasm.PluginConfig) error {
	return nil
}

func (r *tailCallRuntime) Call(_ context.Context, hash, function string, input []byte) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, tailRuntimeCall{hash: hash, function: function, input: append([]byte(nil), input...)})
	if err := r.errs[hash]; err != nil {
		return nil, err
	}
	return append([]byte(nil), r.outputs[hash]...), nil
}

func (r *tailCallRuntime) DropCompiled(context.Context, string) {}

func (r *tailCallRuntime) callsSnapshot() []tailRuntimeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tailRuntimeCall(nil), r.calls...)
}

func tailCallMarker(t *testing.T, uri string, input []byte) []byte {
	t.Helper()
	marker, err := json.Marshal(struct {
		Kind  string `json:"kind"`
		URI   string `json:"uri"`
		Input []byte `json:"input,omitempty"`
	}{
		Kind:  "tail_call",
		URI:   uri,
		Input: input,
	})
	require.NoError(t, err)
	return marker
}

func newTailCallService(local types.PeerKey, rt *tailCallRuntime) *Service {
	s := New(local, tailCallStore(local, map[string]map[types.PeerKey]struct{}{
		tailHashA: {local: {}},
		tailHashB: {local: {}},
	}), &mockBlobs{}, rt)
	s.budget = newBudget(0)
	registerWorkloads(s, tailHashA, tailHashB)
	return s
}

func tailCallStore(local types.PeerKey, claims map[string]map[types.PeerKey]struct{}) *mockStore {
	return &mockStore{
		specs: map[string]state.WorkloadSpecView{
			tailHashA: {Spec: state.WorkloadSpec{Hash: tailHashA, Name: "alpha", MinReplicas: 1}, Publisher: local},
			tailHashB: {Spec: state.WorkloadSpec{Hash: tailHashB, Name: "beta", MinReplicas: 1}, Publisher: local},
		},
		claims:   claims,
		allPeers: []types.PeerKey{local},
		localID:  local,
	}
}

func registerWorkloads(s *Service, hashes ...string) {
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()
	for _, hash := range hashes {
		s.manager.workloads[hash] = &entry{}
	}
}

type routingWorkloadMesh struct {
	t      *testing.T
	self   types.PeerKey
	routes map[types.PeerKey]*Service
	opens  map[types.PeerKey]int
	mu     sync.Mutex
}

func newRoutingWorkloadMesh(t *testing.T, self types.PeerKey) *routingWorkloadMesh {
	t.Helper()
	return &routingWorkloadMesh{
		t:      t,
		self:   self,
		routes: make(map[types.PeerKey]*Service),
		opens:  make(map[types.PeerKey]int),
	}
}

func (m *routingWorkloadMesh) route(peer types.PeerKey, svc *Service) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes[peer] = svc
}

func (m *routingWorkloadMesh) openCount(peer types.PeerKey) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opens[peer]
}

func (m *routingWorkloadMesh) OpenStream(_ context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	require.Equal(m.t, transport.StreamTypeWorkload, st)
	m.mu.Lock()
	remote := m.routes[peer]
	m.opens[peer]++
	m.mu.Unlock()
	if remote == nil {
		return nil, fmt.Errorf("no route to %s", peer.Short())
	}
	server, client := net.Pipe()
	go remote.Serve(server, m.self)
	return client, nil
}
