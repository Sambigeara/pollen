package state

import (
	"fmt"
	"slices"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// convergenceHarness manages multiple stores and provides gossip/sync
// operations for testing CRDT convergence properties.
type convergenceHarness struct {
	t      *testing.T
	stores []*store
	keys   []types.PeerKey
}

func newConvergenceHarness(t *testing.T, n int) *convergenceHarness {
	t.Helper()
	h := &convergenceHarness{t: t, stores: make([]*store, n), keys: make([]types.PeerKey, n)}
	for i := range n {
		h.keys[i] = genKey(t)
		h.stores[i] = newTestStore(h.keys[i])
	}
	return h
}

func newDeterministicHarness(t *testing.T, n int) *convergenceHarness {
	t.Helper()
	h := &convergenceHarness{t: t, stores: make([]*store, n), keys: make([]types.PeerKey, n)}
	for i := range n {
		h.keys[i], _ = peerKey(byte(i + 1))
		h.stores[i] = newTestStore(h.keys[i])
	}
	return h
}

func (h *convergenceHarness) gossipOneway(from, to int) {
	h.t.Helper()
	full := h.stores[from].EncodeFull()
	_, _, err := h.stores[to].ApplyDelta(h.keys[from], full)
	require.NoError(h.t, err)
}

// gossipFullMesh syncs every pair of stores until all digests stabilize.
func (h *convergenceHarness) gossipFullMesh() {
	h.t.Helper()
	for round := range 10 {
		before := h.captureDigests()
		for i := range h.stores {
			for j := range h.stores {
				if i != j {
					h.gossipOneway(i, j)
				}
			}
		}
		after := h.captureDigests()
		if digestsStable(before, after) {
			return
		}
		if round == 9 {
			h.t.Fatal("gossipFullMesh did not stabilize after 10 rounds")
		}
	}
}

func (h *convergenceHarness) captureDigests() []map[string]*statev1.PeerDigest {
	out := make([]map[string]*statev1.PeerDigest, len(h.stores))
	for i, s := range h.stores {
		out[i] = s.testDigest().GetPeers()
	}
	return out
}

// assertConverged verifies that all stores agree on the CRDT log hash for
// every peer they both know about.
func (h *convergenceHarness) assertConverged() {
	h.t.Helper()

	allPeers := make(map[string]struct{})
	digests := make([]*statev1.Digest, len(h.stores))
	for i, s := range h.stores {
		digests[i] = s.testDigest()
		for pk := range digests[i].GetPeers() {
			allPeers[pk] = struct{}{}
		}
	}

	for pk := range allPeers {
		var refCounter uint64
		var refHash uint64
		refIdx := -1
		for i, d := range digests {
			pd := d.GetPeers()[pk]
			if pd == nil {
				continue
			}
			if refIdx < 0 {
				refCounter = pd.GetMaxCounter()
				refHash = pd.GetStateHash()
				refIdx = i
				continue
			}
			require.Equal(h.t, refCounter, pd.GetMaxCounter(),
				"store %d and store %d disagree on maxCounter for peer %s", refIdx, i, pk)
			require.Equal(h.t, refHash, pd.GetStateHash(),
				"store %d and store %d disagree on stateHash for peer %s", refIdx, i, pk)
		}
	}
}

func (h *convergenceHarness) establishFullReachability() {
	for i := range h.stores {
		for j := range h.stores {
			if i != j {
				h.stores[i].setLocalConnected(h.keys[j], true)
			}
		}
	}
}

func (h *convergenceHarness) sortByKey() {
	indices := make([]int, len(h.stores))
	for i := range indices {
		indices[i] = i
	}
	slices.SortFunc(indices, func(a, b int) int {
		return h.keys[a].Compare(h.keys[b])
	})
	stores := make([]*store, len(h.stores))
	keys := make([]types.PeerKey, len(h.keys))
	for i, idx := range indices {
		stores[i] = h.stores[idx]
		keys[i] = h.keys[idx]
	}
	h.stores = stores
	h.keys = keys
}

func digestsStable(a, b []map[string]*statev1.PeerDigest) bool {
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for k, da := range a[i] {
			db, ok := b[i][k]
			if !ok {
				return false
			}
			if da.GetMaxCounter() != db.GetMaxCounter() || da.GetStateHash() != db.GetStateHash() {
				return false
			}
		}
	}
	return true
}

// --- Deterministic convergence tests ---

func TestConvergence_ThreeStoreFullMesh(t *testing.T) {
	h := newConvergenceHarness(t, 3)

	h.stores[0].setLocalNetwork([]string{"10.0.0.1"}, 9000)
	h.stores[0].upsertLocalService(8080, "web")

	h.stores[1].setLocalVivaldiCoord(coords.Coord{X: 1, Y: 2, Height: coords.MinHeight}, 0.5)
	h.stores[1].setLocalNatType(nat.Easy)

	h.stores[2].setLocalWorkloadSpec("hash1", 2, 0, 0) //nolint:errcheck
	h.stores[2].setLocalWorkloadClaim("hash1", true)

	h.establishFullReachability()
	h.gossipFullMesh()
	h.assertConverged()

	for i, s := range h.stores {
		snap := s.Snapshot()
		peerA := snap.Nodes[h.keys[0]]
		require.Equal(t, []string{"10.0.0.1"}, peerA.IPs, "store %d", i)
		require.Contains(t, peerA.Services, "web", "store %d", i)

		peerB := snap.Nodes[h.keys[1]]
		require.NotNil(t, peerB.VivaldiCoord, "store %d", i)
		require.Equal(t, nat.Easy, peerB.NatType, "store %d", i)
	}
}

func TestConvergence_EventReordering(t *testing.T) {
	h := newConvergenceHarness(t, 4) // source + 3 receivers

	h.stores[0].setLocalNetwork([]string{"10.0.0.1", "10.0.0.2"}, 9000)
	h.stores[0].upsertLocalService(8080, "web")
	h.stores[0].upsertLocalService(9090, "api")
	h.stores[0].setObservedAddress("1.2.3.4", 45000)
	h.stores[0].setLocalNatType(nat.Easy)
	h.stores[0].setLocalVivaldiCoord(coords.Coord{X: 1, Y: 2, Height: coords.MinHeight}, 0.5)

	full := h.stores[0].EncodeFull()
	var batch statev1.GossipEventBatch
	require.NoError(t, batch.UnmarshalVT(full))
	events := batch.GetEvents()
	require.NotEmpty(t, events)

	// Forward order.
	for _, ev := range events {
		h.stores[1].applyEvent(ev)
	}

	// Reverse order.
	for i := len(events) - 1; i >= 0; i-- {
		h.stores[2].applyEvent(events[i])
	}

	// Interleaved: odds first, then evens.
	for i := 1; i < len(events); i += 2 {
		h.stores[3].applyEvent(events[i])
	}
	for i := 0; i < len(events); i += 2 {
		h.stores[3].applyEvent(events[i])
	}

	peerAStr := h.keys[0].String()
	ref := h.stores[1].testDigest().GetPeers()[peerAStr]
	require.NotNil(t, ref)
	for i := 2; i <= 3; i++ {
		got := h.stores[i].testDigest().GetPeers()[peerAStr]
		require.NotNil(t, got, "store %d missing peer A", i)
		require.Equal(t, ref.GetMaxCounter(), got.GetMaxCounter(), "store 1 vs store %d maxCounter", i)
		require.Equal(t, ref.GetStateHash(), got.GetStateHash(), "store 1 vs store %d stateHash", i)
	}
}

func TestConvergence_Idempotence(t *testing.T) {
	h := newConvergenceHarness(t, 2)

	h.stores[0].setLocalNetwork([]string{"10.0.0.1"}, 9000)
	h.stores[0].upsertLocalService(8080, "web")
	h.stores[0].setObservedAddress("1.2.3.4", 45000)

	full := h.stores[0].EncodeFull()

	_, _, err := h.stores[1].ApplyDelta(h.keys[0], full)
	require.NoError(t, err)
	d1 := h.stores[1].testDigest()

	_, _, err = h.stores[1].ApplyDelta(h.keys[0], full)
	require.NoError(t, err)
	d2 := h.stores[1].testDigest()

	for pk, pd1 := range d1.GetPeers() {
		pd2 := d2.GetPeers()[pk]
		require.NotNil(t, pd2, "missing peer %s after second apply", pk)
		require.Equal(t, pd1.GetMaxCounter(), pd2.GetMaxCounter(), "peer %s", pk)
		require.Equal(t, pd1.GetStateHash(), pd2.GetStateHash(), "peer %s", pk)
	}
}

func TestConvergence_SelfConflictRecovery(t *testing.T) {
	h := newConvergenceHarness(t, 2)

	h.stores[0].setLocalNetwork([]string{"10.0.0.1"}, 9000)
	h.stores[0].upsertLocalService(8080, "web")

	aDigest := h.stores[0].testDigest()
	aMax := aDigest.GetPeers()[h.keys[0].String()].GetMaxCounter()

	// Synthetic gossip about A with counters beyond its maxCounter.
	staleEvents := []*statev1.GossipEvent{{
		PeerId:  h.keys[0].String(),
		Counter: aMax + 10,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{
			Ips:       []string{"STALE"},
			LocalPort: 1234,
		}},
	}}

	result := h.stores[0].applyEvents(staleEvents)
	require.NotEmpty(t, result.rebroadcast, "expected rebroadcast after self-conflict")

	aMaxAfter := h.stores[0].testDigest().GetPeers()[h.keys[0].String()].GetMaxCounter()
	require.Greater(t, aMaxAfter, aMax+10)

	snapA := h.stores[0].Snapshot()
	require.Equal(t, []string{"10.0.0.1"}, snapA.Nodes[h.keys[0]].IPs)
	require.Equal(t, uint32(9000), snapA.Nodes[h.keys[0]].LocalPort)

	h.gossipOneway(0, 1)
	peerA := h.stores[1].Snapshot().Nodes[h.keys[0]]
	require.Equal(t, []string{"10.0.0.1"}, peerA.IPs)
	require.Equal(t, uint32(9000), peerA.LocalPort)
}

func TestConvergence_PartitionHealing(t *testing.T) {
	h := newConvergenceHarness(t, 3)
	h.establishFullReachability()
	h.gossipFullMesh()
	h.assertConverged()

	// Partition: A<->B communicate, C is isolated.
	h.stores[0].upsertLocalService(8080, "web")
	h.stores[1].setLocalNatType(nat.Easy)
	h.gossipOneway(0, 1)
	h.gossipOneway(1, 0)

	// C evolves independently.
	h.stores[2].upsertLocalService(9090, "api")

	// Heal partition.
	h.gossipFullMesh()
	h.assertConverged()

	for i, s := range h.stores {
		snap := s.Snapshot()
		require.Contains(t, snap.Nodes[h.keys[0]].Services, "web", "store %d missing A's web", i)
		require.Contains(t, snap.Nodes[h.keys[2]].Services, "api", "store %d missing C's api", i)
		require.Equal(t, nat.Easy, snap.Nodes[h.keys[1]].NatType, "store %d missing B's NAT", i)
	}
}

func TestConvergence_EphemeralTombstoneAfterRestart(t *testing.T) {
	pkA := genKey(t)
	pkB := genKey(t)

	storeA := newTestStore(pkA)
	storeB := newTestStore(pkB)

	// A sets ephemeral attrs.
	storeA.setObservedAddress("1.2.3.4", 45000)
	storeA.setLocalNatType(nat.Easy)
	storeA.setLocalPubliclyAccessible(true)
	storeA.setLocalResourceTelemetry(50, 60, 8<<30, 4)

	// Gossip A→B.
	full := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(pkA, full)
	require.NoError(t, err)

	peerA := storeB.Snapshot().Nodes[pkA]
	require.Equal(t, "1.2.3.4", peerA.ObservedExternalIP)
	require.Equal(t, nat.Easy, peerA.NatType)
	require.True(t, peerA.PubliclyAccessible)
	require.Equal(t, uint32(50), peerA.CPUPercent)

	// A "restarts" — new store, same key. tombstoneStaleAttrs runs on init.
	storeAPrime := newTestStore(pkA)

	// B→A' delivers A's old state, triggering self-conflict in A'.
	fullB := storeB.EncodeFull()
	_, _, err = storeAPrime.ApplyDelta(pkB, fullB)
	require.NoError(t, err)

	// A'→B delivers tombstones with bumped counters.
	fullAPrime := storeAPrime.EncodeFull()
	_, _, err = storeB.ApplyDelta(pkA, fullAPrime)
	require.NoError(t, err)

	peerA = storeB.Snapshot().Nodes[pkA]
	require.Empty(t, peerA.ObservedExternalIP)
	require.Equal(t, uint32(0), peerA.ExternalPort)
	require.Equal(t, nat.Unknown, peerA.NatType)
	require.False(t, peerA.PubliclyAccessible)
	require.Equal(t, uint32(0), peerA.CPUPercent)
	require.Equal(t, uint32(0), peerA.MemPercent)
}

func TestConvergence_ServiceSurvivesRestart(t *testing.T) {
	pkA := genKey(t)
	pkB := genKey(t)

	storeA := newTestStore(pkA)
	storeB := newTestStore(pkB)

	// A exposes a service.
	storeA.upsertLocalService(8080, "http")

	// Gossip A→B.
	full := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(pkA, full)
	require.NoError(t, err)

	peerA := storeB.Snapshot().Nodes[pkA]
	require.Contains(t, peerA.Services, "http")
	require.Equal(t, uint32(8080), peerA.Services["http"].Port)

	// A "restarts" — new store, same key.
	storeAPrime := newTestStore(pkA)

	// state.pb loads first (supervisor applies saved gossip before config).
	savedState := storeA.EncodeFull()
	_, _, err = storeAPrime.ApplyDelta(pkA, savedState)
	require.NoError(t, err)

	// Config restores the service (supervisor.New sets InitialServices after state.pb).
	storeAPrime.upsertLocalService(8080, "http")

	// B→A' delivers A's old state, triggering self-conflict in A'.
	fullB := storeB.EncodeFull()
	_, _, err = storeAPrime.ApplyDelta(pkB, fullB)
	require.NoError(t, err)

	// A' must still have its service after self-conflict resolution.
	snapA := storeAPrime.Snapshot()
	require.Contains(t, snapA.Nodes[pkA].Services, "http")
	require.Equal(t, uint32(8080), snapA.Nodes[pkA].Services["http"].Port)

	// A'→B delivers updated state.
	fullAPrime := storeAPrime.EncodeFull()
	_, _, err = storeB.ApplyDelta(pkA, fullAPrime)
	require.NoError(t, err)

	// B must still see A's service.
	peerA = storeB.Snapshot().Nodes[pkA]
	require.Contains(t, peerA.Services, "http")
	require.Equal(t, uint32(8080), peerA.Services["http"].Port)
}

func TestConvergence_WorkloadSpecTieBreaking(t *testing.T) {
	h := newConvergenceHarness(t, 3)
	h.sortByKey()

	// Each publishes same hash before gossip (so no remote-ownership rejection).
	h.stores[0].setLocalWorkloadSpec("shared", 1, 0, 0) //nolint:errcheck
	h.stores[1].setLocalWorkloadSpec("shared", 2, 0, 0) //nolint:errcheck
	h.stores[2].setLocalWorkloadSpec("shared", 3, 0, 0) //nolint:errcheck

	h.establishFullReachability()
	h.gossipFullMesh()
	h.assertConverged()

	for i, s := range h.stores {
		snap := s.Snapshot()
		spec, ok := snap.Specs["shared"]
		require.True(t, ok, "store %d missing shared spec", i)
		require.Equal(t, h.keys[0], spec.Publisher, "store %d wrong publisher", i)
		require.Equal(t, uint32(1), spec.Spec.GetReplicas(), "store %d wrong replicas", i)
	}
}

// --- Fuzz test (run with: go test ./pkg/state/ -fuzz=FuzzConvergence) ---

type fuzzReader struct {
	data []byte
	pos  int
}

func (r *fuzzReader) remaining() int { return len(r.data) - r.pos }

func (r *fuzzReader) readByte() byte {
	if r.pos >= len(r.data) {
		return 0
	}
	b := r.data[r.pos]
	r.pos++
	return b
}

func (r *fuzzReader) readUint16() uint16 {
	return uint16(r.readByte())<<8 | uint16(r.readByte())
}

const numFuzzMutations = 10

func applyFuzzMutation(s *store, kind int, peers []types.PeerKey, r *fuzzReader) {
	switch kind {
	case 0: // network
		ip := fmt.Sprintf("10.0.%d.%d", r.readByte(), r.readByte())
		port := uint32(r.readUint16()) + 1
		s.setLocalNetwork([]string{ip}, port)
	case 1: // service
		port := uint32(r.readUint16()) + 1
		name := fmt.Sprintf("svc-%d", r.readByte()%8)
		s.upsertLocalService(port, name)
	case 2: // remove service
		name := fmt.Sprintf("svc-%d", r.readByte()%8)
		s.removeLocalService(name)
	case 3: // observed address
		ip := fmt.Sprintf("1.2.%d.%d", r.readByte(), r.readByte())
		port := uint32(r.readUint16())
		s.setObservedAddress(ip, port)
	case 4: // NAT type
		s.setLocalNatType(nat.TypeFromUint32(uint32(r.readByte() % 3)))
	case 5: // vivaldi
		x := float64(int8(r.readByte())) / 10.0
		y := float64(int8(r.readByte())) / 10.0
		s.setLocalVivaldiCoord(coords.Coord{X: x, Y: y, Height: coords.MinHeight}, 0.5)
	case 6: // reachability
		idx := int(r.readByte()) % len(peers)
		if peers[idx] == s.localID {
			return
		}
		s.setLocalConnected(peers[idx], r.readByte()%2 == 0)
	case 7: // workload spec
		hash := fmt.Sprintf("wl-%d", r.readByte()%8)
		replicas := uint32(r.readByte()%4) + 1
		s.setLocalWorkloadSpec(hash, replicas, 0, 0) //nolint:errcheck
	case 8: // workload claim
		hash := fmt.Sprintf("wl-%d", r.readByte()%8)
		s.setLocalWorkloadClaim(hash, r.readByte()%2 == 0)
	case 9: // resource telemetry
		cpu := uint32(r.readByte())
		mem := uint32(r.readByte())
		s.setLocalResourceTelemetry(cpu, mem, 8<<30, 4)
	}
}

func FuzzConvergence(f *testing.F) {
	f.Add([]byte{
		0, 0, 10, 1, 0, 35,
		1, 1, 0, 80, 3,
		2, 7, 5, 1,
		0, 5, 10, 20,
		1, 4, 1,
		2, 9, 42, 77,
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}

		h := newDeterministicHarness(t, 3)
		r := &fuzzReader{data: data}

		for i := 0; i < 50 && r.remaining() >= 2; i++ {
			idx := int(r.readByte()) % len(h.stores)
			kind := int(r.readByte()) % numFuzzMutations
			applyFuzzMutation(h.stores[idx], kind, h.keys, r)
		}

		h.gossipFullMesh()
		h.assertConverged()
	})
}
