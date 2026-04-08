package state

import (
	"fmt"
	"net/netip"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type convergenceHarness struct {
	t      *testing.T
	stores []StateStore
	keys   []types.PeerKey
}

func newDeterministicHarness(t *testing.T, n int) *convergenceHarness {
	t.Helper()
	h := &convergenceHarness{t: t, stores: make([]StateStore, n), keys: make([]types.PeerKey, n)}
	for i := range n {
		pub := make([]byte, 32)
		pub[0] = byte(i + 1)
		h.keys[i] = types.PeerKeyFromBytes(pub)
		h.stores[i] = New(h.keys[i])
	}
	return h
}

func (h *convergenceHarness) gossipOneway(from, to int) {
	h.t.Helper()
	full := h.stores[from].EncodeFull()
	_, _, err := h.stores[to].ApplyDelta(h.keys[from], full)
	require.NoError(h.t, err)
}

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
		out[i] = s.Snapshot().Digest().proto.GetPeers()
	}
	return out
}

func (h *convergenceHarness) assertConverged() {
	h.t.Helper()

	allPeers := make(map[string]struct{})
	digests := make([]*statev1.Digest, len(h.stores))
	for i, s := range h.stores {
		digests[i] = s.Snapshot().Digest().proto
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
				refCounter = pd.MaxCounter
				refHash = pd.StateHash
				refIdx = i
				continue
			}
			require.Equal(h.t, refCounter, pd.MaxCounter, "store %d and store %d disagree on maxCounter for peer %s", refIdx, i, pk)
			require.Equal(h.t, refHash, pd.StateHash, "store %d and store %d disagree on stateHash for peer %s", refIdx, i, pk)
		}
	}
}

func (h *convergenceHarness) establishFullReachability() {
	for i := range h.stores {
		var peers []types.PeerKey
		for j := range h.stores {
			if i != j {
				peers = append(peers, h.keys[j])
			}
		}
		h.stores[i].SetLocalReachable(peers)
	}
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
			if da.MaxCounter != db.MaxCounter || da.StateHash != db.StateHash {
				return false
			}
		}
	}
	return true
}

func TestConvergence_ThreeStoreFullMesh(t *testing.T) {
	h := newDeterministicHarness(t, 3)

	h.stores[0].SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	h.stores[0].SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)

	h.stores[1].SetLocalCoord(coords.Coord{X: 1, Y: 2, Height: coords.MinHeight}, 0.5)
	h.stores[1].SetLocalNAT(nat.Easy)

	h.stores[2].SetWorkloadSpec("hash1", "hash1", 2, 0, 0, 0)
	h.stores[2].ClaimWorkload("hash1")

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

func TestConvergence_PartitionHealing(t *testing.T) {
	h := newDeterministicHarness(t, 3)
	h.establishFullReachability()
	h.gossipFullMesh()
	h.assertConverged()

	// Partition: A<->B communicate, C is isolated.
	h.stores[0].SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	h.stores[1].SetLocalNAT(nat.Easy)
	h.gossipOneway(0, 1)
	h.gossipOneway(1, 0)

	// C evolves independently.
	h.stores[2].SetService(9090, "api", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)

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

// --- Fuzz test ---

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

func applyFuzzMutation(s StateStore, kind int, peers []types.PeerKey, r *fuzzReader) {
	switch kind {
	case 0: // network
		ip := fmt.Sprintf("10.0.%d.%d", r.readByte(), r.readByte())
		port := uint32(r.readUint16()) + 1
		addr, err := netip.ParseAddr(ip)
		if err == nil {
			s.SetLocalAddresses([]netip.AddrPort{netip.AddrPortFrom(addr, uint16(port))})
		}
	case 1: // service
		port := uint32(r.readUint16()) + 1
		name := fmt.Sprintf("svc-%d", r.readByte()%8)
		s.SetService(port, name, statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	case 2: // remove service
		name := fmt.Sprintf("svc-%d", r.readByte()%8)
		s.RemoveService(name)
	case 3: // observed address
		ip := fmt.Sprintf("1.2.%d.%d", r.readByte(), r.readByte())
		port := uint32(r.readUint16())
		s.SetLocalObservedAddress(ip, port)
	case 4: // NAT type
		s.SetLocalNAT(nat.TypeFromUint32(uint32(r.readByte() % 3)))
	case 5: // vivaldi
		x := float64(int8(r.readByte())) / 10.0
		y := float64(int8(r.readByte())) / 10.0
		s.SetLocalCoord(coords.Coord{X: x, Y: y, Height: coords.MinHeight}, 0.5)
	case 6: // reachability
		idx := int(r.readByte()) % len(peers)
		snap := s.Snapshot()
		if peers[idx] == snap.LocalID {
			return
		}

		reachable := make([]types.PeerKey, 0, len(snap.Nodes[snap.LocalID].Reachable)+1)
		for pk := range snap.Nodes[snap.LocalID].Reachable {
			if pk != peers[idx] {
				reachable = append(reachable, pk)
			}
		}
		if r.readByte()%2 == 0 {
			reachable = append(reachable, peers[idx])
		}
		s.SetLocalReachable(reachable)
	case 7: // workload spec
		hash := fmt.Sprintf("wl-%d", r.readByte()%8)
		replicas := uint32(r.readByte()%4) + 1
		s.SetWorkloadSpec(hash, hash, replicas, 0, 0, 0)
	case 8: // workload claim
		hash := fmt.Sprintf("wl-%d", r.readByte()%8)
		if r.readByte()%2 == 0 {
			s.ClaimWorkload(hash)
		} else {
			s.ReleaseWorkload(hash)
		}
	case 9: // resource telemetry
		cpu := uint32(r.readByte())
		mem := uint32(r.readByte())
		s.SetLocalResources(float64(cpu), float64(mem), 8<<30, 4, 0, 0)
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
			kind := int(r.readByte()) % 10
			applyFuzzMutation(h.stores[idx], kind, h.keys, r)
		}

		h.gossipFullMesh()
		h.assertConverged()
	})
}
