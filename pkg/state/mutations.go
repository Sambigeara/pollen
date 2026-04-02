package state

import (
	"maps"
	"net/netip"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *store) applyLocal(gossip []*statev1.GossipEvent, ev Event) []Event {
	s.enqueuePendingGossip(gossip)
	if len(gossip) == 0 {
		return nil
	}
	return []Event{ev}
}

func (s *store) DenyPeer(key types.PeerKey) []Event {
	return s.applyLocal(s.denyPeerRaw(key[:]), PeerDenied{Key: key})
}

func (s *store) SetLocalAddresses(addrs []netip.AddrPort) []Event {
	if len(addrs) == 0 {
		return nil
	}
	ips := make([]string, len(addrs))
	for i, a := range addrs {
		ips[i] = a.Addr().String()
	}
	return s.applyLocal(s.setLocalNetwork(ips, uint32(addrs[0].Port())), TopologyChanged{Peer: s.localID})
}

func (s *store) SetLocalNAT(t nat.Type) []Event {
	return s.applyLocal(s.setLocalNatType(t), TopologyChanged{Peer: s.localID})
}

func (s *store) SetLocalCoord(c coords.Coord, coordErr float64) []Event {
	return s.applyLocal(s.setLocalVivaldiCoord(c, coordErr), TopologyChanged{Peer: s.localID})
}

func (s *store) SetLocalReachable(peers []types.PeerKey) []Event {
	wanted := make(map[types.PeerKey]struct{}, len(peers))
	for _, pk := range peers {
		wanted[pk] = struct{}{}
	}

	current := s.Snapshot().Nodes[s.localID].Reachable
	var changed bool
	for pk := range wanted {
		if _, ok := current[pk]; !ok {
			if gossip := s.setLocalConnected(pk, true); len(gossip) > 0 {
				s.enqueuePendingGossip(gossip)
				changed = true
			}
		}
	}
	for pk := range current {
		if _, ok := wanted[pk]; !ok {
			if gossip := s.setLocalConnected(pk, false); len(gossip) > 0 {
				s.enqueuePendingGossip(gossip)
				changed = true
			}
		}
	}

	if !changed {
		return nil
	}
	return []Event{TopologyChanged{Peer: s.localID}}
}

func (s *store) SetLocalObservedAddress(ip string, port uint32) []Event {
	return s.applyLocal(s.setObservedAddress(ip, port), TopologyChanged{Peer: s.localID})
}

func (s *store) SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []Event {
	if replicas == 0 && memoryPages == 0 && timeoutMs == 0 {
		gossip := s.removeLocalWorkloadSpec(hash)
		return s.applyLocal(gossip, WorkloadChanged{Hash: hash})
	}
	gossip, _ := s.setLocalWorkloadSpec(hash, replicas, memoryPages, timeoutMs)
	return s.applyLocal(gossip, WorkloadChanged{Hash: hash})
}

func (s *store) ClaimWorkload(hash string) []Event {
	return s.applyLocal(s.setLocalWorkloadClaim(hash, true), WorkloadChanged{Hash: hash})
}

func (s *store) ReleaseWorkload(hash string) []Event {
	return s.applyLocal(s.setLocalWorkloadClaim(hash, false), WorkloadChanged{Hash: hash})
}

func (s *store) SetLocalResources(cpu, mem float64) []Event {
	return s.applyLocal(s.setLocalResourceTelemetry(uint32(cpu), uint32(mem), 0, 0), TopologyChanged{Peer: s.localID})
}

func (s *store) SetService(port uint32, name string) []Event {
	return s.applyLocal(s.upsertLocalService(port, name), ServiceChanged{Peer: s.localID, Name: name})
}

func (s *store) RemoveService(name string) []Event {
	return s.applyLocal(s.removeLocalService(name), ServiceChanged{Peer: s.localID, Name: name})
}

func (s *store) SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event {
	s.mu.Lock()
	merged := maps.Clone(s.nodes[s.localID].TrafficRates)
	s.mu.Unlock()

	if merged == nil {
		merged = make(map[types.PeerKey]TrafficSnapshot, 1)
	}
	merged[peer] = TrafficSnapshot{BytesIn: in, BytesOut: out}

	s.enqueuePendingGossip(s.setLocalTrafficHeatmap(merged))
	return nil
}
