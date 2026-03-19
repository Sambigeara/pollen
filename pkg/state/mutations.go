package state

import (
	"maps"
	"net/netip"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *Store) applyLocal(gossip []*statev1.GossipEvent, ev Event) []Event {
	s.enqueuePendingGossip(gossip)
	if len(gossip) == 0 {
		return nil
	}
	return []Event{ev}
}

func (s *Store) DenyPeer(key types.PeerKey) []Event {
	return s.applyLocal(s.denyPeerRaw(key[:]), PeerDenied{Key: key})
}

func (s *Store) SetLocalAddresses(addrs []netip.AddrPort) []Event {
	if len(addrs) == 0 {
		return nil
	}
	ips := make([]string, len(addrs))
	for i, a := range addrs {
		ips[i] = a.Addr().String()
	}
	return s.applyLocal(s.setLocalNetwork(ips, uint32(addrs[0].Port())), TopologyChanged{Peer: s.LocalID})
}

func (s *Store) SetLocalNAT(t nat.Type) []Event {
	return s.applyLocal(s.setLocalNatType(t), TopologyChanged{Peer: s.LocalID})
}

func (s *Store) SetLocalCoord(c coords.Coord) []Event {
	return s.applyLocal(s.setLocalVivaldiCoord(c), TopologyChanged{Peer: s.LocalID})
}

func (s *Store) SetLocalReachable(peers []types.PeerKey) []Event {
	wanted := make(map[types.PeerKey]struct{}, len(peers))
	for _, pk := range peers {
		wanted[pk] = struct{}{}
	}

	current := s.Snapshot().Nodes[s.LocalID].Reachable
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
	return []Event{TopologyChanged{Peer: s.LocalID}}
}

func (s *Store) SetLocalObservedExternalIP(ip string) []Event {
	return s.applyLocal(s.setObservedExternalIP(ip), TopologyChanged{Peer: s.LocalID})
}

func (s *Store) SetLocalExternalPort(port uint32) []Event {
	return s.applyLocal(s.setExternalPort(port), TopologyChanged{Peer: s.LocalID})
}

func (s *Store) SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []Event {
	if replicas == 0 && memoryPages == 0 && timeoutMs == 0 {
		gossip := s.removeLocalWorkloadSpec(hash)
		return s.applyLocal(gossip, WorkloadChanged{Hash: hash})
	}
	gossip, _ := s.setLocalWorkloadSpec(hash, replicas, memoryPages, timeoutMs)
	return s.applyLocal(gossip, WorkloadChanged{Hash: hash})
}

func (s *Store) ClaimWorkload(hash string) []Event {
	return s.applyLocal(s.setLocalWorkloadClaim(hash, true), WorkloadChanged{Hash: hash})
}

func (s *Store) ReleaseWorkload(hash string) []Event {
	return s.applyLocal(s.setLocalWorkloadClaim(hash, false), WorkloadChanged{Hash: hash})
}

func (s *Store) SetLocalResources(cpu, mem float64) []Event {
	return s.applyLocal(s.setLocalResourceTelemetry(uint32(cpu), uint32(mem), 0, 0), TopologyChanged{Peer: s.LocalID})
}

func (s *Store) SetService(port uint32, name string) []Event {
	return s.applyLocal(s.upsertLocalService(port, name), ServiceChanged{Peer: s.LocalID, Name: name})
}

func (s *Store) RemoveService(name string) []Event {
	return s.applyLocal(s.removeLocalService(name), ServiceChanged{Peer: s.LocalID, Name: name})
}

func (s *Store) SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event {
	s.mu.Lock()
	merged := maps.Clone(s.nodes[s.LocalID].TrafficRates)
	s.mu.Unlock()

	if merged == nil {
		merged = make(map[types.PeerKey]TrafficSnapshot, 1)
	}
	merged[peer] = TrafficSnapshot{BytesIn: in, BytesOut: out}

	s.enqueuePendingGossip(s.setLocalTrafficHeatmap(merged))
	return nil
}
