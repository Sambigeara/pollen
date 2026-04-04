package state

import (
	"net/netip"
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *store) mutateLocal(fn func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event)) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := s.nodes[s.localID]
	gossips, events := fn(&rec)

	if len(gossips) == 0 {
		return events
	}

	now := s.nowFunc()
	for _, ev := range gossips {
		key, _ := getAttrKey(ev)
		rec.maxCounter++
		ev.PeerId = s.localID.String()
		ev.Counter = rec.maxCounter
		rec.log[key] = ev
		s.pendingGossip = append(s.pendingGossip, ev)
	}

	rec.lastEventAt = now
	s.lastLocalEmit = now
	s.nodes[s.localID] = rec
	s.updateSnapshotLocked()
	s.notify()

	return events
}

func (s *store) DenyPeer(key types.PeerKey) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.denied[key]; ok {
		return nil
	}
	s.denied[key] = struct{}{}

	ev := &statev1.GossipEvent{Change: &statev1.GossipEvent_Deny{Deny: &statev1.DenyChange{PeerPub: key.Bytes()}}}

	now := s.nowFunc()
	rec := s.nodes[s.localID]
	rec.maxCounter++
	ev.PeerId = s.localID.String()
	ev.Counter = rec.maxCounter
	ak, _ := getAttrKey(ev)
	rec.log[ak] = ev
	rec.lastEventAt = now
	s.lastLocalEmit = now
	s.nodes[s.localID] = rec

	s.pendingGossip = append(s.pendingGossip, ev)
	s.updateSnapshotLocked()
	s.notify()
	return []Event{PeerDenied{Key: key}}
}

func (s *store) SetLocalAddresses(addrs []netip.AddrPort) []Event {
	if len(addrs) == 0 {
		return nil
	}

	ips := make([]string, len(addrs))
	for i, a := range addrs {
		ips[i] = a.Addr().String()
	}
	port := uint32(addrs[0].Port())

	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrNetwork}]; ok && !ev.Deleted {
			net := ev.GetNetwork()
			if slices.Equal(net.Ips, ips) && net.LocalPort == port {
				return nil, nil
			}
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: ips, LocalPort: port}}}
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}}
	})
}

func (s *store) SetLocalNAT(t nat.Type) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrNatType}]; ok && !ev.Deleted {
			if nat.TypeFromUint32(ev.GetNatType().NatType) == t {
				return nil, nil
			}
		}
		change := &statev1.GossipEvent{
			Deleted: t == nat.Unknown,
			Change:  &statev1.GossipEvent_NatType{NatType: &statev1.NatTypeChange{NatType: t.ToUint32()}},
		}
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}}
	})
}

func (s *store) SetLocalCoord(c coords.Coord, coordErr float64) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrVivaldi}]; ok && !ev.Deleted {
			viv := ev.GetVivaldi()
			old := coords.Coord{X: viv.X, Y: viv.Y, Height: viv.Height}
			if coords.MovementDistance(old, c) <= coords.PublishEpsilon {
				return nil, nil
			}
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_Vivaldi{Vivaldi: &statev1.VivaldiCoordinateChange{X: c.X, Y: c.Y, Height: c.Height, Error: coordErr}}}
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}}
	})
}

func (s *store) SetLocalReachable(peers []types.PeerKey) []Event {
	wanted := make(map[types.PeerKey]struct{}, len(peers))
	for _, p := range peers {
		wanted[p] = struct{}{}
	}

	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		var events []*statev1.GossipEvent
		current := make(map[types.PeerKey]struct{})

		for key, ev := range rec.log {
			if key.kind == attrReachability && !ev.Deleted {
				current[key.peer] = struct{}{}
			}
		}

		for p := range wanted {
			if _, ok := current[p]; !ok {
				events = append(events, &statev1.GossipEvent{Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: p.String()}}})
			}
		}
		for p := range current {
			if _, ok := wanted[p]; !ok {
				events = append(events, &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: p.String()}}})
			}
		}

		if len(events) == 0 {
			return nil, nil
		}
		return events, []Event{TopologyChanged{Peer: s.localID}}
	})
}

func (s *store) SetLocalObservedAddress(ip string, port uint32) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrObservedAddress}]; ok && !ev.Deleted {
			oa := ev.GetObservedAddress()
			if oa.Ip == ip && oa.Port == port {
				return nil, nil
			}
		}
		change := &statev1.GossipEvent{
			Deleted: ip == "" && port == 0,
			Change:  &statev1.GossipEvent_ObservedAddress{ObservedAddress: &statev1.ObservedAddressChange{Ip: ip, Port: port}},
		}
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}}
	})
}

func (s *store) SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		// Check if remotely owned
		for pk, r := range s.nodes {
			if pk != s.localID {
				if ev, ok := r.log[attrKey{kind: attrWorkloadSpec, name: hash}]; ok && !ev.Deleted && s.isValidOwnerLocked(pk) {
					return nil, nil
				}
			}
		}

		ev, ok := rec.log[attrKey{kind: attrWorkloadSpec, name: hash}]

		if replicas == 0 && memoryPages == 0 && timeoutMs == 0 {
			if !ok || ev.Deleted {
				return nil, nil
			}
			change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash}}}
			return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
		}

		if ok && !ev.Deleted {
			ws := ev.GetWorkloadSpec()
			if ws.Replicas == replicas && ws.MemoryPages == memoryPages && ws.TimeoutMs == timeoutMs {
				return nil, nil
			}
		}

		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash, Replicas: replicas, MemoryPages: memoryPages, TimeoutMs: timeoutMs},
		}}
		return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
	})
}

func (s *store) ClaimWorkload(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, true)
}

func (s *store) ReleaseWorkload(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, false)
}

func (s *store) setWorkloadClaimLocked(hash string, claimed bool) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrWorkloadClaim, name: hash}]
		exists := ok && !ev.Deleted

		if claimed == exists {
			return nil, nil
		}

		change := &statev1.GossipEvent{Deleted: !claimed, Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: hash}}}
		return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
	})
}

func (s *store) SetLocalResources(cpu, mem float64) []Event {
	c, m := uint32(cpu), uint32(mem)
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrResourceTelemetry}]; ok && !ev.Deleted {
			rt := ev.GetResourceTelemetry()

			cpuDelta := rt.CpuPercent - c
			if c > rt.CpuPercent {
				cpuDelta = c - rt.CpuPercent
			}
			memDelta := rt.MemPercent - m
			if m > rt.MemPercent {
				memDelta = m - rt.MemPercent
			}

			if cpuDelta < 2 && memDelta < 2 {
				return nil, nil
			}
		}

		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_ResourceTelemetry{ResourceTelemetry: &statev1.ResourceTelemetryChange{CpuPercent: c, MemPercent: m}}}
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}}
	})
}

func (s *store) SetService(port uint32, name string) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrService, name: name}]; ok && !ev.Deleted {
			if ev.GetService().Port == port {
				return nil, nil
			}
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_Service{Service: &statev1.ServiceChange{Name: name, Port: port}}}
		return []*statev1.GossipEvent{change}, []Event{ServiceChanged{Peer: s.localID, Name: name}}
	})
}

func (s *store) RemoveService(name string) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrService, name: name}]; !ok || ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_Service{Service: &statev1.ServiceChange{Name: name}}}
		return []*statev1.GossipEvent{change}, []Event{ServiceChanged{Peer: s.localID, Name: name}}
	})
}

func (s *store) SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		var rates []*statev1.TrafficRate
		var updated bool

		if ev, ok := rec.log[attrKey{kind: attrTrafficHeatmap}]; ok && !ev.Deleted { //nolint:nestif
			for _, r := range ev.GetTrafficHeatmap().Rates {
				if r.PeerId == peer.String() {
					if r.BytesIn == in && r.BytesOut == out {
						return nil, nil
					}
					updated = true
					if in > 0 || out > 0 {
						rates = append(rates, &statev1.TrafficRate{PeerId: r.PeerId, BytesIn: in, BytesOut: out})
					}
				} else {
					rates = append(rates, r)
				}
			}
		}

		if !updated && (in > 0 || out > 0) {
			rates = append(rates, &statev1.TrafficRate{PeerId: peer.String(), BytesIn: in, BytesOut: out})
		}

		change := &statev1.GossipEvent{
			Deleted: len(rates) == 0,
			Change:  &statev1.GossipEvent_TrafficHeatmap{TrafficHeatmap: &statev1.TrafficHeatmapChange{Rates: rates}},
		}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) SetBootstrapPublic() {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrPubliclyAccessible}]; ok && !ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_PubliclyAccessible{PubliclyAccessible: &statev1.PubliclyAccessibleChange{}}}
		return []*statev1.GossipEvent{change}, nil
	})
}
