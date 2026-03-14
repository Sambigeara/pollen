package store

import (
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *Store) SetLocalNetwork(ips []string, port uint32) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if slices.Equal(local.IPs, ips) && local.LocalPort == port {
		return nil
	}

	local.IPs = append([]string(nil), ips...)
	local.LocalPort = port

	local.maxCounter++
	counter := local.maxCounter
	local.log[networkAttrKey()] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{
				Ips:       append([]string(nil), ips...),
				LocalPort: port,
			},
		},
	}}
}

func (s *Store) SetExternalPort(port uint32) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.ExternalPort == port {
		return nil
	}

	local.ExternalPort = port

	local.maxCounter++
	counter := local.maxCounter
	local.log[externalPortAttrKey()] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: port},
		},
	}}
}

func (s *Store) SetObservedExternalIP(ip string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.ObservedExternalIP == ip {
		return nil
	}

	local.ObservedExternalIP = ip

	local.maxCounter++
	counter := local.maxCounter
	deleted := ip == ""
	local.log[observedExternalIPAttrKey()] = logEntry{Counter: counter, Deleted: deleted}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: deleted,
		Change: &statev1.GossipEvent_ObservedExternalIp{
			ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: ip},
		},
	}}
}

func (s *Store) SetLocalConnected(peerID types.PeerKey, connected bool) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	_, exists := local.Reachable[peerID]
	if connected == exists {
		return nil
	}

	if connected {
		local.Reachable[peerID] = struct{}{}
	} else {
		delete(local.Reachable, peerID)
	}

	key := reachabilityAttrKey(peerID)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: !connected}
	s.nodes[s.LocalID] = local

	event := &statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: !connected,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{
				PeerId: peerID.String(),
			},
		},
	}

	return []*statev1.GossipEvent{event}
}

func (s *Store) SetLocalPubliclyAccessible(accessible bool) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.PubliclyAccessible == accessible {
		return nil
	}

	local.PubliclyAccessible = accessible

	key := publiclyAccessibleAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: !accessible}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: !accessible,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	}}
}

func (s *Store) SetLocalNatType(natType nat.Type) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.NatType == natType {
		return nil
	}

	local.NatType = natType

	key := natTypeAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	deleted := natType == nat.Unknown
	local.log[key] = logEntry{Counter: counter, Deleted: deleted}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: deleted,
		Change: &statev1.GossipEvent_NatType{
			NatType: &statev1.NatTypeChange{NatType: natType.ToUint32()},
		},
	}}
}

const resourceTelemetryDeadband = 2

func (s *Store) SetLocalResourceTelemetry(cpuPercent, memPercent uint32, memTotalBytes uint64, numCPU uint32) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	cpuDelta := absDiff(local.CPUPercent, cpuPercent)
	memDelta := absDiff(local.MemPercent, memPercent)
	if cpuDelta < resourceTelemetryDeadband && memDelta < resourceTelemetryDeadband && local.MemTotalBytes == memTotalBytes && local.NumCPU == numCPU {
		return nil
	}

	local.CPUPercent = cpuPercent
	local.MemPercent = memPercent
	local.MemTotalBytes = memTotalBytes
	local.NumCPU = numCPU

	key := resourceTelemetryAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_ResourceTelemetry{
			ResourceTelemetry: &statev1.ResourceTelemetryChange{
				CpuPercent:    cpuPercent,
				MemPercent:    memPercent,
				MemTotalBytes: memTotalBytes,
				NumCpu:        numCPU,
			},
		},
	}}
}

// SetLocalTrafficHeatmap publishes a traffic heatmap snapshot to gossip.
// Entries with zero bytes are omitted. If the snapshot transitions from
// non-empty to empty, a deletion event is emitted so stale data clears.
func (s *Store) SetLocalTrafficHeatmap(rates map[types.PeerKey]TrafficSnapshot) []*statev1.GossipEvent {
	// Filter zero entries.
	filtered := make(map[types.PeerKey]trafficRate, len(rates))
	for pk, r := range rates {
		if r.BytesIn > 0 || r.BytesOut > 0 {
			filtered[pk] = trafficRate(r)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	key := trafficHeatmapAttrKey()

	if len(filtered) == 0 {
		// No traffic this tick. If we previously had data, emit a deletion.
		if len(local.TrafficRates) == 0 {
			return nil
		}
		local.TrafficRates = nil
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter, Deleted: true}
		s.nodes[s.LocalID] = local
		return []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: true,
			Change: &statev1.GossipEvent_TrafficHeatmap{
				TrafficHeatmap: &statev1.TrafficHeatmapChange{},
			},
		}}
	}

	local.TrafficRates = filtered

	protoRates := make([]*statev1.TrafficRate, 0, len(filtered))
	for pk, r := range filtered {
		protoRates = append(protoRates, &statev1.TrafficRate{
			PeerId:   pk.String(),
			BytesIn:  r.BytesIn,
			BytesOut: r.BytesOut,
		})
	}

	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_TrafficHeatmap{
			TrafficHeatmap: &statev1.TrafficHeatmapChange{Rates: protoRates},
		},
	}}
}

func (s *Store) SetLocalCertExpiry(expiry int64) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.CertExpiry == expiry {
		return nil
	}

	local.CertExpiry = expiry

	local.maxCounter++
	counter := local.maxCounter
	local.log[identityAttrKey()] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{
				IdentityPub:    append([]byte(nil), local.IdentityPub...),
				CertExpiryUnix: expiry,
			},
		},
	}}
}

func (s *Store) SetLocalVivaldiCoord(coord topology.Coord) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.VivaldiCoord != nil && topology.MovementDistance(*local.VivaldiCoord, coord) <= topology.PublishEpsilon {
		return nil
	}

	local.VivaldiCoord = &coord

	key := vivaldiAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{
				X:      coord.X,
				Y:      coord.Y,
				Height: coord.Height,
			},
		},
	}}
}

func (s *Store) SetLastAddr(peerID types.PeerKey, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return
	}
	rec.LastAddr = addr
	s.nodes[peerID] = rec
}

func (s *Store) DenyPeer(subjectPub []byte) []*statev1.GossipEvent {
	subjectKey := types.PeerKeyFromBytes(subjectPub)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.denied[subjectKey]; ok {
		return nil
	}

	s.denied[subjectKey] = struct{}{}

	local := s.nodes[s.LocalID]
	key := denyAttrKey(subjectKey.String())
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: append([]byte(nil), subjectPub...)},
		},
	}}
}

func (s *Store) UpsertLocalService(port uint32, name string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if existing, ok := local.Services[name]; ok && existing.GetPort() == port {
		return nil
	}

	local.Services[name] = &statev1.Service{
		Name: name,
		Port: port,
	}

	key := serviceAttrKey(name)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: name, Port: port},
		},
	}}
}

func (s *Store) RemoveLocalServices(name string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if _, ok := local.Services[name]; !ok {
		return nil
	}

	delete(local.Services, name)

	key := serviceAttrKey(name)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: true,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: name},
		},
	}}
}

func absDiff(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}
