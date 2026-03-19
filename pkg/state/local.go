package state

import (
	"maps"
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *Store) emitLocal(local *nodeRecord, key attrKey, deleted bool, evt *statev1.GossipEvent) []*statev1.GossipEvent {
	local.maxCounter++
	evt.PeerId = s.LocalID.String()
	evt.Counter = local.maxCounter
	evt.Deleted = deleted
	local.log[key] = logEntry{Counter: local.maxCounter, Deleted: deleted}
	s.nodes[s.LocalID] = *local
	s.updateSnapshot()
	return []*statev1.GossipEvent{evt}
}

func (s *Store) setLocalNetwork(ips []string, port uint32) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if slices.Equal(local.IPs, ips) && local.LocalPort == port {
			return
		}
		local.IPs = append([]string(nil), ips...)
		local.LocalPort = port
		events = s.emitLocal(&local, attrKey{kind: attrNetwork}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: append([]string(nil), ips...), LocalPort: port},
			},
		})
	})
	return events
}

func (s *Store) setExternalPort(port uint32) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.ExternalPort == port {
			return
		}
		local.ExternalPort = port
		events = s.emitLocal(&local, attrKey{kind: attrExternalPort}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_ExternalPort{
				ExternalPort: &statev1.ExternalPortChange{ExternalPort: port},
			},
		})
	})
	return events
}

func (s *Store) setObservedExternalIP(ip string) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.ObservedExternalIP == ip {
			return
		}
		local.ObservedExternalIP = ip
		events = s.emitLocal(&local, attrKey{kind: attrObservedExternalIP}, ip == "", &statev1.GossipEvent{
			Change: &statev1.GossipEvent_ObservedExternalIp{
				ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: ip},
			},
		})
	})
	return events
}

func (s *Store) setLocalConnected(peerID types.PeerKey, connected bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		_, exists := local.Reachable[peerID]
		if connected == exists {
			return
		}
		if connected {
			local.Reachable[peerID] = struct{}{}
		} else {
			delete(local.Reachable, peerID)
		}
		events = s.emitLocal(&local, attrKey{kind: attrReachability, peer: peerID}, !connected, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_Reachability{
				Reachability: &statev1.ReachabilityChange{PeerId: peerID.String()},
			},
		})
	})
	return events
}

func (s *Store) setLocalPubliclyAccessible(accessible bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.PubliclyAccessible == accessible {
			return
		}
		local.PubliclyAccessible = accessible
		events = s.emitLocal(&local, attrKey{kind: attrPubliclyAccessible}, !accessible, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_PubliclyAccessible{
				PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
			},
		})
	})
	return events
}

func (s *Store) setLocalNatType(natType nat.Type) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.NatType == natType {
			return
		}
		local.NatType = natType
		events = s.emitLocal(&local, attrKey{kind: attrNatType}, natType == nat.Unknown, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_NatType{
				NatType: &statev1.NatTypeChange{NatType: natType.ToUint32()},
			},
		})
	})
	return events
}

const resourceTelemetryDeadband = 2

func (s *Store) setLocalResourceTelemetry(cpuPercent, memPercent uint32, memTotalBytes uint64, numCPU uint32) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		cpuDelta := absDiff(local.CPUPercent, cpuPercent)
		memDelta := absDiff(local.MemPercent, memPercent)
		if cpuDelta < resourceTelemetryDeadband && memDelta < resourceTelemetryDeadband && local.MemTotalBytes == memTotalBytes && local.NumCPU == numCPU {
			return
		}
		local.CPUPercent = cpuPercent
		local.MemPercent = memPercent
		local.MemTotalBytes = memTotalBytes
		local.NumCPU = numCPU
		events = s.emitLocal(&local, attrKey{kind: attrResourceTelemetry}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_ResourceTelemetry{
				ResourceTelemetry: &statev1.ResourceTelemetryChange{
					CpuPercent: cpuPercent, MemPercent: memPercent,
					MemTotalBytes: memTotalBytes, NumCpu: numCPU,
				},
			},
		})
	})
	return events
}

func (s *Store) setLocalTrafficHeatmap(rates map[types.PeerKey]TrafficSnapshot) []*statev1.GossipEvent {
	filtered := make(map[types.PeerKey]TrafficSnapshot, len(rates))
	for pk, r := range rates {
		if r.BytesIn > 0 || r.BytesOut > 0 {
			filtered[pk] = r
		}
	}

	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		key := attrKey{kind: attrTrafficHeatmap}

		if len(filtered) == 0 {
			if len(local.TrafficRates) == 0 {
				return
			}
			local.TrafficRates = nil
			events = s.emitLocal(&local, key, true, &statev1.GossipEvent{
				Change: &statev1.GossipEvent_TrafficHeatmap{
					TrafficHeatmap: &statev1.TrafficHeatmapChange{},
				},
			})
			return
		}

		local.TrafficRates = filtered
		protoRates := make([]*statev1.TrafficRate, 0, len(filtered))
		for pk, r := range filtered {
			protoRates = append(protoRates, &statev1.TrafficRate{
				PeerId: pk.String(), BytesIn: r.BytesIn, BytesOut: r.BytesOut,
			})
		}
		events = s.emitLocal(&local, key, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_TrafficHeatmap{
				TrafficHeatmap: &statev1.TrafficHeatmapChange{Rates: protoRates},
			},
		})
	})
	return events
}

func absDiff(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}

func (s *Store) setLocalVivaldiCoord(coord coords.Coord) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.VivaldiCoord != nil && coords.MovementDistance(*local.VivaldiCoord, coord) <= coords.PublishEpsilon {
			return
		}
		local.VivaldiCoord = &coord
		events = s.emitLocal(&local, attrKey{kind: attrVivaldi}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_Vivaldi{
				Vivaldi: &statev1.VivaldiCoordinateChange{X: coord.X, Y: coord.Y, Height: coord.Height},
			},
		})
	})
	return events
}

func (s *Store) upsertLocalService(port uint32, name string) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if existing, ok := local.Services[name]; ok && existing.GetPort() == port {
			return
		}
		local.Services[name] = &statev1.Service{Name: name, Port: port}
		events = s.emitLocal(&local, attrKey{kind: attrService, name: name}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_Service{
				Service: &statev1.ServiceChange{Name: name, Port: port},
			},
		})
	})
	return events
}

func (s *Store) deleteLocalNamedAttr(key attrKey, fn func(local *nodeRecord, evt *statev1.GossipEvent) bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		evt := &statev1.GossipEvent{}
		if !fn(&local, evt) {
			return
		}
		events = s.emitLocal(&local, key, true, evt)
	})
	return events
}

func (s *Store) removeLocalService(name string) []*statev1.GossipEvent {
	return s.deleteLocalNamedAttr(attrKey{kind: attrService, name: name}, func(local *nodeRecord, evt *statev1.GossipEvent) bool {
		if _, ok := local.Services[name]; !ok {
			return false
		}
		delete(local.Services, name)
		evt.Change = &statev1.GossipEvent_Service{Service: &statev1.ServiceChange{Name: name}}
		return true
	})
}

func (s *Store) denyPeerRaw(subjectPub []byte) []*statev1.GossipEvent {
	subjectKey := types.PeerKeyFromBytes(subjectPub)
	var events []*statev1.GossipEvent
	s.do(func() {
		if _, ok := s.denied[subjectKey]; ok {
			return
		}
		s.denied[subjectKey] = struct{}{}
		local := s.nodes[s.LocalID]
		events = s.emitLocal(&local, attrKey{kind: attrDeny, name: subjectKey.String()}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_Deny{
				Deny: &statev1.DenyChange{SubjectPub: append([]byte(nil), subjectPub...)},
			},
		})
	})
	return events
}

func (s *Store) setLocalWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) ([]*statev1.GossipEvent, error) {
	var (
		events []*statev1.GossipEvent
		err    error
	)
	s.do(func() {
		for pk, rec := range s.nodes {
			if pk == s.LocalID {
				continue
			}
			if _, has := rec.WorkloadSpecs[hash]; !has {
				continue
			}
			if s.isValidOwnerLocked(pk) {
				err = errSpecOwnedRemotely
				return
			}
		}

		local := s.nodes[s.LocalID]
		if existing, ok := local.WorkloadSpecs[hash]; ok &&
			existing.GetReplicas() == replicas && existing.GetMemoryPages() == memoryPages &&
			existing.GetTimeoutMs() == timeoutMs {
			return
		}

		m := make(map[string]*statev1.WorkloadSpecChange, len(local.WorkloadSpecs)+1)
		maps.Copy(m, local.WorkloadSpecs)
		m[hash] = &statev1.WorkloadSpecChange{Hash: hash, Replicas: replicas, MemoryPages: memoryPages, TimeoutMs: timeoutMs}
		local.WorkloadSpecs = m
		events = s.emitLocal(&local, attrKey{kind: attrWorkloadSpec, name: hash}, false, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_WorkloadSpec{
				WorkloadSpec: &statev1.WorkloadSpecChange{
					Hash: hash, Replicas: replicas, MemoryPages: memoryPages, TimeoutMs: timeoutMs,
				},
			},
		})
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Store) removeLocalWorkloadSpec(hash string) []*statev1.GossipEvent {
	return s.deleteLocalNamedAttr(attrKey{kind: attrWorkloadSpec, name: hash}, func(local *nodeRecord, evt *statev1.GossipEvent) bool {
		if _, ok := local.WorkloadSpecs[hash]; !ok {
			return false
		}
		delete(local.WorkloadSpecs, hash)
		evt.Change = &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash}}
		return true
	})
}

func (s *Store) setLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		_, exists := local.WorkloadClaims[hash]
		if claimed == exists {
			return
		}
		if claimed {
			m := make(map[string]struct{}, len(local.WorkloadClaims)+1)
			maps.Copy(m, local.WorkloadClaims)
			m[hash] = struct{}{}
			local.WorkloadClaims = m
		} else {
			delete(local.WorkloadClaims, hash)
		}
		events = s.emitLocal(&local, attrKey{kind: attrWorkloadClaim, name: hash}, !claimed, &statev1.GossipEvent{
			Change: &statev1.GossipEvent_WorkloadClaim{
				WorkloadClaim: &statev1.WorkloadClaimChange{Hash: hash},
			},
		})
	})
	return events
}
