package state

import (
	"maps"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type attrKind uint8

const (
	attrNetwork attrKind = iota + 1
	attrExternalPort
	attrObservedExternalIP
	attrIdentity
	attrService
	attrReachability
	attrDeny
	attrPubliclyAccessible
	attrVivaldi
	attrNatType
	attrResourceTelemetry
	attrWorkloadSpec
	attrWorkloadClaim
	attrTrafficHeatmap
)

type attrKey struct {
	name string
	peer types.PeerKey
	kind attrKind
}

type logEntry struct {
	Counter uint64
	Deleted bool
}

type nodeRecord struct {
	log map[attrKey]logEntry
	NodeView
	maxCounter uint64
}

func (r nodeRecord) clone() nodeRecord {
	c := r
	c.NodeView = r.NodeView.clone()
	return c
}

func ensureNodeInit(rec *nodeRecord, peerID types.PeerKey) {
	if rec.Reachable == nil {
		rec.Reachable = make(map[types.PeerKey]struct{})
	}
	if rec.Services == nil {
		rec.Services = make(map[string]*statev1.Service)
	}
	if rec.WorkloadSpecs == nil {
		rec.WorkloadSpecs = make(map[string]*statev1.WorkloadSpecChange)
	}
	if rec.WorkloadClaims == nil {
		rec.WorkloadClaims = make(map[string]struct{})
	}
	if rec.log == nil {
		rec.log = make(map[attrKey]logEntry)
	}
	if len(rec.IdentityPub) == 0 {
		rec.IdentityPub = append([]byte(nil), peerID[:]...)
	}
}

func tombstoneStaleAttrs(rec *nodeRecord) {
	staleKeys := []attrKey{
		{kind: attrPubliclyAccessible},
		{kind: attrNatType},
		{kind: attrObservedExternalIP},
		{kind: attrExternalPort},
		{kind: attrResourceTelemetry},
		{kind: attrTrafficHeatmap},
	}
	for _, key := range staleKeys {
		rec.maxCounter++
		rec.log[key] = logEntry{Counter: rec.maxCounter, Deleted: true}
	}
}

func eventAttrKey(event *statev1.GossipEvent) (attrKey, bool) {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		return attrKey{kind: attrNetwork}, true
	case *statev1.GossipEvent_ExternalPort:
		return attrKey{kind: attrExternalPort}, true
	case *statev1.GossipEvent_ObservedExternalIp:
		return attrKey{kind: attrObservedExternalIP}, true
	case *statev1.GossipEvent_IdentityPub:
		return attrKey{kind: attrIdentity}, true
	case *statev1.GossipEvent_Service:
		if v.Service == nil || v.Service.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrService, name: v.Service.GetName()}, true
	case *statev1.GossipEvent_Reachability:
		pk, err := types.PeerKeyFromString(v.Reachability.GetPeerId())
		if err != nil {
			return attrKey{}, false
		}
		return attrKey{kind: attrReachability, peer: pk}, true
	case *statev1.GossipEvent_Deny:
		subjectKey := types.PeerKeyFromBytes(v.Deny.GetSubjectPub())
		return attrKey{kind: attrDeny, name: subjectKey.String()}, true
	case *statev1.GossipEvent_PubliclyAccessible:
		return attrKey{kind: attrPubliclyAccessible}, true
	case *statev1.GossipEvent_Vivaldi:
		return attrKey{kind: attrVivaldi}, true
	case *statev1.GossipEvent_NatType:
		return attrKey{kind: attrNatType}, true
	case *statev1.GossipEvent_ResourceTelemetry:
		return attrKey{kind: attrResourceTelemetry}, true
	case *statev1.GossipEvent_WorkloadSpec:
		if v.WorkloadSpec == nil || v.WorkloadSpec.GetHash() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrWorkloadSpec, name: v.WorkloadSpec.GetHash()}, true
	case *statev1.GossipEvent_WorkloadClaim:
		if v.WorkloadClaim == nil || v.WorkloadClaim.GetHash() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrWorkloadClaim, name: v.WorkloadClaim.GetHash()}, true
	case *statev1.GossipEvent_TrafficHeatmap:
		return attrKey{kind: attrTrafficHeatmap}, true
	default:
		return attrKey{}, false
	}
}

func applyDeleteLocked(rec *nodeRecord, key attrKey) {
	switch key.kind {
	case attrNetwork:
		rec.IPs = nil
		rec.LocalPort = 0
	case attrExternalPort:
		rec.ExternalPort = 0
	case attrObservedExternalIP:
		rec.ObservedExternalIP = ""
	case attrIdentity:
		rec.IdentityPub = nil
		rec.CertExpiry = 0
	case attrService:
		delete(rec.Services, key.name)
	case attrReachability:
		delete(rec.Reachable, key.peer)
	case attrDeny:
	case attrPubliclyAccessible:
		rec.PubliclyAccessible = false
	case attrVivaldi:
		rec.VivaldiCoord = nil
	case attrNatType:
		rec.NatType = nat.Unknown
	case attrResourceTelemetry:
		rec.CPUPercent = 0
		rec.MemPercent = 0
		rec.MemTotalBytes = 0
		rec.NumCPU = 0
	case attrWorkloadSpec:
		delete(rec.WorkloadSpecs, key.name)
	case attrWorkloadClaim:
		delete(rec.WorkloadClaims, key.name)
	case attrTrafficHeatmap:
		rec.TrafficRates = nil
	}
}

func applyValueLocked(rec *nodeRecord, event *statev1.GossipEvent, key attrKey) bool {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		if v.Network == nil {
			return true
		}
		rec.IPs = append([]string(nil), v.Network.GetIps()...)
		rec.LocalPort = v.Network.GetLocalPort()
	case *statev1.GossipEvent_ExternalPort:
		if v.ExternalPort != nil {
			rec.ExternalPort = v.ExternalPort.GetExternalPort()
		}
	case *statev1.GossipEvent_ObservedExternalIp:
		if v.ObservedExternalIp != nil {
			rec.ObservedExternalIP = v.ObservedExternalIp.GetIp()
		}
	case *statev1.GossipEvent_IdentityPub:
		if v.IdentityPub != nil {
			rec.IdentityPub = append([]byte(nil), v.IdentityPub.GetIdentityPub()...)
			rec.CertExpiry = v.IdentityPub.GetCertExpiryUnix()
		}
	case *statev1.GossipEvent_Service:
		if v.Service != nil {
			m := make(map[string]*statev1.Service, len(rec.Services)+1)
			maps.Copy(m, rec.Services)
			m[key.name] = &statev1.Service{Name: key.name, Port: v.Service.GetPort()}
			rec.Services = m
		}
	case *statev1.GossipEvent_Reachability:
		rec.Reachable[key.peer] = struct{}{}
	case *statev1.GossipEvent_PubliclyAccessible:
		rec.PubliclyAccessible = true
	case *statev1.GossipEvent_Vivaldi:
		if v.Vivaldi != nil {
			rec.VivaldiCoord = &coords.Coord{
				X:      v.Vivaldi.GetX(),
				Y:      v.Vivaldi.GetY(),
				Height: v.Vivaldi.GetHeight(),
			}
		}
	case *statev1.GossipEvent_NatType:
		if v.NatType != nil {
			rec.NatType = nat.TypeFromUint32(v.NatType.GetNatType())
		}
	case *statev1.GossipEvent_ResourceTelemetry:
		if v.ResourceTelemetry != nil {
			rec.CPUPercent = v.ResourceTelemetry.GetCpuPercent()
			rec.MemPercent = v.ResourceTelemetry.GetMemPercent()
			rec.MemTotalBytes = v.ResourceTelemetry.GetMemTotalBytes()
			rec.NumCPU = v.ResourceTelemetry.GetNumCpu()
		}
	case *statev1.GossipEvent_WorkloadSpec:
		if v.WorkloadSpec != nil {
			m := make(map[string]*statev1.WorkloadSpecChange, len(rec.WorkloadSpecs)+1)
			maps.Copy(m, rec.WorkloadSpecs)
			m[v.WorkloadSpec.GetHash()] = v.WorkloadSpec
			rec.WorkloadSpecs = m
		}
	case *statev1.GossipEvent_WorkloadClaim:
		if v.WorkloadClaim != nil {
			m := make(map[string]struct{}, len(rec.WorkloadClaims)+1)
			maps.Copy(m, rec.WorkloadClaims)
			m[v.WorkloadClaim.GetHash()] = struct{}{}
			rec.WorkloadClaims = m
		}
	case *statev1.GossipEvent_TrafficHeatmap:
		if v.TrafficHeatmap != nil {
			m := make(map[types.PeerKey]TrafficSnapshot, len(v.TrafficHeatmap.GetRates()))
			for _, r := range v.TrafficHeatmap.GetRates() {
				pk, err := types.PeerKeyFromString(r.GetPeerId())
				if err != nil {
					return false
				}
				m[pk] = TrafficSnapshot{BytesIn: r.GetBytesIn(), BytesOut: r.GetBytesOut()}
			}
			rec.TrafficRates = m
		}
	default:
		return true
	}
	return true
}

func buildEventFromLog(peerIDStr string, key attrKey, entry logEntry, rec nodeRecord) *statev1.GossipEvent {
	event := &statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: entry.Counter,
		Deleted: entry.Deleted,
	}

	switch key.kind {
	case attrNetwork:
		change := &statev1.NetworkChange{}
		if !entry.Deleted {
			change.Ips = append([]string(nil), rec.IPs...)
			change.LocalPort = rec.LocalPort
		}
		event.Change = &statev1.GossipEvent_Network{Network: change}
	case attrExternalPort:
		change := &statev1.ExternalPortChange{}
		if !entry.Deleted {
			change.ExternalPort = rec.ExternalPort
		}
		event.Change = &statev1.GossipEvent_ExternalPort{ExternalPort: change}
	case attrObservedExternalIP:
		change := &statev1.ObservedExternalIPChange{}
		if !entry.Deleted {
			change.Ip = rec.ObservedExternalIP
		}
		event.Change = &statev1.GossipEvent_ObservedExternalIp{ObservedExternalIp: change}
	case attrIdentity:
		change := &statev1.IdentityChange{}
		if !entry.Deleted {
			change.IdentityPub = append([]byte(nil), rec.IdentityPub...)
			change.CertExpiryUnix = rec.CertExpiry
		}
		event.Change = &statev1.GossipEvent_IdentityPub{IdentityPub: change}
	case attrService:
		change := &statev1.ServiceChange{Name: key.name}
		if !entry.Deleted {
			svc := rec.Services[key.name]
			change.Port = svc.GetPort()
		}
		event.Change = &statev1.GossipEvent_Service{Service: change}
	case attrReachability:
		event.Change = &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: key.peer.String()},
		}
	case attrPubliclyAccessible:
		event.Change = &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		}
	case attrVivaldi:
		change := &statev1.VivaldiCoordinateChange{}
		if !entry.Deleted && rec.VivaldiCoord != nil {
			change.X = rec.VivaldiCoord.X
			change.Y = rec.VivaldiCoord.Y
			change.Height = rec.VivaldiCoord.Height
		}
		event.Change = &statev1.GossipEvent_Vivaldi{Vivaldi: change}
	case attrNatType:
		change := &statev1.NatTypeChange{}
		if !entry.Deleted {
			change.NatType = rec.NatType.ToUint32()
		}
		event.Change = &statev1.GossipEvent_NatType{NatType: change}
	case attrResourceTelemetry:
		change := &statev1.ResourceTelemetryChange{}
		if !entry.Deleted {
			change.CpuPercent = rec.CPUPercent
			change.MemPercent = rec.MemPercent
			change.MemTotalBytes = rec.MemTotalBytes
			change.NumCpu = rec.NumCPU
		}
		event.Change = &statev1.GossipEvent_ResourceTelemetry{ResourceTelemetry: change}
	case attrDeny:
		subjectKey, err := types.PeerKeyFromString(key.name)
		if err != nil {
			panic("invalid deny key in log")
		}
		event.Change = &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: append([]byte(nil), subjectKey[:]...)},
		}
	case attrWorkloadSpec:
		change := &statev1.WorkloadSpecChange{Hash: key.name}
		if !entry.Deleted {
			if spec, ok := rec.WorkloadSpecs[key.name]; ok {
				change.Replicas = spec.GetReplicas()
				change.MemoryPages = spec.GetMemoryPages()
				change.TimeoutMs = spec.GetTimeoutMs()
			}
		}
		event.Change = &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: change}
	case attrWorkloadClaim:
		event.Change = &statev1.GossipEvent_WorkloadClaim{
			WorkloadClaim: &statev1.WorkloadClaimChange{Hash: key.name},
		}
	case attrTrafficHeatmap:
		change := &statev1.TrafficHeatmapChange{}
		if !entry.Deleted {
			for pk, rate := range rec.TrafficRates {
				change.Rates = append(change.Rates, &statev1.TrafficRate{
					PeerId:   pk.String(),
					BytesIn:  rate.BytesIn,
					BytesOut: rate.BytesOut,
				})
			}
		}
		event.Change = &statev1.GossipEvent_TrafficHeatmap{TrafficHeatmap: change}
	}

	return event
}
