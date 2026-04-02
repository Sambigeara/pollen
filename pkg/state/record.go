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
	attrObservedAddress
	attrCertExpiry
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

// ephemeralSingletonAttrs are attribute kinds that represent transient runtime
// state with a fixed key (no name/peer). On restart these are tombstoned
// unconditionally so stale values don't survive gossip from peers that still
// hold the previous session's state. When adding a new singleton ephemeral
// attrKind, add it here.
var ephemeralSingletonAttrs = [...]attrKind{
	attrObservedAddress,
	attrPubliclyAccessible,
	attrNatType,
	attrResourceTelemetry,
	attrTrafficHeatmap,
}

// ephemeralKeyed reports whether a keyed attribute kind (name/peer-scoped) is
// ephemeral. Keyed ephemeral attrs are tombstoned on restart only if they
// already exist in the log, since the key set is dynamic.
func (k attrKind) ephemeralKeyed() bool {
	return k == attrReachability
}

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
	NodeView
	log        map[attrKey]logEntry
	maxCounter uint64
}

func (r nodeRecord) clone() nodeRecord {
	c := r
	c.NodeView = r.NodeView.clone()
	return c
}

func newNodeRecord(peerID types.PeerKey) nodeRecord {
	return nodeRecord{
		NodeView: NodeView{
			PeerPub:        peerID.Bytes(),
			Reachable:      make(map[types.PeerKey]struct{}),
			Services:       make(map[string]*Service),
			WorkloadSpecs:  make(map[string]*statev1.WorkloadSpecChange),
			WorkloadClaims: make(map[string]struct{}),
		},
		log: make(map[attrKey]logEntry),
	}
}

func tombstoneStaleAttrs(rec *nodeRecord) {
	for _, kind := range ephemeralSingletonAttrs {
		rec.maxCounter++
		rec.log[attrKey{kind: kind}] = logEntry{Counter: rec.maxCounter, Deleted: true}
	}
	for key := range rec.log {
		if key.kind.ephemeralKeyed() {
			rec.maxCounter++
			rec.log[key] = logEntry{Counter: rec.maxCounter, Deleted: true}
		}
	}
}

func eventAttrKey(event *statev1.GossipEvent) (attrKey, bool) {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		return attrKey{kind: attrNetwork}, true
	case *statev1.GossipEvent_ObservedAddress:
		return attrKey{kind: attrObservedAddress}, true
	case *statev1.GossipEvent_CertExpiry:
		return attrKey{kind: attrCertExpiry}, true
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
		subjectKey := types.PeerKeyFromBytes(v.Deny.GetPeerPub())
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
	case attrObservedAddress:
		rec.ExternalPort = 0
		rec.ObservedExternalIP = ""
	case attrCertExpiry:
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
		rec.VivaldiErr = 0
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
	case *statev1.GossipEvent_ObservedAddress:
		if v.ObservedAddress != nil {
			rec.ObservedExternalIP = v.ObservedAddress.GetIp()
			rec.ExternalPort = v.ObservedAddress.GetPort()
		}
	case *statev1.GossipEvent_CertExpiry:
		if v.CertExpiry != nil {
			rec.CertExpiry = v.CertExpiry.GetExpiryUnix()
		}
	case *statev1.GossipEvent_Service:
		if v.Service != nil {
			m := make(map[string]*Service, len(rec.Services)+1)
			maps.Copy(m, rec.Services)
			m[key.name] = &Service{Name: key.name, Port: v.Service.GetPort()}
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
			rec.VivaldiErr = v.Vivaldi.GetError()
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
	case attrObservedAddress:
		change := &statev1.ObservedAddressChange{}
		if !entry.Deleted {
			change.Ip = rec.ObservedExternalIP
			change.Port = rec.ExternalPort
		}
		event.Change = &statev1.GossipEvent_ObservedAddress{ObservedAddress: change}
	case attrCertExpiry:
		change := &statev1.CertExpiryChange{}
		if !entry.Deleted {
			change.ExpiryUnix = rec.CertExpiry
		}
		event.Change = &statev1.GossipEvent_CertExpiry{CertExpiry: change}
	case attrService:
		change := &statev1.ServiceChange{Name: key.name}
		if !entry.Deleted {
			svc := rec.Services[key.name]
			change.Port = svc.Port
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
			change.Error = rec.VivaldiErr
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
			Deny: &statev1.DenyChange{PeerPub: append([]byte(nil), subjectKey[:]...)},
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
