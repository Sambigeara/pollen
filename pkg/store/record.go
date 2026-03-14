package store

import (
	"encoding/binary"
	"maps"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/topology"
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

func networkAttrKey() attrKey {
	return attrKey{kind: attrNetwork}
}

func externalPortAttrKey() attrKey {
	return attrKey{kind: attrExternalPort}
}

func observedExternalIPAttrKey() attrKey {
	return attrKey{kind: attrObservedExternalIP}
}

func identityAttrKey() attrKey {
	return attrKey{kind: attrIdentity}
}

func serviceAttrKey(name string) attrKey {
	return attrKey{kind: attrService, name: name}
}

func reachabilityAttrKey(peerID types.PeerKey) attrKey {
	return attrKey{kind: attrReachability, peer: peerID}
}

func denyAttrKey(subjectPubHex string) attrKey {
	return attrKey{kind: attrDeny, name: subjectPubHex}
}

func publiclyAccessibleAttrKey() attrKey {
	return attrKey{kind: attrPubliclyAccessible}
}

func vivaldiAttrKey() attrKey {
	return attrKey{kind: attrVivaldi}
}

func natTypeAttrKey() attrKey {
	return attrKey{kind: attrNatType}
}

func resourceTelemetryAttrKey() attrKey {
	return attrKey{kind: attrResourceTelemetry}
}

func workloadSpecAttrKey(hash string) attrKey {
	return attrKey{kind: attrWorkloadSpec, name: hash}
}

func workloadClaimAttrKey(hash string) attrKey {
	return attrKey{kind: attrWorkloadClaim, name: hash}
}

func trafficHeatmapAttrKey() attrKey {
	return attrKey{kind: attrTrafficHeatmap}
}

type trafficRate struct {
	BytesIn  uint64
	BytesOut uint64
}

func tombstoneStaleAttrs(rec *nodeRecord) {
	for _, key := range []attrKey{publiclyAccessibleAttrKey(), natTypeAttrKey(), observedExternalIPAttrKey(), resourceTelemetryAttrKey(), trafficHeatmapAttrKey()} {
		rec.maxCounter++
		rec.log[key] = logEntry{Counter: rec.maxCounter, Deleted: true}
	}
}

type logEntry struct {
	Counter uint64
	Deleted bool
}

type nodeRecord struct {
	TrafficRates       map[types.PeerKey]trafficRate
	Services           map[string]*statev1.Service
	WorkloadSpecs      map[string]*statev1.WorkloadSpecChange
	WorkloadClaims     map[string]struct{}
	log                map[attrKey]logEntry
	VivaldiCoord       *topology.Coord
	Reachable          map[types.PeerKey]struct{}
	LastAddr           string
	ObservedExternalIP string
	IPs                []string
	IdentityPub        []byte
	maxCounter         uint64
	MemTotalBytes      uint64
	NatType            nat.Type
	CertExpiry         int64
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	PubliclyAccessible bool
}

func (r nodeRecord) clone() nodeRecord {
	c := r
	if r.Services != nil {
		c.Services = make(map[string]*statev1.Service, len(r.Services))
		maps.Copy(c.Services, r.Services)
	}
	if r.Reachable != nil {
		c.Reachable = make(map[types.PeerKey]struct{}, len(r.Reachable))
		maps.Copy(c.Reachable, r.Reachable)
	}
	if r.WorkloadSpecs != nil {
		c.WorkloadSpecs = make(map[string]*statev1.WorkloadSpecChange, len(r.WorkloadSpecs))
		maps.Copy(c.WorkloadSpecs, r.WorkloadSpecs)
	}
	if r.WorkloadClaims != nil {
		c.WorkloadClaims = make(map[string]struct{}, len(r.WorkloadClaims))
		maps.Copy(c.WorkloadClaims, r.WorkloadClaims)
	}
	if r.TrafficRates != nil {
		c.TrafficRates = make(map[types.PeerKey]trafficRate, len(r.TrafficRates))
		maps.Copy(c.TrafficRates, r.TrafficRates)
	}
	c.IPs = append([]string(nil), r.IPs...)
	return c
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// computePeerHashLocked returns an order-independent XOR of FNV-1a hashes
// over a peer's compacted log. Two peers with identical log contents produce
// identical hashes regardless of iteration order.
func computePeerHashLocked(rec nodeRecord) uint64 {
	var digest uint64
	for key, entry := range rec.log {
		h := uint64(fnvOffset64)
		mix := func(b []byte) {
			for _, v := range b {
				h ^= uint64(v)
				h *= fnvPrime64
			}
		}
		mix([]byte{byte(key.kind)})
		mix([]byte(key.name))
		mix(key.peer[:])
		var buf [9]byte
		binary.LittleEndian.PutUint64(buf[:8], entry.Counter)
		if entry.Deleted {
			buf[8] = 1
		}
		mix(buf[:])
		digest ^= h
	}
	return digest
}
