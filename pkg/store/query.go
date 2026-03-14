package store

import (
	"maps"
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

// KnownPeer holds connection metadata for a known peer.
type KnownPeer struct {
	VivaldiCoord       *topology.Coord
	LastAddr           string
	ObservedExternalIP string
	IdentityPub        []byte
	IPs                []string
	NatType            nat.Type
	LocalPort          uint32
	ExternalPort       uint32
	PeerID             types.PeerKey
	PubliclyAccessible bool
}

// WorkloadSpecView is a merged view of a workload spec and its publisher.
type WorkloadSpecView struct {
	Spec      *statev1.WorkloadSpecChange
	Publisher types.PeerKey
}

// NodePlacementState holds the data the scheduler needs for traffic-aware placement.
type NodePlacementState struct {
	Coord         *topology.Coord
	TrafficTo     map[types.PeerKey]uint64
	MemTotalBytes uint64
	CPUPercent    uint32
	MemPercent    uint32
	NumCPU        uint32
}

// TrafficSnapshot holds a single peer's accumulated traffic counters.
type TrafficSnapshot struct {
	BytesIn  uint64
	BytesOut uint64
}

// RouteNodeInfo holds the per-node data needed for route computation and status display.
type RouteNodeInfo struct {
	Services           map[string]*statev1.Service
	Reachable          map[types.PeerKey]struct{}
	Coord              *topology.Coord
	ObservedExternalIP string
	IPs                []string
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	PubliclyAccessible bool
}

// LocalNodeView holds a read-only view of the local node's state.
type LocalNodeView struct {
	WorkloadClaims     map[string]struct{}
	ObservedExternalIP string
	IPs                []string
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	PubliclyAccessible bool
}

// getRecord returns a cloned nodeRecord for the given peer.
// Used only within the package (including tests).
func (s *Store) getRecord(peerID types.PeerKey) (nodeRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return nodeRecord{}, false
	}
	return rec.clone(), true
}

// AllRouteInfo returns per-node data for route computation and status display.
// Includes all valid (non-denied, non-expired) nodes.
func (s *Store) AllRouteInfo() map[types.PeerKey]RouteNodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	result := make(map[types.PeerKey]RouteNodeInfo, len(valid))
	for pk, rec := range valid {
		result[pk] = RouteNodeInfo{
			Services:           rec.Services,
			IPs:                rec.IPs,
			Reachable:          rec.Reachable,
			Coord:              rec.VivaldiCoord,
			ObservedExternalIP: rec.ObservedExternalIP,
			PubliclyAccessible: rec.PubliclyAccessible,
			LocalPort:          rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			CPUPercent:         rec.CPUPercent,
			MemPercent:         rec.MemPercent,
			NumCPU:             rec.NumCPU,
		}
	}
	return result
}

// ExternalPort returns the external (STUN-observed) port for the given peer.
func (s *Store) ExternalPort(peerID types.PeerKey) (uint32, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok || rec.ExternalPort == 0 {
		return 0, false
	}
	return rec.ExternalPort, true
}

// LocalRecord returns a read-only view of the local node's state.
func (s *Store) LocalRecord() LocalNodeView {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec := s.nodes[s.localID]
	claims := make(map[string]struct{}, len(rec.WorkloadClaims))
	maps.Copy(claims, rec.WorkloadClaims)
	return LocalNodeView{
		IPs:                rec.IPs,
		LocalPort:          rec.LocalPort,
		ExternalPort:       rec.ExternalPort,
		PubliclyAccessible: rec.PubliclyAccessible,
		ObservedExternalIP: rec.ObservedExternalIP,
		CPUPercent:         rec.CPUPercent,
		MemPercent:         rec.MemPercent,
		NumCPU:             rec.NumCPU,
		WorkloadClaims:     claims,
	}
}

func (s *Store) NodeIPs(peerID types.PeerKey) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok || len(rec.IPs) == 0 {
		return nil
	}

	return rec.IPs
}

func (s *Store) IsConnected(source, target types.PeerKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[source]
	if !ok {
		return false
	}

	_, ok = rec.Reachable[target]
	return ok
}

func (s *Store) HasServicePort(peerID types.PeerKey, port uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	for _, svc := range rec.Services {
		if svc.GetPort() == port {
			return true
		}
	}
	return false
}

func (s *Store) KnownPeers() []KnownPeer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	known := make([]KnownPeer, 0, len(valid))
	for peerID, rec := range valid {
		if peerID == s.localID {
			continue
		}
		if rec.LastAddr == "" && (len(rec.IPs) == 0 || rec.LocalPort == 0) {
			continue
		}
		known = append(known, KnownPeer{
			PeerID:             peerID,
			LocalPort:          rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			ObservedExternalIP: rec.ObservedExternalIP,
			NatType:            rec.NatType,
			IdentityPub:        rec.IdentityPub,
			IPs:                rec.IPs,
			LastAddr:           rec.LastAddr,
			PubliclyAccessible: rec.PubliclyAccessible,
			VivaldiCoord:       rec.VivaldiCoord,
		})
	}

	slices.SortFunc(known, func(a, b KnownPeer) int {
		return a.PeerID.Compare(b.PeerID)
	})

	return known
}

func (s *Store) IsPubliclyAccessible(peerID types.PeerKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	return rec.PubliclyAccessible
}

func (s *Store) PeerVivaldiCoord(peerID types.PeerKey) (*topology.Coord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return nil, false
	}
	return rec.VivaldiCoord, rec.VivaldiCoord != nil
}

func (s *Store) NatType(peerID types.PeerKey) nat.Type {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return nat.Unknown
	}
	return rec.NatType
}

func (s *Store) IdentityPub(peerID types.PeerKey) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok || len(rec.IdentityPub) == 0 {
		return nil, false
	}

	return rec.IdentityPub, true
}

func (s *Store) IsDenied(subjectPub []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.denied[types.PeerKeyFromBytes(subjectPub)]
	return ok
}

func (s *Store) LocalServices() map[string]*statev1.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()

	local := s.nodes[s.localID]
	return maps.Clone(local.Services)
}

// AllPeerKeys returns the PeerKey for every valid, live (reachable from the
// local node) peer. Dead nodes are excluded so the scheduler never assigns
// workload slots to peers that can't fulfil them.
func (s *Store) AllPeerKeys() []types.PeerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	live := s.liveComponentLocked(s.validNodesLocked())
	keys := make([]types.PeerKey, 0, len(live))
	for pk := range live {
		keys = append(keys, pk)
	}
	return keys
}

// AllWorkloadSpecs returns hash → spec merged across valid (non-denied, non-expired) nodes.
// When multiple peers publish a spec for the same hash, the lowest PeerKey
// wins to ensure deterministic conflict resolution across all nodes.
func (s *Store) AllWorkloadSpecs() map[string]WorkloadSpecView {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	out := make(map[string]WorkloadSpecView)
	for peerID, rec := range valid {
		for hash, spec := range rec.WorkloadSpecs {
			existing, ok := out[hash]
			if !ok || peerID.Compare(existing.Publisher) < 0 {
				out[hash] = WorkloadSpecView{Spec: spec, Publisher: peerID}
			}
		}
	}
	return out
}

// AllWorkloadClaims returns hash → set of claimant PeerKeys from valid nodes.
// Claims from remote peers outside the local node's connected component in
// the reachability graph are excluded so that dead-node claims don't block
// under-replication recovery.
func (s *Store) AllWorkloadClaims() map[string]map[types.PeerKey]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	live := s.liveComponentLocked(valid)

	out := make(map[string]map[types.PeerKey]struct{})
	for peerID, rec := range valid {
		if _, ok := live[peerID]; !ok {
			continue
		}
		for hash := range rec.WorkloadClaims {
			if out[hash] == nil {
				out[hash] = make(map[types.PeerKey]struct{})
			}
			out[hash][peerID] = struct{}{}
		}
	}
	return out
}

// AllTrafficHeatmaps returns nodeID → peerID → TrafficSnapshot for valid nodes.
func (s *Store) AllTrafficHeatmaps() map[types.PeerKey]map[types.PeerKey]TrafficSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	out := make(map[types.PeerKey]map[types.PeerKey]TrafficSnapshot, len(valid))
	for pk, rec := range valid {
		if len(rec.TrafficRates) == 0 {
			continue
		}
		m := make(map[types.PeerKey]TrafficSnapshot, len(rec.TrafficRates))
		for peer, rate := range rec.TrafficRates {
			m[peer] = TrafficSnapshot(rate)
		}
		out[pk] = m
	}
	return out
}

// AllNodePlacementStates returns resource, coordinate, and traffic data for
// every valid, live node, suitable for the scheduler's placement scoring
// function. Dead nodes are excluded to stay consistent with AllPeerKeys and
// AllWorkloadClaims.
func (s *Store) AllNodePlacementStates() map[types.PeerKey]NodePlacementState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	live := s.liveComponentLocked(valid)
	out := make(map[types.PeerKey]NodePlacementState, len(live))
	for pk := range live {
		rec := valid[pk]
		nps := NodePlacementState{
			CPUPercent:    rec.CPUPercent,
			MemPercent:    rec.MemPercent,
			MemTotalBytes: rec.MemTotalBytes,
			NumCPU:        rec.NumCPU,
		}
		if rec.VivaldiCoord != nil {
			coord := *rec.VivaldiCoord
			nps.Coord = &coord
		}
		if len(rec.TrafficRates) > 0 {
			nps.TrafficTo = make(map[types.PeerKey]uint64, len(rec.TrafficRates))
			for peer, rate := range rec.TrafficRates {
				nps.TrafficTo[peer] = rate.BytesIn + rate.BytesOut
			}
		}
		out[pk] = nps
	}
	return out
}

// ResolveWorkloadPrefix resolves a hash prefix to a full workload hash from
// valid (non-denied, non-expired) nodes' specs. Returns ("", false) if no
// match, or ("", true, false) if multiple specs match the prefix.
func (s *Store) ResolveWorkloadPrefix(prefix string) (hash string, ambiguous, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valid := s.validNodesLocked()
	var match string
	for _, rec := range valid {
		for h := range rec.WorkloadSpecs {
			if len(h) >= len(prefix) && h[:len(prefix)] == prefix {
				if match != "" && match != h {
					return "", true, false
				}
				match = h
			}
		}
	}
	if match == "" {
		return "", false, false
	}
	return match, false, true
}
