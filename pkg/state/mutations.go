// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/netip"
	"slices"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}, AddressesChanged{Peer: s.localID}}
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
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}, AddressesChanged{Peer: s.localID}}
	})
}

// PublishWorkload emits the spec and the publisher's claim in a single
// gossip batch. Splitting them lets a remote see the spec first, observe
// zero claimants, and decide to claim before the publisher's own claim
// arrives — causing over-replication until it unwinds minutes later.
func (s *store) PublishWorkload(spec WorkloadSpec) ([]Event, error) {
	hash := spec.Hash
	owned := workloadSpecToProto(spec)
	var ownerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		for pk, r := range s.nodes {
			if pk == s.localID {
				continue
			}
			if ev, ok := r.log[attrKey{kind: attrWorkloadSpec, name: hash}]; ok && !ev.Deleted && s.isValidOwnerLocked(pk) {
				ownerErr = fmt.Errorf("%w: %s owns %s", ErrSpecOwnedByPeer, pk.Short(), hash)
				return nil, nil
			}
		}

		var gossips []*statev1.GossipEvent
		if ev, ok := rec.log[attrKey{kind: attrWorkloadSpec, name: hash}]; !ok || ev.Deleted || !proto.Equal(ev.GetWorkloadSpec(), owned) {
			gossips = append(gossips, &statev1.GossipEvent{Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: owned}})
		}
		if ev, ok := rec.log[attrKey{kind: attrWorkloadClaim, name: hash}]; !ok || ev.Deleted {
			gossips = append(gossips, &statev1.GossipEvent{Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: hash}}})
		}
		if len(gossips) == 0 {
			return nil, nil
		}
		return gossips, []Event{WorkloadChanged{Hash: hash}}
	})
	return events, ownerErr
}

func (s *store) DeleteWorkloadSpec(hash string) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrWorkloadSpec, name: hash}]
		if !ok || ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash}}}
		return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
	})
}

func (s *store) ClaimWorkload(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, true, false)
}

func (s *store) ReleaseWorkload(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, false, false)
}

// MarkWorkloadDraining flips an existing claim into the draining state.
// The claim stays gossiped — callers continue routing to this peer until
// the actual ReleaseWorkload — but other peers see the draining flag and
// can issue a replacement claim during the drain window so make-before-
// break overlap is preserved across the handover. No-op if the local node
// is not currently claiming the workload.
func (s *store) MarkWorkloadDraining(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, true, true)
}

func (s *store) setWorkloadClaimLocked(hash string, claimed, draining bool) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrWorkloadClaim, name: hash}]
		exists := ok && !ev.Deleted
		var currDraining bool
		if exists {
			currDraining = ev.GetWorkloadClaim().GetDraining()
		}

		// MarkWorkloadDraining on a non-claimant is a no-op; releasing a
		// non-claim is a no-op; re-claiming an already-active claim is a
		// no-op; flipping the draining flag emits a fresh event.
		if !claimed && !exists {
			return nil, nil
		}
		if claimed && exists && draining == currDraining {
			return nil, nil
		}
		if !claimed && !exists {
			return nil, nil
		}

		change := &statev1.GossipEvent{
			Deleted: !claimed,
			Change: &statev1.GossipEvent_WorkloadClaim{
				WorkloadClaim: &statev1.WorkloadClaimChange{
					Hash:     hash,
					Draining: draining,
				},
			},
		}
		return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
	})
}

func (s *store) SetLocalResources(r NodeResources) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrResourceTelemetry}]; ok && !ev.Deleted {
			rt := ev.GetResourceTelemetry()

			cpuDelta := rt.CpuPercent - r.CPUPercent
			if r.CPUPercent > rt.CpuPercent {
				cpuDelta = r.CPUPercent - rt.CpuPercent
			}
			memDelta := rt.MemPercent - r.MemPercent
			if r.MemPercent > rt.MemPercent {
				memDelta = r.MemPercent - rt.MemPercent
			}

			if cpuDelta < 2 && memDelta < 2 &&
				rt.MemTotalBytes == r.MemTotalBytes && rt.NumCpu == r.NumCPU {
				return nil, nil
			}
		}

		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_ResourceTelemetry{ResourceTelemetry: &statev1.ResourceTelemetryChange{
			CpuPercent:    r.CPUPercent,
			MemPercent:    r.MemPercent,
			MemTotalBytes: r.MemTotalBytes,
			NumCpu:        r.NumCPU,
		}}}
		return []*statev1.GossipEvent{change}, []Event{TopologyChanged{Peer: s.localID}}
	})
}

// SetBackoffTTL publishes a BackoffTTL gossip event with the
// emitter's-clock absolute expiry. Idempotent — repeats with the
// same expires_at are skipped.
func (s *store) SetBackoffTTL(expiresAt time.Time) []Event {
	expiresAtMs := expiresAt.UnixMilli()
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrBackoffTTL}]; ok && !ev.Deleted {
			if cur := ev.GetBackoffTtl(); cur != nil && cur.ExpiresAtUnixMs == expiresAtMs {
				return nil, nil
			}
		}
		change := &statev1.BackoffTTLChange{ExpiresAtUnixMs: expiresAtMs}
		return []*statev1.GossipEvent{
			{Change: &statev1.GossipEvent_BackoffTtl{BackoffTtl: change}},
		}, nil
	})
}

// SetPerSeedCallCounts publishes the per-seed call-count window for
// this node. Replaces any prior counts; an empty map publishes a
// tombstone so peers can drop our last window.
func (s *store) SetPerSeedCallCounts(counts map[string]uint64) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if len(counts) == 0 {
			if ev, ok := rec.log[attrKey{kind: attrPerSeedCallCounts}]; ok && ev.Deleted {
				return nil, nil
			}
			change := &statev1.PerSeedCallCountsChange{}
			return []*statev1.GossipEvent{
				{Deleted: true, Change: &statev1.GossipEvent_PerSeedCallCounts{PerSeedCallCounts: change}},
			}, nil
		}
		change := &statev1.PerSeedCallCountsChange{Counts: counts}
		return []*statev1.GossipEvent{
			{Change: &statev1.GossipEvent_PerSeedCallCounts{PerSeedCallCounts: change}},
		}, nil
	})
}

func (s *store) SetService(port uint32, name string, protocol statev1.ServiceProtocol, properties *structpb.Struct) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		owned := &statev1.ServiceChange{Name: name, Port: port, Protocol: protocol, Properties: properties}
		if ev, ok := rec.log[attrKey{kind: attrService, name: name}]; ok && !ev.Deleted && proto.Equal(ev.GetService(), owned) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_Service{Service: owned}}}, []Event{ServiceChanged{Peer: s.localID, Name: name}}
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

func (s *store) SetStaticSpec(spec StaticSpec) ([]Event, error) {
	name := spec.Name
	digest, err := hex.DecodeString(spec.ManifestDigest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.ManifestDigest)
	}
	var ownerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		for pk, r := range s.nodes {
			if pk == s.localID {
				continue
			}
			if ev, ok := r.log[attrKey{kind: attrStaticSpec, name: name}]; ok && !ev.Deleted && s.isValidOwnerLocked(pk) {
				ownerErr = fmt.Errorf("%w: %s owns %q", ErrSpecOwnedByPeer, pk.Short(), name)
				return nil, nil
			}
		}
		owned := &statev1.StaticSpecChange{
			Name:           name,
			ManifestDigest: digest,
		}
		if ev, ok := rec.log[attrKey{kind: attrStaticSpec, name: name}]; ok && !ev.Deleted && proto.Equal(ev.GetStaticSpec(), owned) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_StaticSpec{StaticSpec: owned}}}, []Event{StaticChanged{Name: name}}
	})
	return events, ownerErr
}

func (s *store) DeleteStaticSpec(name string) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrStaticSpec, name: name}]
		if !ok || ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_StaticSpec{StaticSpec: &statev1.StaticSpecChange{Name: name}}}
		return []*statev1.GossipEvent{change}, []Event{StaticChanged{Name: name}}
	})
}

func (s *store) ClaimStatic(name string) []Event {
	return s.setStaticClaimLocked(name, true)
}

func (s *store) ReleaseStatic(name string) []Event {
	return s.setStaticClaimLocked(name, false)
}

func (s *store) setStaticClaimLocked(name string, claimed bool) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrStaticClaim, name: name}]
		exists := ok && !ev.Deleted
		if claimed == exists {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: !claimed, Change: &statev1.GossipEvent_StaticClaim{StaticClaim: &statev1.StaticClaimChange{Name: name}}}
		return []*statev1.GossipEvent{change}, []Event{StaticChanged{Name: name}}
	})
}

func (s *store) SetBlobSpec(spec BlobSpec) ([]Event, error) {
	digest, err := hex.DecodeString(spec.Digest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.Digest)
	}
	owned := &statev1.BlobSpecChange{
		Name:   spec.Name,
		Digest: digest,
	}
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{kind: attrBlobSpec, name: spec.Digest}
		if ev, ok := rec.log[key]; ok && !ev.Deleted && proto.Equal(ev.GetBlobSpec(), owned) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_BlobSpec{BlobSpec: owned}}}, nil
	})
	return events, nil
}

func (s *store) DeleteBlobSpec(digest string) []Event {
	raw, err := hex.DecodeString(digest)
	if err != nil || len(raw) != sha256Len {
		return nil
	}
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{kind: attrBlobSpec, name: digest}
		ev, ok := rec.log[key]
		if !ok || ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_BlobSpec{BlobSpec: &statev1.BlobSpecChange{Digest: raw}}}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) SetLocalBlobs(digests []string) []Event {
	want := make([][]byte, 0, len(digests))
	seen := make(map[string]struct{}, len(digests))
	for _, h := range digests {
		if _, dup := seen[h]; dup {
			continue
		}
		seen[h] = struct{}{}
		raw, err := hex.DecodeString(h)
		if err != nil || len(raw) != sha256Len {
			continue
		}
		want = append(want, raw)
	}
	slices.SortFunc(want, bytes.Compare)

	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrBlobAvailability}]; ok {
			if ev.Deleted && len(want) == 0 {
				return nil, nil
			}
			if !ev.Deleted && slices.EqualFunc(ev.GetBlobAvailability().GetDigests(), want, bytes.Equal) {
				return nil, nil
			}
		}
		change := &statev1.GossipEvent{
			Deleted: len(want) == 0,
			Change:  &statev1.GossipEvent_BlobAvailability{BlobAvailability: &statev1.BlobAvailabilityChange{Digests: want}},
		}
		return []*statev1.GossipEvent{change}, nil
	})
}

const sha256Len = 32

func (s *store) SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		var rates []*statev1.TrafficRate
		var updated bool

		if ev, ok := rec.log[attrKey{kind: attrTrafficHeatmap}]; ok && !ev.Deleted { //nolint:nestif
			for _, r := range ev.GetTrafficHeatmap().Rates {
				if r.PeerId == peer.String() {
					if r.RateIn == in && r.RateOut == out {
						return nil, nil
					}
					updated = true
					if in > 0 || out > 0 {
						rates = append(rates, &statev1.TrafficRate{PeerId: r.PeerId, RateIn: in, RateOut: out})
					}
				} else {
					rates = append(rates, r)
				}
			}
		}

		if !updated && (in > 0 || out > 0) {
			rates = append(rates, &statev1.TrafficRate{PeerId: peer.String(), RateIn: in, RateOut: out})
		}

		change := &statev1.GossipEvent{
			Deleted: len(rates) == 0,
			Change:  &statev1.GossipEvent_TrafficHeatmap{TrafficHeatmap: &statev1.TrafficHeatmapChange{Rates: rates}},
		}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) SetPublic() {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrPubliclyAccessible}]; ok && !ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_PubliclyAccessible{PubliclyAccessible: &statev1.PubliclyAccessibleChange{}}}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) SetAdmin() {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrAdminCapable}]; ok && !ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_AdminCapable{AdminCapable: &statev1.AdminCapableChange{}}}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) ClearAdmin() {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrAdminCapable}]; !ok || ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_AdminCapable{AdminCapable: &statev1.AdminCapableChange{}}}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) SetStaticCapable() {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[attrKey{kind: attrStaticCapable}]; ok && !ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_StaticCapable{StaticCapable: &statev1.StaticCapableChange{}}}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) SetNodeName(name string) {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{kind: attrNodeName}
		if name == "" {
			if ev, ok := rec.log[key]; !ok || ev.Deleted {
				return nil, nil
			}
			change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_NodeName{NodeName: &statev1.NodeNameChange{Name: name}}}
			return []*statev1.GossipEvent{change}, nil
		}
		if ev, ok := rec.log[key]; ok && !ev.Deleted && ev.GetNodeName().Name == name {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_NodeName{NodeName: &statev1.NodeNameChange{Name: name}}}
		return []*statev1.GossipEvent{change}, nil
	})
}
