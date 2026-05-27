// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/proto"
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
	denyOrGrantChanged := false
	grantChanged := false
	for _, ev := range gossips {
		key, _ := getAttrKey(ev)
		if key.kind == attrDeny || key.kind == attrGrant {
			denyOrGrantChanged = true
		}
		if key.kind == attrGrant {
			grantChanged = true
		}
		rec.maxCounter++
		ev.PeerId = s.localID.String()
		ev.Counter = rec.maxCounter
		rec.put(key, ev)
		s.pendingGossip = append(s.pendingGossip, ev)
	}

	rec.lastEventAt = now
	s.lastLocalEmit = now
	s.nodes[s.localID] = rec

	if denyOrGrantChanged {
		events = append(events, s.recomputeDeniedLocked()...)
	}
	if grantChanged {
		events = append(events, GrantChanged{Peer: s.localID})
	}

	s.updateSnapshotLocked()
	s.notify()

	return events
}

func (s *store) DenyPeer(key types.PeerKey) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ak := attrKey{kind: attrDeny, name: key.String()}
		if ev, ok := rec.log[ak]; ok && !ev.Deleted {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_Deny{Deny: &statev1.DenyChange{PeerPub: key.Bytes()}}}
		// PeerDenied for the new effective denied set is emitted by the
		// recompute pass triggered after mutateLocal commits, so that
		// cascade victims (subtree of a revoked admin) are also reported.
		return []*statev1.GossipEvent{change}, nil
	})
}

// SetLocalGrant publishes the local node's current grant into the CRDT
// so every other node can evaluate chain-scoped rules (most
// importantly: subtree-bounded deny authorisation).
//
// subjectSig must be a valid ed25519 signature by grant.subject_pub
// (== local node identity) over the grant's claims, produced by
// identity.SignGrantSubject. Without subject proof-of-possession, any
// admin could forge a grant for another peer's pub and bypass deny
// scoping. Callers in production wire this from the signing key; tests
// that exercise non-grant paths can leave subjectSig empty (apply-time
// verification is skipped when rootPub is unset).
func (s *store) SetLocalGrant(grant *identityv1.Grant, subjectSig []byte) []Event {
	if grant == nil {
		return nil
	}
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		owned := &statev1.GrantChange{Grant: grant, SubjectSignature: subjectSig}
		if ev, ok := rec.log[attrKey{kind: attrGrant}]; ok && !ev.Deleted && proto.Equal(ev.GetGrant(), owned) {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_Grant{Grant: owned}}
		return []*statev1.GossipEvent{change}, nil
	})
}

// RegisterPeerGrant adopts a wire caller's grant into that caller's CRDT
// slot so cluster-scoped admission can resolve the authority for a
// presigned Fact whose publisher runs no daemon to gossip its own grant.
// The serving node is a pure relay: the grant is admitted only if it
// clears the identical proof-of-possession gate (isAcceptableGrantEvent)
// a gossiped grant must, so no new trust is introduced. Idempotent: a
// grant whose content already matches the stored one is a no-op, so
// steady-state publishing does not churn the slot or the gossip stream.
func (s *store) RegisterPeerGrant(peer types.PeerKey, grant *identityv1.Grant, subjectSig []byte) []Event {
	if grant == nil || peer == s.localID {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	change := &statev1.GossipEvent{
		PeerId: peer.String(),
		Change: &statev1.GossipEvent_Grant{Grant: &statev1.GrantChange{Grant: grant, SubjectSignature: subjectSig}},
	}
	if !s.isAcceptableGrantEvent(peer, change) {
		return nil
	}

	key := attrKey{kind: attrGrant}
	rec, exists := s.nodes[peer]
	if !exists {
		rec = newNodeRecord()
	}
	if ev, ok := rec.log[key]; ok && !ev.Deleted && proto.Equal(ev.GetGrant(), change.GetGrant()) {
		return nil
	}

	rec.maxCounter++
	change.Counter = rec.maxCounter
	rec.put(key, change)
	rec.lastEventAt = s.nowFunc()
	s.nodes[peer] = rec
	s.pendingGossip = append(s.pendingGossip, change)

	events := append([]Event{GrantChanged{Peer: peer}}, s.recomputeDeniedLocked()...)
	s.updateSnapshotLocked()
	s.notify()
	return events
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

// ownSpecEventLocked returns the rank-winning spec event of kind whose
// content id (workload hash, blob digest, static name) equals want,
// scoped to publications this node authored. The k.peer == s.localID
// filter on the register key picks out registers under our authority
// regardless of which slot stores them: a daemon publish lands in this
// node's own slot, a wire publish in the serving relay's slot.
// Supersession reuses specRank so a stale tombstone never masks a newer
// re-seed, mirroring the snapshot reconciler.
func (s *store) ownSpecEventLocked(kind attrKind, want string, contentOf func(*statev1.SpecChange) string) *statev1.GossipEvent {
	var winner *statev1.GossipEvent
	var winnerRank specRank
	for _, rec := range s.nodes {
		for k, ev := range rec.log {
			if k.kind != kind || k.peer != s.localID {
				continue
			}
			if contentOf(ev.GetSpecChange()) != want {
				continue
			}
			_, r := specRankOf(k, ev)
			if winner == nil || r.supersedes(winnerRank) {
				winner = ev
				winnerRank = r
			}
		}
	}
	return winner
}

// PublishWorkload emits the spec and the publisher's claim in a single
// gossip batch. Splitting them lets a remote see the spec first, observe
// zero claimants, and decide to claim before the publisher's own claim
// arrives, causing over-replication until it unwinds minutes later.
func (s *store) PublishWorkload(spec WorkloadSpec, policy *admissionv1.Predicate) ([]Event, error) {
	hash := spec.Hash
	hashBytes, err := hex.DecodeString(hash)
	if err != nil || len(hashBytes) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, hash)
	}
	if spec.Name == "" {
		return nil, ErrMissingName
	}
	owned := workloadSpecToProto(spec)
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		var gossips []*statev1.GossipEvent
		specChange, err := s.signedSpecChangeLocked(seedResourceID(spec.Name, hashBytes), owned, policy, false)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		// The local-signer paths probe rec.log under (kind, name,
		// peer=s.localID) before emitting; this must equal the key
		// mutateLocal re-derives via specAttrKey from the change it
		// writes. They agree because a self-signed Fact's AuthorityPub
		// is s.localID by construction (signedSpecChangeLocked signs
		// with the node's own identity). That invariant is load-bearing
		// for the dedup check here, in SetStaticSpec and in SetBlobSpec.
		specKey := attrKey{kind: attrWorkloadSpec, name: spec.Name, peer: s.localID}
		if ev, ok := rec.log[specKey]; !ok || ev.Deleted || !proto.Equal(ev.GetSpecChange(), specChange) {
			gossips = append(gossips, &statev1.GossipEvent{Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}})
		}
		if ev, ok := rec.log[attrKey{kind: attrWorkloadClaim, name: hash}]; !ok || ev.Deleted {
			gossips = append(gossips, &statev1.GossipEvent{Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: hash}}})
		}
		if len(gossips) == 0 {
			return nil, nil
		}
		return gossips, []Event{WorkloadChanged{Hash: hash}}
	})
	if signerErr != nil {
		return events, signerErr
	}
	return events, nil
}

func (s *store) DeleteWorkloadSpec(hash string) ([]Event, error) {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil || len(hashBytes) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, hash)
	}
	var mutateErr error
	events := s.mutateLocal(func(_ *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev := s.ownSpecEventLocked(attrWorkloadSpec, hash, func(sc *statev1.SpecChange) string {
			return sc.GetWorkload().GetHash()
		})
		if ev == nil {
			mutateErr = ErrUnseedNotAuthored
			return nil, nil
		}
		if ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetWorkload()
		specChange, err := s.signedSpecChangeLocked(seedResourceID(body.GetName(), hashBytes), body, ev.GetSpecChange().GetFact().GetPolicy(), true)
		if err != nil {
			mutateErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
	})
	return events, mutateErr
}

func (s *store) ClaimWorkload(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, true, false)
}

func (s *store) ReleaseWorkload(hash string) []Event {
	return s.setWorkloadClaimLocked(hash, false, false)
}

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

		if !claimed && !exists {
			return nil, nil
		}
		if claimed && exists && draining == currDraining {
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

func (s *store) SetService(port uint32, name string, protocol statev1.ServiceProtocol, policy *admissionv1.Predicate) ([]Event, error) {
	var signerErr error
	var events []Event
	owned := &statev1.ServiceChange{Name: name, Port: port, Protocol: protocol}
	events = s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		specChange, err := s.signedSpecChangeLocked(serviceResourceID(owned), owned, policy, false)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		if ev, ok := rec.log[attrKey{kind: attrService, name: name}]; ok && !ev.Deleted && proto.Equal(ev.GetSpecChange(), specChange) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}}, []Event{ServiceChanged{Peer: s.localID, Name: name}}
	})
	return events, signerErr
}

func (s *store) RemoveService(name string) ([]Event, error) {
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrService, name: name}]
		if !ok || ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetService()
		specChange, err := s.signedSpecChangeLocked(serviceResourceID(body), body, ev.GetSpecChange().GetFact().GetPolicy(), true)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, []Event{ServiceChanged{Peer: s.localID, Name: name}}
	})
	return events, signerErr
}

func (s *store) SetStaticSpec(spec StaticSpec, policy *admissionv1.Predicate) ([]Event, error) {
	name := spec.Name
	if name == "" {
		return nil, ErrMissingName
	}
	digest, err := hex.DecodeString(spec.ManifestDigest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.ManifestDigest)
	}
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		owned := &statev1.StaticSpecChange{
			Name:           name,
			ManifestDigest: digest,
		}
		specChange, err := s.signedSpecChangeLocked(staticResourceID(owned), owned, policy, false)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		if ev, ok := rec.log[attrKey{kind: attrStaticSpec, name: name, peer: s.localID}]; ok && !ev.Deleted && proto.Equal(ev.GetSpecChange(), specChange) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}}, []Event{StaticChanged{Name: name}}
	})
	if signerErr != nil {
		return events, signerErr
	}
	return events, nil
}

func (s *store) DeleteStaticSpec(name string) ([]Event, error) {
	var mutateErr error
	events := s.mutateLocal(func(_ *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev := s.ownSpecEventLocked(attrStaticSpec, name, func(sc *statev1.SpecChange) string {
			return sc.GetStatic().GetName()
		})
		if ev == nil {
			mutateErr = ErrUnseedNotAuthored
			return nil, nil
		}
		if ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetStatic()
		specChange, err := s.signedSpecChangeLocked(staticResourceID(body), body, ev.GetSpecChange().GetFact().GetPolicy(), true)
		if err != nil {
			mutateErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, []Event{StaticChanged{Name: name}}
	})
	return events, mutateErr
}

func (s *store) ClaimStatic(name string, authority types.PeerKey) []Event {
	return s.setStaticClaimLocked(name, authority, true)
}

func (s *store) ReleaseStatic(name string, authority types.PeerKey) []Event {
	return s.setStaticClaimLocked(name, authority, false)
}

func (s *store) setStaticClaimLocked(name string, authority types.PeerKey, claimed bool) []Event {
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrStaticClaim, name: name, peer: authority}]
		exists := ok && !ev.Deleted
		if claimed == exists {
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: !claimed, Change: &statev1.GossipEvent_StaticClaim{StaticClaim: &statev1.StaticClaimChange{Name: name, AuthorityPub: authority.Bytes()}}}
		return []*statev1.GossipEvent{change}, []Event{StaticChanged{Name: name}}
	})
}

func (s *store) SetBlobSpec(spec BlobSpec, policy *admissionv1.Predicate) ([]Event, error) {
	if spec.Name == "" {
		return nil, ErrMissingName
	}
	digest, err := hex.DecodeString(spec.Digest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.Digest)
	}
	owned := &statev1.BlobSpecChange{
		Name:   spec.Name,
		Digest: digest,
	}
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{kind: attrBlobSpec, name: spec.Name, peer: s.localID}
		specChange, err := s.signedSpecChangeLocked(blobResourceID(owned), owned, policy, false)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		if ev, ok := rec.log[key]; ok && !ev.Deleted && proto.Equal(ev.GetSpecChange(), specChange) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}}, nil
	})
	if signerErr != nil {
		return events, signerErr
	}
	return events, nil
}

func (s *store) DeleteBlobSpec(digest string) ([]Event, error) {
	raw, err := hex.DecodeString(digest)
	if err != nil || len(raw) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, digest)
	}
	var mutateErr error
	events := s.mutateLocal(func(_ *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev := s.ownSpecEventLocked(attrBlobSpec, digest, func(sc *statev1.SpecChange) string {
			return hex.EncodeToString(sc.GetBlob().GetDigest())
		})
		if ev == nil {
			mutateErr = ErrUnseedNotAuthored
			return nil, nil
		}
		if ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetBlob()
		specChange, err := s.signedSpecChangeLocked(blobResourceID(body), body, ev.GetSpecChange().GetFact().GetPolicy(), true)
		if err != nil {
			mutateErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, nil
	})
	return events, mutateErr
}

// ErrNoSigner is returned when a publish path runs on a node that holds
// no spec signer. The local mutation would otherwise produce a
// SpecChange with Auth: nil, which the local store accepts but every
// remote rejects on the validate hook: a silent partial publish.
var ErrNoSigner = errors.New("local node has no spec signer")

// ErrMissingName is returned when a workload, static or blob spec is
// published without a logical name. The name is half the publication
// identity (authority, name); without it the spec has no register to
// occupy and the proto-level min_len guard has been bypassed (an
// in-process local-signer publish never round-trips through the wire's
// buf.validate).
var ErrMissingName = errors.New("spec name required")

// ErrPresignedAuthRequired is returned when a presigned mutation path
// receives a nil Fact. Wire-mode callers must supply the Fact
// signed under their own authority key.
var ErrPresignedAuthRequired = errors.New("presigned spec auth required")

// ErrNoValidator is returned when a presigned mutation is attempted on
// a store without a registered validate hook. Presigned writes carry
// externally-signed auth and must be validated against the cluster's
// root before landing in the log.
var ErrNoValidator = errors.New("presigned mutations require a validate hook")

// PublishWorkloadPresigned stores a tenant-signed workload spec without
// re-signing. The daemon acts as a relay: the Fact is supplied by the
// wire-mode caller, validated against the cluster root, and gossipped
// as-is. No auto-claim is emitted; placement is decided by reconcilers
// on hosts that match the policy.
func (s *store) PublishWorkloadPresigned(spec WorkloadSpec, presignedFact *factv1.Fact) ([]Event, error) {
	hashBytes, err := hex.DecodeString(spec.Hash)
	if err != nil || len(hashBytes) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.Hash)
	}
	if spec.Name == "" {
		return nil, ErrMissingName
	}
	specChange, publisher, err := s.preparePresignedSpec(workloadSpecToProto(spec), presignedFact)
	if err != nil {
		return nil, err
	}
	return s.applyPresignedSpec(attrKey{kind: attrWorkloadSpec, name: spec.Name, peer: publisher}, specChange, WorkloadChanged{Hash: spec.Hash})
}

// SetStaticSpecPresigned stores a tenant-signed static-site spec without
// re-signing. See PublishWorkloadPresigned.
func (s *store) SetStaticSpecPresigned(spec StaticSpec, presignedFact *factv1.Fact) ([]Event, error) {
	digest, err := hex.DecodeString(spec.ManifestDigest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.ManifestDigest)
	}
	if spec.Name == "" {
		return nil, ErrMissingName
	}
	body := &statev1.StaticSpecChange{Name: spec.Name, ManifestDigest: digest}
	specChange, publisher, err := s.preparePresignedSpec(body, presignedFact)
	if err != nil {
		return nil, err
	}
	return s.applyPresignedSpec(attrKey{kind: attrStaticSpec, name: spec.Name, peer: publisher}, specChange, StaticChanged{Name: spec.Name})
}

// SetBlobSpecPresigned stores a tenant-signed blob spec without
// re-signing. See PublishWorkloadPresigned.
func (s *store) SetBlobSpecPresigned(spec BlobSpec, presignedFact *factv1.Fact) ([]Event, error) {
	digest, err := hex.DecodeString(spec.Digest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.Digest)
	}
	if spec.Name == "" {
		return nil, ErrMissingName
	}
	body := &statev1.BlobSpecChange{Name: spec.Name, Digest: digest}
	specChange, publisher, err := s.preparePresignedSpec(body, presignedFact)
	if err != nil {
		return nil, err
	}
	return s.applyPresignedSpec(attrKey{kind: attrBlobSpec, name: spec.Name, peer: publisher}, specChange, nil)
}

// preparePresignedSpec wraps the body in a SpecChange with the supplied
// auth and runs the validate hook. The hook (the admission pipeline in
// production) re-derives the resource ID from the body and rejects
// mismatches, so callers can't smuggle a mismatched resource through.
func (s *store) preparePresignedSpec(body fact.Body, presignedFact *factv1.Fact) (*statev1.SpecChange, types.PeerKey, error) {
	if presignedFact == nil {
		return nil, types.PeerKey{}, ErrPresignedAuthRequired
	}
	if s.validate == nil {
		return nil, types.PeerKey{}, ErrNoValidator
	}
	specChange := wrapSpecBody(presignedFact, body)
	if err := s.validate(specChange); err != nil {
		return nil, types.PeerKey{}, err
	}
	publisher := types.PeerKeyFromBytes(presignedFact.GetAuthorityPub())
	return specChange, publisher, nil
}

// applyPresignedSpec writes specChange to our slot under key. The key
// is (kind, logical-name, authority Principal): distinct authorities
// occupy distinct registers, so a cross-publisher collision on a shared
// name or shared content is structurally impossible and needs no
// conflict scan. Re-applying an identical spec is a no-op.
func (s *store) applyPresignedSpec(key attrKey, specChange *statev1.SpecChange, domainEvent Event) ([]Event, error) {
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[key]; ok && !ev.Deleted && proto.Equal(ev.GetSpecChange(), specChange) {
			return nil, nil
		}
		gossip := &statev1.GossipEvent{Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		var domain []Event
		if domainEvent != nil {
			domain = []Event{domainEvent}
		}
		return []*statev1.GossipEvent{gossip}, domain
	})
	return events, nil
}

// presignedResourceName returns the logical name the publisher signed
// into a presigned Fact's ResourceID. A tombstone's CRDT register key
// is (kind, this name, authority), identical to the live spec's, so
// the tombstone lands on exactly the slot it kills regardless of which
// peer relayed the original spec.
func presignedResourceName(f *factv1.Fact) string {
	switch r := f.GetResource().GetBody().(type) {
	case *admissionv1.ResourceID_Seed:
		return r.Seed.GetName()
	case *admissionv1.ResourceID_Static:
		return r.Static.GetName()
	case *admissionv1.ResourceID_Blob:
		return r.Blob.GetName()
	case *admissionv1.ResourceID_Service:
		return r.Service.GetName()
	}
	return ""
}

// DeleteWorkloadSpecPresigned applies a tenant-signed workload-spec
// tombstone. The tombstone is keyed by (authority, logical name),
// the same register the live spec occupies, so unseeds work even when the
// daemon serving the RPC isn't the one that originally accepted the
// spec. hash is retained only to reject a malformed content digest.
func (s *store) DeleteWorkloadSpecPresigned(hash string, presignedFact *factv1.Fact) ([]Event, error) {
	if _, err := hex.DecodeString(hash); err != nil || len(hash) != sha256HexLen {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, hash)
	}
	key := attrKey{kind: attrWorkloadSpec, name: presignedResourceName(presignedFact), peer: types.PeerKeyFromBytes(presignedFact.GetAuthorityPub())}
	return s.applyPresignedTombstone(key, presignedFact, WorkloadChanged{Hash: hash})
}

// DeleteStaticSpecPresigned applies a tenant-signed static-spec tombstone.
func (s *store) DeleteStaticSpecPresigned(name string, presignedFact *factv1.Fact) ([]Event, error) {
	key := attrKey{kind: attrStaticSpec, name: presignedResourceName(presignedFact), peer: types.PeerKeyFromBytes(presignedFact.GetAuthorityPub())}
	return s.applyPresignedTombstone(key, presignedFact, StaticChanged{Name: name})
}

// DeleteBlobSpecPresigned applies a tenant-signed blob-spec tombstone.
// digest is retained only to reject a malformed content digest.
func (s *store) DeleteBlobSpecPresigned(digest string, presignedFact *factv1.Fact) ([]Event, error) {
	if _, err := hex.DecodeString(digest); err != nil || len(digest) != sha256HexLen {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, digest)
	}
	key := attrKey{kind: attrBlobSpec, name: presignedResourceName(presignedFact), peer: types.PeerKeyFromBytes(presignedFact.GetAuthorityPub())}
	return s.applyPresignedTombstone(key, presignedFact, nil)
}

// ErrTombstoneNoLiveSpec is returned when a presigned tombstone arrives
// for a spec the cluster has no record of (neither locally nor on any
// other peer). The publisher's signature can't be verified without the
// body it was signed over, so the daemon refuses rather than emit an
// unverifiable tombstone.
var ErrTombstoneNoLiveSpec = errors.New("presigned tombstone: live spec not found on any peer")

// ErrUnseedNotAuthored is returned from DeleteWorkloadSpec /
// DeleteStaticSpec / DeleteBlobSpec when no live publication keyed by
// this daemon's authority exists in any slot. Either the name is not
// published at all, or it belongs to another publisher who must unseed
// it themselves (the daemon cannot tombstone a Fact it did not sign).
// Surfacing this lets the handler return a real NotFound instead of a
// silent success.
var ErrUnseedNotAuthored = errors.New("no live publication under this daemon's authority")

func (s *store) applyPresignedTombstone(key attrKey, presignedFact *factv1.Fact, domainEvent Event) ([]Event, error) {
	if presignedFact == nil {
		return nil, ErrPresignedAuthRequired
	}
	if !presignedFact.GetDeleted() {
		return nil, errors.New("presigned tombstone must have Deleted=true")
	}
	if s.validate == nil {
		return nil, ErrNoValidator
	}
	var rebuildErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		if ev, ok := rec.log[key]; ok && ev.Deleted {
			return nil, nil
		}
		body := s.findLiveSpecBodyLocked(key, presignedFact.GetBodyHash())
		if body == nil {
			rebuildErr = ErrTombstoneNoLiveSpec
			return nil, nil
		}
		specChange := wrapSpecBody(presignedFact, body)
		if err := s.validate(specChange); err != nil {
			rebuildErr = err
			return nil, nil
		}
		gossip := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		var domain []Event
		if domainEvent != nil {
			domain = []Event{domainEvent}
		}
		return []*statev1.GossipEvent{gossip}, domain
	})
	return events, rebuildErr
}

// findLiveSpecBodyLocked returns the body the publisher signed at
// create time, looking first at the local slot and then across every
// other peer's log. key is (kind, logical name, authority), so every
// event found under it already belongs to the right authority; the
// register itself is the publisher gate. wantBodyHash is the body_hash
// the tombstone was signed over; matching on it disambiguates re-seeded
// content (otherwise map-iteration order could return a stale body from
// a previous tenure still cached on some peer). s.mu must be held by
// the caller.
func (s *store) findLiveSpecBodyLocked(key attrKey, wantBodyHash []byte) fact.Body {
	matches := func(ev *statev1.GossipEvent) fact.Body {
		if ev == nil || ev.Deleted {
			return nil
		}
		sc := ev.GetSpecChange()
		// When the tombstone's body_hash is unset (older callers), accept
		// any body at this register. Otherwise require the live spec's
		// own signed body_hash to match so map-iteration order doesn't
		// pick a stale body cached on a relay peer.
		if len(wantBodyHash) > 0 && !bytes.Equal(sc.GetFact().GetBodyHash(), wantBodyHash) {
			return nil
		}
		return liveSpecBody(sc)
	}
	if rec, ok := s.nodes[s.localID]; ok {
		if ev, ok := rec.log[key]; ok {
			if body := matches(ev); body != nil {
				return body
			}
		}
	}
	for pk, rec := range s.nodes {
		if pk == s.localID {
			continue
		}
		ev, ok := rec.log[key]
		if !ok {
			continue
		}
		if body := matches(ev); body != nil {
			return body
		}
	}
	return nil
}

// liveSpecBody returns the typed body proto for the live spec change,
// so a presigned tombstone can be rewrapped against the same body the
// publisher signed at create time.
func liveSpecBody(sc *statev1.SpecChange) fact.Body {
	switch v := sc.GetBody().(type) {
	case *statev1.SpecChange_Workload:
		return v.Workload
	case *statev1.SpecChange_Static:
		return v.Static
	case *statev1.SpecChange_Blob:
		return v.Blob
	case *statev1.SpecChange_Service:
		return v.Service
	}
	return nil
}

// RevokeOwnSpecs tombstones this node's published Facts whose required
// publish kind is not present in retain. Used on a capability change
// (downgrade, or admin-issued upgrade that drops a kind): the still-valid
// old signer is used to sign the tombstones before the new grant
// replaces it, so each tombstone chains correctly. A nil retain
// argument means no kinds are retained, equivalent to a full publish
// drop. The returned events are emitted to peers like any other publish
// change; remotes drop the resources from their CRDTs as the
// tombstones land.
func (s *store) RevokeOwnSpecs(retain *identityv1.Capabilities) ([]Event, error) {
	p := retain.GetPublish()
	snap := s.Snapshot()
	var events []Event
	var firstErr error
	record := func(evs []Event, err error) {
		events = append(events, evs...)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if !p.GetServices() {
		if local, ok := snap.Nodes[snap.LocalID]; ok {
			for name := range local.Services {
				record(s.RemoveService(name))
			}
		}
	}
	// Iterate the per-(authority,name) publication sources, not the
	// deduped content-addressed maps: when a colliding remote tenant
	// wins the dedupe, snap.Specs[hash].Publisher is that tenant, so a
	// deduped-map scan would skip this node's own spec and a
	// cap-downgraded principal would keep a live gossiped spec it has
	// lost authority to publish. The delete helpers resolve the local
	// (authority, name) register by content id.
	if !p.GetFunctions() {
		for _, spec := range snap.SpecsAll {
			if spec.Publisher == snap.LocalID {
				record(s.DeleteWorkloadSpec(spec.Spec.Hash))
			}
		}
	}
	if !p.GetSites() {
		for _, spec := range snap.StaticSpecsAll {
			if spec.Publisher == snap.LocalID {
				record(s.DeleteStaticSpec(spec.Spec.Name))
			}
		}
	}
	if !p.GetBlobs() {
		for _, spec := range snap.BlobSpecsAll {
			if spec.Publisher == snap.LocalID {
				record(s.DeleteBlobSpec(spec.Spec.Digest))
			}
		}
	}
	return events, firstErr
}

// SetBlobWrapping gossips a pre-signed wrapping. The wrapping must
// have already been produced by auth.IssueBlobWrapping (or an
// equivalent path that signs with the local node's identity key). The
// CRDT keys it as (blob_hash, recipient) per peer; replaying with the
// same payload is a no-op.
func (s *store) SetBlobWrapping(wrapping *factv1.BlobWrapping) []Event {
	if wrapping == nil {
		return nil
	}
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{
			kind: attrBlobWrapping,
			name: hex.EncodeToString(wrapping.GetBlobHash()),
			peer: types.PeerKeyFromBytes(wrapping.GetRecipientPub()),
		}
		if ev, ok := rec.log[key]; ok && !ev.Deleted && proto.Equal(ev.GetBlobWrapping(), wrapping) {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_BlobWrapping{BlobWrapping: wrapping}}
		return []*statev1.GossipEvent{change}, nil
	})
}

func (s *store) signedSpecChangeLocked(resource *admissionv1.ResourceID, body fact.Body, policy *admissionv1.Predicate, deleted bool) (*statev1.SpecChange, error) {
	if s.signer == nil {
		return nil, ErrNoSigner
	}
	f, err := s.signer.IssueFact(resource, body, policy, deleted)
	if err != nil {
		return nil, err
	}
	sc := wrapSpecBody(f, body)
	// Route local self-signed mutations through the same admission
	// pipeline gossip and presigned writes already traverse, so a local
	// seed/unseed cannot bypass authorise/account (closing the
	// UnseedStatic and self-signed-delete gaps). Runs under s.mu via
	// mutateLocal; the pipeline reads only the lock-free Snapshot(), so
	// this is non-re-entrant. Gated on a configured validator: state
	// tests that wire no validator keep the pre-pipeline behaviour.
	if s.validate != nil {
		if err := s.validate(sc); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

// wrapSpecBody assembles a SpecChange from a pre-built Fact and a body.
// Used by the local-signer path (signedSpecChangeLocked) and by
// presigned wire-mode paths that supply the Fact themselves.
func wrapSpecBody(f *factv1.Fact, body fact.Body) *statev1.SpecChange {
	specChange := &statev1.SpecChange{Fact: f}
	switch v := body.(type) {
	case *statev1.WorkloadSpecChange:
		specChange.Body = &statev1.SpecChange_Workload{Workload: v}
	case *statev1.ServiceChange:
		specChange.Body = &statev1.SpecChange_Service{Service: v}
	case *statev1.StaticSpecChange:
		specChange.Body = &statev1.SpecChange_Static{Static: v}
	case *statev1.BlobSpecChange:
		specChange.Body = &statev1.SpecChange_Blob{Blob: v}
	}
	return specChange
}

func seedResourceID(name string, hash []byte) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: name,
		Hash: hash,
	}}}
}

func serviceResourceID(body *statev1.ServiceChange) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Service{Service: &admissionv1.ServiceID{Name: body.GetName()}}}
}

func staticResourceID(body *statev1.StaticSpecChange) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           body.GetName(),
		ManifestDigest: body.GetManifestDigest(),
	}}}
}

func blobResourceID(body *statev1.BlobSpecChange) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{
		Name:   body.GetName(),
		Digest: body.GetDigest(),
	}}}
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

const (
	sha256Len    = 32
	sha256HexLen = 64
)

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

// setLocalStringAttr is the shared tombstone-on-empty / dedupe / upsert
// path for single-string per-node attributes. currentValue reads the
// value from an existing event for the dedupe check; mk builds a fresh
// gossip event for a non-empty value (the helper flips Deleted for the
// tombstone case).
func (s *store) setLocalStringAttr(
	kind attrKind,
	value string,
	currentValue func(*statev1.GossipEvent) string,
	mk func(string) *statev1.GossipEvent,
) {
	s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{kind: kind}
		if value == "" {
			ev, ok := rec.log[key]
			if !ok || ev.Deleted {
				return nil, nil
			}
			tomb := mk(value)
			tomb.Deleted = true
			return []*statev1.GossipEvent{tomb}, nil
		}
		if ev, ok := rec.log[key]; ok && !ev.Deleted && currentValue(ev) == value {
			return nil, nil
		}
		return []*statev1.GossipEvent{mk(value)}, nil
	})
}

func (s *store) SetNodeName(name string) {
	s.setLocalStringAttr(attrNodeName, name,
		func(ev *statev1.GossipEvent) string { return ev.GetNodeName().GetName() },
		func(v string) *statev1.GossipEvent {
			return &statev1.GossipEvent{Change: &statev1.GossipEvent_NodeName{NodeName: &statev1.NodeNameChange{Name: v}}}
		})
}

func (s *store) SetControlAddr(addr string) {
	s.setLocalStringAttr(attrControlAddr, addr,
		func(ev *statev1.GossipEvent) string { return ev.GetControlAddr().GetAddr() },
		func(v string) *statev1.GossipEvent {
			return &statev1.GossipEvent{Change: &statev1.GossipEvent_ControlAddr{ControlAddr: &statev1.ControlAddrChange{Addr: v}}}
		})
}

func (s *store) SetGatewayDomain(domain string) {
	s.setLocalStringAttr(attrGatewayDomain, domain,
		func(ev *statev1.GossipEvent) string { return ev.GetGatewayDomain().GetDomain() },
		func(v string) *statev1.GossipEvent {
			return &statev1.GossipEvent{Change: &statev1.GossipEvent_GatewayDomain{GatewayDomain: &statev1.GatewayDomainChange{Domain: v}}}
		})
}
