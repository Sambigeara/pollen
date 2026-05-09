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
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
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
	denyOrCertChanged := false
	for _, ev := range gossips {
		key, _ := getAttrKey(ev)
		if key.kind == attrDeny || key.kind == attrDelegationCert {
			denyOrCertChanged = true
		}
		rec.maxCounter++
		ev.PeerId = s.localID.String()
		ev.Counter = rec.maxCounter
		rec.log[key] = ev
		s.pendingGossip = append(s.pendingGossip, ev)
	}

	rec.lastEventAt = now
	s.lastLocalEmit = now
	s.nodes[s.localID] = rec

	if denyOrCertChanged {
		events = append(events, s.recomputeDeniedLocked()...)
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

// SetLocalDelegationCert publishes the local node's current delegation
// cert into the CRDT so every other node can evaluate chain-scoped
// rules (most importantly: subtree-bounded deny authorisation).
//
// subjectSig must be a valid ed25519 signature by cert.subject_pub
// (== local node identity) over the cert's claims, produced by
// auth.SignDelegationCertSubject. Without subject proof-of-possession,
// any admin could forge a cert for another peer's pub and bypass deny
// scoping. Callers in production wire this from the signing key; tests
// that exercise non-cert paths can leave subjectSig empty (apply-time
// verification is skipped when rootPub is unset).
func (s *store) SetLocalDelegationCert(cert *admissionv1.DelegationCert, subjectSig []byte) []Event {
	if cert == nil {
		return nil
	}
	return s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		owned := &statev1.DelegationCertChange{Cert: cert, SubjectSignature: subjectSig}
		if ev, ok := rec.log[attrKey{kind: attrDelegationCert}]; ok && !ev.Deleted && proto.Equal(ev.GetDelegationCert(), owned) {
			return nil, nil
		}
		change := &statev1.GossipEvent{Change: &statev1.GossipEvent_DelegationCert{DelegationCert: owned}}
		return []*statev1.GossipEvent{change}, nil
	})
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
func (s *store) PublishWorkload(spec WorkloadSpec, policy *admissionv1.Predicate) ([]Event, error) {
	hash := spec.Hash
	hashBytes, err := hex.DecodeString(hash)
	if err != nil || len(hashBytes) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, hash)
	}
	owned := workloadSpecToProto(spec)
	var ownerErr error
	var signerErr error
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
		specChange, err := s.signedSpecChangeLocked(seedResourceID(spec.Name, hashBytes), owned, policy, false)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		if ev, ok := rec.log[attrKey{kind: attrWorkloadSpec, name: hash}]; !ok || ev.Deleted || !proto.Equal(ev.GetSpecChange(), specChange) {
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
	return events, ownerErr
}

func (s *store) DeleteWorkloadSpec(hash string) ([]Event, error) {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil || len(hashBytes) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, hash)
	}
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrWorkloadSpec, name: hash}]
		if !ok || ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetWorkload()
		specChange, err := s.signedSpecChangeLocked(seedResourceID(body.GetName(), hashBytes), body, ev.GetSpecChange().GetAuth().GetPolicy(), true)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, []Event{WorkloadChanged{Hash: hash}}
	})
	return events, signerErr
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

func (s *store) SetService(port uint32, name string, protocol statev1.ServiceProtocol, properties *structpb.Struct, policy *admissionv1.Predicate) ([]Event, error) {
	var signerErr error
	var events []Event
	owned := &statev1.ServiceChange{Name: name, Port: port, Protocol: protocol, Properties: properties}
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
		specChange, err := s.signedSpecChangeLocked(serviceResourceID(body), body, ev.GetSpecChange().GetAuth().GetPolicy(), true)
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
	digest, err := hex.DecodeString(spec.ManifestDigest)
	if err != nil || len(digest) != sha256Len {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, spec.ManifestDigest)
	}
	var ownerErr error
	var signerErr error
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
		specChange, err := s.signedSpecChangeLocked(staticResourceID(owned), owned, policy, false)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		if ev, ok := rec.log[attrKey{kind: attrStaticSpec, name: name}]; ok && !ev.Deleted && proto.Equal(ev.GetSpecChange(), specChange) {
			return nil, nil
		}
		return []*statev1.GossipEvent{{Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}}, []Event{StaticChanged{Name: name}}
	})
	if signerErr != nil {
		return events, signerErr
	}
	return events, ownerErr
}

func (s *store) DeleteStaticSpec(name string) ([]Event, error) {
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		ev, ok := rec.log[attrKey{kind: attrStaticSpec, name: name}]
		if !ok || ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetStatic()
		specChange, err := s.signedSpecChangeLocked(staticResourceID(body), body, ev.GetSpecChange().GetAuth().GetPolicy(), true)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, []Event{StaticChanged{Name: name}}
	})
	return events, signerErr
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

func (s *store) SetBlobSpec(spec BlobSpec, policy *admissionv1.Predicate) ([]Event, error) {
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
		key := attrKey{kind: attrBlobSpec, name: spec.Digest}
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
	var signerErr error
	events := s.mutateLocal(func(rec *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		key := attrKey{kind: attrBlobSpec, name: digest}
		ev, ok := rec.log[key]
		if !ok || ev.Deleted {
			return nil, nil
		}
		body := ev.GetSpecChange().GetBlob()
		specChange, err := s.signedSpecChangeLocked(blobResourceID(body), body, ev.GetSpecChange().GetAuth().GetPolicy(), true)
		if err != nil {
			signerErr = err
			return nil, nil
		}
		change := &statev1.GossipEvent{Deleted: true, Change: &statev1.GossipEvent_SpecChange{SpecChange: specChange}}
		return []*statev1.GossipEvent{change}, nil
	})
	return events, signerErr
}

// ErrNoSigner is returned when a publish path runs on a node that holds
// no delegation signer. The local mutation would otherwise produce a
// SpecChange with Auth: nil, which the local store accepts but every
// remote rejects on the validate hook — silent partial publish.
var ErrNoSigner = errors.New("local node has no delegation signer; publish requires admin authority")

func (s *store) signedSpecChangeLocked(resource *admissionv1.ResourceID, body auth.SpecBody, policy *admissionv1.Predicate, deleted bool) (*statev1.SpecChange, error) {
	if s.signer == nil {
		return nil, ErrNoSigner
	}
	specAuth, err := s.signer.IssueSpecAuth(resource, body, policy, deleted)
	if err != nil {
		return nil, err
	}
	specChange := &statev1.SpecChange{Auth: specAuth}
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
	return specChange, nil
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
