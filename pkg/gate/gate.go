// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package gate

import (
	"encoding/hex"
	"errors"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"google.golang.org/protobuf/proto"
)

const CallerKey = "pln.caller"

type StateReader interface {
	Snapshot() state.Snapshot
}

// Gate runs admission checks (Admit) and runtime decisions (Invoke,
// Fetch, Connect) against a single store.
//
// Trust contract: runtime decisions trust SpecAuth values pulled from
// the store without re-verifying signatures. This is sound iff every
// write path into the CRDT log either runs Admit or is locally signed
// by the configured LocalSigner. Today the state package satisfies
// both halves: applyBatchLocked invokes the validator on inbound
// gossip; handleSelfConflictLocked invokes acceptableSelfEventLocked
// on live self-conflict events; mutateLocal goes through
// signedSpecChangeLocked which requires a non-nil LocalSigner. Any
// new log-writing path must satisfy one of those two invariants,
// otherwise runtime decisions can be poisoned with attacker-supplied
// policy.
type Gate struct {
	store   StateReader
	rootPub []byte
}

func New(rootPub []byte, store StateReader) *Gate {
	if store == nil {
		panic("gate.New: store is required")
	}
	return &Gate{store: store, rootPub: rootPub}
}

func (g *Gate) Admit(sc *statev1.SpecChange) error {
	body, expected, err := decodeSpecChange(sc)
	if err != nil {
		return err
	}
	specAuth := sc.GetAuth()
	if specAuth == nil {
		return errors.New("gate: spec change missing auth")
	}
	if !proto.Equal(specAuth.GetResource(), expected) {
		return errors.New("gate: spec auth resource mismatch")
	}
	return auth.VerifySpecAuth(specAuth, body, g.rootPub, time.Now())
}

func (g *Gate) Invoke(peerKey types.PeerKey, hash string) (wasm.CallerInfo, error) {
	snap := g.store.Snapshot()
	caller, ok := snap.Nodes[peerKey]
	if !ok || caller.Cert == nil {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	_, sv, ok := resolveSeedSpec(snap, hash)
	if !ok || sv.Auth == nil {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	if err := decide(caller.Cert, sv.Auth, time.Now()); err != nil {
		return wasm.CallerInfo{}, err
	}
	return wasm.CallerInfo{PeerKey: peerKey, Attributes: caller.Cert.GetClaims().GetCapabilities().GetAttributes().AsMap()}, nil
}

func (g *Gate) Fetch(peerKey types.PeerKey, hash string) error {
	snap := g.store.Snapshot()
	caller, ok := snap.Nodes[peerKey]
	if !ok || caller.Cert == nil {
		return wasm.ErrTargetNotFound
	}
	view, ok := snap.BlobSpecs[hash]
	if !ok || view.Auth == nil {
		return wasm.ErrTargetNotFound
	}
	return decide(caller.Cert, view.Auth, time.Now())
}

// Connect authorises callerKey to open a connection to (hostPeer, port).
// The decision is direction-agnostic: callers pass the local peer as
// hostPeer when authorising an inbound stream, and the remote peer when
// authorising one this node is about to open.
func (g *Gate) Connect(callerKey, hostPeer types.PeerKey, port uint32) error {
	snap := g.store.Snapshot()
	caller, ok := snap.Nodes[callerKey]
	if !ok || caller.Cert == nil {
		return wasm.ErrTargetNotFound
	}
	target, ok := snap.Nodes[hostPeer]
	if !ok {
		return wasm.ErrTargetNotFound
	}
	for _, svc := range target.Services {
		if svc.Port != port || svc.Auth == nil {
			continue
		}
		return decide(caller.Cert, svc.Auth, time.Now())
	}
	return wasm.ErrTargetNotFound
}

// MayHost authorises hostCert to host the workload described by specAuth.
// Hosting includes loopback invocation, so the spec's policy must hold
// against the host's own cert.
func (g *Gate) MayHost(hostCert *admissionv1.DelegationCert, specAuth *admissionv1.SpecAuth) error {
	if hostCert == nil || specAuth == nil {
		return wasm.ErrTargetNotFound
	}
	return decide(hostCert, specAuth, time.Now())
}

func decide(cert *admissionv1.DelegationCert, specAuth *admissionv1.SpecAuth, now time.Time) error {
	if auth.IsCertExpired(cert, now) {
		return wasm.ErrTargetNotFound
	}
	policy := specAuth.GetPolicy()
	if policy != nil && policy.GetInline() == nil {
		return wasm.ErrTargetNotFound
	}

	ctx := make(map[string]string)
	for k, v := range cert.GetClaims().GetCapabilities().GetAttributes().GetFields() {
		if s := v.GetStringValue(); s != "" {
			ctx[k] = s
		}
	}
	ctx[CallerKey] = hex.EncodeToString(cert.GetClaims().GetSubjectPub())

	for _, clause := range policy.GetInline().GetClauses() {
		got, ok := ctx[clause.GetKey()]
		if !ok || got != clause.GetEquals() {
			return wasm.ErrTargetNotFound
		}
	}
	return nil
}

// resolveSeedSpec accepts either a workload hash or a workload name.
// snap.Specs is keyed by hash, so the hash lookup wins when the
// identifier matches one; otherwise we fall back to a by-name scan.
func resolveSeedSpec(snap state.Snapshot, identifier string) (string, state.WorkloadSpecView, bool) {
	if sv, ok := snap.Specs[identifier]; ok {
		return identifier, sv, true
	}
	return snap.SpecByName(identifier)
}

func decodeSpecChange(sc *statev1.SpecChange) (auth.SpecBody, *admissionv1.ResourceID, error) {
	switch body := sc.GetBody().(type) {
	case *statev1.SpecChange_Workload:
		hashBytes, err := hex.DecodeString(body.Workload.GetHash())
		if err != nil {
			return nil, nil, err
		}
		return body.Workload, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.Workload.GetName(), Hash: hashBytes}}}, nil
	case *statev1.SpecChange_Service:
		return body.Service, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Service{Service: &admissionv1.ServiceID{Name: body.Service.GetName()}}}, nil
	case *statev1.SpecChange_Static:
		return body.Static, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{Name: body.Static.GetName(), ManifestDigest: body.Static.GetManifestDigest()}}}, nil
	case *statev1.SpecChange_Blob:
		return body.Blob, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: body.Blob.GetName(), Digest: body.Blob.GetDigest()}}}, nil
	}
	return nil, nil, errors.New("gate: empty spec body")
}
