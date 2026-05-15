// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package gate

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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

type accessTokenCtxKey struct{}

// WithAccessToken attaches an AccessToken to ctx so downstream gate
// decisions can substitute token-based authorisation for cert-based
// authorisation. The anonymous HTTP gateway sets the token here; the
// peer cert path leaves the ctx untouched and falls through to the
// existing checks.
func WithAccessToken(ctx context.Context, token *admissionv1.AccessToken) context.Context {
	if token == nil {
		return ctx
	}
	return context.WithValue(ctx, accessTokenCtxKey{}, token)
}

// AccessTokenFromContext returns the AccessToken set by WithAccessToken,
// if any.
func AccessTokenFromContext(ctx context.Context) (*admissionv1.AccessToken, bool) {
	t, ok := ctx.Value(accessTokenCtxKey{}).(*admissionv1.AccessToken)
	return t, ok && t != nil
}

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
	store     StateReader
	manifests state.ManifestPaths
	rootPub   []byte
}

func New(rootPub []byte, store StateReader) *Gate {
	if store == nil {
		panic("gate.New: store is required")
	}
	return &Gate{store: store, rootPub: rootPub}
}

// SetManifestPaths wires a static-manifest reader so Fetch can authorise
// blobs nested inside a published static site. Without it, only direct
// references (workload hash, blob digest, manifest digest itself) are
// authorised — nested file blobs are denied, breaking cross-node static
// replication. Call once after the blobs service is constructed.
func (g *Gate) SetManifestPaths(mp state.ManifestPaths) {
	g.manifests = mp
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
	// Durable specs bind to the publisher's authority horizon plus the
	// denylist (see auth.VerifySpecAuth). Snapshot() is a lock-free
	// atomic load, so sourcing deny here is safe even though Admit runs
	// as the store's validate hook under its lock. A deny arriving in
	// the same gossip batch is caught by the post-admission publisher
	// filter in buildSnapshot.
	denied := g.store.Snapshot().DenyChecker()
	if err := auth.VerifySpecAuth(specAuth, body, g.rootPub, time.Now(), denied); err != nil {
		return err
	}
	// A spec that carries both public=true and inline clauses looks
	// gated to a casual reader but admits anyone at runtime (decide
	// short-circuits on public). The CLI rejects the combination at
	// publish time; rejecting it at Admit closes the same door against
	// tampered or hand-crafted specs.
	policy := specAuth.GetPolicy()
	if policy.GetPublic() && policy.GetInline() != nil {
		return errors.New("gate: predicate has both public=true and inline clauses")
	}
	// Defence-in-depth against a publisher whose slug grinds against a
	// live mesh peer's slug. PublisherSlug is 60 bits, so a real
	// collision is statistically unreachable; the check guarantees
	// canonical-URL routing stays unambiguous against active peers.
	publisher := types.PeerKeyFromBytes(specAuth.GetPublisher().GetClaims().GetSubjectPub())
	slug := publisher.Slug()
	for peer := range g.store.Snapshot().Nodes {
		if peer != publisher && peer.Slug() == slug {
			return fmt.Errorf("gate: publisher slug %q collides with existing peer %s", slug, peer.Short())
		}
	}
	return nil
}

// Invoke authorises callerCert to invoke the workload at hash. A nil
// callerCert is admitted only when the spec's policy has public=true;
// in that case the returned CallerInfo is empty, mirroring the
// InvokeByToken path. Mesh-peer callers resolve the cert from
// snap.Nodes via LookupCert, wire-mode callers pass their
// mTLS-validated cert directly.
func (g *Gate) Invoke(callerCert *admissionv1.DelegationCert, hash string) (wasm.CallerInfo, error) {
	snap := g.store.Snapshot()
	sv, ok := resolveSeedSpec(snap, hash)
	if !ok || sv.Auth == nil {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	if err := decide(callerCert, sv.Auth, time.Now()); err != nil {
		return wasm.CallerInfo{}, err
	}
	if callerCert == nil {
		return wasm.CallerInfo{}, nil
	}
	return wasm.CallerInfo{
		PeerKey:    types.PeerKeyFromBytes(callerCert.GetClaims().GetSubjectPub()),
		Attributes: callerCert.GetClaims().GetCapabilities().GetAttributes().AsMap(),
	}, nil
}

// Fetch authorises callerCert to read the CAS object at hash. The same
// stream type carries workload binaries, named-blob payloads, static
// manifests, and the file blobs nested inside those manifests, so the
// lookup unions every referencing spec's auth and admits the caller if
// any one of them allows the cert. Without unioning, a non-publisher
// replica can never fetch the bytes from the publisher and stays stuck
// in a fetch-EOF loop. A nil callerCert is admitted only when at least
// one referencing spec has policy.public=true.
func (g *Gate) Fetch(callerCert *admissionv1.DelegationCert, hash string) error {
	snap := g.store.Snapshot()
	auths := snap.BlobEntitlements(hash, g.manifests)
	if len(auths) == 0 {
		return wasm.ErrTargetNotFound
	}
	now := time.Now()
	for _, sa := range auths {
		if decide(callerCert, sa, now) == nil {
			return nil
		}
	}
	return wasm.ErrTargetNotFound
}

// Connect authorises callerCert to open a connection to (hostPeer, port).
// The decision is direction-agnostic: callers pass the local peer as
// hostPeer when authorising an inbound stream, and the remote peer when
// authorising one this node is about to open. A nil callerCert is
// admitted only when the target service's policy has public=true.
func (g *Gate) Connect(callerCert *admissionv1.DelegationCert, hostPeer types.PeerKey, port uint32) error {
	snap := g.store.Snapshot()
	target, ok := snap.Nodes[hostPeer]
	if !ok {
		return wasm.ErrTargetNotFound
	}
	for _, svc := range target.Services {
		if svc.Port != port || svc.Auth == nil {
			continue
		}
		return decide(callerCert, svc.Auth, time.Now())
	}
	return wasm.ErrTargetNotFound
}

// LookupCert resolves a mesh peer's cert via the gossiped snapshot. Use
// it on transport-authenticated inbound paths (mesh streams) where the
// only thing the caller can present is their peer key; wire-mode RPC
// paths already carry the cert in the request context and should pass
// it directly.
func (g *Gate) LookupCert(peerKey types.PeerKey) *admissionv1.DelegationCert {
	snap := g.store.Snapshot()
	nv, ok := snap.Nodes[peerKey]
	if !ok {
		return nil
	}
	return nv.Cert
}

// FetchByToken authorises an anonymous caller holding token to read the
// CAS object at hash. The token must verify (signature, expiry) and the
// token's resource must correspond to a spec whose publisher signed the
// token and whose entitlements cover hash.
func (g *Gate) FetchByToken(token *admissionv1.AccessToken, hash string) error {
	if err := auth.VerifyAccessToken(token, time.Now()); err != nil {
		return wasm.ErrTargetNotFound
	}
	resource := token.GetClaims().GetResource()
	issuer := token.GetClaims().GetIssuerPub()
	snap := g.store.Snapshot()
	for _, sa := range snap.BlobEntitlements(hash, g.manifests) {
		if !bytes.Equal(sa.GetPublisher().GetClaims().GetSubjectPub(), issuer) {
			continue
		}
		if !proto.Equal(sa.GetResource(), resource) {
			continue
		}
		return nil
	}
	return wasm.ErrTargetNotFound
}

// InvokeByToken authorises an anonymous caller holding token to invoke
// the workload at hash. Same shape as Invoke but the identity comes
// from the token rather than a peer cert; the returned CallerInfo has
// no attributes since anonymous callers carry no cert.
func (g *Gate) InvokeByToken(token *admissionv1.AccessToken, hash string) (wasm.CallerInfo, error) {
	if err := auth.VerifyAccessToken(token, time.Now()); err != nil {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	snap := g.store.Snapshot()
	sv, ok := resolveSeedSpec(snap, hash)
	if !ok || sv.Auth == nil {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	if !bytes.Equal(sv.Auth.GetPublisher().GetClaims().GetSubjectPub(), token.GetClaims().GetIssuerPub()) {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	if !proto.Equal(sv.Auth.GetResource(), token.GetClaims().GetResource()) {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	return wasm.CallerInfo{}, nil
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

// MayPublish reports whether cert satisfies policy at publish time.
// The returned error is descriptive so the local publisher can see
// exactly why their cert doesn't qualify. The other gate methods
// (Invoke, Fetch, Connect, MayHost) return opaque ErrTargetNotFound
// to avoid leaking admission state to remote callers; MayPublish
// runs against the local cert only, so descriptive errors are safe.
//
// A nil policy is always permitted, even when cert is nil, so that
// publishes during the bootstrap window (before the local cert lands
// in gossip) keep working.
func (g *Gate) MayPublish(cert *admissionv1.DelegationCert, policy *admissionv1.Predicate) error {
	if policy == nil {
		return nil
	}
	if cert == nil {
		return errors.New("local cert is not yet published")
	}
	if auth.IsCertExpired(cert, time.Now()) {
		return errors.New("local cert is expired")
	}
	return checkPolicyClauses(cert, policy)
}

// decide authorises cert against specAuth. A nil cert (anonymous
// caller) is admitted only when the spec's policy has public=true;
// otherwise the cert is checked for expiry and predicate-clause match.
// Public also short-circuits cert-bearing callers: anyone reaching a
// public spec is admitted regardless of their attribute claims.
func decide(cert *admissionv1.DelegationCert, specAuth *admissionv1.SpecAuth, now time.Time) error {
	policy := specAuth.GetPolicy()
	if policy.GetPublic() {
		return nil
	}
	if cert == nil {
		return wasm.ErrTargetNotFound
	}
	if auth.IsCertExpired(cert, now) {
		return wasm.ErrTargetNotFound
	}
	if err := checkPolicyClauses(cert, policy); err != nil {
		return wasm.ErrTargetNotFound
	}
	return nil
}

// checkPolicyClauses returns nil if cert satisfies every clause of
// policy, or a descriptive error otherwise. A nil policy is permitted;
// a policy without inline clauses is rejected (no other shapes are
// supported today).
func checkPolicyClauses(cert *admissionv1.DelegationCert, policy *admissionv1.Predicate) error {
	if policy == nil {
		return nil
	}
	inline := policy.GetInline()
	if inline == nil {
		return errors.New("policy has no inline clauses")
	}
	ctx := certContext(cert)
	for _, clause := range inline.GetClauses() {
		got, ok := ctx[clause.GetKey()]
		if !ok {
			return fmt.Errorf("local cert is missing prop %q (policy requires %q)", clause.GetKey(), clause.GetEquals())
		}
		if got != clause.GetEquals() {
			return fmt.Errorf("local cert prop %q is %q; policy requires %q", clause.GetKey(), got, clause.GetEquals())
		}
	}
	return nil
}

func certContext(cert *admissionv1.DelegationCert) map[string]string {
	ctx := make(map[string]string)
	for k, v := range cert.GetClaims().GetCapabilities().GetAttributes().GetFields() {
		if s := v.GetStringValue(); s != "" {
			ctx[k] = s
		}
	}
	ctx[CallerKey] = hex.EncodeToString(cert.GetClaims().GetSubjectPub())
	return ctx
}

// resolveSeedSpec accepts either a workload hash or a workload name.
// snap.Specs is keyed by hash, so the hash lookup wins when the
// identifier matches one; otherwise we fall back to a by-name scan.
func resolveSeedSpec(snap state.Snapshot, identifier string) (state.WorkloadSpecView, bool) {
	if sv, ok := snap.Specs[identifier]; ok {
		return sv, true
	}
	_, sv, ok := snap.SpecByName(identifier)
	return sv, ok
}

// AllowAnonymous reports whether an anonymous caller (no cert) may
// access a spec described by auth. Used by the HTTP gateway's canonical
// URL handlers once they have resolved (publisher-slug, resource-name)
// against the snapshot.
func (g *Gate) AllowAnonymous(auth *admissionv1.SpecAuth) error {
	return decide(nil, auth, time.Now())
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
