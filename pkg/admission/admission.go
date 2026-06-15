// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"google.golang.org/protobuf/proto"
)

const CallerKey = "pln.caller"

// errLocalGrantUnpublished is returned when a node acts on its own
// authority before its Grant has gossiped. MayPublish and
// resolveAndVerify share this one sentinel so the bootstrap-window rule
// has a single owner.
var errLocalGrantUnpublished = errors.New("local grant is not yet published")

// ErrRejected wraps an authorise- or account-stage verdict so the
// control layer can surface the operator-facing reason as
// FailedPrecondition instead of a generic Internal, exactly as it does
// for placement.ErrPublishDenied. Its text is "admission", so a wrapped
// error reads "admission: <reason>". authenticate-stage failures are
// deliberately not wrapped: they are integrity faults, not
// authorisation verdicts, and stay opaque. Runtime decisions
// (Invoke/Fetch/Connect) likewise do not wrap with this; they return
// opaque wasm.ErrTargetNotFound to avoid leaking admission state to
// remote callers.
var ErrRejected = errors.New("admission")

type accessTokenCtxKey struct{}

// WithAccessToken attaches an AccessToken to ctx so downstream
// decisions can substitute token-based authorisation for grant-based
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

// Publication selects the (authority, name) publication a runtime
// decision authorises against. Policy is a per-publication property:
// two authorities can publish byte-identical content under different
// policies. Resolving through this selector rather than the deduped
// artefact map is what stops a co-publisher of identical bytes masking
// or satisfying another publication's policy.
type Publication struct {
	Name         string
	AuthorityPub []byte
}

type invokedPubCtxKey struct{}

// WithInvokedPublication records the (authority, name) publication the
// caller addressed so a remote dispatch hop authorises the same
// publication the gateway resolved, mirroring WithAccessToken. The
// selector is unauthenticated and only narrows the decision: the hop
// re-resolves it against its own gossiped state and binds it to the
// dispatched content hash, so a forged selector can only name a real
// publication whose own policy is then enforced. A nil selector or
// empty fields leave the context untouched.
func WithInvokedPublication(ctx context.Context, pub *Publication) context.Context {
	if pub == nil || len(pub.AuthorityPub) == 0 || pub.Name == "" {
		return ctx
	}
	return context.WithValue(ctx, invokedPubCtxKey{}, pub)
}

// InvokedPublicationFromContext returns the publication set by
// WithInvokedPublication, if any.
func InvokedPublicationFromContext(ctx context.Context) (*Publication, bool) {
	p, ok := ctx.Value(invokedPubCtxKey{}).(*Publication)
	return p, ok && p != nil
}

// PublicationFromToken derives the (authority, name) publication an
// access token authorises: the issuer is the publishing authority and
// the token's seed resource carries the logical name. Returns false for
// a non-seed token (e.g. a blob token presented to the fn gateway) or a
// token with no issuer.
func PublicationFromToken(token *admissionv1.AccessToken) (*Publication, bool) {
	seed := token.GetClaims().GetResource().GetSeed()
	if seed == nil {
		return nil, false
	}
	issuer := token.GetClaims().GetIssuerPub()
	if len(issuer) == 0 {
		return nil, false
	}
	return &Publication{AuthorityPub: issuer, Name: seed.GetName()}, true
}

type StateReader interface {
	Snapshot() state.Snapshot
}

// Pipeline runs the four-stage admission check (Admit:
// authenticate->authorise->account) and the runtime decisions (Invoke,
// Fetch, Connect) against a single store.
//
// Trust contract: runtime decisions trust Facts pulled from the store
// without re-verifying their signatures. This is sound because every
// write into the CRDT log first passes the integrity check for its
// kind: spec changes through this Admit pipeline (gossip, presigned,
// local self-signed), grants through the chain + subject-PoP verify,
// blob wrappings through their authority's grant-bound signature, or
// own-disk replay for the restore path. Any new write path must
// satisfy the kind's check, otherwise runtime decisions can be
// poisoned with attacker-supplied policy.
type Pipeline struct {
	store     StateReader
	manifests state.ManifestPaths
	rootPub   []byte
}

func New(rootPub []byte, store StateReader) *Pipeline {
	if store == nil {
		panic("admission.New: store is required")
	}
	return &Pipeline{store: store, rootPub: rootPub}
}

// SetManifestPaths wires a static-manifest reader so Fetch can authorise
// blobs nested inside a published static site. Without it, only direct
// references (workload hash, blob digest, manifest digest itself) are
// authorised: nested file blobs are denied, breaking cross-node static
// replication. Call once after the blobs service is constructed.
func (p *Pipeline) SetManifestPaths(mp state.ManifestPaths) {
	p.manifests = mp
}

// Admit is the four-stage Fact pipeline run on every write into the
// CRDT log (gossip-received, presigned wire, and local self-signed).
// authenticate proves the Fact and resolves the authority's Grant;
// authorise checks the per-kind publish capability and the publisher's
// own policy attributes; AccountCheck enforces the per-Principal count
// budget; admit (the CRDT merge) is the caller's mutateLocal. One
// snapshot is taken and threaded through every stage: Snapshot() is a
// lock-free atomic load, so this is safe even though Admit runs as the
// store's validate hook under its lock.
func (p *Pipeline) Admit(sc *statev1.SpecChange) error {
	snap := p.store.Snapshot()
	authGrant, err := p.authenticate(snap, sc)
	if err != nil {
		return err
	}
	if err := authorise(sc, authGrant); err != nil {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	if err := AccountCheck(snap, sc.GetFact(), authGrant); err != nil {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return nil
}

// Invoke authorises caller to invoke the workload at hash. When pub is
// non-nil the caller addressed one specific (authority, name)
// publication (an anonymous named URL, an access token, or a name
// resolved within the caller's own authority): the decision is against
// exactly that publication's Fact, and the publication's content hash
// must equal hash so a permissive policy cannot be paired with other
// bytes. When pub is nil (a downstream relay hop, or a bare-hash
// invoke that names no publication) the decision is a union over every
// publication of these bytes, mirroring Fetch: admitted if any one
// allows the caller. A nil caller is admitted only by a public policy;
// in that case the returned CallerInfo is empty, mirroring the
// InvokeByToken path. Mesh-peer callers resolve their grant from
// snap.Nodes via LookupGrant, wire-mode callers pass their
// session-bound grant directly.
func (p *Pipeline) Invoke(caller *identityv1.Grant, pub *Publication, hash string) (wasm.CallerInfo, error) {
	snap := p.store.Snapshot()
	var facts []*factv1.Fact
	if pub != nil {
		f, ok := publicationFact(snap, *pub, hash)
		if !ok {
			return wasm.CallerInfo{}, wasm.ErrTargetNotFound
		}
		facts = []*factv1.Fact{f}
	} else {
		facts = snap.WorkloadEntitlements(hash)
		if len(facts) == 0 {
			return wasm.CallerInfo{}, wasm.ErrTargetNotFound
		}
	}
	now := time.Now()
	denied := snap.DenyChecker()
	for _, f := range facts {
		if p.decide(caller, f, now, denied) != nil {
			continue
		}
		if caller == nil {
			return wasm.CallerInfo{}, nil
		}
		return wasm.CallerInfo{
			PeerKey:    types.PeerKeyFromBytes(caller.GetClaims().GetSubjectPub()),
			Attributes: caller.GetClaims().GetCapabilities().GetAttributes().AsMap(),
		}, nil
	}
	return wasm.CallerInfo{}, wasm.ErrTargetNotFound
}

// Fetch authorises caller to read the CAS object at hash. The same
// stream type carries workload binaries, named-blob payloads, static
// manifests, and the file blobs nested inside those manifests, so the
// lookup unions every referencing Fact and admits the caller if any one
// of them allows the grant. Without unioning, a non-publisher replica
// can never fetch the bytes from the publisher and stays stuck in a
// fetch-EOF loop. A nil caller is admitted only when at least one
// referencing Fact has policy.public=true.
func (p *Pipeline) Fetch(caller *identityv1.Grant, hash string) error {
	snap := p.store.Snapshot()
	facts := snap.BlobEntitlements(hash, p.manifests)
	if len(facts) == 0 {
		return wasm.ErrTargetNotFound
	}
	now := time.Now()
	denied := snap.DenyChecker()
	for _, f := range facts {
		if p.decide(caller, f, now, denied) == nil {
			return nil
		}
	}
	return wasm.ErrTargetNotFound
}

// Connect authorises caller to open a connection to (hostPeer, port).
// The decision is direction-agnostic: callers pass the local peer as
// hostPeer when authorising an inbound stream, and the remote peer when
// authorising one this node is about to open. A nil caller is admitted
// only when the target service's policy has public=true.
func (p *Pipeline) Connect(caller *identityv1.Grant, hostPeer types.PeerKey, port uint32) error {
	snap := p.store.Snapshot()
	target, ok := snap.Nodes[hostPeer]
	if !ok {
		return wasm.ErrTargetNotFound
	}
	// Union over every service on this port (mirroring Invoke/Fetch): admit if
	// any one allows the caller, so the verdict can't depend on map order when
	// two services share a port.
	now := time.Now()
	denied := snap.DenyChecker()
	for _, svc := range target.Services {
		if svc.Port != port || svc.Fact == nil {
			continue
		}
		if p.decide(caller, svc.Fact, now, denied) == nil {
			return nil
		}
	}
	return wasm.ErrTargetNotFound
}

// LookupGrant resolves a mesh peer's grant via the gossiped snapshot.
// Use it on transport-authenticated inbound paths (mesh streams) where
// the only thing the caller can present is their peer key; wire-mode
// RPC paths already carry the grant in the request context and should
// pass it directly.
func (p *Pipeline) LookupGrant(peerKey types.PeerKey) *identityv1.Grant {
	snap := p.store.Snapshot()
	nv, ok := snap.Nodes[peerKey]
	if !ok {
		return nil
	}
	return nv.Grant
}

// issuerGrantValid binds an access token to its issuer's live authority:
// VerifyAccessToken checks only the token's own signature and expiry, so
// without this an expired or denied publisher's `pln share` URLs would
// keep serving for the token's full TTL. Fail-closed: an unresolved issuer
// grant denies.
func (p *Pipeline) issuerGrantValid(snap state.Snapshot, issuer []byte) bool {
	grant := snap.GrantFor(issuer)
	if grant == nil {
		return false
	}
	return identity.CheckGrant(grant, p.rootPub, time.Now(), issuer, snap.DenyChecker()).Status.Valid()
}

// FetchByToken authorises an anonymous caller holding token to read the
// CAS object at hash. The token must verify (signature, expiry), the
// issuer's grant must still be live (issuerGrantValid), and the token's
// resource must correspond to a Fact whose authority signed the token and
// whose entitlements cover hash.
func (p *Pipeline) FetchByToken(token *admissionv1.AccessToken, hash string) error {
	if err := auth.VerifyAccessToken(token, time.Now()); err != nil {
		return wasm.ErrTargetNotFound
	}
	resource := token.GetClaims().GetResource()
	issuer := token.GetClaims().GetIssuerPub()
	snap := p.store.Snapshot()
	if !p.issuerGrantValid(snap, issuer) {
		return wasm.ErrTargetNotFound
	}
	for _, f := range snap.BlobEntitlements(hash, p.manifests) {
		if !bytes.Equal(f.GetAuthorityPub(), issuer) {
			continue
		}
		if !proto.Equal(f.GetResource(), resource) {
			continue
		}
		return nil
	}
	return wasm.ErrTargetNotFound
}

// InvokeByToken authorises an anonymous caller holding token to invoke
// the workload at hash. Same shape as Invoke but the identity comes
// from the token rather than a peer grant; the returned CallerInfo has
// no attributes since anonymous callers carry no grant.
func (p *Pipeline) InvokeByToken(token *admissionv1.AccessToken, hash string) (wasm.CallerInfo, error) {
	if err := auth.VerifyAccessToken(token, time.Now()); err != nil {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	pub, ok := PublicationFromToken(token)
	if !ok {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	snap := p.store.Snapshot()
	if !p.issuerGrantValid(snap, token.GetClaims().GetIssuerPub()) {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	// The token is itself the authorisation: its issuer is the publishing
	// authority and its seed resource the name. publicationFact resolves
	// that exact (authority, name) publication and binds it to hash, so a
	// signature-verified token admits the invoke regardless of the spec's
	// own policy, mirroring FetchByToken. Routing through decide(nil, ...)
	// instead would wrongly demand public=true and break `pln share` of a
	// gated workload. The caller is anonymous and carries no attributes.
	if _, ok := publicationFact(snap, *pub, hash); !ok {
		return wasm.CallerInfo{}, wasm.ErrTargetNotFound
	}
	return wasm.CallerInfo{}, nil
}

// MayHost authorises hostGrant to host the workload described by the
// single Fact f. Hosting includes loopback invocation, so the policy
// must hold against the host's own grant. Callers that hold one
// specific referencing Fact (blobs unions BlobEntitlements itself) use
// this; workload hosting, where the bytes are shared across
// publications, goes through MayHostByHash.
func (p *Pipeline) MayHost(hostGrant *identityv1.Grant, f *factv1.Fact) error {
	if hostGrant == nil || f == nil {
		return wasm.ErrTargetNotFound
	}
	return p.decide(hostGrant, f, time.Now(), p.store.Snapshot().DenyChecker())
}

// MayHostByHash authorises hostGrant to host the bytes at hash. Hosting
// is artefact-shared: one module serves every publication of identical
// bytes, so the decision is a union over WorkloadEntitlements(hash):
// the host may run the bytes if any referencing publication's policy
// admits it, mirroring blobs.MayStore over BlobEntitlements. A
// co-publisher's stricter policy therefore cannot suppress hosting of a
// permissive publication of the same bytes.
func (p *Pipeline) MayHostByHash(hostGrant *identityv1.Grant, hash string) error {
	if hostGrant == nil {
		return wasm.ErrTargetNotFound
	}
	snap := p.store.Snapshot()
	facts := snap.WorkloadEntitlements(hash)
	if len(facts) == 0 {
		return wasm.ErrTargetNotFound
	}
	now := time.Now()
	denied := snap.DenyChecker()
	for _, f := range facts {
		if p.decide(hostGrant, f, now, denied) == nil {
			return nil
		}
	}
	return wasm.ErrTargetNotFound
}

// MayPublish reports whether grant satisfies policy at publish time.
// The returned error is descriptive so the local publisher can see
// exactly why their grant doesn't qualify. The other pipeline methods
// (Invoke, Fetch, Connect, MayHost) return opaque ErrTargetNotFound
// to avoid leaking admission state to remote callers; MayPublish
// runs against the local grant only, so descriptive errors are safe.
//
// A nil policy is always permitted, even when grant is nil, so that
// publishes during the bootstrap window (before the local grant lands
// in gossip) keep working.
func (p *Pipeline) MayPublish(grant *identityv1.Grant, policy *admissionv1.Predicate) error {
	if policy == nil {
		return nil
	}
	if grant == nil {
		return errLocalGrantUnpublished
	}
	if chk := identity.CheckGrant(grant, p.rootPub, time.Now(), nil, nil); !chk.Status.Valid() {
		return fmt.Errorf("local grant %s: %s", chk.Status, chk.Reason)
	}
	return checkPolicyClauses(grant, policy)
}

// decide authorises grant against f's policy. A nil grant (anonymous
// caller) is admitted only when the spec's policy has public=true;
// otherwise the grant is held to the durable-authority rule (chain to
// root, within horizon, not denied) and its attributes are matched
// against the policy's inline clauses. Public also short-circuits
// grant-bearing callers: anyone reaching a public spec is admitted
// regardless of their attribute claims.
func (p *Pipeline) decide(grant *identityv1.Grant, f *factv1.Fact, now time.Time, denied identity.DenyChecker) error {
	policy := f.GetPolicy()
	if policy.GetPublic() {
		return nil
	}
	if grant == nil {
		return wasm.ErrTargetNotFound
	}
	if chk := identity.CheckGrant(grant, p.rootPub, now, nil, denied); !chk.Status.Valid() {
		return wasm.ErrTargetNotFound
	}
	if err := checkPolicyClauses(grant, policy); err != nil {
		return wasm.ErrTargetNotFound
	}
	return nil
}

// checkPolicyClauses returns nil if grant satisfies policy, or a
// descriptive error otherwise. A nil or public policy is trivially
// satisfied; an inline policy is matched clause by clause against the
// grant's attributes.
func checkPolicyClauses(grant *identityv1.Grant, policy *admissionv1.Predicate) error {
	if policy == nil {
		return nil
	}
	if policy.GetPublic() {
		return nil
	}
	inline := policy.GetInline()
	if inline == nil {
		return errors.New("policy has no inline clauses")
	}
	ctx := grantContext(grant)
	for _, clause := range inline.GetClauses() {
		got, ok := ctx[clause.GetKey()]
		if !ok {
			return fmt.Errorf("local grant is missing prop %q (policy requires %q)", clause.GetKey(), clause.GetEquals())
		}
		if got != clause.GetEquals() {
			return fmt.Errorf("local grant prop %q is %q; policy requires %q", clause.GetKey(), got, clause.GetEquals())
		}
	}
	return nil
}

func grantContext(grant *identityv1.Grant) map[string]string {
	ctx := make(map[string]string)
	for k, v := range grant.GetClaims().GetCapabilities().GetAttributes().GetFields() {
		if s := v.GetStringValue(); s != "" {
			ctx[k] = s
		}
	}
	ctx[CallerKey] = hex.EncodeToString(grant.GetClaims().GetSubjectPub())
	return ctx
}

// publicationFact resolves the Fact for the (authority, name)
// publication pub via SpecByName, requiring its content hash to equal
// hash.
func publicationFact(snap state.Snapshot, pub Publication, hash string) (*factv1.Fact, bool) {
	_, sv, ok := snap.SpecByName(pub.Name, types.PeerKeyFromBytes(pub.AuthorityPub))
	if !ok || sv.Fact == nil || sv.Spec.Hash != hash {
		return nil, false
	}
	return sv.Fact, true
}

// AllowAnonymous reports whether an anonymous caller (no grant) may
// access a spec described by f. Used by the HTTP gateway's canonical
// URL handlers once they have resolved (authority-slug, resource-name)
// against the snapshot.
func (p *Pipeline) AllowAnonymous(f *factv1.Fact) error {
	return p.decide(nil, f, time.Now(), p.store.Snapshot().DenyChecker())
}

func decodeSpecChange(sc *statev1.SpecChange) (fact.Body, *admissionv1.ResourceID, error) {
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
	return nil, nil, errors.New("admission: empty spec body")
}
