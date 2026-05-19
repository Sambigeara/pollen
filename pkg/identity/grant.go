// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"buf.build/go/protovalidate"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// DefaultGrantDeadlineTTL is the horizon callers (join/bootstrap and
// membership) apply to delegated grants when the issuer specifies none.
// This package does not impose it: IssueGrant leaves a zero deadline as
// no-horizon and clamps only to the parent. Past the applied horizon a
// grant can no longer mint sessions or vouch for facts and a fresh join
// is required. Admin/root grants carry no horizon (zero).
const DefaultGrantDeadlineTTL = 30 * 24 * time.Hour

// renewLeadWindow is how far before its deadline a grant becomes due
// for proactive renewal. It must comfortably exceed the renewal poll
// interval (the daemon tick, or how often a wire tenant runs pln) so a
// node gets many attempts against a possibly unreachable delegating
// peer before the deadline bites. One third of the default horizon
// gives roughly ten days of runway on the 30-day default.
const renewLeadWindow = DefaultGrantDeadlineTTL / 3

// GrantRenewDue reports whether grant has a finite horizon within the
// renewal lead window of now. Grants with no horizon (admin/root) are
// never due. It is the single owner of the renew-now policy shared by
// the daemon maintenance loop and the wire-mode CLI.
func GrantRenewDue(grant *identityv1.Grant, now time.Time) bool {
	dl := grant.GetClaims().GetGrantDeadlineUnix()
	if dl == 0 {
		return false
	}
	return time.Unix(dl, 0).Sub(now) < renewLeadWindow
}

var ErrGrantInvalid = errors.New("grant invalid")

func FullCapabilities() *identityv1.Capabilities {
	return &identityv1.Capabilities{
		CanDelegate: true,
		CanAdmit:    true,
		MaxDepth:    255, //nolint:mnd
		Publish: &identityv1.PublishCapability{
			Functions: true,
			Blobs:     true,
			Sites:     true,
			Services:  true,
		},
	}
}

func PublisherCapabilities() *identityv1.Capabilities {
	return &identityv1.Capabilities{
		Publish: &identityv1.PublishCapability{
			Functions: true,
			Blobs:     true,
			Sites:     true,
			Services:  true,
		},
	}
}

func LeafCapabilities() *identityv1.Capabilities {
	return &identityv1.Capabilities{Publish: &identityv1.PublishCapability{}}
}

// UnlimitedBudget is the ceiling for admin/root grants: every field
// zero, read as "no limit" by the account stage.
func UnlimitedBudget() *identityv1.Budget {
	return &identityv1.Budget{}
}

// IssueGrant signs a grant for subjectPub. parent is the signer's own
// grant (the immediate delegating authority) or nil for a root
// self-issue. The embedded chain is derived from parent alone, so a
// caller cannot truncate the lineage: signGrant flattens parent plus
// parent's own already-flattened chain.
func IssueGrant(
	signerPriv ed25519.PrivateKey,
	parent *identityv1.Grant,
	subjectPub ed25519.PublicKey,
	caps *identityv1.Capabilities,
	budget *identityv1.Budget,
	notBefore, grantDeadline time.Time,
) (*identityv1.Grant, error) {
	if len(subjectPub) != ed25519.PublicKeySize {
		return nil, errors.New("invalid subject key length")
	}
	if caps == nil {
		return nil, errors.New("capabilities required")
	}
	if budget == nil {
		return nil, errors.New("budget required")
	}
	if !grantDeadline.IsZero() && !grantDeadline.After(notBefore) {
		return nil, errors.New("invalid grant validity window")
	}
	if err := ValidateAttributes(caps.GetAttributes()); err != nil {
		return nil, err
	}

	signerPub := signerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	if parent != nil {
		clamped, err := applyParent(parent, signerPub, caps, budget, grantDeadline)
		if err != nil {
			return nil, err
		}
		grantDeadline = clamped
	}
	return signGrant(signerPriv, parent, subjectPub, caps, budget, notBefore, grantDeadline)
}

// applyParent clamps a child grant's horizon to its parent's and
// enforces that the signer owns the parent and is not granting beyond
// its own authority. These are issuance-time conveniences that fail
// early with a clear error; they are not the security boundary.
// verifyGrantChain re-enforces the same subset and horizon rules on
// every link and re-anchors the chain at the pinned root, so a caller
// passing an unverified or forged parent here cannot produce a grant
// that verifies.
func applyParent(
	parent *identityv1.Grant,
	signerPub ed25519.PublicKey,
	caps *identityv1.Capabilities,
	budget *identityv1.Budget,
	grantDeadline time.Time,
) (time.Time, error) {
	if pd := parent.GetClaims().GetGrantDeadlineUnix(); pd > 0 {
		parentDeadline := time.Unix(pd, 0)
		if grantDeadline.IsZero() || grantDeadline.After(parentDeadline) {
			grantDeadline = parentDeadline
		}
	}
	if !bytes.Equal(parent.GetClaims().GetSubjectPub(), signerPub) {
		return time.Time{}, errors.New("signer key does not match parent grant subject")
	}
	if err := validateChildCapabilities(caps, parent.GetClaims().GetCapabilities()); err != nil {
		return time.Time{}, err
	}
	if err := validateChildBudget(budget, parent.GetClaims().GetBudget()); err != nil {
		return time.Time{}, err
	}
	return grantDeadline, nil
}

func signGrant(
	signerPriv ed25519.PrivateKey,
	parent *identityv1.Grant,
	subjectPub ed25519.PublicKey,
	caps *identityv1.Capabilities,
	budget *identityv1.Budget,
	notBefore, grantDeadline time.Time,
) (*identityv1.Grant, error) {
	signerPub := signerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	var serialBytes [8]byte
	if _, err := rand.Read(serialBytes[:]); err != nil {
		return nil, err
	}
	serial := binary.BigEndian.Uint64(serialBytes[:])
	if serial == 0 {
		serial = 1
	}

	claims := &identityv1.GrantClaims{
		SubjectPub:    subjectPub,
		IssuerPub:     signerPub,
		Capabilities:  caps,
		Budget:        budget,
		NotBeforeUnix: notBefore.Unix(),
		Serial:        serial,
	}
	if !grantDeadline.IsZero() {
		claims.GrantDeadlineUnix = grantDeadline.Unix()
	}

	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	sig, err := SignPayload(signerPriv, msg, sigContextGrant)
	if err != nil {
		return nil, err
	}

	return &identityv1.Grant{
		Claims:    claims,
		Chain:     flattenLineage(parent),
		Signature: sig,
	}, nil
}

// validateChildCapabilities enforces that an issued grant cannot grant
// itself authority its issuer lacks. Skipped only at root self-issuance
// (parent == nil), where the root defines its own ceiling.
func validateChildCapabilities(child, parent *identityv1.Capabilities) error {
	if !parent.GetCanDelegate() {
		return errors.New("parent grant lacks CanDelegate capability")
	}
	if child.GetCanAdmit() && !parent.GetCanAdmit() {
		return errors.New("child capabilities exceed parent: CanAdmit")
	}
	cp, pp := child.GetPublish(), parent.GetPublish()
	if cp.GetFunctions() && !pp.GetFunctions() {
		return errors.New("child capabilities exceed parent: publish functions")
	}
	if cp.GetBlobs() && !pp.GetBlobs() {
		return errors.New("child capabilities exceed parent: publish blobs")
	}
	if cp.GetSites() && !pp.GetSites() {
		return errors.New("child capabilities exceed parent: publish sites")
	}
	if cp.GetServices() && !pp.GetServices() {
		return errors.New("child capabilities exceed parent: publish services")
	}
	if child.GetMaxDepth() > parent.GetMaxDepth() {
		return errors.New("child capabilities exceed parent: MaxDepth")
	}
	return validateAttributesSubset(child.GetAttributes(), parent.GetAttributes())
}

// validateAttributesSubset enforces that every key the child claims is
// also held by the parent with an equal value. The child may omit keys
// to narrow scope, but cannot add new keys or change a value: gate
// policy decisions trust attribute claims, so an issuer must not be
// able to mint scopes it never received.
func validateAttributesSubset(child, parent *structpb.Struct) error {
	if len(child.GetFields()) == 0 {
		return nil
	}
	parentFields := parent.GetFields()
	for k, cv := range child.GetFields() {
		pv, ok := parentFields[k]
		if !ok {
			return fmt.Errorf("child capabilities exceed parent: attribute %q not granted by parent", k)
		}
		if !proto.Equal(cv, pv) {
			return fmt.Errorf("child capabilities exceed parent: attribute %q value not granted by parent", k)
		}
	}
	return nil
}

// validateChildBudget enforces that a child grant's per-Principal count
// budget does not exceed its parent's in any dimension, under the
// zero-means-unlimited rule: a parent dimension of 0 imposes no bound,
// but a child of 0 beneath a bounded parent is itself unlimited and so
// exceeds it. This is the budget analogue of validateChildCapabilities.
// The control IssueGrant ceiling enforces the same relation against the
// RPC caller on the `pln grant` path; enforcing it here on the signing
// chain additionally bounds the invite->redeem path and any grant signed
// directly against the proto, where no control-layer caller check runs.
func validateChildBudget(child, parent *identityv1.Budget) error {
	check := func(kind string, c, p uint32) error {
		if p == 0 {
			return nil
		}
		if c == 0 || c > p {
			return fmt.Errorf("child budget exceeds parent: %s", kind)
		}
		return nil
	}
	if err := check("functions", child.GetMaxFunctions(), parent.GetMaxFunctions()); err != nil {
		return err
	}
	if err := check("blobs", child.GetMaxBlobs(), parent.GetMaxBlobs()); err != nil {
		return err
	}
	return check("sites", child.GetMaxSites(), parent.GetMaxSites())
}

// childDeadlineWithinParent enforces that a horizon cannot widen down
// the chain: a child of a horizon-bounded parent must itself carry a
// horizon no later than its parent's. A parent with no horizon (the
// cluster root) places no bound. Issuance clamps this; verification
// rejects a forged grant that tried to extend its own horizon.
func childDeadlineWithinParent(child, parent *identityv1.GrantClaims) error {
	pd := parent.GetGrantDeadlineUnix()
	if pd == 0 {
		return nil
	}
	cd := child.GetGrantDeadlineUnix()
	if cd == 0 || cd > pd {
		return errors.New("grant chain escalation: child horizon exceeds parent")
	}
	return nil
}

// flattenLineage returns the child's embedded chain: parent followed by
// parent's own (already-flattened) ancestors, leaf-to-root, with every
// entry's nested Chain cleared. Deriving the full lineage here from the
// single parent, rather than trusting a caller-supplied slice, makes a
// truncated chain unrepresentable: a depth-N issuer can no longer
// produce a grant that anchors at the wrong root. nil parent (a root
// self-issue) yields no chain.
func flattenLineage(parent *identityv1.Grant) []*identityv1.Grant {
	if parent == nil {
		return nil
	}
	ancestors := parent.GetChain()
	out := make([]*identityv1.Grant, 0, len(ancestors)+1)
	out = append(out, stripped(parent))
	for _, g := range ancestors {
		out = append(out, stripped(g))
	}
	return out
}

// stripped copies a grant with its nested Chain dropped: an entry in a
// flattened lineage carries only its own claims and signature.
func stripped(g *identityv1.Grant) *identityv1.Grant {
	return &identityv1.Grant{Claims: g.GetClaims(), Signature: g.GetSignature()}
}

// maxGrantChainDepth bounds the chain walk. Real delegation topologies
// (root, regional admin, tenant admin, tenant, workload) are a handful
// deep; the cap stops a hostile peer gossiping a multi-thousand-entry
// chain that would force that many signature verifications per check.
const maxGrantChainDepth = 64

func verifyGrantChain(grant *identityv1.Grant) (ed25519.PublicKey, error) {
	if err := protovalidate.Validate(grant); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrGrantInvalid, err)
	}
	if len(grant.GetChain()) > maxGrantChainDepth {
		return nil, fmt.Errorf("grant chain exceeds max depth %d", maxGrantChainDepth)
	}

	current := grant
	// depthBelow counts delegation hops beneath the parent at each step.
	// max_depth is the depth of the subtree a grant may root, so it must
	// be enforced against the realised chain here at verification, not
	// only as a monotone scalar at issuance: an attacker holding a
	// shallow-budget grant could otherwise sign an arbitrarily deep
	// subtree directly.
	depthBelow := 0
	for _, parent := range grant.GetChain() {
		if !bytes.Equal(current.GetClaims().GetIssuerPub(), parent.GetClaims().GetSubjectPub()) {
			return nil, errors.New("grant chain issuer/subject mismatch")
		}
		msg, err := SignaturePayload(current.GetClaims())
		if err != nil {
			return nil, err
		}
		if err := VerifyPayload(ed25519.PublicKey(current.GetClaims().GetIssuerPub()), msg, current.GetSignature(), sigContextGrant); err != nil {
			return nil, errors.New("grant signature invalid")
		}
		// The capability subset, attribute subset and horizon clamp are
		// enforced again here, not only at issuance: an attacker holding
		// any delegating grant owns its subject key and can sign a child
		// proto directly, bypassing IssueGrant. Verification, not
		// issuance, is the authority boundary, so every parent->child
		// link is re-checked against the same predicate.
		if err := validateChildCapabilities(current.GetClaims().GetCapabilities(), parent.GetClaims().GetCapabilities()); err != nil {
			return nil, fmt.Errorf("grant chain escalation: %w", err)
		}
		if err := validateChildBudget(current.GetClaims().GetBudget(), parent.GetClaims().GetBudget()); err != nil {
			return nil, fmt.Errorf("grant chain escalation: %w", err)
		}
		if err := childDeadlineWithinParent(current.GetClaims(), parent.GetClaims()); err != nil {
			return nil, err
		}
		depthBelow++
		if int64(parent.GetClaims().GetCapabilities().GetMaxDepth()) < int64(depthBelow) {
			return nil, fmt.Errorf("grant chain escalation: subtree depth %d exceeds parent max_depth %d", depthBelow, parent.GetClaims().GetCapabilities().GetMaxDepth())
		}
		current = parent
	}

	rootPub := ed25519.PublicKey(current.GetClaims().GetIssuerPub())
	msg, err := SignaturePayload(current.GetClaims())
	if err != nil {
		return nil, err
	}
	if err := VerifyPayload(rootPub, msg, current.GetSignature(), sigContextGrant); err != nil {
		return nil, errors.New("grant root signature invalid")
	}

	return rootPub, nil
}

// ChainSubjectPubs returns every pub authoritatively above (and
// including) the leaf in this grant's delegation lineage. Walks every
// grant from leaf to root, collecting each grant's subject_pub plus the
// topmost issuer_pub. The topmost issuer is the root signing key — for
// fully-chained grants it duplicates the root's subject; for short
// chains it surfaces root authority explicitly so root-issued denies
// remain authorisable.
func ChainSubjectPubs(grant *identityv1.Grant) [][]byte {
	if grant == nil {
		return nil
	}
	chain := grant.GetChain()
	out := make([][]byte, 0, len(chain)+2) //nolint:mnd
	out = append(out, grant.GetClaims().GetSubjectPub())
	for _, parent := range chain {
		out = append(out, parent.GetClaims().GetSubjectPub())
	}
	top := grant
	if len(chain) > 0 {
		top = chain[len(chain)-1]
	}
	if issuer := top.GetClaims().GetIssuerPub(); len(issuer) > 0 {
		out = append(out, issuer)
	}
	return out
}

// SignGrantSubject produces a subject-side proof-of-possession over a
// grant's claims, signed with the subject's identity key. Required when
// gossiping a node's current grant: grant.signature alone is by the
// issuer, which would let any admin re-parent any peer pub. Verifying
// this with grant.subject_pub proves the peer actually owns the
// published grant.
func SignGrantSubject(grant *identityv1.Grant, subjectPriv ed25519.PrivateKey) ([]byte, error) {
	if grant == nil {
		return nil, errors.New("grant is nil")
	}
	subjectPub := subjectPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if !bytes.Equal(grant.GetClaims().GetSubjectPub(), subjectPub) {
		return nil, errors.New("signing key does not match grant subject")
	}
	msg, err := SignaturePayload(grant.GetClaims())
	if err != nil {
		return nil, err
	}
	return SignPayload(subjectPriv, msg, sigContextGrantSubject)
}

// VerifyGrantSubject checks the subject-side signature against
// grant.subject_pub. Pair with VerifyGrantStructure to fully
// authenticate a gossiped grant event.
func VerifyGrantSubject(grant *identityv1.Grant, signature []byte) error {
	if grant == nil {
		return errors.New("grant is nil")
	}
	msg, err := SignaturePayload(grant.GetClaims())
	if err != nil {
		return err
	}
	return VerifyPayload(ed25519.PublicKey(grant.GetClaims().GetSubjectPub()), msg, signature, sigContextGrantSubject)
}

// VerifyGrantStructure validates the chain signatures and root anchor
// without enforcing the validity window. Use when applying a gossiped
// grant: an expired grant is still authoritative for chain-scoped
// policy (denies issued by a now-expired admin still bind its subtree).
func VerifyGrantStructure(grant *identityv1.Grant, rootPub []byte) error {
	got, err := verifyGrantChain(grant)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, rootPub) {
		return errors.New("grant chain root mismatch")
	}
	return nil
}

// DenyChecker reports whether a subject pubkey is currently denied.
// Implementations typically wrap a cluster snapshot.
type DenyChecker func(subjectPub []byte) bool

// GrantStatus is the typed result of CheckGrant. A grant has a single
// durable horizon and no NeedsRenewal state: it is authoritative until
// grant_deadline, and the short liveness window lives entirely in
// Session.
type GrantStatus int

// GrantStatusInvalidChain is deliberately the zero value: a
// default-constructed GrantCheck is fail-closed, so a future code path
// that returns one before setting Status explicitly denies rather than
// silently authorises.
const (
	GrantStatusInvalidChain GrantStatus = iota
	GrantStatusOK
	GrantStatusNotYetValid
	GrantStatusExpired
	GrantStatusRevoked
	GrantStatusSubjectMismatch
)

func (s GrantStatus) String() string {
	switch s {
	case GrantStatusOK:
		return "ok"
	case GrantStatusNotYetValid:
		return "not-yet-valid"
	case GrantStatusExpired:
		return "expired"
	case GrantStatusRevoked:
		return "revoked"
	case GrantStatusInvalidChain:
		return "invalid-chain"
	case GrantStatusSubjectMismatch:
		return "subject-mismatch"
	}
	return fmt.Sprintf("unknown(%d)", int(s))
}

// Valid reports whether a grant in this status may mint sessions and
// vouch for facts.
func (s GrantStatus) Valid() bool { return s == GrantStatusOK }

// GrantCheck is the typed result of CheckGrant. Time fields are always
// populated from claims, even on failure paths.
type GrantCheck struct {
	NotBefore     time.Time
	GrantDeadline time.Time
	Reason        string
	Status        GrantStatus
}

// CheckGrant validates a grant and returns its typed status. Performs
// chain + signature verification, optional subject-match, chain-aware
// denylist consultation, then the time window, in that order.
//
// expectedSubject = nil skips subject matching; denied = nil skips the
// denylist check. The denylist check is chain-aware: rejected if the
// leaf subject OR any chain ancestor is denied, so revoking an admin
// poisons its whole subtree. grant_deadline = 0 means no hard horizon
// (admin/root grants).
func CheckGrant(
	grant *identityv1.Grant,
	rootPub []byte,
	now time.Time,
	expectedSubject []byte,
	denied DenyChecker,
) GrantCheck {
	if grant == nil {
		return GrantCheck{Status: GrantStatusInvalidChain, Reason: "grant is nil"}
	}
	claims := grant.GetClaims()
	nb := time.Unix(claims.GetNotBeforeUnix(), 0)
	var gd time.Time
	if d := claims.GetGrantDeadlineUnix(); d > 0 {
		gd = time.Unix(d, 0)
	}
	out := GrantCheck{NotBefore: nb, GrantDeadline: gd}

	got, err := verifyGrantChain(grant)
	if err != nil {
		out.Status = GrantStatusInvalidChain
		out.Reason = err.Error()
		return out
	}
	if !bytes.Equal(got, rootPub) {
		out.Status = GrantStatusInvalidChain
		out.Reason = "chain root mismatch"
		return out
	}
	if len(expectedSubject) > 0 && !bytes.Equal(claims.GetSubjectPub(), expectedSubject) {
		out.Status = GrantStatusSubjectMismatch
		out.Reason = "subject does not match expected"
		return out
	}
	if denied != nil {
		if slices.ContainsFunc(ChainSubjectPubs(grant), denied) {
			out.Status = GrantStatusRevoked
			out.Reason = "subject or chain ancestor is denied"
			return out
		}
	}
	if now.Before(nb.Add(-TimeSkewAllowance)) {
		out.Status = GrantStatusNotYetValid
		out.Reason = fmt.Sprintf("not yet valid until %s", nb.UTC().Format(time.RFC3339))
		return out
	}
	if !gd.IsZero() && now.After(gd.Add(TimeSkewAllowance)) {
		out.Status = GrantStatusExpired
		out.Reason = fmt.Sprintf("grant deadline passed at %s; re-join required", gd.UTC().Format(time.RFC3339))
		return out
	}
	out.Status = GrantStatusOK
	return out
}
