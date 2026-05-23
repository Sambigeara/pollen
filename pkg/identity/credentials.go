// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

var (
	ErrCredentialsNotFound = errors.New("node identity credentials not found")
	ErrDifferentCluster    = errors.New("node already has credentials for a different cluster root")
)

// Credentials is a node's durable identity: the root it trusts, its
// own signing key, and the long-lived Grant that vouches for it. The
// session is the short-lived liveness proof, re-minted locally on
// demand with no network round-trip.
type Credentials struct {
	grant       *identityv1.Grant
	session     *identityv1.Session
	identityDir string
	rootPub     ed25519.PublicKey
	signPriv    ed25519.PrivateKey
	mu          sync.RWMutex
}

// NewCredentials builds an in-memory Credentials handle that AdoptGrant
// will not persist. Production callers use LoadCredentials or
// EnrollGrant for a persistent handle.
func NewCredentials(rootPub ed25519.PublicKey, signPriv ed25519.PrivateKey, grant *identityv1.Grant) *Credentials {
	return &Credentials{rootPub: rootPub, signPriv: signPriv, grant: grant}
}

func (c *Credentials) RootPub() ed25519.PublicKey { return c.rootPub }

func (c *Credentials) SubjectPub() ed25519.PublicKey {
	return c.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
}

func (c *Credentials) Grant() *identityv1.Grant {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.grant
}

// AdoptGrant validates g and installs it as the live grant, clearing
// the cached session and rewriting the on-disk copy under one lock so
// readers never observe a half-committed state. g must chain to our
// root, be within its horizon, carry our own signing key as its
// subject, and not be denied; otherwise the current grant is kept and
// an error returned. The same entry point covers first-time enrol,
// proactive renewal and admin-initiated upgrade. denied may be nil
// when the issuing server has already enforced the denylist and the
// caller has no local cluster view (wire client); the mesh handler
// supplies a real DenyChecker.
func (c *Credentials) AdoptGrant(g *identityv1.Grant, now time.Time, denied DenyChecker) error {
	chk := CheckGrant(g, c.rootPub, now, c.SubjectPub(), denied)
	if !chk.Status.Valid() {
		return fmt.Errorf("grant rejected: %s: %s", chk.Status, chk.Reason)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prevGrant, prevSession := c.grant, c.session
	c.grant = g
	c.session = nil
	if c.identityDir == "" {
		return nil
	}
	if err := writeCredentials(c.identityDir, c.rootPub, g); err != nil {
		c.grant = prevGrant
		c.session = prevSession
		return fmt.Errorf("persist grant: %w", err)
	}
	return nil
}

// EnsureFreshSession returns a session valid for at least refreshBefore
// into the future, minting a new one from the held grant if the cached
// session is absent or close to expiry. Local only: no issuer, no mesh.
func (c *Credentials) EnsureFreshSession(now time.Time, ttl, refreshBefore time.Duration) (*identityv1.Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		na := time.Unix(c.session.GetClaims().GetNotAfterUnix(), 0)
		if now.Add(refreshBefore).Before(na) {
			return c.session, nil
		}
	}
	session, err := MintSession(c.grant, c.signPriv, now, ttl)
	if err != nil {
		return nil, err
	}
	c.session = session
	return session, nil
}

// IssueInvite signs an invite ticket as this node's authority. The
// node must hold a delegating grant for the ticket to be redeemable.
func (c *Credentials) IssueInvite(
	bootstrap []*admissionv1.BootstrapPeer,
	subjectPub ed25519.PublicKey,
	caps *identityv1.Capabilities,
	budget *identityv1.Budget,
	grantDeadline, now time.Time,
	ttl time.Duration,
) (*identityv1.InviteTicket, error) {
	return IssueInviteTicket(c.signPriv, bootstrap, subjectPub, caps, budget, grantDeadline, now, ttl)
}

// IssueGrant mints a child grant for subjectPub under this node's
// grant chain, clamped to this node's authority and horizon. The node
// must hold a delegating grant.
func (c *Credentials) IssueGrant(
	subjectPub ed25519.PublicKey,
	caps *identityv1.Capabilities,
	budget *identityv1.Budget,
	now, grantDeadline time.Time,
) (*identityv1.Grant, error) {
	c.mu.RLock()
	grant := c.grant
	c.mu.RUnlock()
	return IssueGrant(c.signPriv, grant, subjectPub, caps, budget, now, grantDeadline)
}

// IssueGrantToken wraps an already-issued grant into a GrantToken
// signed by this node, for direct (non-invite) enrolment such as the
// SSH-bridge bootstrap path where the joiner key is known up front.
func (c *Credentials) IssueGrantToken(
	grant *identityv1.Grant,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	ttl time.Duration,
) (*identityv1.GrantToken, error) {
	return IssueGrantToken(c.signPriv, grant, bootstrap, c.rootPub, now, ttl)
}

// RedeemInvite is the host side of a join: it mints a joiner grant
// under this node's grant chain and wraps it into a GrantToken. The
// node must be the ticket's named issuer and hold a delegating grant.
func (c *Credentials) RedeemInvite(
	ticket *identityv1.InviteTicket,
	joinerPub ed25519.PublicKey,
	now time.Time,
	tokenTTL time.Duration,
) (*identityv1.GrantToken, error) {
	c.mu.RLock()
	grant := c.grant
	c.mu.RUnlock()
	return RedeemInviteTicket(c.signPriv, grant, c.rootPub, ticket, joinerPub, now, tokenTTL)
}

func grantPath(identityDir string) string { return filepath.Join(identityDir, grantCertName) }
func rootPubPath(identityDir string) string {
	return filepath.Join(identityDir, rootPubName)
}

func LoadCredentials(identityDir string) (*Credentials, error) {
	rootRaw, err := os.ReadFile(rootPubPath(identityDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}
	if len(rootRaw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("root pub invalid: expected %d bytes, got %d", ed25519.PublicKeySize, len(rootRaw))
	}

	grantRaw, err := os.ReadFile(grantPath(identityDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}
	grant := &identityv1.Grant{}
	if err := grant.UnmarshalVT(grantRaw); err != nil {
		return nil, err
	}

	signPriv, _, err := loadKeyPair(
		filepath.Join(identityDir, signingKeyName),
		filepath.Join(identityDir, signingPubKeyName),
		pemTypePriv, pemTypePub,
	)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}

	return &Credentials{
		rootPub:     ed25519.PublicKey(rootRaw),
		signPriv:    signPriv,
		grant:       grant,
		identityDir: identityDir,
	}, nil
}

// SaveCredentials writes c's root pub and grant to identityDir. Useful
// for callers that build a Credentials directly (tests, integration
// harnesses) and want the durable record laid down once; the live
// adopt and refresh path goes through AdoptGrant, which calls the
// underlying writeCredentials helper while holding c.mu.
func SaveCredentials(identityDir string, c *Credentials) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return writeCredentials(identityDir, c.rootPub, c.grant)
}

func writeCredentials(identityDir string, rootPub ed25519.PublicKey, grant *identityv1.Grant) error {
	if err := plnfs.EnsureDir(identityDir); err != nil {
		return err
	}
	if err := plnfs.WriteGroupReadable(rootPubPath(identityDir), []byte(rootPub)); err != nil {
		return err
	}
	raw, err := grant.MarshalVT()
	if err != nil {
		return err
	}
	return plnfs.WriteGroupReadable(grantPath(identityDir), raw)
}

// EnsureLocalRootGrant re-issues the root's self-signed grant whenever
// subject, issuer, root pub, capabilities, or attributes drift from the
// persisted grant. Root grants carry no horizon: the root is the
// trust anchor and rotates via its own admin key, not a deadline.
func EnsureLocalRootGrant(identityDir string, nodePub ed25519.PublicKey, attrs *structpb.Struct, now time.Time) (*Credentials, error) {
	if err := ValidateAttributes(attrs); err != nil {
		return nil, err
	}

	existing, err := LoadCredentials(identityDir)
	if err != nil && !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}

	adminPriv, adminPub, err := EnsureAdminKey(identityDir)
	if err != nil {
		return nil, err
	}
	signPriv, _, err := EnsureIdentityKey(identityDir)
	if err != nil {
		return nil, err
	}

	if existing != nil && rootGrantHealthy(existing, nodePub, adminPub) &&
		attrsEqual(existing.grant.GetClaims().GetCapabilities().GetAttributes(), attrs) {
		existing.signPriv = signPriv
		return existing, nil
	}

	caps := RootCapabilities()
	caps.Attributes = attrs

	grant, err := IssueGrant(adminPriv, nil, nodePub, caps, UnlimitedBudget(), now, time.Time{})
	if err != nil {
		return nil, err
	}

	creds := &Credentials{
		rootPub:     adminPub,
		signPriv:    signPriv,
		grant:       grant,
		identityDir: identityDir,
	}
	if err := SaveCredentials(identityDir, creds); err != nil {
		return nil, err
	}
	return creds, nil
}

func rootGrantHealthy(existing *Credentials, nodePub, adminPub ed25519.PublicKey) bool {
	claims := existing.grant.GetClaims()
	if !bytes.Equal(claims.GetSubjectPub(), nodePub) {
		return false
	}
	if !bytes.Equal(claims.GetIssuerPub(), adminPub) {
		return false
	}
	if !bytes.Equal(existing.rootPub, adminPub) {
		return false
	}
	if len(existing.grant.GetChain()) != 0 {
		return false
	}
	want := RootCapabilities()
	got := claims.GetCapabilities()
	return got.GetCanDelegate() == want.CanDelegate &&
		got.GetCanAdmit() == want.CanAdmit &&
		got.GetIsInfrastructure() == want.IsInfrastructure &&
		got.GetMaxDepth() == want.MaxDepth &&
		proto.Equal(got.GetPublish(), want.Publish)
}

// attrsEqual treats nil and an empty Struct as equivalent: yaml
// round-trips can produce either, and a daemon restart with no config
// change must not trigger a spurious grant re-issue.
func attrsEqual(a, b *structpb.Struct) bool {
	if len(a.GetFields()) == 0 && len(b.GetFields()) == 0 {
		return true
	}
	return proto.Equal(a, b)
}
