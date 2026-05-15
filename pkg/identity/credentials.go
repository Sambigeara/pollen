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
	grant    *identityv1.Grant
	session  *identityv1.Session
	rootPub  ed25519.PublicKey
	signPriv ed25519.PrivateKey
	mu       sync.RWMutex
}

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

func (c *Credentials) SetGrant(grant *identityv1.Grant) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.grant = grant
	c.session = nil
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
		rootPub:  ed25519.PublicKey(rootRaw),
		signPriv: signPriv,
		grant:    grant,
	}, nil
}

func SaveCredentials(identityDir string, c *Credentials) error {
	if err := plnfs.EnsureDir(identityDir); err != nil {
		return err
	}
	if err := plnfs.WriteGroupReadable(rootPubPath(identityDir), []byte(c.rootPub)); err != nil {
		return err
	}
	raw, err := c.Grant().MarshalVT()
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

	caps := FullCapabilities()
	caps.Attributes = attrs

	grant, err := IssueGrant(adminPriv, nil, nodePub, caps, UnlimitedBudget(), now, time.Time{})
	if err != nil {
		return nil, err
	}

	creds := NewCredentials(adminPub, signPriv, grant)
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
	want := FullCapabilities()
	got := claims.GetCapabilities()
	return got.GetCanDelegate() == want.CanDelegate &&
		got.GetCanAdmit() == want.CanAdmit &&
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
