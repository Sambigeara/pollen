// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
)

// IssueGrantToken bundles a freshly-minted grant with bootstrap peers
// and the cluster root pub into a short-lived, issuer-signed envelope a
// joiner redeems. Replaces the member-cert JoinToken: the joiner
// persists the grant and self-mints sessions, never round-tripping the
// issuer again.
func IssueGrantToken(
	issuerPriv ed25519.PrivateKey,
	grant *identityv1.Grant,
	bootstrap []*admissionv1.BootstrapPeer,
	rootPub ed25519.PublicKey,
	now time.Time,
	ttl time.Duration,
) (*identityv1.GrantToken, error) {
	if ttl <= 0 {
		return nil, errors.New("token ttl must be positive")
	}
	issuerPub := issuerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	claims := &identityv1.GrantTokenClaims{
		TokenId:       uuid.NewString(),
		IssuerPub:     issuerPub,
		Grant:         grant,
		Bootstrap:     bootstrap,
		RootPub:       rootPub,
		IssuedAtUnix:  now.Unix(),
		ExpiresAtUnix: now.Add(ttl).Unix(),
	}
	if err := protovalidate.Validate(claims); err != nil {
		return nil, fmt.Errorf("grant token claims invalid: %w", err)
	}
	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	sig, err := SignPayload(issuerPriv, msg, sigContextGrantToken)
	if err != nil {
		return nil, err
	}
	return &identityv1.GrantToken{Claims: claims, Signature: sig}, nil
}

type VerifiedGrantToken struct {
	Grant     *identityv1.Grant
	RootPub   ed25519.PublicKey
	Bootstrap []*admissionv1.BootstrapPeer
}

// VerifyGrantToken authenticates a redeemed token: the envelope
// signature is by the named issuer, the window is current, and the
// embedded grant chains to the token's root pub and is for the
// expected subject.
func VerifyGrantToken(token *identityv1.GrantToken, expectedSubject ed25519.PublicKey, now time.Time) (*VerifiedGrantToken, error) {
	if err := protovalidate.Validate(token); err != nil {
		return nil, fmt.Errorf("grant token invalid: %w", err)
	}
	claims := token.GetClaims()

	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	if err := VerifyPayload(ed25519.PublicKey(claims.GetIssuerPub()), msg, token.GetSignature(), sigContextGrantToken); err != nil {
		return nil, errors.New("grant token signature invalid")
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-TimeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(TimeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("grant token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return nil, errors.New("grant token expired or not yet valid")
	}

	grant := claims.GetGrant()
	if err := VerifyGrantStructure(grant, claims.GetRootPub()); err != nil {
		return nil, fmt.Errorf("grant token grant invalid: %w", err)
	}
	if len(expectedSubject) > 0 && !bytes.Equal(grant.GetClaims().GetSubjectPub(), expectedSubject) {
		return nil, errors.New("grant token subject mismatch")
	}

	return &VerifiedGrantToken{
		Grant:     grant,
		RootPub:   ed25519.PublicKey(claims.GetRootPub()),
		Bootstrap: claims.GetBootstrap(),
	}, nil
}

func EncodeGrantToken(token *identityv1.GrantToken) (string, error) {
	b, err := token.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeGrantToken(s string) (*identityv1.GrantToken, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	token := &identityv1.GrantToken{}
	if err := token.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return token, nil
}

// EnrollGrant verifies a grant token for the local node and persists
// the grant + root pub as the node's durable credential. First-time
// enrol and same-subject re-enrol (e.g., an admin-issued upgrade token
// redeemed by an already-joined node) funnel through the same path:
// the on-disk identity key is reused, the new grant is adopted via
// AdoptGrant, and the durable record on disk is rewritten as part of
// that adoption.
func EnrollGrant(identityDir string, nodePub ed25519.PublicKey, token *identityv1.GrantToken, now time.Time) (*Credentials, error) {
	verified, err := VerifyGrantToken(token, nodePub, now)
	if err != nil {
		return nil, err
	}

	signPriv, _, err := EnsureIdentityKey(identityDir)
	if err != nil {
		return nil, err
	}

	existing, err := LoadCredentials(identityDir)
	if err != nil && !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}
	if existing != nil && !bytes.Equal(existing.rootPub, verified.RootPub) {
		return nil, ErrDifferentCluster
	}

	creds := &Credentials{
		rootPub:     verified.RootPub,
		signPriv:    signPriv,
		identityDir: identityDir,
	}
	if err := creds.AdoptGrant(verified.Grant, now, nil); err != nil {
		return nil, err
	}
	return creds, nil
}
