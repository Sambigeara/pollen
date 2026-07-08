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
	"time"

	"buf.build/go/protovalidate"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
)

var ErrSessionInvalid = errors.New("session invalid")

// VerifiedSession is the authenticated result of VerifySession: the
// subject that holds the session, its embedded grant, and the grant's
// authority and horizon. Callers downstream read capabilities and
// budget off this rather than re-walking the grant.
type VerifiedSession struct {
	GrantDeadline time.Time
	SessionExpiry time.Time
	Grant         *identityv1.Grant
	Capabilities  *identityv1.Capabilities
	Budget        *identityv1.Budget
	SubjectPub    ed25519.PublicKey
}

// MintSession produces a short-lived liveness proof from a held grant,
// signed by the grant subject's own key. This is purely local: it needs
// only the grant (already on disk) and the subject private key, never
// the issuer or the mesh. Renewal is re-minting.
func MintSession(
	grant *identityv1.Grant,
	subjectPriv ed25519.PrivateKey,
	now time.Time,
	ttl time.Duration,
) (*identityv1.Session, error) {
	if grant == nil {
		return nil, errors.New("grant required")
	}
	if ttl <= 0 {
		return nil, errors.New("session ttl must be positive")
	}
	subjectPub := subjectPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if !bytes.Equal(grant.GetClaims().GetSubjectPub(), subjectPub) {
		return nil, errors.New("subject key does not match grant subject")
	}

	var nonceBytes [8]byte
	if _, err := rand.Read(nonceBytes[:]); err != nil {
		return nil, err
	}

	claims := &identityv1.SessionClaims{
		Grant:         grant,
		NotBeforeUnix: now.Unix(),
		NotAfterUnix:  now.Add(ttl).Unix(),
		Nonce:         binary.BigEndian.Uint64(nonceBytes[:]),
	}
	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	sig, err := SignPayload(subjectPriv, msg, sigContextSession)
	if err != nil {
		return nil, err
	}
	subjectSig, err := SignGrantSubject(grant, subjectPriv)
	if err != nil {
		return nil, err
	}
	session := &identityv1.Session{Claims: claims, Signature: sig, SubjectSignature: subjectSig}
	if err := protovalidate.Validate(session); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionInvalid, err)
	}
	return session, nil
}

// VerifySession authenticates a presented session. The embedded grant
// is held to the durable-authority rule (chain + grant_deadline +
// chain-aware denylist); the session signature must be by the grant
// subject; the short session window must be current. expectedSubject,
// when set, binds the session to a transport leaf key (the grant
// subject must equal it), defeating cert-replay impersonation. denied
// may be nil to skip the denylist consultation.
func VerifySession(
	session *identityv1.Session,
	rootPub []byte,
	now time.Time,
	expectedSubject []byte,
	denied DenyChecker,
) (*VerifiedSession, error) {
	if err := protovalidate.Validate(session); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionInvalid, err)
	}
	claims := session.GetClaims()
	grant := claims.GetGrant()

	chk := CheckGrant(grant, rootPub, now, expectedSubject, denied)
	if !chk.Status.Valid() {
		return nil, fmt.Errorf("%w: grant %s: %s", ErrSessionInvalid, chk.Status, chk.Reason)
	}

	nb := time.Unix(claims.GetNotBeforeUnix(), 0)
	na := time.Unix(claims.GetNotAfterUnix(), 0)
	if !na.After(nb) {
		return nil, fmt.Errorf("%w: session validity window invalid", ErrSessionInvalid)
	}
	if now.Before(nb.Add(-TimeSkewAllowance)) {
		return nil, fmt.Errorf("%w: session not yet valid", ErrSessionInvalid)
	}
	if now.After(na.Add(TimeSkewAllowance)) {
		return nil, fmt.Errorf("%w: session expired at %s", ErrSessionInvalid, na.UTC().Format(time.RFC3339))
	}

	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	if err := VerifyPayload(ed25519.PublicKey(grant.GetClaims().GetSubjectPub()), msg, session.GetSignature(), sigContextSession); err != nil {
		return nil, fmt.Errorf("%w: signature invalid", ErrSessionInvalid)
	}
	// The grant-subject proof rides every session so the serving node can
	// relay a daemonless publisher's grant into cluster state through the
	// same gate a gossiped grant passes. It is by the same key as the
	// session signature, so a stripped or swapped proof simply fails here.
	if err := VerifyGrantSubject(grant, session.GetSubjectSignature()); err != nil {
		return nil, fmt.Errorf("%w: subject proof invalid", ErrSessionInvalid)
	}

	gc := grant.GetClaims()
	return &VerifiedSession{
		SubjectPub:    ed25519.PublicKey(gc.GetSubjectPub()),
		Grant:         grant,
		Capabilities:  gc.GetCapabilities(),
		Budget:        gc.GetBudget(),
		GrantDeadline: chk.GrantDeadline,
		SessionExpiry: na,
	}, nil
}
