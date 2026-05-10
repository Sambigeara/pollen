// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type DelegationSigner struct {
	issuer *admissionv1.DelegationCert
	priv   ed25519.PrivateKey
	root   bool
}

func NewDelegationSigner(identityDir string, nodePriv ed25519.PrivateKey) (*DelegationSigner, error) {
	creds, err := LoadNodeCredentials(identityDir)
	if err != nil {
		return nil, err
	}

	nodePub := nodePriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	adminPub, isRoot, err := LocalRootAuthority(identityDir, creds)
	if err != nil {
		return nil, err
	}

	if isRoot && !rootCertHealthy(creds, nodePub, adminPub, time.Now()) {
		attrs := creds.cert.GetClaims().GetCapabilities().GetAttributes()
		creds, err = EnsureLocalRootCredentials(identityDir, nodePub, attrs, time.Now(), DefaultDelegationTTL)
		if err != nil {
			return nil, fmt.Errorf("refresh stale root cert: %w", err)
		}
	}

	if IsCertExpired(creds.cert, time.Now()) {
		return nil, fmt.Errorf("delegation cert: %w", ErrCertExpired)
	}
	if !creds.cert.GetClaims().GetCapabilities().GetCanDelegate() {
		return nil, errors.New("node cert lacks delegation capability")
	}
	if !bytes.Equal(creds.cert.GetClaims().GetSubjectPub(), nodePub) {
		return nil, errors.New("delegation cert subject does not match local node identity")
	}

	return &DelegationSigner{
		priv:   nodePriv,
		issuer: creds.cert,
		root:   isRoot,
	}, nil
}

func NewDelegationSignerFromCert(nodePriv ed25519.PrivateKey, cert *admissionv1.DelegationCert) *DelegationSigner {
	return &DelegationSigner{priv: nodePriv, issuer: cert}
}

func (s *DelegationSigner) IsRoot() bool {
	return s.root
}

// IssuerPub is the subject pub of this signer's own cert. Tokens carry it
// so verifiers and forwarders can authenticate against the issuing key
// without embedding the full cert.
func (s *DelegationSigner) IssuerPub() ed25519.PublicKey {
	return ed25519.PublicKey(s.issuer.GetClaims().GetSubjectPub())
}

// IssuerCert returns the signer's own cert. Used by callers that need the
// issuer's expiry or chain (e.g. redemption-side validity checks).
func (s *DelegationSigner) IssuerCert() *admissionv1.DelegationCert {
	return s.issuer
}

func (s *DelegationSigner) IssueSpecAuth(
	resource *admissionv1.ResourceID,
	body SpecBody,
	policy *admissionv1.Predicate,
	deleted bool,
) (*admissionv1.SpecAuth, error) {
	return IssueSpecAuth(s.priv, s.issuer, resource, body, policy, deleted)
}

func (s *DelegationSigner) IssueInviteToken(
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
	attributes *structpb.Struct,
	admin bool,
) (*admissionv1.InviteToken, error) {
	if tokenTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}
	if err := ValidateAttributes(attributes); err != nil {
		return nil, err
	}
	if err := protovalidate.Validate(s.issuer); err != nil {
		return nil, fmt.Errorf("invite issuer delegation cert invalid: %w", err)
	}

	claims := &admissionv1.InviteTokenClaims{
		TokenId:              uuid.NewString(),
		IssuerPub:            s.IssuerPub(),
		Bootstrap:            bootstrap,
		SubjectPub:           subject,
		IssuedAtUnix:         now.Unix(),
		ExpiresAtUnix:        now.Add(tokenTTL).Unix(),
		MembershipTtlSeconds: int64(membershipTTL / time.Second),
		Attributes:           attributes,
		Admin:                admin,
	}
	if err := protovalidate.Validate(claims); err != nil {
		return nil, fmt.Errorf("invite token claims invalid: %w", err)
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(s.priv, msg, sigContextInvite)
	if err != nil {
		return nil, err
	}

	return &admissionv1.InviteToken{Claims: claims, Signature: sig}, nil
}

func (s *DelegationSigner) IssueJoinToken(
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
	accessDeadline time.Time,
	attributes *structpb.Struct,
	admin bool,
) (*admissionv1.JoinToken, error) {
	if err := ValidateAttributes(attributes); err != nil {
		return nil, err
	}

	parentChain := append([]*admissionv1.DelegationCert{s.issuer}, s.issuer.GetChain()...)

	notAfter := now.Add(membershipTTL)
	if !accessDeadline.IsZero() && notAfter.After(accessDeadline) {
		notAfter = accessDeadline
	}

	caps := LeafCapabilities()
	if admin {
		caps = FullCapabilities()
	}
	caps.Attributes = attributes

	memberCert, err := IssueDelegationCert(
		s.priv,
		parentChain,
		subject,
		caps,
		now,
		notAfter,
		accessDeadline,
	)
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.JoinTokenClaims{
		TokenId:       uuid.NewString(),
		IssuerPub:     s.IssuerPub(),
		MemberCert:    memberCert,
		Bootstrap:     bootstrap,
		IssuedAtUnix:  now.Unix(),
		ExpiresAtUnix: now.Add(tokenTTL).Unix(),
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(s.priv, msg, sigContextJoinClaims)
	if err != nil {
		return nil, err
	}

	return &admissionv1.JoinToken{Claims: claims, Signature: sig}, nil
}

func (s *DelegationSigner) IssueMemberCert(
	subjectPub ed25519.PublicKey,
	caps *admissionv1.Capabilities,
	notBefore, notAfter time.Time,
	accessDeadline time.Time,
) (*admissionv1.DelegationCert, error) {
	parentChain := append([]*admissionv1.DelegationCert{s.issuer}, s.issuer.GetChain()...)
	return IssueDelegationCert(s.priv, parentChain, subjectPub, caps, notBefore, notAfter, accessDeadline)
}
