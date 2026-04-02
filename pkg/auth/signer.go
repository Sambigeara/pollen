package auth

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

type DelegationSigner struct {
	issuer *admissionv1.DelegationCert
	priv   ed25519.PrivateKey
}

func NewDelegationSigner(pollenDir string, nodePriv ed25519.PrivateKey, delegationTTL time.Duration) (*DelegationSigner, error) {
	now := time.Now()

	// Root admin: local admin key matches the persisted cluster root.
	adminPriv, adminPub, err := LoadAdminKey(pollenDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if adminPriv != nil {
		rootPub, err := os.ReadFile(filepath.Join(pollenDir, keysDir, rootPubName))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if bytes.Equal(adminPub, rootPub) {
			issuer, err := IssueDelegationCert(
				adminPriv, nil, adminPub,
				FullCapabilities(),
				now, now.Add(delegationTTL), time.Time{},
			)
			if err != nil {
				return nil, err
			}
			return &DelegationSigner{priv: adminPriv, issuer: issuer}, nil
		}
	}

	// Delegated admin: node cert carries CanDelegate.
	creds, err := LoadNodeCredentials(pollenDir)
	if err != nil {
		return nil, err
	}
	if IsCertExpired(creds.cert, now) {
		return nil, fmt.Errorf("delegated admin cert: %w", ErrCertExpired)
	}
	if !creds.cert.GetClaims().GetCapabilities().GetCanDelegate() {
		return nil, errors.New("node cert lacks delegation capability")
	}

	return &DelegationSigner{priv: nodePriv, issuer: creds.cert}, nil
}

func (s *DelegationSigner) IsRoot() bool {
	claims := s.issuer.GetClaims()
	return bytes.Equal(claims.GetIssuerPub(), claims.GetSubjectPub())
}

func (s *DelegationSigner) IssueInviteToken(
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
) (*admissionv1.InviteToken, error) {
	if tokenTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}
	if err := protovalidate.Validate(s.issuer); err != nil {
		return nil, fmt.Errorf("invite issuer delegation cert invalid: %w", err)
	}

	claims := &admissionv1.InviteTokenClaims{
		TokenId:              uuid.NewString(),
		Issuer:               s.issuer,
		Bootstrap:            bootstrap,
		SubjectPub:           subject,
		IssuedAtUnix:         now.Unix(),
		ExpiresAtUnix:        now.Add(tokenTTL).Unix(),
		MembershipTtlSeconds: int64(membershipTTL / time.Second),
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
) (*admissionv1.JoinToken, error) {
	parentChain := append([]*admissionv1.DelegationCert{s.issuer}, s.issuer.GetChain()...)

	notAfter := now.Add(membershipTTL)
	if !accessDeadline.IsZero() && notAfter.After(accessDeadline) {
		notAfter = accessDeadline
	}

	memberCert, err := IssueDelegationCert(
		s.priv,
		parentChain,
		subject,
		LeafCapabilities(),
		now,
		notAfter,
		accessDeadline,
	)
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.JoinTokenClaims{
		TokenId:       uuid.NewString(),
		Issuer:        s.issuer,
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

// IssueMemberCert issues a delegation cert for a subject using this signer's
// chain and private key.
func (s *DelegationSigner) IssueMemberCert(
	subjectPub ed25519.PublicKey,
	caps *admissionv1.Capabilities,
	notBefore, notAfter time.Time,
	accessDeadline time.Time,
) (*admissionv1.DelegationCert, error) {
	parentChain := append([]*admissionv1.DelegationCert{s.issuer}, s.issuer.GetChain()...)
	return IssueDelegationCert(s.priv, parentChain, subjectPub, caps, notBefore, notAfter, accessDeadline)
}
