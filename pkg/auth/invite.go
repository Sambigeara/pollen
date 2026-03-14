package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/perm"
)

// InviteConsumer tracks which invite tokens have been redeemed.
type InviteConsumer interface {
	TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error)
}

type DelegationSigner struct {
	Trust    *admissionv1.TrustBundle
	Issuer   *admissionv1.DelegationCert
	Consumed InviteConsumer
	Priv     ed25519.PrivateKey
}


func SaveDelegationCert(pollenDir string, cert *admissionv1.DelegationCert) error {
	if cert == nil || cert.GetClaims() == nil {
		return errors.New("delegation cert missing claims")
	}

	raw, err := cert.MarshalVT()
	if err != nil {
		return err
	}

	dir := filepath.Join(pollenDir, keysDir)
	if err := perm.EnsureDir(dir); err != nil {
		return err
	}

	return perm.WriteGroupReadable(filepath.Join(dir, delegationCertName), raw)
}

func LoadDelegationSigner(pollenDir string, nodePriv ed25519.PrivateKey, now time.Time, delegationTTL time.Duration) (*DelegationSigner, error) {
	// Root admin path: admin key exists and matches the cluster root.
	if signer, err := loadRootDelegationSigner(pollenDir, now, delegationTTL); err != nil {
		return nil, err
	} else if signer != nil {
		return signer, nil
	}

	// Delegated admin path: node credentials carry a delegation cert
	// with CanDelegate, issued to the node identity key.
	nodePub := nodePriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	creds, err := loadNodeCredentials(pollenDir)
	if err != nil {
		return nil, err
	}
	if err := VerifyDelegationCert(creds.Cert, creds.Trust, now, nodePub); err != nil {
		return nil, fmt.Errorf("delegated admin cert invalid: %w", err)
	}
	if !creds.Cert.GetClaims().GetCapabilities().GetCanDelegate() {
		return nil, errors.New("node cert lacks delegation capability")
	}

	return &DelegationSigner{
		Priv:   nodePriv,
		Trust:  creds.Trust,
		Issuer: creds.Cert,
	}, nil
}

// loadRootDelegationSigner returns a signer if the local admin key is the
// cluster root. Returns (nil, nil) when admin key is absent or non-root.
func loadRootDelegationSigner(pollenDir string, now time.Time, delegationTTL time.Duration) (*DelegationSigner, error) {
	adminPriv, adminPub, err := LoadAdminKey(pollenDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	trust, err := loadTrustBundleForSigner(pollenDir, adminPub)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(adminPub, trust.GetRootPub()) {
		return nil, nil
	}

	issuer, err := IssueDelegationCert(
		adminPriv,
		nil,
		trust.GetClusterId(),
		adminPub,
		FullCapabilities(),
		now.Add(-timeSkewAllowance),
		now.Add(delegationTTL),
		time.Time{},
	)
	if err != nil {
		return nil, err
	}
	return &DelegationSigner{
		Priv:   adminPriv,
		Trust:  trust,
		Issuer: issuer,
	}, nil
}

func IssueInviteTokenWithSigner(
	signer *DelegationSigner,
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
) (*admissionv1.InviteToken, error) {
	if signer == nil {
		return nil, errors.New("missing delegation signer")
	}
	if tokenTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}
	if err := protovalidate.Validate(signer.Issuer); err != nil {
		return nil, fmt.Errorf("invite issuer delegation cert invalid: %w", err)
	}

	issuerPub := signer.Issuer.GetClaims().GetSubjectPub()
	if !bytes.Equal(signer.Priv.Public().(ed25519.PublicKey), issuerPub) { //nolint:forcetypeassert
		return nil, errors.New("invite signer key does not match issuer cert")
	}

	claims := &admissionv1.InviteTokenClaims{
		TokenId:              uuid.NewString(),
		Trust:                signer.Trust,
		Issuer:               signer.Issuer,
		Bootstrap:            bootstrap,
		SubjectPub:           append([]byte(nil), subject...),
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

	sig, err := signPayload(signer.Priv, msg, sigContextInvite)
	if err != nil {
		return nil, err
	}
	return &admissionv1.InviteToken{Claims: claims, Signature: sig}, nil
}

func VerifyInviteToken(token *admissionv1.InviteToken, expectedSubject ed25519.PublicKey, now time.Time) error {
	if token == nil {
		return errors.New("invite token is nil")
	}
	if err := protovalidate.Validate(token); err != nil {
		return fmt.Errorf("invite token invalid: %w", err)
	}

	claims := token.GetClaims()

	trust := claims.GetTrust()
	if err := validateTrustBundle(trust); err != nil {
		return err
	}

	issuer := claims.GetIssuer()
	if err := VerifyDelegationCert(issuer, trust, now, nil); err != nil {
		return fmt.Errorf("invite token issuer invalid: %w", err)
	}

	issuerPub := issuer.GetClaims().GetSubjectPub()
	msg, err := signaturePayload(claims)
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(issuerPub), msg, token.GetSignature(), sigContextInvite); err != nil {
		return errors.New("invite token signature invalid")
	}

	if len(claims.GetSubjectPub()) == ed25519.PublicKeySize && len(expectedSubject) > 0 {
		if !bytes.Equal(claims.GetSubjectPub(), expectedSubject) {
			return errors.New("invite token subject mismatch")
		}
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-timeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(timeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return errors.New("invite token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return errors.New("invite token expired or not yet valid")
	}

	return nil
}

func EncodeInviteToken(token *admissionv1.InviteToken) (string, error) {
	b, err := token.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeInviteToken(s string) (*admissionv1.InviteToken, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	token := &admissionv1.InviteToken{}
	if err := token.UnmarshalVT(b); err != nil {
		return nil, err
	}

	return token, nil
}

func loadTrustBundleForSigner(pollenDir string, adminPub ed25519.PublicKey) (*admissionv1.TrustBundle, error) {
	creds, err := loadNodeCredentials(pollenDir)
	if err == nil {
		return creds.Trust, nil
	}
	if !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}

	raw, err := os.ReadFile(filepath.Join(pollenDir, keysDir, trustBundleName))
	if err == nil {
		trust := &admissionv1.TrustBundle{}
		if err := trust.UnmarshalVT(raw); err != nil {
			return nil, err
		}
		if err := validateTrustBundle(trust); err != nil {
			return nil, err
		}
		return trust, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return NewTrustBundle(adminPub), nil
}

func MarshalDelegationCertBase64(cert *admissionv1.DelegationCert) (string, error) {
	b, err := cert.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func UnmarshalDelegationCertBase64(encoded string) (*admissionv1.DelegationCert, error) {
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	cert := &admissionv1.DelegationCert{}
	if err := cert.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return cert, nil
}
