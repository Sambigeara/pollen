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
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/perm"
)

type AdminSigner struct {
	Trust    *admissionv1.TrustBundle
	Issuer   *admissionv1.AdminCert
	Consumed *config.ConsumedInvites
	Priv     ed25519.PrivateKey
}

type VerifiedInviteToken struct {
	Claims *admissionv1.InviteTokenClaims
	Trust  *admissionv1.TrustBundle
	Issuer *admissionv1.AdminCert
}

func loadAdminCert(pollenDir string) (*admissionv1.AdminCert, error) {
	path := filepath.Join(pollenDir, keysDir, adminCertName)
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cert := &admissionv1.AdminCert{}
	if err := cert.UnmarshalVT(raw); err != nil {
		return nil, err
	}

	return cert, nil
}

func SaveAdminCert(pollenDir string, cert *admissionv1.AdminCert) error {
	if cert == nil || cert.GetClaims() == nil {
		return errors.New("admin cert missing claims")
	}

	raw, err := cert.MarshalVT()
	if err != nil {
		return err
	}

	dir := filepath.Join(pollenDir, keysDir)
	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return err
	}
	if err := perm.SetGroupDir(dir); err != nil {
		return err
	}

	path := filepath.Join(dir, adminCertName)
	if err := os.WriteFile(path, raw, keyFilePerm); err != nil {
		return err
	}
	return perm.SetGroupReadable(path)
}

func LoadAdminSigner(pollenDir string, now time.Time, adminCertTTL time.Duration) (*AdminSigner, error) {
	adminPriv, adminPub, err := LoadAdminKey(pollenDir)
	if err != nil {
		return nil, err
	}

	trust, err := loadTrustBundleForSigner(pollenDir, adminPub)
	if err != nil {
		return nil, err
	}

	issuer, err := resolveAdminIssuer(pollenDir, adminPriv, adminPub, trust, now, adminCertTTL)
	if err != nil {
		return nil, err
	}

	consumed, err := config.LoadConsumedInvites(pollenDir, now)
	if err != nil {
		return nil, err
	}

	return &AdminSigner{
		Priv:     adminPriv,
		Trust:    trust,
		Issuer:   issuer,
		Consumed: consumed,
	}, nil
}

func resolveAdminIssuer(pollenDir string, adminPriv ed25519.PrivateKey, adminPub ed25519.PublicKey, trust *admissionv1.TrustBundle, now time.Time, adminCertTTL time.Duration) (*admissionv1.AdminCert, error) {
	if bytes.Equal(adminPub, trust.GetRootPub()) {
		return IssueAdminCert(
			adminPriv,
			trust.GetClusterId(),
			adminPub,
			now.Add(-timeSkewAllowance),
			now.Add(adminCertTTL),
		)
	}

	issuer, err := loadAdminCert(pollenDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("delegated admin cert not installed")
		}
		return nil, err
	}

	if err := VerifyAdminCert(issuer, trust, now); err != nil {
		return nil, fmt.Errorf("delegated admin cert invalid: %w", err)
	}
	return issuer, nil
}

func IssueInviteTokenWithSigner(
	signer *AdminSigner,
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
) (*admissionv1.InviteToken, error) {
	if signer == nil {
		return nil, errors.New("missing admin signer")
	}
	if tokenTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}
	if err := protovalidate.Validate(signer.Issuer); err != nil {
		return nil, fmt.Errorf("invite issuer admin cert invalid: %w", err)
	}

	issuerPub := signer.Issuer.GetClaims().GetAdminPub()

	privPub, ok := signer.Priv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("admin private key is not ed25519")
	}
	if !bytes.Equal(privPub, issuerPub) {
		return nil, errors.New("invite signer key does not match issuer cert")
	}

	var membershipTTLSeconds int64
	if membershipTTL > 0 {
		membershipTTLSeconds = int64(membershipTTL / time.Second)
	}

	claims := &admissionv1.InviteTokenClaims{
		TokenId:              uuid.NewString(),
		Trust:                signer.Trust,
		Issuer:               signer.Issuer,
		Bootstrap:            bootstrap,
		SubjectPub:           append([]byte(nil), subject...),
		IssuedAtUnix:         now.Unix(),
		ExpiresAtUnix:        now.Add(tokenTTL).Unix(),
		MembershipTtlSeconds: membershipTTLSeconds,
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

func VerifyInviteToken(token *admissionv1.InviteToken, expectedSubject ed25519.PublicKey, now time.Time) (*VerifiedInviteToken, error) {
	if err := protovalidate.Validate(token); err != nil {
		return nil, fmt.Errorf("invite token invalid: %w", err)
	}

	claims := token.GetClaims()

	trust := claims.GetTrust()
	if err := validateTrustBundle(trust); err != nil {
		return nil, err
	}

	issuer := claims.GetIssuer()
	if err := VerifyAdminCert(issuer, trust, now); err != nil {
		return nil, fmt.Errorf("invite token issuer invalid: %w", err)
	}

	issuerPub := issuer.GetClaims().GetAdminPub()
	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}
	if err := verifyPayload(ed25519.PublicKey(issuerPub), msg, token.GetSignature(), sigContextInvite); err != nil {
		return nil, errors.New("invite token signature invalid")
	}

	if len(claims.GetSubjectPub()) == ed25519.PublicKeySize && len(expectedSubject) > 0 {
		if !bytes.Equal(claims.GetSubjectPub(), expectedSubject) {
			return nil, errors.New("invite token subject mismatch")
		}
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-timeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(timeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("invite token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return nil, errors.New("invite token expired or not yet valid")
	}

	return &VerifiedInviteToken{
		Claims: claims,
		Trust:  trust,
		Issuer: issuer,
	}, nil
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
	if err == nil && creds != nil && creds.Trust != nil {
		return creds.Trust, nil
	}
	if err != nil && !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}

	path := filepath.Join(pollenDir, keysDir, trustBundleName)
	raw, readErr := os.ReadFile(path)
	if readErr == nil {
		trust := &admissionv1.TrustBundle{}
		if unmarshalErr := trust.UnmarshalVT(raw); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		if validateErr := validateTrustBundle(trust); validateErr != nil {
			return nil, validateErr
		}
		return trust, nil
	}
	if !errors.Is(readErr, os.ErrNotExist) {
		return nil, readErr
	}

	return NewTrustBundle(adminPub), nil
}

func MarshalAdminCertBase64(cert *admissionv1.AdminCert) (string, error) {
	b, err := cert.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func UnmarshalAdminCertBase64(encoded string) (*admissionv1.AdminCert, error) {
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	cert := &admissionv1.AdminCert{}
	if err := cert.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return cert, nil
}
