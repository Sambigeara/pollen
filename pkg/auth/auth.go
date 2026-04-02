package auth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"buf.build/go/protovalidate"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"google.golang.org/protobuf/proto"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

const (
	keysDir = "keys"

	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"

	adminPrivKeyName = "admin_ed25519.key"
	adminPubKeyName  = "admin_ed25519.pub"

	rootPubName        = "root.pub"
	delegationCertName = "delegation.cert.pb"

	pemTypePriv = "ED25519 PRIVATE KEY"
	pemTypePub  = "ED25519 PUBLIC KEY"

	pemTypeAdminPriv = "POLLEN ADMIN ED25519 PRIVATE KEY"
	pemTypeAdminPub  = "POLLEN ADMIN ED25519 PUBLIC KEY"

	sigContextDelegation = "pollen.delegation.v1"
	sigContextJoinClaims = "pollen.join.v1"
	sigContextInvite     = "pollen.invite.v1"

	timeSkewAllowance = time.Minute
)

var (
	ErrCredentialsNotFound = errors.New("node membership credentials not found")
	ErrDifferentCluster    = errors.New("node already has membership credentials for a different cluster")
	ErrCertExpired         = errors.New("delegation cert has expired")
)

// FullCapabilities returns capabilities for root/admin certs.
func FullCapabilities() *admissionv1.Capabilities {
	return &admissionv1.Capabilities{
		CanDelegate: true,
		CanAdmit:    true,
		MaxDepth:    255, //nolint:mnd
	}
}

// LeafCapabilities returns capabilities for regular member certs.
func LeafCapabilities() *admissionv1.Capabilities {
	return &admissionv1.Capabilities{
		CanDelegate: false,
		CanAdmit:    false,
		MaxDepth:    0,
	}
}

func CertExpiresAt(cert *admissionv1.DelegationCert) time.Time {
	return time.Unix(cert.GetClaims().GetNotAfterUnix(), 0)
}

func IsCertExpired(cert *admissionv1.DelegationCert, now time.Time) bool {
	return IsExpiredAt(CertExpiresAt(cert), now)
}

func IsExpiredAt(expiresAt, now time.Time) bool {
	return now.After(expiresAt.Add(timeSkewAllowance))
}

func CertTTL(cert *admissionv1.DelegationCert) time.Duration {
	claims := cert.GetClaims()
	return time.Unix(claims.GetNotAfterUnix(), 0).Sub(time.Unix(claims.GetNotBeforeUnix(), 0))
}

// CertAccessDeadline returns the hard access deadline from a delegation cert.
// Returns the zero time and false if no deadline is set.
func CertAccessDeadline(cert *admissionv1.DelegationCert) (time.Time, bool) {
	dl := cert.GetClaims().GetAccessDeadlineUnix()
	if dl == 0 {
		return time.Time{}, false
	}
	return time.Unix(dl, 0), true
}

type NodeCredentials struct {
	cert          *admissionv1.DelegationCert
	delegationKey *DelegationSigner
	rootPub       ed25519.PublicKey
}

func NewNodeCredentials(rootPub ed25519.PublicKey, cert *admissionv1.DelegationCert) *NodeCredentials {
	return &NodeCredentials{rootPub: rootPub, cert: cert}
}

func (c *NodeCredentials) Cert() *admissionv1.DelegationCert        { return c.cert }
func (c *NodeCredentials) RootPub() ed25519.PublicKey               { return c.rootPub }
func (c *NodeCredentials) DelegationKey() *DelegationSigner         { return c.delegationKey }
func (c *NodeCredentials) SetDelegationKey(s *DelegationSigner)     { c.delegationKey = s }
func (c *NodeCredentials) SetCert(cert *admissionv1.DelegationCert) { c.cert = cert }

func EnrollNodeCredentials(pollenDir string, nodePub ed25519.PublicKey, token *admissionv1.JoinToken, now time.Time) (*NodeCredentials, error) {
	existing, err := LoadNodeCredentials(pollenDir)
	if err != nil && !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}

	verified, err := VerifyJoinToken(token, nodePub, now)
	if err != nil {
		return nil, err
	}

	creds := &NodeCredentials{rootPub: verified.RootPub, cert: verified.Cert}
	if existing != nil {
		if !bytes.Equal(existing.rootPub, creds.rootPub) {
			return nil, ErrDifferentCluster
		}
		if !IsCertExpired(existing.cert, now) &&
			existing.cert.GetClaims().GetNotAfterUnix() >= creds.cert.GetClaims().GetNotAfterUnix() {
			return existing, nil
		}
	}

	if err := SaveNodeCredentials(pollenDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

func EnsureLocalRootCredentials(pollenDir string, nodePub ed25519.PublicKey, now time.Time, membershipTTL, delegationTTL time.Duration) (*NodeCredentials, error) {
	existing, err := LoadNodeCredentials(pollenDir)
	if err != nil && !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}

	// Root has admin key — fall through to re-issue a fresh cert when expired.
	if existing != nil && !IsCertExpired(existing.cert, now) {
		return existing, nil
	}

	adminPriv, _, err := EnsureAdminKey(pollenDir)
	if err != nil {
		return nil, err
	}

	adminPub := adminPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	// Root issues a delegation cert for itself with full capabilities.
	cert, err := IssueDelegationCert(
		adminPriv,
		nil, // no parent chain — root-issued
		nodePub,
		FullCapabilities(),
		now,
		now.Add(delegationTTL),
		time.Time{},
	)
	if err != nil {
		return nil, err
	}

	creds := &NodeCredentials{rootPub: adminPub, cert: cert}
	if err := SaveNodeCredentials(pollenDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

func LoadNodeCredentials(pollenDir string) (*NodeCredentials, error) {
	dir := filepath.Join(pollenDir, keysDir)
	rootPubPath := filepath.Join(dir, rootPubName)
	certPath := filepath.Join(dir, delegationCertName)

	rootPub, err := os.ReadFile(rootPubPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}
	if len(rootPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("root pub invalid: expected %d bytes, got %d", ed25519.PublicKeySize, len(rootPub))
	}

	certRaw, err := os.ReadFile(certPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}

	cert := &admissionv1.DelegationCert{}
	if err := cert.UnmarshalVT(certRaw); err != nil {
		return nil, err
	}

	return &NodeCredentials{rootPub: ed25519.PublicKey(rootPub), cert: cert}, nil
}

func SaveNodeCredentials(pollenDir string, creds *NodeCredentials) error {
	dir := filepath.Join(pollenDir, keysDir)
	if err := plnfs.EnsureDir(dir); err != nil {
		return err
	}
	if err := plnfs.WriteGroupReadable(filepath.Join(dir, rootPubName), []byte(creds.rootPub)); err != nil {
		return err
	}

	return saveDelegationCert(pollenDir, creds.cert)
}

// InstallDelegationCert decodes a base64-encoded delegation cert, verifies it
// against the cluster root and subject, and persists it to disk.
func InstallDelegationCert(pollenDir, encoded string, rootPub, subject ed25519.PublicKey, now time.Time) error {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return err
	}
	cert := &admissionv1.DelegationCert{}
	if err := cert.UnmarshalVT(b); err != nil {
		return err
	}

	if err := VerifyDelegationCert(cert, rootPub, now, subject); err != nil {
		return err
	}

	return saveDelegationCert(pollenDir, cert)
}

func saveDelegationCert(pollenDir string, cert *admissionv1.DelegationCert) error {
	raw, err := cert.MarshalVT()
	if err != nil {
		return err
	}
	return plnfs.WriteGroupReadable(filepath.Join(pollenDir, keysDir, delegationCertName), raw)
}

func EnsureAdminKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	priv, pub, err := LoadAdminKey(pollenDir)
	if err == nil {
		return priv, pub, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	return generateKeyPair(filepath.Join(pollenDir, keysDir), adminPrivKeyName, adminPubKeyName, pemTypeAdminPriv, pemTypeAdminPub)
}

// generateKeyPair creates an ed25519 key pair, writes both as PEM files into
// dir, and returns the keys.
func generateKeyPair(dir, privName, pubName, privPEMType, pubPEMType string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	if err := plnfs.EnsureDir(dir); err != nil {
		return nil, nil, err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privPEM := pem.EncodeToMemory(&pem.Block{Type: privPEMType, Bytes: priv.Seed()})
	if err := plnfs.WriteGroupReadable(filepath.Join(dir, privName), privPEM); err != nil {
		return nil, nil, err
	}

	pubPEM := pem.EncodeToMemory(&pem.Block{Type: pubPEMType, Bytes: pub})
	if err := plnfs.WriteGroupReadable(filepath.Join(dir, pubName), pubPEM); err != nil {
		return nil, nil, err
	}

	return priv, pub, nil
}

func LoadAdminKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	return loadKeyPair(
		filepath.Join(pollenDir, keysDir, adminPrivKeyName),
		filepath.Join(pollenDir, keysDir, adminPubKeyName),
		pemTypeAdminPriv,
		pemTypeAdminPub,
	)
}

func loadKeyPair(privPath, pubPath, privPEMType, pubPEMType string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	privRaw, err := os.ReadFile(privPath)
	if err != nil {
		return nil, nil, err
	}
	pubRaw, err := os.ReadFile(pubPath)
	if err != nil {
		return nil, nil, err
	}

	privBlock, _ := pem.Decode(privRaw)
	if privBlock == nil || privBlock.Type != privPEMType {
		return nil, nil, errors.New("invalid private key PEM")
	}

	pubBlock, _ := pem.Decode(pubRaw)
	if pubBlock == nil || pubBlock.Type != pubPEMType {
		return nil, nil, errors.New("invalid public key PEM")
	}

	return ed25519.NewKeyFromSeed(privBlock.Bytes), ed25519.PublicKey(pubBlock.Bytes), nil
}

// IssueDelegationCert creates a new DelegationCert signed by signerPriv.
// parentChain is nil for root-issued certs; otherwise it's the issuer's chain
// (issuer's own cert + issuer's chain).
func IssueDelegationCert(
	signerPriv ed25519.PrivateKey,
	parentChain []*admissionv1.DelegationCert,
	subjectPub ed25519.PublicKey,
	caps *admissionv1.Capabilities,
	notBefore, notAfter time.Time,
	accessDeadline time.Time,
) (*admissionv1.DelegationCert, error) {
	if len(subjectPub) != ed25519.PublicKeySize {
		return nil, errors.New("invalid subject key length")
	}
	if !notAfter.After(notBefore) {
		return nil, errors.New("invalid delegation certificate validity window")
	}
	if caps == nil {
		return nil, errors.New("capabilities required")
	}

	signerPub := signerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	// Clamp notAfter to parent's expiry and verify signer matches parent.
	if len(parentChain) > 0 {
		parentExpiry := time.Unix(parentChain[0].GetClaims().GetNotAfterUnix(), 0)
		if notAfter.After(parentExpiry) {
			notAfter = parentExpiry
		}
		if !bytes.Equal(parentChain[0].GetClaims().GetSubjectPub(), signerPub) {
			return nil, errors.New("signer key does not match parent cert subject")
		}
	}

	var serialBytes [8]byte
	if _, err := rand.Read(serialBytes[:]); err != nil {
		return nil, err
	}
	serial := binary.BigEndian.Uint64(serialBytes[:])
	if serial == 0 {
		serial = 1
	}

	claims := &admissionv1.DelegationCertClaims{
		SubjectPub:    subjectPub,
		IssuerPub:     signerPub,
		Capabilities:  caps,
		NotBeforeUnix: notBefore.Unix(),
		NotAfterUnix:  notAfter.Unix(),
		Serial:        serial,
	}
	if !accessDeadline.IsZero() {
		claims.AccessDeadlineUnix = accessDeadline.Unix()
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(signerPriv, msg, sigContextDelegation)
	if err != nil {
		return nil, err
	}

	return &admissionv1.DelegationCert{
		Claims:    claims,
		Chain:     parentChain,
		Signature: sig,
	}, nil
}

// VerifyDelegationCert verifies a DelegationCert chain against a root public
// key, including time validity.
func VerifyDelegationCert(
	cert *admissionv1.DelegationCert,
	rootPub []byte,
	now time.Time,
	expectedSubject []byte,
) error {
	got, err := verifyDelegationCertChain(cert)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, rootPub) {
		return errors.New("delegation cert chain root mismatch")
	}
	if len(expectedSubject) > 0 && !bytes.Equal(cert.GetClaims().GetSubjectPub(), expectedSubject) {
		return errors.New("delegation cert subject mismatch")
	}

	notBefore := time.Unix(cert.GetClaims().GetNotBeforeUnix(), 0).Add(-timeSkewAllowance)
	if now.Before(notBefore) {
		return fmt.Errorf("%w: delegation cert not yet valid", ErrCertExpired)
	}
	if IsCertExpired(cert, now) {
		return fmt.Errorf("%w: delegation cert expired", ErrCertExpired)
	}

	return nil
}

// verifyDelegationCertChain verifies every signature in the delegation cert
// chain and returns the root public key (the terminal cert's issuer_pub).
// Trust and subject assertions are the caller's responsibility.
func verifyDelegationCertChain(cert *admissionv1.DelegationCert) (ed25519.PublicKey, error) {
	if err := protovalidate.Validate(cert); err != nil {
		return nil, fmt.Errorf("delegation cert invalid: %w", err)
	}

	// Walk chain from leaf to root verifying signatures.
	current := cert
	for _, parent := range cert.GetChain() {
		if !bytes.Equal(current.GetClaims().GetIssuerPub(), parent.GetClaims().GetSubjectPub()) {
			return nil, errors.New("delegation cert chain issuer/subject mismatch")
		}
		msg, err := signaturePayload(current.GetClaims())
		if err != nil {
			return nil, err
		}
		if err := verifyPayload(ed25519.PublicKey(current.GetClaims().GetIssuerPub()), msg, current.GetSignature(), sigContextDelegation); err != nil {
			return nil, errors.New("delegation cert signature invalid")
		}
		current = parent
	}

	// Verify the terminal (root-most) cert's signature.
	rootPub := ed25519.PublicKey(current.GetClaims().GetIssuerPub())
	msg, err := signaturePayload(current.GetClaims())
	if err != nil {
		return nil, err
	}
	if err := verifyPayload(rootPub, msg, current.GetSignature(), sigContextDelegation); err != nil {
		return nil, errors.New("delegation cert root signature invalid")
	}

	return rootPub, nil
}

func MarshalDelegationCertBase64(cert *admissionv1.DelegationCert) (string, error) {
	b, err := cert.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

type VerifiedToken struct {
	Claims  *admissionv1.JoinTokenClaims
	Cert    *admissionv1.DelegationCert
	RootPub ed25519.PublicKey
}

func VerifyJoinToken(token *admissionv1.JoinToken, expectedSubject ed25519.PublicKey, now time.Time) (*VerifiedToken, error) {
	if err := protovalidate.Validate(token); err != nil {
		return nil, fmt.Errorf("join token invalid: %w", err)
	}

	tokenClaims := token.GetClaims()

	// Issuer chain integrity is verified by the member cert chain walk below
	// (memberCert.Chain includes the issuer cert). Here we only need the time check.
	if IsCertExpired(tokenClaims.GetIssuer(), now) {
		return nil, fmt.Errorf("join token issuer: %w", ErrCertExpired)
	}

	issuerPub := tokenClaims.GetIssuer().GetClaims().GetSubjectPub()
	msg, err := signaturePayload(tokenClaims)
	if err != nil {
		return nil, err
	}
	if err := verifyPayload(ed25519.PublicKey(issuerPub), msg, token.GetSignature(), sigContextJoinClaims); err != nil {
		return nil, errors.New("join token signature invalid")
	}

	issuedAt := time.Unix(tokenClaims.GetIssuedAtUnix(), 0).Add(-timeSkewAllowance)
	expiresAt := time.Unix(tokenClaims.GetExpiresAtUnix(), 0).Add(timeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("join token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return nil, errors.New("join token expired or not yet valid")
	}

	memberCert := tokenClaims.GetMemberCert()
	rootPub, err := verifyDelegationCertChain(memberCert)
	if err != nil {
		return nil, fmt.Errorf("join token membership cert invalid: %w", err)
	}
	if len(expectedSubject) > 0 && !bytes.Equal(memberCert.GetClaims().GetSubjectPub(), expectedSubject) {
		return nil, errors.New("join token membership cert subject mismatch")
	}
	if IsCertExpired(memberCert, now) {
		return nil, fmt.Errorf("join token membership cert: %w", ErrCertExpired)
	}

	return &VerifiedToken{
		Claims:  tokenClaims,
		RootPub: rootPub,
		Cert:    memberCert,
	}, nil
}

func EncodeJoinToken(token *admissionv1.JoinToken) (string, error) {
	b, err := token.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeJoinToken(s string) (*admissionv1.JoinToken, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	token := &admissionv1.JoinToken{}
	if err := token.UnmarshalVT(b); err != nil {
		return nil, err
	}

	return token, nil
}

func EnsureIdentityKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	dir := filepath.Join(pollenDir, keysDir)
	privPath := filepath.Join(dir, signingKeyName)
	pubPath := filepath.Join(dir, signingPubKeyName)

	if priv, pub, err := loadKeyPair(privPath, pubPath, pemTypePriv, pemTypePub); err == nil {
		return priv, pub, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	return generateKeyPair(dir, signingKeyName, signingPubKeyName, pemTypePriv, pemTypePub)
}

// ReadIdentityPub reads the public key from disk without requiring private key access.
func ReadIdentityPub(pollenDir string) (ed25519.PublicKey, error) {
	raw, err := os.ReadFile(filepath.Join(pollenDir, keysDir, signingPubKeyName))
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != pemTypePub {
		return nil, errors.New("invalid public key PEM")
	}
	return ed25519.PublicKey(block.Bytes), nil
}

func signaturePayload(msg proto.Message) ([]byte, error) {
	return (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
}

func signPayload(privateKey ed25519.PrivateKey, payload []byte, context string) ([]byte, error) {
	return privateKey.Sign(nil, payload, &ed25519.Options{Context: context})
}

func verifyPayload(publicKey ed25519.PublicKey, payload, signature []byte, context string) error {
	return ed25519.VerifyWithOptions(publicKey, payload, signature, &ed25519.Options{Context: context})
}
