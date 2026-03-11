package auth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/perm"
	"google.golang.org/protobuf/proto"
)

const (
	keysDir = "keys"

	adminPrivKeyName = "admin_ed25519.key"
	adminPubKeyName  = "admin_ed25519.pub"

	trustBundleName    = "cluster.trust.pb"
	delegationCertName = "delegation.cert.pb"

	pemTypeAdminPriv = "POLLEN ADMIN ED25519 PRIVATE KEY"
	pemTypeAdminPub  = "POLLEN ADMIN ED25519 PUBLIC KEY"

	keyFilePerm = 0o640

	timeSkewAllowance = time.Minute

	sigContextDelegation = "pollen.delegation.v1"
	sigContextJoinClaims = "pollen.join.v1"
	sigContextInvite     = "pollen.invite.v1"
)

var (
	ErrCredentialsNotFound = errors.New("node membership credentials not found")
	ErrDifferentCluster    = errors.New("node already has membership credentials for a different cluster")
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

func IsCertExpiredAt(expiresAt, now time.Time) bool {
	return now.After(expiresAt.Add(timeSkewAllowance))
}

func IsCertExpired(cert *admissionv1.DelegationCert, now time.Time) bool {
	return IsCertExpiredAt(CertExpiresAt(cert), now)
}

// CertTTL returns the original TTL of the cert (notAfter - notBefore).
func CertTTL(cert *admissionv1.DelegationCert) time.Duration {
	claims := cert.GetClaims()
	return time.Duration(claims.GetNotAfterUnix()-claims.GetNotBeforeUnix()) * time.Second
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

// IsCapSubset returns true if child capabilities are a subset of parent's.
func IsCapSubset(child, parent *admissionv1.Capabilities) bool {
	if child.GetCanDelegate() && !parent.GetCanDelegate() {
		return false
	}
	if child.GetCanAdmit() && !parent.GetCanAdmit() {
		return false
	}
	if child.GetMaxDepth() > parent.GetMaxDepth() {
		return false
	}
	return true
}

type NodeCredentials struct {
	Trust         *admissionv1.TrustBundle
	Cert          *admissionv1.DelegationCert
	DelegationKey *DelegationSigner
}

type VerifiedToken struct {
	Claims *admissionv1.JoinTokenClaims
	Trust  *admissionv1.TrustBundle
	Cert   *admissionv1.DelegationCert
}

func EnsureNodeCredentialsFromToken(pollenDir string, nodePub ed25519.PublicKey, token *admissionv1.JoinToken, now time.Time) (*NodeCredentials, error) {
	existing, err := loadNodeCredentialsIfPresent(pollenDir)
	if err != nil {
		return nil, err
	}

	verified, err := VerifyJoinToken(token, nodePub, now)
	if err != nil {
		return nil, err
	}

	creds := &NodeCredentials{
		Trust: verified.Trust,
		Cert:  verified.Cert,
	}

	if existing != nil {
		if !proto.Equal(existing.Trust, creds.Trust) {
			return nil, ErrDifferentCluster
		}
		if VerifyDelegationCert(existing.Cert, existing.Trust, now, nodePub) == nil &&
			existing.Cert.GetClaims().GetNotAfterUnix() >= creds.Cert.GetClaims().GetNotAfterUnix() {
			return existing, nil
		}
	}

	if err := SaveNodeCredentials(pollenDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

func LoadOrEnrollNodeCredentials(pollenDir string, nodePub ed25519.PublicKey, token *admissionv1.JoinToken, now time.Time) (*NodeCredentials, error) {
	if token == nil {
		return LoadExistingNodeCredentials(pollenDir, nodePub, now)
	}

	return EnsureNodeCredentialsFromToken(pollenDir, nodePub, token, now)
}

func EnsureLocalRootCredentials(pollenDir string, nodePub ed25519.PublicKey, now time.Time, membershipTTL, delegationTTL time.Duration) (*NodeCredentials, error) {
	existing, err := loadNodeCredentialsIfPresent(pollenDir)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		if err := VerifyDelegationCert(existing.Cert, existing.Trust, now, nodePub); err != nil {
			return nil, fmt.Errorf("invalid stored delegation credentials: %w", err)
		}
		return existing, nil
	}

	adminPriv, _, err := LoadOrCreateAdminKey(pollenDir)
	if err != nil {
		return nil, err
	}

	adminPub := adminPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	trust := NewTrustBundle(adminPub)

	// Root issues a delegation cert for itself with full capabilities.
	cert, err := IssueDelegationCert(
		adminPriv,
		nil, // no parent chain — root-issued
		trust.GetClusterId(),
		nodePub,
		FullCapabilities(),
		now.Add(-timeSkewAllowance),
		now.Add(delegationTTL),
		time.Time{},
	)
	if err != nil {
		return nil, err
	}

	creds := &NodeCredentials{Trust: trust, Cert: cert}
	if err := SaveNodeCredentials(pollenDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

func loadNodeCredentialsIfPresent(pollenDir string) (*NodeCredentials, error) {
	creds, err := loadNodeCredentials(pollenDir)
	if err != nil {
		if errors.Is(err, ErrCredentialsNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return creds, nil
}

func LoadExistingNodeCredentials(pollenDir string, nodePub ed25519.PublicKey, now time.Time) (*NodeCredentials, error) {
	creds, err := loadNodeCredentials(pollenDir)
	if err != nil {
		return nil, err
	}

	if err := VerifyDelegationCert(creds.Cert, creds.Trust, now, nodePub); err != nil {
		return nil, fmt.Errorf("invalid stored delegation credentials: %w", err)
	}

	return creds, nil
}

func loadNodeCredentials(pollenDir string) (*NodeCredentials, error) {
	dir := filepath.Join(pollenDir, keysDir)
	trustPath := filepath.Join(dir, trustBundleName)
	certPath := filepath.Join(dir, delegationCertName)

	trustRaw, err := os.ReadFile(trustPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}

	certRaw, err := os.ReadFile(certPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCredentialsNotFound
		}
		return nil, err
	}

	trust := &admissionv1.TrustBundle{}
	if err := trust.UnmarshalVT(trustRaw); err != nil {
		return nil, err
	}
	if err := validateTrustBundle(trust); err != nil {
		return nil, err
	}

	cert := &admissionv1.DelegationCert{}
	if err := cert.UnmarshalVT(certRaw); err != nil {
		return nil, err
	}

	return &NodeCredentials{Trust: trust, Cert: cert}, nil
}

func SaveNodeCredentials(pollenDir string, creds *NodeCredentials) error {
	dir := filepath.Join(pollenDir, keysDir)
	if err := perm.EnsureDir(dir); err != nil {
		return err
	}

	trustRaw, err := creds.Trust.MarshalVT()
	if err != nil {
		return err
	}

	certRaw, err := creds.Cert.MarshalVT()
	if err != nil {
		return err
	}

	if err := perm.WriteGroupReadable(filepath.Join(dir, trustBundleName), trustRaw); err != nil {
		return err
	}

	return perm.WriteGroupReadable(filepath.Join(dir, delegationCertName), certRaw)
}

func LoadAdminKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	privPath := filepath.Join(pollenDir, keysDir, adminPrivKeyName)
	pubPath := filepath.Join(pollenDir, keysDir, adminPubKeyName)

	privRaw, err := os.ReadFile(privPath)
	if err != nil {
		return nil, nil, err
	}

	pubRaw, err := os.ReadFile(pubPath)
	if err != nil {
		return nil, nil, err
	}

	privBlock, _ := pem.Decode(privRaw)
	if privBlock == nil || privBlock.Type != pemTypeAdminPriv {
		return nil, nil, errors.New("invalid admin private key PEM")
	}
	if len(privBlock.Bytes) != ed25519.SeedSize {
		return nil, nil, errors.New("invalid admin private key length")
	}
	priv := ed25519.NewKeyFromSeed(privBlock.Bytes)

	pubBlock, _ := pem.Decode(pubRaw)
	if pubBlock == nil || pubBlock.Type != pemTypeAdminPub {
		return nil, nil, errors.New("invalid admin public key PEM")
	}
	if len(pubBlock.Bytes) != ed25519.PublicKeySize {
		return nil, nil, errors.New("invalid admin public key length")
	}
	pub := ed25519.PublicKey(pubBlock.Bytes)

	if !bytes.Equal(priv.Public().(ed25519.PublicKey), pub) { //nolint:forcetypeassert
		return nil, nil, errors.New("admin keypair mismatch")
	}

	return priv, pub, nil
}

func LoadOrCreateAdminKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	priv, pub, err := LoadAdminKey(pollenDir)
	if err == nil {
		return priv, pub, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	dir := filepath.Join(pollenDir, keysDir)
	if err := perm.EnsureDir(dir); err != nil {
		return nil, nil, err
	}

	pub, priv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privPath := filepath.Join(dir, adminPrivKeyName)
	pubPath := filepath.Join(dir, adminPubKeyName)

	privFile, err := os.OpenFile(privPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, keyFilePerm)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return LoadAdminKey(pollenDir)
		}
		return nil, nil, err
	}

	if err := pem.Encode(privFile, &pem.Block{Type: pemTypeAdminPriv, Bytes: priv.Seed()}); err != nil {
		_ = privFile.Close()
		return nil, nil, err
	}

	if err := privFile.Close(); err != nil {
		return nil, nil, err
	}

	if err := perm.SetGroupReadable(privPath); err != nil {
		return nil, nil, err
	}

	pubFile, err := os.OpenFile(pubPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, keyFilePerm)
	if err != nil {
		return nil, nil, err
	}

	if err := pem.Encode(pubFile, &pem.Block{Type: pemTypeAdminPub, Bytes: pub}); err != nil {
		_ = pubFile.Close()
		return nil, nil, err
	}

	if err := pubFile.Close(); err != nil {
		return nil, nil, err
	}

	if err := perm.SetGroupReadable(pubPath); err != nil {
		return nil, nil, err
	}

	return priv, pub, nil
}

func NewTrustBundle(rootPub ed25519.PublicKey) *admissionv1.TrustBundle {
	clusterID := sha256.Sum256(rootPub)
	return &admissionv1.TrustBundle{
		ClusterId: clusterID[:],
		RootPub:   rootPub,
	}
}

func validateParentChain(
	parentChain []*admissionv1.DelegationCert,
	caps *admissionv1.Capabilities,
	signerPub ed25519.PublicKey,
	notAfter *time.Time,
) error {
	parent := parentChain[0]
	parentCaps := parent.GetClaims().GetCapabilities()

	if !parentCaps.GetCanDelegate() {
		return errors.New("parent cert lacks delegation capability")
	}
	if !IsCapSubset(caps, parentCaps) {
		return errors.New("child capabilities exceed parent capabilities")
	}
	if caps.GetMaxDepth() >= parentCaps.GetMaxDepth() {
		return errors.New("child max_depth must be less than parent max_depth")
	}

	parentExpiry := time.Unix(parent.GetClaims().GetNotAfterUnix(), 0)
	if notAfter.After(parentExpiry) {
		*notAfter = parentExpiry
	}

	if !bytes.Equal(parent.GetClaims().GetSubjectPub(), signerPub) {
		return errors.New("signer key does not match parent cert subject")
	}
	return nil
}

// IssueDelegationCert creates a new DelegationCert signed by signerPriv.
// parentChain is nil for root-issued certs; otherwise it's the issuer's chain
// (issuer's own cert + issuer's chain).
func IssueDelegationCert(
	signerPriv ed25519.PrivateKey,
	parentChain []*admissionv1.DelegationCert,
	clusterID []byte,
	subjectPub ed25519.PublicKey,
	caps *admissionv1.Capabilities,
	notBefore, notAfter time.Time,
	accessDeadline time.Time,
) (*admissionv1.DelegationCert, error) {
	if len(clusterID) != sha256.Size {
		return nil, errors.New("invalid cluster id length")
	}
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

	// Enforce attenuation against parent if not root-issued.
	if len(parentChain) > 0 {
		if err := validateParentChain(parentChain, caps, signerPub, &notAfter); err != nil {
			return nil, err
		}
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.DelegationCertClaims{
		ClusterId:     append([]byte(nil), clusterID...),
		SubjectPub:    append([]byte(nil), subjectPub...),
		IssuerPub:     append([]byte(nil), signerPub...),
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

// VerifyDelegationCert verifies a DelegationCert chain against a TrustBundle.
func VerifyDelegationCert(
	cert *admissionv1.DelegationCert,
	trust *admissionv1.TrustBundle,
	now time.Time,
	expectedSubject []byte,
) error {
	if cert == nil {
		return errors.New("delegation cert is nil")
	}
	if err := protovalidate.Validate(cert); err != nil {
		return fmt.Errorf("delegation cert invalid: %w", err)
	}
	if !bytes.Equal(cert.GetClaims().GetClusterId(), trust.GetClusterId()) {
		return errors.New("delegation cert not scoped to cluster")
	}

	// Check leaf cert time validity (sufficient because issuance clamps child.notAfter <= parent.notAfter).
	notBefore := time.Unix(cert.GetClaims().GetNotBeforeUnix(), 0).Add(-timeSkewAllowance)
	notAfter := time.Unix(cert.GetClaims().GetNotAfterUnix(), 0).Add(timeSkewAllowance)
	if now.Before(notBefore) || now.After(notAfter) {
		return errors.New("delegation cert outside validity window")
	}

	// Walk chain from leaf to root verifying signatures.
	current := cert
	for _, parent := range cert.GetChain() {
		if !bytes.Equal(current.GetClaims().GetIssuerPub(), parent.GetClaims().GetSubjectPub()) {
			return errors.New("delegation cert chain issuer/subject mismatch")
		}
		msg, err := signaturePayload(current.GetClaims())
		if err != nil {
			return err
		}
		if err := verifyPayload(ed25519.PublicKey(current.GetClaims().GetIssuerPub()), msg, current.GetSignature(), sigContextDelegation); err != nil {
			return errors.New("delegation cert signature invalid")
		}
		current = parent
	}

	// Final cert in chain must be signed by root_pub.
	if !bytes.Equal(current.GetClaims().GetIssuerPub(), trust.GetRootPub()) {
		return errors.New("delegation cert chain root mismatch")
	}
	msg, err := signaturePayload(current.GetClaims())
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(trust.GetRootPub()), msg, current.GetSignature(), sigContextDelegation); err != nil {
		return errors.New("delegation cert root signature invalid")
	}

	if len(expectedSubject) > 0 && !bytes.Equal(cert.GetClaims().GetSubjectPub(), expectedSubject) {
		return errors.New("delegation cert subject mismatch")
	}

	return nil
}

func IssueJoinToken(
	adminPriv ed25519.PrivateKey,
	trust *admissionv1.TrustBundle,
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
	accessDeadline time.Time,
) (*admissionv1.JoinToken, error) {
	if trust == nil {
		return nil, errors.New("missing trust bundle")
	}

	adminPub := adminPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	if !bytes.Equal(adminPub, trust.GetRootPub()) {
		return nil, errors.New("issuer delegation certificate required for non-root admin key")
	}

	// Root issues its own delegation cert as the issuer (no deadline on the issuer cert).
	issuerCert, err := IssueDelegationCert(
		adminPriv,
		nil, // root-issued
		trust.GetClusterId(),
		adminPub,
		FullCapabilities(),
		now.Add(-timeSkewAllowance),
		now.Add(membershipTTL),
		time.Time{},
	)
	if err != nil {
		return nil, err
	}

	return IssueJoinTokenWithIssuer(adminPriv, trust, issuerCert, subject, bootstrap, now, tokenTTL, membershipTTL, accessDeadline)
}

func IssueJoinTokenWithIssuer(
	signerPriv ed25519.PrivateKey,
	trust *admissionv1.TrustBundle,
	issuer *admissionv1.DelegationCert,
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
	membershipTTL time.Duration,
	accessDeadline time.Time,
) (*admissionv1.JoinToken, error) {
	if tokenTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}

	if err := VerifyDelegationCert(issuer, trust, now, nil); err != nil {
		return nil, fmt.Errorf("issuer delegation cert invalid: %w", err)
	}

	// Build parent chain for the new member cert: issuer's cert + issuer's chain.
	parentChain := make([]*admissionv1.DelegationCert, 0, 1+len(issuer.GetChain()))
	parentChain = append(parentChain, issuer)
	parentChain = append(parentChain, issuer.GetChain()...)

	notAfter := now.Add(membershipTTL)
	if !accessDeadline.IsZero() && notAfter.After(accessDeadline) {
		notAfter = accessDeadline
	}

	memberCert, err := IssueDelegationCert(
		signerPriv,
		parentChain,
		trust.GetClusterId(),
		subject,
		LeafCapabilities(),
		now.Add(-timeSkewAllowance),
		notAfter,
		accessDeadline,
	)
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.JoinTokenClaims{
		TokenId:       uuid.NewString(),
		Trust:         trust,
		Issuer:        issuer,
		MemberCert:    memberCert,
		Bootstrap:     bootstrap,
		IssuedAtUnix:  now.Unix(),
		ExpiresAtUnix: now.Add(tokenTTL).Unix(),
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(signerPriv, msg, sigContextJoinClaims)
	if err != nil {
		return nil, err
	}
	return &admissionv1.JoinToken{Claims: claims, Signature: sig}, nil
}

func VerifyJoinToken(token *admissionv1.JoinToken, expectedSubject ed25519.PublicKey, now time.Time) (*VerifiedToken, error) {
	if token == nil {
		return nil, errors.New("join token missing claims")
	}
	if err := protovalidate.Validate(token); err != nil {
		return nil, fmt.Errorf("join token invalid: %w", err)
	}

	claims := token.GetClaims()
	trust := claims.GetTrust()
	if err := validateTrustBundle(trust); err != nil {
		return nil, err
	}

	if err := VerifyDelegationCert(claims.GetIssuer(), trust, now, nil); err != nil {
		return nil, fmt.Errorf("join token issuer delegation cert invalid: %w", err)
	}

	issuerPub := claims.GetIssuer().GetClaims().GetSubjectPub()
	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}
	if err := verifyPayload(ed25519.PublicKey(issuerPub), msg, token.GetSignature(), sigContextJoinClaims); err != nil {
		return nil, errors.New("join token signature invalid")
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-timeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(timeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("join token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return nil, errors.New("join token expired or not yet valid")
	}

	if err := VerifyDelegationCert(claims.GetMemberCert(), trust, now, expectedSubject); err != nil {
		return nil, fmt.Errorf("join token membership cert invalid: %w", err)
	}

	return &VerifiedToken{
		Claims: claims,
		Trust:  trust,
		Cert:   claims.GetMemberCert(),
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

func validateTrustBundle(trust *admissionv1.TrustBundle) error {
	if trust == nil {
		return errors.New("missing trust bundle")
	}
	if err := protovalidate.Validate(trust); err != nil {
		return fmt.Errorf("trust bundle invalid: %w", err)
	}

	expectedClusterID := sha256.Sum256(trust.GetRootPub())
	if !bytes.Equal(expectedClusterID[:], trust.GetClusterId()) {
		return errors.New("trust bundle cluster id mismatch")
	}

	return nil
}

func randomSerial() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	serial := binary.BigEndian.Uint64(b[:])
	if serial == 0 {
		return 1, nil
	}
	return serial, nil
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
