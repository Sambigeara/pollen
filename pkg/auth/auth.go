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
	"github.com/sambigeara/pollen/pkg/config"
	"google.golang.org/protobuf/proto"
)

const (
	keysDir = "keys"

	adminPrivKeyName = "admin_ed25519.key"
	adminPubKeyName  = "admin_ed25519.pub"

	trustBundleName    = "cluster.trust.pb"
	membershipCertName = "membership.cert.pb"
	adminCertName      = "admin.cert.pb"

	pemTypeAdminPriv = "POLLEN ADMIN ED25519 PRIVATE KEY"
	pemTypeAdminPub  = "POLLEN ADMIN ED25519 PUBLIC KEY"

	keyDirPerm  = 0o700
	keyFilePerm = 0o600

	timeSkewAllowance = time.Minute

	sigContextAdminCert  = "pollen.admin.v1"
	sigContextMembership = "pollen.membership.v1"
	sigContextJoinClaims = "pollen.join.v1"
	sigContextInvite     = "pollen.invite.v1"
	sigContextRevocation = "pollen.revocation.v1"
)

var (
	ErrCredentialsNotFound = errors.New("node membership credentials not found")
	ErrDifferentCluster    = errors.New("node already has membership credentials for a different cluster")
)

type NodeCredentials struct {
	Trust        *admissionv1.TrustBundle
	Cert         *admissionv1.MembershipCert
	InviteSigner *AdminSigner
}

type VerifiedToken struct {
	Claims *admissionv1.JoinTokenClaims
	Trust  *admissionv1.TrustBundle
	Cert   *admissionv1.MembershipCert
}

func EnsureNodeCredentialsFromToken(pollenDir string, nodePub ed25519.PublicKey, token *admissionv1.JoinToken, now time.Time) (*NodeCredentials, error) {
	existing, err := loadNodeCredentialsIfPresent(pollenDir)
	if err != nil {
		return nil, err
	}

	return ensureNodeCredentialsFromToken(pollenDir, nodePub, token, now, existing)
}

func LoadOrEnrollNodeCredentials(pollenDir string, nodePub ed25519.PublicKey, token *admissionv1.JoinToken, now time.Time) (*NodeCredentials, error) {
	if token == nil {
		return LoadExistingNodeCredentials(pollenDir, nodePub, now)
	}

	return EnsureNodeCredentialsFromToken(pollenDir, nodePub, token, now)
}

func EnsureLocalRootCredentials(pollenDir string, nodePub ed25519.PublicKey, now time.Time, membershipTTL time.Duration) (*NodeCredentials, error) {
	existing, err := loadNodeCredentialsIfPresent(pollenDir)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		if err := VerifyMembershipCert(existing.Cert, existing.Trust, now, nodePub); err != nil {
			return nil, fmt.Errorf("invalid stored membership credentials: %w", err)
		}
		return existing, nil
	}

	if membershipTTL <= 0 {
		membershipTTL = time.Duration(config.DefaultMembershipDays) * 24 * time.Hour
	}

	adminPriv, adminPub, err := LoadOrCreateAdminKey(pollenDir)
	if err != nil {
		return nil, err
	}

	trust := NewTrustBundle(adminPub)
	cert, err := IssueMembershipCert(adminPriv, trust.GetClusterId(), nodePub, now.Add(-timeSkewAllowance), now.Add(membershipTTL))
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

func ensureNodeCredentialsFromToken(
	pollenDir string,
	nodePub ed25519.PublicKey,
	token *admissionv1.JoinToken,
	now time.Time,
	existing *NodeCredentials,
) (*NodeCredentials, error) {
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

		if err := VerifyMembershipCert(existing.Cert, existing.Trust, now, nodePub); err == nil {
			return existing, nil
		}
	}

	if err := SaveNodeCredentials(pollenDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

func LoadExistingNodeCredentials(pollenDir string, nodePub ed25519.PublicKey, now time.Time) (*NodeCredentials, error) {
	creds, err := loadNodeCredentials(pollenDir)
	if err != nil {
		return nil, err
	}

	if err := VerifyMembershipCert(creds.Cert, creds.Trust, now, nodePub); err != nil {
		return nil, fmt.Errorf("invalid stored membership credentials: %w", err)
	}

	return creds, nil
}

func loadNodeCredentials(pollenDir string) (*NodeCredentials, error) {
	dir := filepath.Join(pollenDir, keysDir)
	trustPath := filepath.Join(dir, trustBundleName)
	certPath := filepath.Join(dir, membershipCertName)

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

	cert := &admissionv1.MembershipCert{}
	if err := cert.UnmarshalVT(certRaw); err != nil {
		return nil, err
	}

	return &NodeCredentials{Trust: trust, Cert: cert}, nil
}

func SaveNodeCredentials(pollenDir string, creds *NodeCredentials) error {
	// TODO(saml) state refactor
	dir := filepath.Join(pollenDir, keysDir)
	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
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

	trustPath := filepath.Join(dir, trustBundleName)
	certPath := filepath.Join(dir, membershipCertName)

	if err := os.WriteFile(trustPath, trustRaw, keyFilePerm); err != nil {
		return err
	}

	if err := os.WriteFile(certPath, certRaw, keyFilePerm); err != nil {
		return err
	}

	return nil
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

	derivedPub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, nil, errors.New("admin private key is not ed25519")
	}
	if !bytes.Equal(derivedPub, pub) {
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
	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return nil, nil, err
	}

	pub, priv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privPath := filepath.Join(dir, adminPrivKeyName)
	pubPath := filepath.Join(dir, adminPubKeyName)

	privFile, err := os.OpenFile(privPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, keyFilePerm)
	if err != nil {
		return nil, nil, err
	}

	if err := pem.Encode(privFile, &pem.Block{Type: pemTypeAdminPriv, Bytes: priv.Seed()}); err != nil {
		_ = privFile.Close()
		return nil, nil, err
	}

	if err := privFile.Close(); err != nil {
		return nil, nil, err
	}

	pubFile, err := os.OpenFile(pubPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, keyFilePerm)
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

	return priv, pub, nil
}

func NewTrustBundle(rootPub ed25519.PublicKey) *admissionv1.TrustBundle {
	clusterID := sha256.Sum256(rootPub)
	return &admissionv1.TrustBundle{
		ClusterId: clusterID[:],
		RootPub:   rootPub,
	}
}

func IssueAdminCert(
	rootPriv ed25519.PrivateKey,
	clusterID []byte,
	adminPub ed25519.PublicKey,
	notBefore time.Time,
	notAfter time.Time,
) (*admissionv1.AdminCert, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.AdminCertClaims{
		ClusterId:     clusterID,
		AdminPub:      adminPub,
		NotBeforeUnix: notBefore.Unix(),
		NotAfterUnix:  notAfter.Unix(),
		Serial:        serial,
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(rootPriv, msg, sigContextAdminCert)
	if err != nil {
		return nil, err
	}
	return &admissionv1.AdminCert{Claims: claims, Signature: sig}, nil
}

func VerifyAdminCert(
	cert *admissionv1.AdminCert,
	trust *admissionv1.TrustBundle,
	now time.Time,
) error {
	if cert == nil {
		return errors.New("admin cert missing claims")
	}
	if err := protovalidate.Validate(cert); err != nil {
		return fmt.Errorf("admin cert invalid: %w", err)
	}

	msg, err := signaturePayload(cert.GetClaims())
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(trust.GetRootPub()), msg, cert.GetSignature(), sigContextAdminCert); err != nil {
		return errors.New("admin cert signature invalid")
	}

	notBefore := time.Unix(cert.GetClaims().GetNotBeforeUnix(), 0).Add(-timeSkewAllowance)
	notAfter := time.Unix(cert.GetClaims().GetNotAfterUnix(), 0).Add(timeSkewAllowance)
	if now.Before(notBefore) || now.After(notAfter) {
		return errors.New("admin cert outside validity window")
	}

	return nil
}

func IssueMembershipCert(
	adminPriv ed25519.PrivateKey,
	clusterID []byte,
	subject ed25519.PublicKey,
	notBefore time.Time,
	notAfter time.Time,
) (*admissionv1.MembershipCert, error) {
	adminPub, ok := adminPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("admin private key is not ed25519")
	}

	now := time.Now()
	adminTTL := time.Duration(config.DefaultAdminDays) * 24 * time.Hour
	issuer, err := IssueAdminCert(
		adminPriv,
		clusterID,
		adminPub,
		now.Add(-timeSkewAllowance),
		now.Add(adminTTL),
	)
	if err != nil {
		return nil, err
	}

	return IssueMembershipCertWithIssuer(adminPriv, issuer, clusterID, subject, notBefore, notAfter)
}

func IssueMembershipCertWithIssuer(
	adminPriv ed25519.PrivateKey,
	issuer *admissionv1.AdminCert,
	clusterID []byte,
	subject ed25519.PublicKey,
	notBefore time.Time,
	notAfter time.Time,
) (*admissionv1.MembershipCert, error) {
	if len(clusterID) != sha256.Size {
		return nil, errors.New("invalid cluster id length")
	}
	if len(subject) != ed25519.PublicKeySize {
		return nil, errors.New("invalid subject key length")
	}
	if !notAfter.After(notBefore) {
		return nil, errors.New("invalid membership certificate validity window")
	}

	adminPub, ok := adminPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("admin private key is not ed25519")
	}

	if issuer == nil || issuer.GetClaims() == nil {
		return nil, errors.New("missing issuer admin cert")
	}
	if !bytes.Equal(issuer.GetClaims().GetClusterId(), clusterID) {
		return nil, errors.New("issuer admin cert cluster mismatch")
	}
	if !bytes.Equal(issuer.GetClaims().GetAdminPub(), adminPub) {
		return nil, errors.New("issuer admin cert does not match signing admin key")
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.MembershipCertClaims{
		ClusterId:      append([]byte(nil), clusterID...),
		SubjectPub:     append([]byte(nil), subject...),
		IssuerAdminPub: append([]byte(nil), adminPub...),
		NotBeforeUnix:  notBefore.Unix(),
		NotAfterUnix:   notAfter.Unix(),
		Serial:         serial,
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(adminPriv, msg, sigContextMembership)
	if err != nil {
		return nil, err
	}
	return &admissionv1.MembershipCert{
		Claims:    claims,
		Issuer:    issuer,
		Signature: sig,
	}, nil
}

func VerifyMembershipCert(cert *admissionv1.MembershipCert, trust *admissionv1.TrustBundle, now time.Time, expectedSubject []byte) error {
	// trust is the verification root, not extra metadata: it anchors issuer
	// verification at root_pub and binds the cert to this cluster_id.
	if cert == nil {
		return errors.New("membership cert missing claims")
	}
	if err := protovalidate.Validate(cert); err != nil {
		return fmt.Errorf("membership cert invalid: %w", err)
	}
	if !bytes.Equal(cert.GetClaims().GetClusterId(), trust.GetClusterId()) {
		return errors.New("membership cert not scoped to cluster")
	}

	issuer := cert.GetIssuer()
	if err := VerifyAdminCert(issuer, trust, now); err != nil {
		return fmt.Errorf("membership cert issuer invalid: %w", err)
	}
	if !bytes.Equal(issuer.GetClaims().GetAdminPub(), cert.GetClaims().GetIssuerAdminPub()) {
		return errors.New("membership cert issuer key mismatch")
	}
	msg, err := signaturePayload(cert.GetClaims())
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(cert.GetClaims().GetIssuerAdminPub()), msg, cert.GetSignature(), sigContextMembership); err != nil {
		return errors.New("membership cert signature invalid")
	}

	notBefore := time.Unix(cert.GetClaims().GetNotBeforeUnix(), 0).Add(-timeSkewAllowance)
	notAfter := time.Unix(cert.GetClaims().GetNotAfterUnix(), 0).Add(timeSkewAllowance)
	if now.Before(notBefore) || now.After(notAfter) {
		return errors.New("membership cert outside validity window")
	}

	if !bytes.Equal(cert.GetClaims().GetSubjectPub(), expectedSubject) {
		return errors.New("membership cert subject mismatch")
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
) (*admissionv1.JoinToken, error) {
	if trust == nil {
		return nil, errors.New("missing trust bundle")
	}

	adminPub, ok := adminPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("admin private key is not ed25519")
	}

	if !bytes.Equal(adminPub, trust.GetRootPub()) {
		return nil, errors.New("issuer admin certificate required for non-root admin key")
	}

	adminTTL := time.Duration(config.DefaultAdminDays) * 24 * time.Hour
	issuer, err := IssueAdminCert(
		adminPriv,
		trust.GetClusterId(),
		adminPub,
		now.Add(-timeSkewAllowance),
		now.Add(adminTTL),
	)
	if err != nil {
		return nil, err
	}

	return IssueJoinTokenWithIssuer(adminPriv, trust, issuer, subject, bootstrap, now, tokenTTL)
}

func IssueJoinTokenWithIssuer(
	adminPriv ed25519.PrivateKey,
	trust *admissionv1.TrustBundle,
	issuer *admissionv1.AdminCert,
	subject ed25519.PublicKey,
	bootstrap []*admissionv1.BootstrapPeer,
	now time.Time,
	tokenTTL time.Duration,
) (*admissionv1.JoinToken, error) {
	if tokenTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}

	if _, ok := adminPriv.Public().(ed25519.PublicKey); !ok {
		return nil, errors.New("admin private key is not ed25519")
	}
	if err := VerifyAdminCert(issuer, trust, now); err != nil {
		return nil, fmt.Errorf("issuer admin cert invalid: %w", err)
	}

	cert, err := IssueMembershipCertWithIssuer(
		adminPriv,
		issuer,
		trust.GetClusterId(),
		subject,
		now.Add(-timeSkewAllowance),
		now.Add(time.Duration(config.DefaultMembershipDays)*24*time.Hour),
	)
	if err != nil {
		return nil, err
	}

	claims := &admissionv1.JoinTokenClaims{
		TokenId:       uuid.NewString(),
		Trust:         trust,
		Issuer:        issuer,
		MemberCert:    cert,
		Bootstrap:     bootstrap,
		IssuedAtUnix:  now.Unix(),
		ExpiresAtUnix: now.Add(tokenTTL).Unix(),
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(adminPriv, msg, sigContextJoinClaims)
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

	if err := VerifyAdminCert(claims.GetIssuer(), trust, now); err != nil {
		return nil, fmt.Errorf("join token issuer admin cert invalid: %w", err)
	}

	issuerPub := claims.GetIssuer().GetClaims().GetAdminPub()
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

	if err := VerifyMembershipCert(claims.GetMemberCert(), trust, now, expectedSubject); err != nil {
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

func SignRevocation(adminPriv ed25519.PrivateKey, clusterID, subjectPub []byte, now time.Time) (*admissionv1.SignedRevocation, error) {
	adminPub, ok := adminPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("admin private key is not ed25519")
	}

	claims := &admissionv1.RevocationClaims{
		ClusterId:     append([]byte(nil), clusterID...),
		SubjectPub:    append([]byte(nil), subjectPub...),
		AdminPub:      append([]byte(nil), adminPub...),
		RevokedAtUnix: now.Unix(),
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return nil, err
	}

	sig, err := signPayload(adminPriv, msg, sigContextRevocation)
	if err != nil {
		return nil, err
	}

	return &admissionv1.SignedRevocation{Claims: claims, Signature: sig}, nil
}

func VerifyRevocation(rev *admissionv1.SignedRevocation, trust *admissionv1.TrustBundle) error {
	if rev == nil || rev.GetClaims() == nil {
		return errors.New("revocation missing claims")
	}

	claims := rev.GetClaims()
	if !bytes.Equal(claims.GetClusterId(), trust.GetClusterId()) {
		return errors.New("revocation cluster id mismatch")
	}

	if !bytes.Equal(claims.GetAdminPub(), trust.GetRootPub()) {
		return errors.New("revocation not signed by cluster root key")
	}

	msg, err := signaturePayload(claims)
	if err != nil {
		return err
	}

	return verifyPayload(ed25519.PublicKey(claims.GetAdminPub()), msg, rev.GetSignature(), sigContextRevocation)
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
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func signPayload(privateKey ed25519.PrivateKey, payload []byte, context string) ([]byte, error) {
	return privateKey.Sign(nil, payload, &ed25519.Options{Context: context})
}

func verifyPayload(publicKey ed25519.PublicKey, payload, signature []byte, context string) error {
	return ed25519.VerifyWithOptions(publicKey, payload, signature, &ed25519.Options{Context: context})
}
