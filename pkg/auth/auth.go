// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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
	"strings"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

const (
	keysSubdir = "keys"

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

	sigContextBlobWrapping      = "pollen.blobwrapping.v1"
	sigContextDelegation        = "pollen.delegation.v1"
	sigContextJoinClaims        = "pollen.join.v1"
	sigContextInvite            = "pollen.invite.v1"
	sigContextDelegationSubject = "pollen.delegation.subject.v1"
	sigContextSpecAuth          = "pollen.specauth.v1"

	timeSkewAllowance = time.Minute
)

const MaxAttributesSize = 4096

// DefaultDelegationTTL is the lifetime of a freshly-issued delegation cert
// in the absence of a caller-specified TTL. Lives here rather than in
// pkg/config so pkg/auth can self-refresh root certs without inverting the
// dependency direction.
const DefaultDelegationTTL = 30 * 24 * time.Hour

var (
	ErrCredentialsNotFound = errors.New("node membership credentials not found")
	ErrDifferentCluster    = errors.New("node already has membership credentials for a different cluster")
	ErrCertExpired         = errors.New("delegation cert has expired")
)

func IdentityPath(pollenDir string) string {
	return filepath.Join(pollenDir, keysSubdir)
}

func FullCapabilities() *admissionv1.Capabilities {
	return &admissionv1.Capabilities{
		CanDelegate: true,
		CanAdmit:    true,
		CanPublish:  true,
		MaxDepth:    255, //nolint:mnd
	}
}

func PublisherCapabilities() *admissionv1.Capabilities {
	return &admissionv1.Capabilities{
		CanPublish: true,
		MaxDepth:   0,
	}
}

func LeafCapabilities() *admissionv1.Capabilities {
	return &admissionv1.Capabilities{
		MaxDepth: 0,
	}
}

func ValidateAttributes(attrs *structpb.Struct) error {
	if attrs == nil {
		return nil
	}
	b, err := proto.Marshal(attrs)
	if err != nil {
		return fmt.Errorf("invalid attributes: %w", err)
	}
	if len(b) > MaxAttributesSize {
		return fmt.Errorf("attributes too large: %d bytes (max %d)", len(b), MaxAttributesSize)
	}
	return nil
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

func CertAccessDeadline(cert *admissionv1.DelegationCert) (time.Time, bool) {
	dl := cert.GetClaims().GetAccessDeadlineUnix()
	if dl == 0 {
		return time.Time{}, false
	}
	return time.Unix(dl, 0), true
}

type NodeCredentials struct {
	cert          *admissionv1.DelegationCert
	specSigner    *SpecSigner
	delegationKey *DelegationSigner
	rootPub       ed25519.PublicKey
	mu            sync.RWMutex
}

func NewNodeCredentials(rootPub ed25519.PublicKey, cert *admissionv1.DelegationCert) *NodeCredentials {
	return &NodeCredentials{rootPub: rootPub, cert: cert}
}

func (c *NodeCredentials) Cert() *admissionv1.DelegationCert {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cert
}

func (c *NodeCredentials) RootPub() ed25519.PublicKey { return c.rootPub }

// SpecSigner returns the signer used to publish resource specs. Non-nil
// for any node whose cert holds CanPublish (publisher or admin tiers).
func (c *NodeCredentials) SpecSigner() *SpecSigner {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.specSigner
}

func (c *NodeCredentials) SetSpecSigner(s *SpecSigner) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.specSigner = s
}

// DelegationKey returns the signer used to mint child certs and tokens.
// Non-nil only when the local cert holds CanDelegate (admin tier).
func (c *NodeCredentials) DelegationKey() *DelegationSigner {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.delegationKey
}

// SetDelegationKey stores the delegation signer and also exposes its
// embedded SpecSigner via SpecSigner(), since admins are publishers too.
func (c *NodeCredentials) SetDelegationKey(s *DelegationSigner) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delegationKey = s
	if s != nil {
		c.specSigner = s.SpecSigner
	}
}

func (c *NodeCredentials) SetCert(cert *admissionv1.DelegationCert) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cert = cert
}

func EnrollNodeCredentials(identityDir string, nodePub ed25519.PublicKey, token *admissionv1.JoinToken, now time.Time) (*NodeCredentials, error) {
	existing, err := LoadNodeCredentials(identityDir)
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

	if err := SaveNodeCredentials(identityDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

// EnsureLocalRootCredentials re-issues the root's self-signed delegation cert
// whenever liveness, attributes, subject pub, or root pub drift from the
// persisted cert. Also serves as the root self-renewal path at boot, on
// prop changes, and when expiry approaches.
func EnsureLocalRootCredentials(identityDir string, nodePub ed25519.PublicKey, attrs *structpb.Struct, now time.Time, delegationTTL time.Duration) (*NodeCredentials, error) {
	if err := ValidateAttributes(attrs); err != nil {
		return nil, err
	}

	existing, err := LoadNodeCredentials(identityDir)
	if err != nil && !errors.Is(err, ErrCredentialsNotFound) {
		return nil, err
	}

	adminPriv, adminPub, err := EnsureAdminKey(identityDir)
	if err != nil {
		return nil, err
	}

	if existing != nil && rootCertHealthy(existing, nodePub, adminPub, now) &&
		attrsEqual(existing.cert.GetClaims().GetCapabilities().GetAttributes(), attrs) {
		return existing, nil
	}

	caps := FullCapabilities()
	caps.Attributes = attrs

	cert, err := IssueDelegationCert(
		adminPriv,
		nil, // no parent chain — root-issued
		nodePub,
		caps,
		now,
		now.Add(delegationTTL),
		time.Time{},
	)
	if err != nil {
		return nil, err
	}

	creds := &NodeCredentials{rootPub: adminPub, cert: cert}
	if err := SaveNodeCredentials(identityDir, creds); err != nil {
		return nil, err
	}

	return creds, nil
}

// Attrs are deliberately excluded; callers compose attrsEqual on top so
// config-driven changes can be checked independently.
func rootCertHealthy(existing *NodeCredentials, nodePub, adminPub ed25519.PublicKey, now time.Time) bool {
	cert := existing.cert
	if IsCertExpired(cert, now) {
		return false
	}
	claims := cert.GetClaims()
	if !bytes.Equal(claims.GetSubjectPub(), nodePub) {
		return false
	}
	if !bytes.Equal(claims.GetIssuerPub(), adminPub) {
		return false
	}
	if !bytes.Equal(existing.RootPub(), adminPub) {
		return false
	}
	if len(cert.GetChain()) != 0 {
		return false
	}
	want := FullCapabilities()
	got := claims.GetCapabilities()
	if got.GetCanDelegate() != want.CanDelegate ||
		got.GetCanAdmit() != want.CanAdmit ||
		got.GetMaxDepth() != want.MaxDepth {
		return false
	}
	return true
}

// attrsEqual treats nil and an empty Struct as equivalent — yaml round-trips
// can produce either, and a daemon restart with no config change must not
// trigger a spurious cert re-issue.
func attrsEqual(a, b *structpb.Struct) bool {
	if len(a.GetFields()) == 0 && len(b.GetFields()) == 0 {
		return true
	}
	return proto.Equal(a, b)
}

func LoadNodeCredentials(identityDir string) (*NodeCredentials, error) {
	rootPubPath := filepath.Join(identityDir, rootPubName)
	certPath := filepath.Join(identityDir, delegationCertName)

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

func SaveNodeCredentials(identityDir string, creds *NodeCredentials) error {
	if err := plnfs.EnsureDir(identityDir); err != nil {
		return err
	}
	if err := plnfs.WriteGroupReadable(filepath.Join(identityDir, rootPubName), []byte(creds.rootPub)); err != nil {
		return err
	}

	return saveDelegationCert(identityDir, creds.cert)
}

func InstallDelegationCert(identityDir, encoded string, rootPub, subject ed25519.PublicKey, now time.Time) error {
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

	return saveDelegationCert(identityDir, cert)
}

func saveDelegationCert(identityDir string, cert *admissionv1.DelegationCert) error {
	raw, err := cert.MarshalVT()
	if err != nil {
		return err
	}
	return plnfs.WriteGroupReadable(filepath.Join(identityDir, delegationCertName), raw)
}

func EnsureAdminKey(identityDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	priv, pub, err := LoadAdminKey(identityDir)
	if err == nil {
		return priv, pub, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	return generateKeyPair(identityDir, adminPrivKeyName, adminPubKeyName, pemTypeAdminPriv, pemTypeAdminPub)
}

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

func LoadAdminKey(identityDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	return loadKeyPair(
		filepath.Join(identityDir, adminPrivKeyName),
		filepath.Join(identityDir, adminPubKeyName),
		pemTypeAdminPriv,
		pemTypeAdminPub,
	)
}

// LocalRootAuthority checks issuer against the persisted root pub (not the
// local admin pub) so admin-key rotation self-heals. The non-empty-chain
// gate prevents a delegated admin with a stray local admin key from being
// classified as root, which would rewrite root.pub and fork trust.
func LocalRootAuthority(identityDir string, creds *NodeCredentials) (ed25519.PublicKey, bool, error) {
	_, adminPub, err := LoadAdminKey(identityDir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("load admin key: %w", err)
	}
	if creds == nil {
		return adminPub, true, nil
	}
	cert := creds.cert
	if len(cert.GetChain()) != 0 {
		return nil, false, nil
	}
	if !bytes.Equal(cert.GetClaims().GetIssuerPub(), creds.RootPub()) {
		return nil, false, nil
	}
	return adminPub, true, nil
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
	if err := ValidateAttributes(caps.GetAttributes()); err != nil {
		return nil, err
	}

	signerPub := signerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	if len(parentChain) > 0 {
		parent := parentChain[0]
		parentExpiry := time.Unix(parent.GetClaims().GetNotAfterUnix(), 0)
		if notAfter.After(parentExpiry) {
			notAfter = parentExpiry
		}
		if !bytes.Equal(parent.GetClaims().GetSubjectPub(), signerPub) {
			return nil, errors.New("signer key does not match parent cert subject")
		}
		if err := validateChildCapabilities(caps, parent.GetClaims().GetCapabilities()); err != nil {
			return nil, err
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
		Chain:     stripChainEntries(parentChain),
		Signature: sig,
	}, nil
}

// validateChildCapabilities enforces that an issued cert cannot grant
// itself capabilities its issuer lacks. Skipped only at root self-issuance
// (parent == nil), where the root defines its own ceiling.
func validateChildCapabilities(child, parent *admissionv1.Capabilities) error {
	if !parent.GetCanDelegate() {
		return errors.New("parent cert lacks CanDelegate capability")
	}
	if child.GetCanAdmit() && !parent.GetCanAdmit() {
		return errors.New("child capabilities exceed parent: CanAdmit")
	}
	if child.GetCanPublish() && !parent.GetCanPublish() {
		return errors.New("child capabilities exceed parent: CanPublish")
	}
	if child.GetMaxDepth() > parent.GetMaxDepth() {
		return errors.New("child capabilities exceed parent: MaxDepth")
	}
	return validateAttributesSubset(child.GetAttributes(), parent.GetAttributes())
}

// validateAttributesSubset enforces that every key the child claims is
// also held by the parent with an equal value. The child may omit keys
// to narrow scope, but cannot add new keys or change a value: gate
// policy decisions trust attribute claims, so an issuer must not be
// able to mint scopes it never received.
func validateAttributesSubset(child, parent *structpb.Struct) error {
	if len(child.GetFields()) == 0 {
		return nil
	}
	parentFields := parent.GetFields()
	for k, cv := range child.GetFields() {
		pv, ok := parentFields[k]
		if !ok {
			return fmt.Errorf("child capabilities exceed parent: attribute %q not granted by parent", k)
		}
		if !proto.Equal(cv, pv) {
			return fmt.Errorf("child capabilities exceed parent: attribute %q value not granted by parent", k)
		}
	}
	return nil
}

// stripChainEntries returns parents with each entry's own Chain cleared.
// verifyDelegationCertChain walks one level via cert.GetChain(), so leaving
// nested chains populated duplicates every ancestor at every level.
func stripChainEntries(parents []*admissionv1.DelegationCert) []*admissionv1.DelegationCert {
	if len(parents) == 0 {
		return nil
	}
	out := make([]*admissionv1.DelegationCert, len(parents))
	for i, p := range parents {
		out[i] = &admissionv1.DelegationCert{
			Claims:    p.GetClaims(),
			Signature: p.GetSignature(),
		}
	}
	return out
}

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

type SpecBody interface {
	proto.Message
}

func HashSpecBody(body SpecBody) ([]byte, error) {
	if body == nil {
		return nil, errors.New("spec body is nil")
	}
	b, err := signaturePayload(body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}

func IssueSpecAuth(
	publisherPriv ed25519.PrivateKey,
	publisher *admissionv1.DelegationCert,
	resource *admissionv1.ResourceID,
	body SpecBody,
	policy *admissionv1.Predicate,
	deleted bool,
) (*admissionv1.SpecAuth, error) {
	if err := protovalidate.Validate(publisher); err != nil {
		return nil, fmt.Errorf("publisher cert invalid: %w", err)
	}
	publisherPub := publisherPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if !bytes.Equal(publisher.GetClaims().GetSubjectPub(), publisherPub) {
		return nil, errors.New("publisher key does not match cert subject")
	}
	bodyHash, err := HashSpecBody(body)
	if err != nil {
		return nil, err
	}
	specAuth := &admissionv1.SpecAuth{
		Resource:  resource,
		Policy:    policy,
		BodyHash:  bodyHash,
		Publisher: publisher,
		Deleted:   deleted,
	}
	msg, err := specAuthPayload(specAuth)
	if err != nil {
		return nil, err
	}
	sig, err := signPayload(publisherPriv, msg, sigContextSpecAuth)
	if err != nil {
		return nil, err
	}
	specAuth.Signature = sig
	if err := protovalidate.Validate(specAuth); err != nil {
		return nil, fmt.Errorf("spec auth invalid: %w", err)
	}
	return specAuth, nil
}

func VerifySpecAuth(specAuth *admissionv1.SpecAuth, body SpecBody, rootPub []byte, now time.Time) error {
	if err := protovalidate.Validate(specAuth); err != nil {
		return fmt.Errorf("spec auth invalid: %w", err)
	}
	bodyHash, err := HashSpecBody(body)
	if err != nil {
		return err
	}
	if !bytes.Equal(bodyHash, specAuth.GetBodyHash()) {
		return errors.New("spec auth body hash mismatch")
	}
	publisher := specAuth.GetPublisher()
	if err := VerifyDelegationCert(publisher, rootPub, now, nil); err != nil {
		return fmt.Errorf("publisher cert invalid: %w", err)
	}
	msg, err := specAuthPayload(specAuth)
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(publisher.GetClaims().GetSubjectPub()), msg, specAuth.GetSignature(), sigContextSpecAuth); err != nil {
		return errors.New("spec auth signature invalid")
	}
	return nil
}

// IssueBlobWrapping seals dek's wrapping into a signed gossip payload.
// wrapperPriv must match wrapper.subject_pub. The signature scope is
// pollen.blobwrapping.v1 so wrappings cannot be replayed under any
// other signature context.
func IssueBlobWrapping(
	wrapperPriv ed25519.PrivateKey,
	wrapper *admissionv1.DelegationCert,
	blobHash, recipientPub, wrappedDEK []byte,
) (*statev1.BlobWrappingChange, error) {
	if err := protovalidate.Validate(wrapper); err != nil {
		return nil, fmt.Errorf("wrapper cert invalid: %w", err)
	}
	wrapperPub := wrapperPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if !bytes.Equal(wrapper.GetClaims().GetSubjectPub(), wrapperPub) {
		return nil, errors.New("wrapper key does not match cert subject")
	}
	wrapping := &statev1.BlobWrappingChange{
		BlobHash:        blobHash,
		RecipientPubkey: recipientPub,
		WrappedDek:      wrappedDEK,
		Wrapper:         wrapper,
	}
	msg, err := blobWrappingPayload(wrapping)
	if err != nil {
		return nil, err
	}
	sig, err := signPayload(wrapperPriv, msg, sigContextBlobWrapping)
	if err != nil {
		return nil, err
	}
	wrapping.Signature = sig
	if err := protovalidate.Validate(wrapping); err != nil {
		return nil, fmt.Errorf("blob wrapping invalid: %w", err)
	}
	return wrapping, nil
}

// VerifyBlobWrapping checks the wrapper's cert chain against rootPub
// and the signature against the wrapper's pubkey. Tampered or
// expired-chain wrappings fail closed.
func VerifyBlobWrapping(wrapping *statev1.BlobWrappingChange, rootPub []byte, now time.Time) error {
	if err := protovalidate.Validate(wrapping); err != nil {
		return fmt.Errorf("blob wrapping invalid: %w", err)
	}
	wrapper := wrapping.GetWrapper()
	if err := VerifyDelegationCert(wrapper, rootPub, now, nil); err != nil {
		return fmt.Errorf("wrapper cert invalid: %w", err)
	}
	msg, err := blobWrappingPayload(wrapping)
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(wrapper.GetClaims().GetSubjectPub()), msg, wrapping.GetSignature(), sigContextBlobWrapping); err != nil {
		return errors.New("blob wrapping signature invalid")
	}
	return nil
}

func blobWrappingPayload(w *statev1.BlobWrappingChange) ([]byte, error) {
	payload := proto.Clone(w).(*statev1.BlobWrappingChange) //nolint:forcetypeassert
	payload.Signature = nil
	return signaturePayload(payload)
}

func specAuthPayload(specAuth *admissionv1.SpecAuth) ([]byte, error) {
	payload := proto.Clone(specAuth).(*admissionv1.SpecAuth) //nolint:forcetypeassert
	payload.Signature = nil
	return signaturePayload(payload)
}

func verifyDelegationCertChain(cert *admissionv1.DelegationCert) (ed25519.PublicKey, error) {
	if err := protovalidate.Validate(cert); err != nil {
		return nil, fmt.Errorf("delegation cert invalid: %w", err)
	}

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

// ChainSubjectPubs returns every pub authoritatively above (and
// including) the leaf in this cert's delegation lineage. Used to
// evaluate subtree-scoped policies (e.g. "issuer must be an ancestor
// of subject in the delegation tree").
//
// Walks every cert from leaf to root, collecting each cert's
// subject_pub plus the topmost issuer_pub. The topmost issuer is
// the root signing key -- for fully-chained certs it duplicates the
// root cert's subject; for short chains (e.g. a leaf whose Chain is
// empty because the parent cert wasn't bundled) it surfaces root
// authority explicitly so root-issued denies remain authorisable.
func ChainSubjectPubs(cert *admissionv1.DelegationCert) [][]byte {
	if cert == nil {
		return nil
	}
	chain := cert.GetChain()
	out := make([][]byte, 0, len(chain)+2) //nolint:mnd
	out = append(out, cert.GetClaims().GetSubjectPub())
	for _, parent := range chain {
		out = append(out, parent.GetClaims().GetSubjectPub())
	}
	// For short/legacy chains the topmost issuer is the only place
	// root authority surfaces; for full chains it duplicates the last
	// subject and the dedupe is the caller's problem.
	top := cert
	if len(chain) > 0 {
		top = chain[len(chain)-1]
	}
	if issuer := top.GetClaims().GetIssuerPub(); len(issuer) > 0 {
		out = append(out, issuer)
	}
	return out
}

// SignDelegationCertSubject produces a subject-side proof-of-possession
// over the cert's claims, signed with the subject's identity key.
// Required when gossiping a peer's current cert: cert.signature alone
// is by the issuer, which would let any admin re-parent any peer pub.
// Verifying this signature with cert.subject_pub guarantees the peer
// actually owns the published cert.
func SignDelegationCertSubject(cert *admissionv1.DelegationCert, subjectPriv ed25519.PrivateKey) ([]byte, error) {
	if cert == nil {
		return nil, errors.New("cert is nil")
	}
	subjectPub := subjectPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if !bytes.Equal(cert.GetClaims().GetSubjectPub(), subjectPub) {
		return nil, errors.New("signing key does not match cert subject")
	}
	msg, err := signaturePayload(cert.GetClaims())
	if err != nil {
		return nil, err
	}
	return signPayload(subjectPriv, msg, sigContextDelegationSubject)
}

// VerifyDelegationCertSubject checks the subject-side signature against
// cert.subject_pub. Pair with VerifyDelegationCertStructure to fully
// authenticate a gossiped cert event.
func VerifyDelegationCertSubject(cert *admissionv1.DelegationCert, signature []byte) error {
	if cert == nil {
		return errors.New("cert is nil")
	}
	msg, err := signaturePayload(cert.GetClaims())
	if err != nil {
		return err
	}
	return verifyPayload(ed25519.PublicKey(cert.GetClaims().GetSubjectPub()), msg, signature, sigContextDelegationSubject)
}

// VerifyDelegationCertStructure validates the cert chain's signatures,
// issuer/subject linkage, and root anchor without enforcing the
// validity window. Use this when applying a gossiped cert: an expired
// cert is still authoritative for chain-scoped policy decisions
// (e.g. denies issued by a now-expired admin remain effective for
// peers still chained to that admin).
func VerifyDelegationCertStructure(cert *admissionv1.DelegationCert, rootPub []byte) error {
	got, err := verifyDelegationCertChain(cert)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, rootPub) {
		return errors.New("delegation cert chain root mismatch")
	}
	return nil
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

	// The member cert is signed by the issuer and its Chain walks to root,
	// so we verify the token signature against IssuerPub directly and let
	// the chain walk below establish trust and expiry of the issuing line.
	msg, err := signaturePayload(tokenClaims)
	if err != nil {
		return nil, err
	}
	if err := verifyPayload(ed25519.PublicKey(tokenClaims.GetIssuerPub()), msg, token.GetSignature(), sigContextJoinClaims); err != nil {
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

func EnsureIdentityKey(identityDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	privPath := filepath.Join(identityDir, signingKeyName)
	pubPath := filepath.Join(identityDir, signingPubKeyName)

	if priv, pub, err := loadKeyPair(privPath, pubPath, pemTypePriv, pemTypePub); err == nil {
		return priv, pub, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	return generateKeyPair(identityDir, signingKeyName, signingPubKeyName, pemTypePriv, pemTypePub)
}

func ReadIdentityPub(identityDir string) (ed25519.PublicKey, error) {
	raw, err := os.ReadFile(filepath.Join(identityDir, signingPubKeyName))
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
