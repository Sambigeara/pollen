package mesh

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

// ErrIdentityMismatch is returned when a peer presents a different public key
// than the one we expected (e.g., IP reuse after instance replacement).
var ErrIdentityMismatch = errors.New("peer identity mismatch")

const (
	certSerialBits = 128
	certValidity   = 10 * 365 * 24 * time.Hour

	alpnMesh   = "pollen/1"
	alpnInvite = "pollen-invite/1"
)

// UUID-derived private OID for pollen membership cert extension.
// UUID: 9197192d-fb57-43c0-8399-ad704a8b0245.
var oidPollenMembershipCert = asn1.ObjectIdentifier{2, 25, 37271, 6445, 64343, 17344, 33689, 44400, 19083, 581, 1, 1}

func marshalMembershipExtension(cert *admissionv1.MembershipCert) (pkix.Extension, error) {
	raw, err := cert.MarshalVT()
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("marshal membership cert: %w", err)
	}
	val, err := asn1.Marshal(raw)
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("asn1 wrap membership cert: %w", err)
	}
	return pkix.Extension{
		Id:    oidPollenMembershipCert,
		Value: val,
	}, nil
}

func parseMembershipExtension(certDER []byte) (*admissionv1.MembershipCert, error) {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("parse x509 certificate: %w", err)
	}

	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidPollenMembershipCert) {
			var raw []byte
			if _, err := asn1.Unmarshal(ext.Value, &raw); err != nil {
				return nil, fmt.Errorf("asn1 unwrap membership cert: %w", err)
			}
			mc := &admissionv1.MembershipCert{}
			if err := mc.UnmarshalVT(raw); err != nil {
				return nil, fmt.Errorf("unmarshal membership cert: %w", err)
			}
			return mc, nil
		}
	}

	return nil, nil
}

func generateIdentityCert(signPriv ed25519.PrivateKey, membershipCert *admissionv1.MembershipCert, validity time.Duration) (tls.Certificate, error) {
	pub, ok := signPriv.Public().(ed25519.PublicKey)
	if !ok {
		return tls.Certificate{}, errors.New("identity private key is not ed25519")
	}

	if validity <= 0 {
		validity = certValidity
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), certSerialBits))
	if err != nil {
		return tls.Certificate{}, err
	}

	now := time.Now().UTC()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "pollen-peer"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(validity),

		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	if membershipCert != nil {
		ext, err := marshalMembershipExtension(membershipCert)
		if err != nil {
			return tls.Certificate{}, err
		}
		tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, ext)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, signPriv)
	if err != nil {
		return tls.Certificate{}, err
	}

	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  signPriv,
		Leaf:        leaf,
	}, nil
}

func peerKeyFromRawCert(certDER []byte) (types.PeerKey, error) {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return types.PeerKey{}, errors.New("failed to parse peer certificate")
	}

	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return types.PeerKey{}, errors.New("peer certificate does not contain ed25519 public key")
	}

	return types.PeerKeyFromBytes(pub), nil
}

func peerKeyFromConn(qc *quic.Conn) (types.PeerKey, error) {
	certs := qc.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		return types.PeerKey{}, errors.New("no peer certificate")
	}
	pub, ok := certs[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		return types.PeerKey{}, errors.New("peer cert is not ed25519")
	}
	return types.PeerKeyFromBytes(pub), nil
}

type verifyMeshPeerOpts struct {
	trustBundle      *admissionv1.TrustBundle
	expectedPeer     *types.PeerKey
	isSubjectRevoked func([]byte) bool
}

func verifyMeshPeerCert(opts verifyMeshPeerOpts) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no peer certificate presented")
		}

		peerKey, err := peerKeyFromRawCert(rawCerts[0])
		if err != nil {
			return err
		}

		if opts.expectedPeer != nil && peerKey != *opts.expectedPeer {
			return fmt.Errorf("%w: expected %s got %s", ErrIdentityMismatch, opts.expectedPeer.Short(), peerKey.Short())
		}

		mc, err := parseMembershipExtension(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse membership extension: %w", err)
		}
		if mc == nil {
			return errors.New("peer certificate missing membership extension")
		}

		if err := auth.VerifyMembershipCert(mc, opts.trustBundle, time.Now(), peerKey.Bytes()); err != nil {
			return err
		}

		if opts.isSubjectRevoked != nil && opts.isSubjectRevoked(peerKey.Bytes()) {
			return errors.New("peer certificate revoked")
		}

		return nil
	}
}

func verifyIdentityOnly(expectedPeer *types.PeerKey) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no peer certificate presented")
		}

		peerKey, err := peerKeyFromRawCert(rawCerts[0])
		if err != nil {
			return err
		}

		if expectedPeer != nil && peerKey != *expectedPeer {
			return fmt.Errorf("%w: expected %s got %s", ErrIdentityMismatch, expectedPeer.Short(), peerKey.Short())
		}

		return nil
	}
}

type serverTLSParams struct {
	meshCert         tls.Certificate
	inviteCert       tls.Certificate
	trustBundle      *admissionv1.TrustBundle
	isSubjectRevoked func([]byte) bool
	inviteEnabled    bool
}

func newServerTLSConfig(p serverTLSParams) *tls.Config {
	meshConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{p.meshCert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{alpnMesh},
		VerifyPeerCertificate: verifyMeshPeerCert(verifyMeshPeerOpts{
			trustBundle:      p.trustBundle,
			isSubjectRevoked: p.isSubjectRevoked,
		}),
	}

	inviteConfig := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{p.inviteCert},
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{alpnInvite},
		VerifyPeerCertificate: verifyIdentityOnly(nil),
	}

	nextProtos := []string{alpnMesh}
	if p.inviteEnabled {
		nextProtos = []string{alpnMesh, alpnInvite}
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: nextProtos,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			if p.inviteEnabled && slices.Contains(hello.SupportedProtos, alpnInvite) {
				return inviteConfig, nil
			}
			return meshConfig, nil
		},
	}
}

func newExpectedPeerTLSConfig(ourCert tls.Certificate, expectedPeer types.PeerKey, trustBundle *admissionv1.TrustBundle, isSubjectRevoked func([]byte) bool) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{ourCert},
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{alpnMesh},
		VerifyPeerCertificate: verifyMeshPeerCert(verifyMeshPeerOpts{
			trustBundle:      trustBundle,
			expectedPeer:     &expectedPeer,
			isSubjectRevoked: isSubjectRevoked,
		}),
	}
}

func newInviteDialerTLSConfig(bareCert tls.Certificate, expectedPeer types.PeerKey) *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{bareCert},
		InsecureSkipVerify:    true, //nolint:gosec
		NextProtos:            []string{alpnInvite},
		VerifyPeerCertificate: verifyIdentityOnly(&expectedPeer),
	}
}
