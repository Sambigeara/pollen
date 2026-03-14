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
	"sync/atomic"
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

	alpnMesh   = "pollen/1"
	alpnInvite = "pollen-invite/1"
)

// UUID-derived private OID for pollen delegation cert extension.
// UUID: 9197192d-fb57-43c0-8399-ad704a8b0245.
var oidPollenDelegationCert = asn1.ObjectIdentifier{2, 25, 37271, 6445, 64343, 17344, 33689, 44400, 19083, 581, 1, 1}

func marshalDelegationExtension(cert *admissionv1.DelegationCert) (pkix.Extension, error) {
	raw, err := cert.MarshalVT()
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("marshal delegation cert: %w", err)
	}
	val, err := asn1.Marshal(raw)
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("asn1 wrap delegation cert: %w", err)
	}
	return pkix.Extension{
		Id:    oidPollenDelegationCert,
		Value: val,
	}, nil
}

func parseDelegationExtension(certDER []byte) (*admissionv1.DelegationCert, error) {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("parse x509 certificate: %w", err)
	}

	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidPollenDelegationCert) {
			var raw []byte
			if _, err := asn1.Unmarshal(ext.Value, &raw); err != nil {
				return nil, fmt.Errorf("asn1 unwrap delegation cert: %w", err)
			}
			dc := &admissionv1.DelegationCert{}
			if err := dc.UnmarshalVT(raw); err != nil {
				return nil, fmt.Errorf("unmarshal delegation cert: %w", err)
			}
			return dc, nil
		}
	}

	return nil, nil
}

func GenerateIdentityCert(signPriv ed25519.PrivateKey, delegationCert *admissionv1.DelegationCert, validity time.Duration) (tls.Certificate, error) {
	pub := signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

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

	if delegationCert != nil {
		ext, err := marshalDelegationExtension(delegationCert)
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

func delegationCertFromConn(qc *quic.Conn) *admissionv1.DelegationCert {
	tlsState := qc.ConnectionState().TLS
	if len(tlsState.PeerCertificates) == 0 {
		return nil
	}
	dc, err := parseDelegationExtension(tlsState.PeerCertificates[0].Raw)
	if err != nil || dc == nil {
		return nil
	}
	return dc
}

type verifyMeshPeerOpts struct {
	trustBundle     *admissionv1.TrustBundle
	expectedPeer    *types.PeerKey
	reconnectWindow time.Duration
}

func verifyPeerIdentity(rawCerts [][]byte, expectedPeer *types.PeerKey) (types.PeerKey, error) {
	if len(rawCerts) == 0 {
		return types.PeerKey{}, errors.New("no peer certificate presented")
	}

	peerKey, err := peerKeyFromRawCert(rawCerts[0])
	if err != nil {
		return types.PeerKey{}, err
	}

	if expectedPeer != nil && peerKey != *expectedPeer {
		return types.PeerKey{}, fmt.Errorf("%w: expected %s got %s", ErrIdentityMismatch, expectedPeer.Short(), peerKey.Short())
	}

	return peerKey, nil
}

func verifyMeshPeerCert(opts verifyMeshPeerOpts) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		peerKey, err := verifyPeerIdentity(rawCerts, opts.expectedPeer)
		if err != nil {
			return err
		}

		dc, err := parseDelegationExtension(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse delegation extension: %w", err)
		}
		if dc == nil {
			return errors.New("peer certificate missing delegation extension")
		}

		now := time.Now()
		if err := auth.VerifyDelegationCert(dc, opts.trustBundle, now, peerKey.Bytes()); err != nil {
			return tryReconnectWindow(dc, opts, now, peerKey, err)
		}

		return nil
	}
}

// tryReconnectWindow checks whether an expired-cert error can be recovered
// because the cert is crypto-valid and within the reconnect grace period.
func tryReconnectWindow(dc *admissionv1.DelegationCert, opts verifyMeshPeerOpts, now time.Time, peerKey types.PeerKey, origErr error) error {
	if opts.reconnectWindow <= 0 || !errors.Is(origErr, auth.ErrCertExpired) {
		return origErr
	}
	if err := auth.VerifyDelegationCertChain(dc, opts.trustBundle, peerKey.Bytes()); err != nil {
		return err
	}
	if !auth.IsCertWithinReconnectWindow(dc, now, opts.reconnectWindow) {
		return origErr
	}
	return nil
}

func verifyIdentityOnly(expectedPeer *types.PeerKey) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		_, err := verifyPeerIdentity(rawCerts, expectedPeer)
		return err
	}
}

type serverTLSParams struct {
	meshCertPtr     *atomic.Pointer[tls.Certificate]
	inviteCert      tls.Certificate
	trustBundle     *admissionv1.TrustBundle
	reconnectWindow time.Duration
	inviteEnabled   bool
}

func newServerTLSConfig(p serverTLSParams) *tls.Config {
	meshConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return p.meshCertPtr.Load(), nil
		},
		ClientAuth: tls.RequireAnyClientCert,
		NextProtos: []string{alpnMesh},
		VerifyPeerCertificate: verifyMeshPeerCert(verifyMeshPeerOpts{
			trustBundle:     p.trustBundle,
			reconnectWindow: p.reconnectWindow,
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

func newExpectedPeerTLSConfig(certPtr *atomic.Pointer[tls.Certificate], expectedPeer types.PeerKey, trustBundle *admissionv1.TrustBundle, reconnectWindow time.Duration) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return certPtr.Load(), nil
		},
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return certPtr.Load(), nil
		},
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{alpnMesh},
		VerifyPeerCertificate: verifyMeshPeerCert(verifyMeshPeerOpts{
			trustBundle:     trustBundle,
			expectedPeer:    &expectedPeer,
			reconnectWindow: reconnectWindow,
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
