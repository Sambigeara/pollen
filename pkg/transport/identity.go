// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

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

var ErrIdentityMismatch = errors.New("peer identity mismatch")

const (
	certSerialBits = 128

	alpnMesh   = "pollen/1"
	alpnInvite = "pollen-invite/1"
)

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

// ParseDelegationExtension extracts the pollen DelegationCert embedded
// as an ASN.1 extension in an x509 cert. Returns (nil, nil) if the
// extension is absent.
func ParseDelegationExtension(certDER []byte) (*admissionv1.DelegationCert, error) {
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

func delegationCertFromConn(qc *quic.Conn) *admissionv1.DelegationCert {
	tlsState := qc.ConnectionState().TLS
	if len(tlsState.PeerCertificates) == 0 {
		return nil
	}
	dc, err := ParseDelegationExtension(tlsState.PeerCertificates[0].Raw)
	if err != nil || dc == nil {
		return nil
	}
	return dc
}

type verifyMeshPeerOpts struct {
	expectedPeer    *types.PeerKey
	denied          auth.DenyChecker
	rootPub         []byte
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

		dc, err := ParseDelegationExtension(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse delegation extension: %w", err)
		}
		if dc == nil {
			return errors.New("peer certificate missing delegation extension")
		}

		now := time.Now()
		chk := auth.CheckCert(dc, opts.rootPub, now, peerKey.Bytes(), opts.denied)
		return admitMeshCert(chk, opts.reconnectWindow, now)
	}
}

// admitMeshCert maps a mesh peer cert check to a handshake decision. A
// cert past its renewable window (not_after) is admitted within
// reconnectWindow so the membership service can drive renewal over the
// reconnected session. The access_deadline ceiling, denial, and chain
// failures are hard stops the reconnect window cannot bypass.
func admitMeshCert(chk auth.CertCheck, reconnectWindow time.Duration, now time.Time) error {
	if chk.Status.CanAuthenticate() {
		return nil
	}
	// NeedsRenewal, or a legacy no-ceiling cert past not_after, may
	// reconnect within the grace window; the membership service renews
	// over the new session. A cert past access_deadline is Expired with
	// a non-zero deadline and does not qualify.
	pastNotAfterRenewable := chk.Status == auth.CertStatusNeedsRenewal ||
		(chk.Status == auth.CertStatusExpired && chk.AccessDeadline.IsZero())
	if pastNotAfterRenewable && reconnectWindow > 0 && now.Before(chk.NotAfter.Add(reconnectWindow)) {
		return nil
	}
	return fmt.Errorf("mesh peer cert rejected (%s): %s", chk.Status, chk.Reason)
}

func verifyIdentityOnly(expectedPeer *types.PeerKey) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		_, err := verifyPeerIdentity(rawCerts, expectedPeer)
		return err
	}
}

// VerifyDelegatedCounterparty builds a TLS VerifyPeerCertificate callback
// suitable for both the wire-mode mTLS server (peer = client) and dialer
// (peer = server). It verifies that the peer presented a single x509
// leaf bound to an ed25519 public key, whose DelegationCert extension
// chains back to rootPub. Use this when you have a public host string
// (not a known peer key) and need to admit any caller whose authority
// chains to the cluster root.
func VerifyDelegatedCounterparty(rootPub []byte, denied auth.DenyChecker) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("transport: no peer certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse peer leaf: %w", err)
		}
		leafPub, ok := leaf.PublicKey.(ed25519.PublicKey)
		if !ok {
			return errors.New("transport: peer leaf must use ed25519")
		}
		dc, err := ParseDelegationExtension(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse delegation extension: %w", err)
		}
		if dc == nil {
			return errors.New("transport: peer certificate missing delegation extension")
		}
		// Admit OK and NeedsRenewal: a cert past not_after but within
		// access_deadline must be able to reach the RenewCert RPC. The
		// control service's interceptor restricts NeedsRenewal callers
		// to RenewCert only; Expired/Revoked/invalid are rejected here.
		chk := auth.CheckCert(dc, rootPub, time.Now(), leafPub, denied)
		if !chk.Status.CanRenew() {
			return fmt.Errorf("transport: peer cert rejected (%s): %s", chk.Status, chk.Reason)
		}
		return nil
	}
}

type serverTLSParams struct {
	meshCertPtr     *atomic.Pointer[tls.Certificate]
	denied          auth.DenyChecker
	inviteCert      tls.Certificate
	rootPub         []byte
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
			rootPub:         p.rootPub,
			reconnectWindow: p.reconnectWindow,
			denied:          p.denied,
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

func newExpectedPeerTLSConfig(certPtr *atomic.Pointer[tls.Certificate], expectedPeer types.PeerKey, rootPub []byte, reconnectWindow time.Duration, denied auth.DenyChecker) *tls.Config {
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
			rootPub:         rootPub,
			expectedPeer:    &expectedPeer,
			reconnectWindow: reconnectWindow,
			denied:          denied,
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
