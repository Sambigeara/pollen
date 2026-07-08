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
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

var ErrIdentityMismatch = errors.New("peer identity mismatch")

const (
	certSerialBits = 128

	alpnMesh   = "pollen/1"
	alpnInvite = "pollen-invite/1"
)

var oidPollenSession = asn1.ObjectIdentifier{2, 25, 37271, 6445, 64343, 17344, 33689, 44400, 19083, 581, 1, 1}

func marshalSessionExtension(session *identityv1.Session) (pkix.Extension, error) {
	raw, err := session.MarshalVT()
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("marshal session: %w", err)
	}
	val, err := asn1.Marshal(raw)
	if err != nil {
		return pkix.Extension{}, fmt.Errorf("asn1 wrap session: %w", err)
	}
	return pkix.Extension{
		Id:    oidPollenSession,
		Value: val,
	}, nil
}

// ParseSessionExtension extracts the pollen Session embedded as an
// ASN.1 extension in an x509 cert. Returns (nil, nil) if the extension
// is absent (e.g. the bare invite cert).
func ParseSessionExtension(certDER []byte) (*identityv1.Session, error) {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("parse x509 certificate: %w", err)
	}

	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidPollenSession) {
			var raw []byte
			if _, err := asn1.Unmarshal(ext.Value, &raw); err != nil {
				return nil, fmt.Errorf("asn1 unwrap session: %w", err)
			}
			s := &identityv1.Session{}
			if err := s.UnmarshalVT(raw); err != nil {
				return nil, fmt.Errorf("unmarshal session: %w", err)
			}
			return s, nil
		}
	}

	return nil, nil
}

// GenerateIdentityCert mints an ephemeral x509 leaf bound to signPriv's
// ed25519 key, embedding session as the pollen authority extension. A
// nil session yields a bare cert (the invite path, identity only).
func GenerateIdentityCert(signPriv ed25519.PrivateKey, session *identityv1.Session, validity time.Duration) (tls.Certificate, error) {
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

	if session != nil {
		ext, err := marshalSessionExtension(session)
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

func sessionFromConn(qc *quic.Conn) *identityv1.Session {
	tlsState := qc.ConnectionState().TLS
	if len(tlsState.PeerCertificates) == 0 {
		return nil
	}
	s, err := ParseSessionExtension(tlsState.PeerCertificates[0].Raw)
	if err != nil || s == nil {
		return nil
	}
	return s
}

type verifyMeshPeerOpts struct {
	expectedPeer *types.PeerKey
	denied       identity.DenyChecker
	rootPub      []byte
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

// verifyMeshPeerCert authenticates a mesh QUIC counterparty: the leaf
// ed25519 key must match the embedded Session's grant subject, the
// grant must chain to root and be within its horizon and not denied,
// and the short session window must be current. A peer self-mints a
// fresh session locally, so there is no renewal grace: a stale session
// is a peer that failed to re-mint.
func verifyMeshPeerCert(opts verifyMeshPeerOpts) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		peerKey, err := verifyPeerIdentity(rawCerts, opts.expectedPeer)
		if err != nil {
			return err
		}

		session, err := ParseSessionExtension(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse session extension: %w", err)
		}
		if session == nil {
			return errors.New("peer certificate missing session extension")
		}

		if _, err := identity.VerifySession(session, opts.rootPub, time.Now(), peerKey.Bytes(), opts.denied); err != nil {
			return fmt.Errorf("mesh peer session rejected: %w", err)
		}
		return nil
	}
}

func verifyIdentityOnly(expectedPeer *types.PeerKey) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		_, err := verifyPeerIdentity(rawCerts, expectedPeer)
		return err
	}
}

// VerifyDelegatedCounterparty builds a TLS VerifyPeerCertificate
// callback for both the wire-mode mTLS server (peer = client) and
// dialer (peer = server). The peer must present a single x509 leaf
// bound to an ed25519 key whose embedded Session's grant chains to
// rootPub. Use this when you have a public host string (not a known
// peer key) and admit any caller whose authority chains to root.
func VerifyDelegatedCounterparty(rootPub []byte, denied identity.DenyChecker) func([][]byte, [][]*x509.Certificate) error {
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
		session, err := ParseSessionExtension(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse session extension: %w", err)
		}
		if session == nil {
			return errors.New("transport: peer certificate missing session extension")
		}
		if _, err := identity.VerifySession(session, rootPub, time.Now(), leafPub, denied); err != nil {
			return fmt.Errorf("transport: peer session rejected: %w", err)
		}
		return nil
	}
}

type serverTLSParams struct {
	meshCertPtr   *atomic.Pointer[tls.Certificate]
	denied        identity.DenyChecker
	inviteCert    tls.Certificate
	rootPub       []byte
	inviteEnabled bool
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
			rootPub: p.rootPub,
			denied:  p.denied,
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

func newExpectedPeerTLSConfig(certPtr *atomic.Pointer[tls.Certificate], expectedPeer types.PeerKey, rootPub []byte, denied identity.DenyChecker) *tls.Config {
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
			rootPub:      rootPub,
			expectedPeer: &expectedPeer,
			denied:       denied,
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
