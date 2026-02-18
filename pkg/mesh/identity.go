package mesh

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	certSerialBits = 128
	certValidity   = 10 * 365 * 24 * time.Hour
)

type PeerDirectory interface {
	IdentityPub(peerKey types.PeerKey) (ed25519.PublicKey, bool)
}

func generateIdentityCert(signPriv ed25519.PrivateKey) (tls.Certificate, error) {
	pub, ok := signPriv.Public().(ed25519.PublicKey)
	if !ok {
		return tls.Certificate{}, errors.New("identity private key is not ed25519")
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
		NotAfter:     now.Add(certValidity),

		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
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

func newServerTLSConfig(ourCert tls.Certificate) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{ourCert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{"pollen/1"},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("no peer certificate presented")
			}

			_, err := peerKeyFromRawCert(rawCerts[0])
			return err
		},
	}
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

func newExpectedPeerTLSConfig(ourCert tls.Certificate, expectedPeer types.PeerKey) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{ourCert},
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{"pollen/1"},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("no peer certificate presented")
			}

			peerKey, err := peerKeyFromRawCert(rawCerts[0])
			if err != nil {
				return err
			}

			if peerKey != expectedPeer {
				return fmt.Errorf("peer identity mismatch: expected %s got %s", expectedPeer.Short(), peerKey.Short())
			}

			return nil
		},
	}
}
