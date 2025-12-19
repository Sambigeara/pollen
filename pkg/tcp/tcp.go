package tcp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"time"
)

const (
	allowedClockSkewInterval = 2 * time.Minute
)

// GenerateEphemeralCert creates a self-signed certificate for the TLS handshake.
// It returns the tls.Certificate object, and the signature of those bytes created
// by the node's long-term signing key.
func GenerateEphemeralCert(signPriv ed25519.PrivateKey) (tls.Certificate, []byte, error) {
	ephemPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)) //nolint:mnd
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	now := time.Now().UTC()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "pollen-ephemeral-peer"},
		NotBefore:    now.Add(-1 * time.Minute), //nolint:mnd
		NotAfter:     now.Add(5 * time.Minute),  //nolint:mnd

		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Self-signed: parent = template; signed by the ephemeral ECDSA key
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &ephemPriv.PublicKey, ephemPriv)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	tlsCert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  ephemPriv,
		Leaf:        leaf,
	}

	// Sign the raw certificate bytes with our long-term Ed25519 signing key.
	// This is the attestation that proves we generated this ephemeral cert.
	sig := ed25519.Sign(signPriv, certDER)

	return tlsCert, sig, nil
}

// VerifyPeerAttestation checks that the peer's certificate is validly signed and timely.
// This is used by both sides of the handshake.
func VerifyPeerAttestation(peerSigningPub ed25519.PublicKey, certDER, sig []byte) (*x509.Certificate, error) {
	if !ed25519.Verify(peerSigningPub, certDER, sig) {
		return nil, errors.New("peer certificate signature is invalid")
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, errors.New("failed to parse peer certificate")
	}
	now := time.Now().UTC()

	// Allow a small clock skew.
	if now.Before(cert.NotBefore.Add(-allowedClockSkewInterval)) || now.After(cert.NotAfter.Add(allowedClockSkewInterval)) {
		return nil, errors.New("peer certificate is expired or not yet valid")
	}

	return cert, nil
}

// NewPinnedTLSConfig creates a TLS config for either a client or a server that
// authenticates the peer by pinning their exact certificate.
func NewPinnedTLSConfig(isServer bool, ourCert tls.Certificate, peerCert *x509.Certificate) *tls.Config {
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{ourCert},
		// This callback is the core of our security model. It replaces the standard CA chain verification.
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("no peer certificate presented")
			}
			// Pin the exact certificate we received and verified over the secure Noise channel.
			if !bytes.Equal(rawCerts[0], peerCert.Raw) {
				return errors.New("peer certificate mismatch: pinning failed")
			}
			return nil
		},
	}

	if isServer {
		// Server requires the client to present a cert, which we'll verify in the callback.
		cfg.ClientAuth = tls.RequireAnyClientCert
	} else {
		// Client skips standard verification (hostname, CA) because we do it ourselves.
		cfg.InsecureSkipVerify = true
		// It's good practice to set ServerName to the expected CommonName from the cert.
		cfg.ServerName = peerCert.Subject.CommonName
	}

	return cfg
}
