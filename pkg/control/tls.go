// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/transport"
)

// newControlTLSConfig builds a TLS config for the control RPC listener.
// Inbound clients must present a cert whose DelegationCert extension
// chains back to the configured root. The verified DelegationCert is
// later retrieved from the gRPC peer context by callerCertFromContext.
func newControlTLSConfig(serverCert tls.Certificate, rootPub []byte) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{"h2"},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("control tls: no client certificate")
			}
			dc, err := transport.ParseDelegationExtension(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse delegation extension: %w", err)
			}
			if dc == nil {
				return errors.New("control tls: peer certificate missing delegation extension")
			}
			if err := auth.VerifyDelegationCert(dc, rootPub, time.Now(), nil); err != nil {
				return fmt.Errorf("verify delegation cert: %w", err)
			}
			return nil
		},
	}
}

// callerCertFromContext returns the verified caller DelegationCert if
// the inbound gRPC session carried a mTLS peer cert with our delegation
// extension. Returns nil for unix-socket and SSH-bridge transports.
func callerCertFromContext(ctx context.Context) *admissionv1.DelegationCert {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil
	}
	leaf := tlsInfo.State.PeerCertificates[0]
	dc, err := transport.ParseDelegationExtension(leaf.Raw)
	if err != nil || dc == nil {
		return nil
	}
	return dc
}
