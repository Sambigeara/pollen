// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/tls"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/transport"
)

// newControlTLSConfig builds a TLS config for the control RPC listener.
// Inbound clients must present a cert whose Session extension chains
// back to the configured root AND whose TLS leaf public key matches the
// Session's grant subject — without that binding, anyone who has seen
// the victim's gossiped Session can mint a new leaf and impersonate
// them. The verified Session is later retrieved from the gRPC peer
// context by callerGrantFromContext.
func newControlTLSConfig(serverCert tls.Certificate, rootPub []byte, denied identity.DenyChecker) *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{serverCert},
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{"h2"},
		VerifyPeerCertificate: transport.VerifyDelegatedCounterparty(rootPub, denied),
	}
}

// callerGrantFromContext returns the verified caller Grant if the
// inbound gRPC session carried a mTLS peer cert with our pollen Session
// extension. Returns nil for unix-socket and SSH-bridge transports.
func callerGrantFromContext(ctx context.Context) *identityv1.Grant {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil
	}
	leaf := tlsInfo.State.PeerCertificates[0]
	session, err := transport.ParseSessionExtension(leaf.Raw)
	if err != nil || session == nil {
		return nil
	}
	return session.GetClaims().GetGrant()
}
