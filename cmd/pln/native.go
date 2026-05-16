// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/transport"
)

const plnTargetScheme = "pln://"

// clientIdentityTTL is short because the CLI mints a fresh identity
// cert on every connection.
const clientIdentityTTL = 5 * time.Minute

// parsePlnTarget returns (host:port, true) when the target uses the
// pln:// scheme; (_, false) otherwise.
func parsePlnTarget(target string) (string, bool) {
	if !strings.HasPrefix(target, plnTargetScheme) {
		return "", false
	}
	return strings.TrimPrefix(target, plnTargetScheme), true
}

func plnNativeDialer(dir, addr string) func(string, string, *tls.Config) (net.Conn, error) {
	return func(_, _ string, _ *tls.Config) (net.Conn, error) {
		cfg, err := buildPlnClientTLSConfig(dir)
		if err != nil {
			return nil, err
		}
		dialer := &tls.Dialer{Config: cfg}
		return dialer.Dial("tcp", addr)
	}
}

func buildPlnClientTLSConfig(dir string) (*tls.Config, error) {
	identityDir := identity.IdentityPath(dir)
	creds, err := identity.LoadCredentials(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load node credentials: %w", err)
	}
	if creds == nil || creds.Grant() == nil {
		return nil, errors.New("no node credentials in this context; run `pln join` first")
	}
	session, err := creds.EnsureFreshSession(time.Now(), clientIdentityTTL, clientIdentityTTL/2) //nolint:mnd
	if err != nil {
		return nil, fmt.Errorf("mint session: %w", err)
	}
	priv, _, err := identity.EnsureIdentityKey(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load identity key: %w", err)
	}
	clientCert, err := transport.GenerateIdentityCert(priv, session, clientIdentityTTL)
	if err != nil {
		return nil, fmt.Errorf("generate client identity cert: %w", err)
	}
	rootPub := creds.RootPub()
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{clientCert},
		// pln:// targets are not DNS-validated by Go's verifier; the
		// pollen grant chain replaces SAN-based hostname checks.
		// VerifyPeerCertificate below performs full chain plus
		// leaf-key-binding verification.
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{"h2"},
		// Client side has no cluster denylist; nil skips that check. The
		// server's grant chain + horizon are still enforced.
		VerifyPeerCertificate: transport.VerifyDelegatedCounterparty(rootPub, nil),
	}, nil
}
