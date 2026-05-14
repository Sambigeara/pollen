// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/auth"
)

func TestInjectCallerPopulatesContext(t *testing.T) {
	creds := mustNodeCredentials(t)
	srv := New(nil, nil, nil, nil, nil, nil, WithCredentials(creds))

	ctx := srv.injectCaller(context.Background())
	caller, ok := auth.RPCCallerFromContext(ctx)
	require.True(t, ok, "interceptor must inject a caller")
	require.Same(t, creds.Cert(), caller.Cert(),
		"caller cert must match the daemon's own when no inbound peer cert is presented")
}

func TestInjectCallerNoCredsLeavesContextEmpty(t *testing.T) {
	srv := New(nil, nil, nil, nil, nil, nil)

	ctx := srv.injectCaller(context.Background())
	_, ok := auth.RPCCallerFromContext(ctx)
	require.False(t, ok, "interceptor must skip injection when daemon has no creds")
}

func TestInjectCallerRefusesDaemonFallbackOverTLS(t *testing.T) {
	// A TLS peer without a delegation extension must NOT inherit the
	// daemon's privileges. The local-credential fallback exists for
	// unix-socket callers only — leaking it over TLS would erase the
	// wire-mode security boundary if a future credentials.Creds
	// implementation stopped populating AuthInfo with the extension.
	creds := mustNodeCredentials(t)
	srv := New(nil, nil, nil, nil, nil, nil, WithCredentials(creds))
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{}},
		Addr:     &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 7443},
	})
	out := srv.injectCaller(ctx)
	_, ok := auth.RPCCallerFromContext(out)
	require.False(t, ok, "TLS peer without delegation extension must not inherit daemon identity")
}

func TestInjectCallerFallsBackOverUnixSocket(t *testing.T) {
	// Unix-socket peer is the daemon-self path; the fallback IS the
	// intended behaviour because the only caller on this transport is
	// a local process the operator already trusts.
	creds := mustNodeCredentials(t)
	srv := New(nil, nil, nil, nil, nil, nil, WithCredentials(creds))
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.UnixAddr{Name: "/var/run/pln.sock", Net: "unix"},
	})
	out := srv.injectCaller(ctx)
	caller, ok := auth.RPCCallerFromContext(out)
	require.True(t, ok)
	require.Same(t, creds.Cert(), caller.Cert())
}

func mustNodeCredentials(t *testing.T) *auth.NodeCredentials {
	t.Helper()
	dir := t.TempDir()
	nodePub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	creds, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, time.Now(), 24*time.Hour)
	require.NoError(t, err)
	return creds
}
