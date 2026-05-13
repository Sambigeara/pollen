// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestParsePlnTarget(t *testing.T) {
	cases := []struct {
		in     string
		wantOk bool
		wantTo string
	}{
		{"pln://edge.example.com:7443", true, "edge.example.com:7443"},
		{"pln://127.0.0.1:0", true, "127.0.0.1:0"},
		{"user@host", false, ""},
		{"", false, ""},
		{"https://example.com", false, ""},
	}
	for _, tc := range cases {
		got, ok := parsePlnTarget(tc.in)
		require.Equal(t, tc.wantOk, ok, "ok flag for %q", tc.in)
		require.Equal(t, tc.wantTo, got, "addr for %q", tc.in)
	}
}

func TestPlnNativeDial_GetStatusRoundTrip(t *testing.T) {
	// Server side: stand up a control TLS listener with admin creds.
	serverDir := t.TempDir()
	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serverCreds, err := auth.EnsureLocalRootCredentials(serverDir, nodePub, nil, time.Now(), 24*time.Hour)
	require.NoError(t, err)
	signer, err := auth.NewDelegationSigner(serverDir, nodePriv)
	require.NoError(t, err)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	srv := control.New(
		nil,
		stubPlacement{},
		stubTunneling{},
		nil,
		stubStatic{},
		stubState{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{}}},
		control.WithCredentials(serverCreds),
		control.WithSignPriv(nodePriv),
	)
	go func() { _ = srv.ServeTLS(listener) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = listener.Close()
	})

	// Client side: a separate publisher cert in a fresh context dir, dialed
	// via the same plumbing withEnv uses.
	clientDir := t.TempDir()
	_, clientPub, err := auth.EnsureIdentityKey(clientDir)
	require.NoError(t, err)
	clientCert, err := signer.IssueMemberCert(clientPub, auth.PublisherCapabilities(), time.Now(), time.Now().Add(time.Hour), time.Time{})
	require.NoError(t, err)
	clientCreds := auth.NewNodeCredentials(serverCreds.RootPub(), clientCert)
	require.NoError(t, auth.SaveNodeCredentials(clientDir, clientCreds))

	httpClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS:   dialTLSFunc(clientDir, "pln://"+addr),
		},
	}
	client := controlv1connect.NewControlServiceClient(httpClient, "https://"+addr, connect.WithGRPC())

	resp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	require.NoError(t, err)
	require.Empty(t, resp.Msg.GetWorkloads(), "publisher with no specs sees an empty scoped view")
}

// An attacker who has seen a gossiped DelegationCert can mint a fresh
// leaf bound to their own key, attach the victim's DC as the pollen
// extension, and connect. The TLS handshake must reject this because
// the leaf's pubkey doesn't match the DC subject.
func TestPlnNativeDial_RejectsImpersonatedLeaf(t *testing.T) {
	serverDir := t.TempDir()
	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serverCreds, err := auth.EnsureLocalRootCredentials(serverDir, nodePub, nil, time.Now(), 24*time.Hour)
	require.NoError(t, err)
	signer, err := auth.NewDelegationSigner(serverDir, nodePriv)
	require.NoError(t, err)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	srv := control.New(
		nil,
		stubPlacement{},
		stubTunneling{},
		nil,
		stubStatic{},
		stubState{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{}}},
		control.WithCredentials(serverCreds),
		control.WithSignPriv(nodePriv),
	)
	go func() { _ = srv.ServeTLS(listener) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = listener.Close()
	})

	// Victim has a legitimate publisher cert.
	victimPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	victimCert, err := signer.IssueMemberCert(victimPub, auth.PublisherCapabilities(), time.Now(), time.Now().Add(time.Hour), time.Time{})
	require.NoError(t, err)

	// Attacker generates their own keypair and mints a leaf carrying
	// victim's DelegationCert. Without leaf-key binding the server would
	// accept this as the victim.
	_, attackerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	attackerLeaf, err := transport.GenerateIdentityCert(attackerPriv, victimCert, time.Minute)
	require.NoError(t, err)

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{attackerLeaf},
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{"h2"},
	}
	dialer := &tls.Dialer{Config: cfg}
	conn, dialErr := dialer.Dial("tcp", addr)
	if conn != nil {
		t.Cleanup(func() { _ = conn.Close() })
		// TLS 1.3 may delay certificate verification until the first read.
		_, dialErr = conn.Read(make([]byte, 1))
	}
	require.Error(t, dialErr, "server must reject leaf whose pubkey does not match the DelegationCert subject")
}

// Stub deps for the harness — embedded interfaces means methods we
// don't exercise nil-deref instead of bloating this file.

type stubPlacement struct{ control.PlacementControl }

func (stubPlacement) Status() []placement.WorkloadSummary { return nil }

type stubTunneling struct{ control.TunnelingControl }

func (stubTunneling) ListConnections() []tunneling.ConnectionInfo { return nil }

type stubStatic struct{ control.StaticControl }

func (stubStatic) StaticBlobs() map[string]struct{} { return nil }

type stubState struct{ snap state.Snapshot }

func (s stubState) Snapshot() state.Snapshot { return s.snap }
