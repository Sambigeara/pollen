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

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
)

type emptyState struct{}

func (emptyState) Snapshot() state.Snapshot {
	return state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{}}
}

type emptyPlacement struct{ PlacementControl }

func (emptyPlacement) Status() []placement.WorkloadSummary { return nil }

type emptyTunneling struct{ TunnelingControl }

func (emptyTunneling) ListConnections() []tunneling.ConnectionInfo { return nil }

type emptyStatic struct{ StaticControl }

func (emptyStatic) StaticBlobs() map[string]struct{} { return nil }

func TestStartTLS_PeerCertFlowsToCaller(t *testing.T) {
	// Set up a root admin who hosts the control listener.
	dir := t.TempDir()
	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serverCreds, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, time.Now(), 24*time.Hour)
	require.NoError(t, err)
	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err)

	// Mint a separate publisher cert and key for the inbound client.
	clientPub, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	clientCert, err := signer.IssueMemberCert(clientPub, auth.PublisherCapabilities(), time.Now(), time.Now().Add(time.Hour), time.Time{})
	require.NoError(t, err)
	clientTLSCert, err := transport.GenerateIdentityCert(clientPriv, clientCert, time.Hour)
	require.NoError(t, err)

	srv := New(
		nil,
		emptyPlacement{},
		emptyTunneling{},
		nil,
		emptyStatic{},
		emptyState{},
		WithCredentials(serverCreds),
		WithSignPriv(nodePriv),
	)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	serverCert, err := transport.GenerateIdentityCert(nodePriv, serverCreds.Cert(), time.Hour)
	require.NoError(t, err)
	tlsCfg := newControlTLSConfig(serverCert, serverCreds.RootPub())
	tlsListener := tls.NewListener(listener, tlsCfg)
	go func() { _ = srv.Serve(tlsListener) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = tlsListener.Close()
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates:       []tls.Certificate{clientTLSCert},
		InsecureSkipVerify: true, //nolint:gosec
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{"h2"},
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := controlv1.NewControlServiceClient(conn)
	resp, err := client.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	// Client is a publisher, not an admin: GetStatus is scoped to their
	// own publications. With no specs in the harness, the response
	// contains no workloads/sites/blobs but the call succeeds end-to-end.
	require.Empty(t, resp.GetWorkloads())
	require.Empty(t, resp.GetSites())
	require.Empty(t, resp.GetBlobs())
}
