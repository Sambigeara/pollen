// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/require"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/internal/testauth"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

type renewState struct{ denied []types.PeerKey }

func (r renewState) Snapshot() state.Snapshot {
	return state.Snapshot{DeniedKeys: r.denied}
}

type renewMembership struct {
	cert *admissionv1.DelegationCert
	err  error
	from *admissionv1.DelegationCert
}

func (m *renewMembership) DenyPeer(types.PeerKey) error { return nil }
func (m *renewMembership) IssueCert(context.Context, types.PeerKey, *admissionv1.Capabilities, bool) (*admissionv1.DelegationCert, error) {
	return nil, nil
}

func (m *renewMembership) RenewCert(currentCert *admissionv1.DelegationCert) (*admissionv1.DelegationCert, error) {
	m.from = currentCert
	return m.cert, m.err
}

// leafCertCtx returns a context carrying a TLS peer whose leaf embeds
// the delegation extension, matching what callerCertFromContext parses
// in production.
func leafCertCtx(t *testing.T, signer *auth.DelegationSigner, notAfter, accessDeadline time.Time) (context.Context, *admissionv1.DelegationCert) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert, err := signer.IssueMemberCert(pub, auth.LeafCapabilities(), notAfter.Add(-2*time.Hour), notAfter, accessDeadline)
	require.NoError(t, err)
	id, err := transport.GenerateIdentityCert(priv, cert, time.Hour)
	require.NoError(t, err)
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{id.Leaf},
		}},
		Addr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 7443},
	})
	return ctx, cert
}

func TestRenewCertHandlerRejectsNonMTLSCaller(t *testing.T) {
	srv := New(&renewMembership{}, nil, nil, nil, nil, renewState{})
	_, err := srv.Service().RenewCert(context.Background(), &controlv1.RenewCertRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestRenewCertHandlerSuccess(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	signer := cluster.Signer(t)
	now := time.Now()

	want, err := signer.IssueMemberCert(mustGenPub(t), auth.LeafCapabilities(), now, now.Add(time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)
	mem := &renewMembership{cert: want}
	srv := New(mem, nil, nil, nil, nil, renewState{})

	ctx, sessionCert := leafCertCtx(t, signer, now.Add(-time.Hour), now.Add(30*24*time.Hour))
	resp, err := srv.Service().RenewCert(ctx, &controlv1.RenewCertRequest{})
	require.NoError(t, err)
	require.Equal(t, want, resp.GetCert())
	require.Equal(t, sessionCert.GetClaims().GetSubjectPub(), mem.from.GetClaims().GetSubjectPub(),
		"membership must renew the session cert's subject, never a request-supplied one")
}

func TestRenewCertHandlerMapsErrors(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	signer := cluster.Signer(t)
	now := time.Now()

	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"denied", membership.ErrSubjectDenied, codes.PermissionDenied},
		{"ceiling passed", membership.ErrAccessDeadlinePassed, codes.FailedPrecondition},
		{"not admin", membership.ErrNotAdmin, codes.FailedPrecondition},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := New(&renewMembership{err: tc.err}, nil, nil, nil, nil, renewState{})
			ctx, _ := leafCertCtx(t, signer, now.Add(-time.Hour), now.Add(30*24*time.Hour))
			_, err := srv.Service().RenewCert(ctx, &controlv1.RenewCertRequest{})
			require.Equal(t, tc.want, status.Code(err))
		})
	}
}

func TestGateWireRenewalRestrictsNeedsRenewalToRenewCert(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	serverCreds := cluster.CredsFor(t, mustGenPub(t))
	signer := cluster.Signer(t)
	srv := New(&renewMembership{}, nil, nil, nil, nil, renewState{}, WithCredentials(serverCreds))
	now := time.Now()

	t.Run("needs-renewal blocked on non-renew RPC", func(t *testing.T) {
		ctx, _ := leafCertCtx(t, signer, now.Add(-time.Hour), now.Add(30*24*time.Hour))
		err := srv.gateWireRenewal(ctx, "/pollen.control.v1.ControlService/GetStatus")
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("needs-renewal allowed on RenewCert", func(t *testing.T) {
		ctx, _ := leafCertCtx(t, signer, now.Add(-time.Hour), now.Add(30*24*time.Hour))
		require.NoError(t, srv.gateWireRenewal(ctx, renewCertFullMethod))
	})

	t.Run("valid cert passes any RPC", func(t *testing.T) {
		ctx, _ := leafCertCtx(t, signer, now.Add(time.Hour), now.Add(30*24*time.Hour))
		require.NoError(t, srv.gateWireRenewal(ctx, "/pollen.control.v1.ControlService/GetStatus"))
	})

	t.Run("non-wire caller passes", func(t *testing.T) {
		require.NoError(t, srv.gateWireRenewal(context.Background(), "/pollen.control.v1.ControlService/GetStatus"))
	})
}

func mustGenPub(t *testing.T) ed25519.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub
}
