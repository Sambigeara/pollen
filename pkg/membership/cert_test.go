// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

// newTestServiceWithCloser is a newTestService variant that also
// returns the fake session closer so a test can assert rotation
// behaviour after a cert push.
func newTestServiceWithCloser(localID types.PeerKey) (*Service, *fakeNetwork, *fakePeerSessionCloser) {
	net := newFakeNetwork()
	st := newFakeClusterState(localID)
	closer := newFakePeerSessionCloser()
	svc := New(localID, &auth.NodeCredentials{}, net, st, Config{
		Log:              zap.S(),
		TracerProvider:   tracenoop.NewTracerProvider(),
		NodeMetrics:      metrics.NewNodeMetrics(metricnoop.NewMeterProvider()),
		GossipInterval:   time.Hour,
		PeerTickInterval: time.Hour,
		Streams:          &fakeStreamOpener{},
		RTT:              &fakeRTTSource{},
		Certs:            newFakeCertManager(),
		PeerAddrs:        &fakePeerAddressSource{},
		SessionCloser:    closer,
		NATDetector:      nat.NewDetector(),
		ReconnectWindow:  time.Hour,
	})
	return svc, net, closer
}

// TestRotateSessionsForNewCertClosesEveryConnectedPeerExceptIssuer pins
// the fix for the grant-doesn't-propagate bug: after a new cert lands,
// every outbound session must be torn down so peers re-handshake with
// the fresh TLS identity. Without this, `peerProps(target)` on the
// issuer side stays pinned to the old cert's attributes indefinitely.
// The issuing peer is excluded because closing its session would race
// the accept-response datagram and fail the admin's PushCert RPC.
func TestRotateSessionsForNewCertClosesEveryConnectedPeerExceptIssuer(t *testing.T) {
	svc, net, closer := newTestServiceWithCloser(peerKey(1))

	issuer := peerKey(2)
	net.setConnectedPeers(issuer, peerKey(3), peerKey(4))
	svc.rotateSessionsForNewCert(issuer)

	got := closer.snapshot()
	require.Len(t, got, 2, "issuer must not be closed — response datagram is still in flight")
	require.NotContains(t, got, issuer)
	for _, pk := range []types.PeerKey{peerKey(3), peerKey(4)} {
		require.Equal(t, transport.DisconnectCertRotation, got[pk],
			"peer %s must be closed with DisconnectCertRotation", pk.Short())
	}
}

// TestRotateSessionsForNewCertIsANoOpWithoutCloser protects the
// sessionCloser == nil branch — some constructions (non-transport
// tests) don't wire one.
func TestRotateSessionsForNewCertIsANoOpWithoutCloser(t *testing.T) {
	svc, net, _ := newTestServiceWithCloser(peerKey(1))
	svc.sessionCloser = nil
	net.setConnectedPeers(peerKey(2))
	require.NotPanics(t, func() { svc.rotateSessionsForNewCert(types.PeerKey{}) })
}

// TestIssueCertInstallsFreshCertIntoLocalCache pins the fix for the
// stale-issuer bug: after a successful PushCert, the issuer's own
// PeerDelegationCert cache must reflect the cert it just signed, so a
// subsequent renewal or authz evaluation on the issuer sees ground
// truth rather than the pre-grant attrs. Without this, a renewal
// routed through the issuer silently drops the new attributes.
func TestIssueCertInstallsFreshCertIntoLocalCache(t *testing.T) {
	creds := newRootCredentials(t)
	certs := newFakeCertManager()
	svc := newIssuerService(t, creds, certs)

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subject := types.PeerKeyFromBytes(subjectPub)

	attrs, err := structpb.NewStruct(map[string]any{"host": "admin"})
	require.NoError(t, err)

	require.NoError(t, svc.IssueCert(context.Background(), subject, false, attrs))

	got, ok := certs.PeerDelegationCert(subject)
	require.True(t, ok, "issuer must hold a cached cert for the subject after IssueCert")
	require.Equal(t, "admin", got.GetClaims().GetCapabilities().GetAttributes().AsMap()["host"],
		"cached cert must carry the attrs we just signed")
}

// TestHandleCertRenewalInstallsFreshCertIntoLocalCache covers the
// renewal fork of the same fix: the renewing admin must update its
// cache with the cert it just signed, otherwise a subsequent renewal
// through the same admin rebuilds attrs from the pre-renewal cache
// and silently drops them.
func TestHandleCertRenewalInstallsFreshCertIntoLocalCache(t *testing.T) {
	creds := newRootCredentials(t)
	certs := newFakeCertManager()
	svc := newIssuerService(t, creds, certs)

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subject := types.PeerKeyFromBytes(subjectPub)

	// Seed the peer cache with a cert carrying the attrs we expect to
	// be preserved across renewal — same shape applyNewCert would have
	// installed on a fresh handshake.
	attrs, err := structpb.NewStruct(map[string]any{"host": "admin"})
	require.NoError(t, err)
	seed := issueSubjectCert(t, creds, subjectPub, attrs)
	certs.SetPeerDelegationCert(subject, seed)

	svc.handleCertRenewalRequest(context.Background(), subject, &meshv1.CertRenewalRequest{PeerPub: subjectPub})

	got, ok := certs.PeerDelegationCert(subject)
	require.True(t, ok)
	require.Equal(t, "admin", got.GetClaims().GetCapabilities().GetAttributes().AsMap()["host"],
		"renewed cert must carry the same attrs so the cache stays consistent for the next renewal")
	require.Greater(t, got.GetClaims().GetNotAfterUnix(), seed.GetClaims().GetNotAfterUnix(),
		"renewal must extend the NotAfter — otherwise the cache isn't the renewed cert")
}

// newRootCredentials builds NodeCredentials whose DelegationKey is a
// root signer. Root status is unlocked when the admin key on disk
// matches the persisted root.pub, so the helper makes admin==root and
// seeds the node credentials shape `pln init` produces when a cluster
// is bootstrapped. SaveNodeCredentials needs a cert to marshal, but
// NewDelegationSigner's root path ignores it and rebuilds its own
// issuer cert — so the saved cert just has to be a valid marshal.
func newRootCredentials(t *testing.T) *auth.NodeCredentials {
	t.Helper()
	identityDir := t.TempDir()

	adminPriv, adminPub, err := auth.EnsureAdminKey(identityDir)
	require.NoError(t, err)

	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	seedCert, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, auth.FullCapabilities(),
		time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)
	creds := auth.NewNodeCredentials(adminPub, seedCert)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, creds))

	signer, err := auth.NewDelegationSigner(identityDir, nodePriv, time.Hour)
	require.NoError(t, err)
	require.True(t, signer.IsRoot(), "signer must be root — admin key was persisted as root.pub")
	creds.SetDelegationKey(signer)
	return creds
}

// newIssuerService builds a membership.Service configured as the issuer
// (root signer in creds) with a controllable fake cert manager. Used by
// both IssueCert and handleCertRenewalRequest tests.
func newIssuerService(t *testing.T, creds *auth.NodeCredentials, certs CertManager) *Service {
	t.Helper()
	net := newFakeNetwork()
	st := newFakeClusterState(peerKey(1))
	return New(peerKey(1), creds, net, st, Config{
		Log:              zap.S(),
		TracerProvider:   tracenoop.NewTracerProvider(),
		NodeMetrics:      metrics.NewNodeMetrics(metricnoop.NewMeterProvider()),
		GossipInterval:   time.Hour,
		PeerTickInterval: time.Hour,
		Streams:          &fakeStreamOpener{},
		RTT:              &fakeRTTSource{},
		Certs:            certs,
		PeerAddrs:        &fakePeerAddressSource{},
		SessionCloser:    newFakePeerSessionCloser(),
		RoutedSender:     noopRoutedSender{},
		NATDetector:      nat.NewDetector(),
		ReconnectWindow:  time.Hour,
		MembershipTTL:    time.Hour,
	})
}

// noopRoutedSender lets the handlers' response-sending calls complete
// without a wire — the tests assert cache state on the issuer, not on
// the datagram the peer would have received.
type noopRoutedSender struct{}

func (noopRoutedSender) SendMembershipDatagram(context.Context, types.PeerKey, []byte) error {
	return nil
}

// issueSubjectCert produces a leaf cert for subjectPub signed by the
// root signer carried in creds — used to seed a pre-existing cached
// cert the renewal path inherits attrs from.
func issueSubjectCert(t *testing.T, creds *auth.NodeCredentials, subjectPub ed25519.PublicKey, attrs *structpb.Struct) *admissionv1.DelegationCert {
	t.Helper()
	caps := auth.LeafCapabilities()
	caps.Attributes = attrs
	cert, err := creds.DelegationKey().IssueMemberCert(subjectPub, caps,
		time.Now().Add(-time.Minute), time.Now().Add(30*time.Minute), time.Time{})
	require.NoError(t, err)
	return cert
}
