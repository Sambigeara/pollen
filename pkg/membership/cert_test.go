// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
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
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

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

func TestRotateSessionsForNewCertIsANoOpWithoutCloser(t *testing.T) {
	svc, net, _ := newTestServiceWithCloser(peerKey(1))
	svc.sessionCloser = nil
	net.setConnectedPeers(peerKey(2))
	require.NotPanics(t, func() { svc.rotateSessionsForNewCert(types.PeerKey{}) })
}

func TestHandleCertPushSkipsRotationOnPromotionToAdmin(t *testing.T) {
	h := newRecipientHarness(t, false)
	h.network.setConnectedPeers(peerKey(2), peerKey(3))

	issuer := peerKey(4)
	h.svc.handleCertPushRequest(context.Background(), issuer, &meshv1.CertPushRequest{
		Cert: h.issueCert(t, true),
	})

	require.True(t, h.capTrans.upgraded.Load(), "must transition to admin")
	require.Empty(t, h.closer.snapshot(), "no peer sessions should be closed on promotion to admin")
}

func TestHandleCertPushRotatesOnLeafCertPush(t *testing.T) {
	h := newRecipientHarness(t, false)
	h.network.setConnectedPeers(peerKey(2), peerKey(3))

	issuer := peerKey(4)
	h.svc.handleCertPushRequest(context.Background(), issuer, &meshv1.CertPushRequest{
		Cert: h.issueCert(t, false),
	})

	require.True(t, h.capTrans.downgraded.Load(), "must transition to leaf")
	got := h.closer.snapshot()
	require.Len(t, got, 2, "leaf cert push must rotate every connected peer")
	require.NotContains(t, got, issuer, "issuer is excluded — its accept datagram is still flushing")
	for _, pk := range []types.PeerKey{peerKey(2), peerKey(3)} {
		require.Equal(t, transport.DisconnectCertRotation, got[pk])
	}
}

func TestHandleCertPushForwardsRevokeEventsOnPublisherDowngrade(t *testing.T) {
	// Cap-downgrade tombstones every locally-published spec via
	// RevokeOwnSpecs. The returned events must flow out through
	// Events() so the supervisor cascade can tear down live runtime
	// state (tunneling bridges, placement claims). Without forwarding,
	// the spec disappears from gossip but bridged streams stay open.
	h := newRecipientHarness(t, true)
	h.state.revokeEvents = []state.Event{state.ServiceChanged{Peer: types.PeerKeyFromBytes(h.selfPub), Name: "samflix"}}

	h.svc.handleCertPushRequest(context.Background(), peerKey(4), &meshv1.CertPushRequest{
		Cert: h.issueCert(t, false),
	})

	select {
	case ev := <-h.svc.Events():
		got, ok := ev.(state.ServiceChanged)
		require.True(t, ok, "expected ServiceChanged, got %T", ev)
		require.Equal(t, "samflix", got.Name)
	case <-time.After(time.Second):
		require.FailNow(t, "RevokeOwnSpecs events must reach the supervisor cascade via Events()")
	}
}

func TestHandleCertPushRotatesOnAdminToAdminRegrant(t *testing.T) {
	h := newRecipientHarness(t, true)
	h.network.setConnectedPeers(peerKey(2), peerKey(3))

	issuer := peerKey(4)
	h.svc.handleCertPushRequest(context.Background(), issuer, &meshv1.CertPushRequest{
		Cert: h.issueCert(t, true),
	})

	require.True(t, h.capTrans.upgraded.Load())
	got := h.closer.snapshot()
	require.Len(t, got, 2, "admin→admin re-grant must rotate so attribute changes propagate")
	require.NotContains(t, got, issuer)
	for _, pk := range []types.PeerKey{peerKey(2), peerKey(3)} {
		require.Equal(t, transport.DisconnectCertRotation, got[pk])
	}
}

func TestIssueCertInstallsFreshCertIntoLocalCache(t *testing.T) {
	attrs, err := structpb.NewStruct(map[string]any{"host": "admin"})
	require.NoError(t, err)

	creds, _, _, _ := newRootCredentialsWithAttrs(t, attrs)
	certs := newFakeCertManager()
	svc := newIssuerService(t, creds, certs)

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subject := types.PeerKeyFromBytes(subjectPub)

	caps := auth.LeafCapabilities()
	caps.Attributes = attrs
	require.NoError(t, svc.IssueCert(context.Background(), subject, caps))

	got, ok := certs.PeerDelegationCert(subject)
	require.True(t, ok, "issuer must hold a cached cert for the subject after IssueCert")
	require.Equal(t, "admin", got.GetClaims().GetCapabilities().GetAttributes().AsMap()["host"],
		"cached cert must carry the attrs we just signed")
}

func TestHandleCertRenewalInstallsFreshCertIntoLocalCache(t *testing.T) {
	attrs, err := structpb.NewStruct(map[string]any{"host": "admin"})
	require.NoError(t, err)

	creds, _, _, _ := newRootCredentialsWithAttrs(t, attrs)
	certs := newFakeCertManager()
	svc := newIssuerService(t, creds, certs)

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subject := types.PeerKeyFromBytes(subjectPub)

	// Seed the peer cache with a cert carrying the attrs we expect to
	// be preserved across renewal — same shape applyNewCert would have
	// installed on a fresh handshake.
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

func TestAttemptCertRenewalRootSelfRenewsWithoutPeers(t *testing.T) {
	creds, pollenDir, nodePriv, nodePub := newRootCredentialsWithIdentity(t)

	identityDir := auth.IdentityPath(pollenDir)
	adminPriv, _, err := auth.LoadAdminKey(identityDir)
	require.NoError(t, err)
	expired, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, auth.FullCapabilities(),
		time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	creds.SetCert(expired)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, creds))

	self := types.PeerKeyFromBytes(nodePub)
	svc := New(self, creds, newFakeNetwork(), newFakeClusterState(self), Config{
		Log:              zap.S(),
		TracerProvider:   tracenoop.NewTracerProvider(),
		NodeMetrics:      metrics.NewNodeMetrics(metricnoop.NewMeterProvider()),
		GossipInterval:   time.Hour,
		PeerTickInterval: time.Hour,
		Streams:          &fakeStreamOpener{},
		RTT:              &fakeRTTSource{},
		Certs:            newFakeCertManager(),
		PeerAddrs:        &fakePeerAddressSource{},
		SessionCloser:    newFakePeerSessionCloser(),
		RoutedSender:     noopRoutedSender{},
		NATDetector:      nat.NewDetector(),
		ReconnectWindow:  time.Hour,
		MembershipTTL:    time.Hour,
		SignPriv:         nodePriv,
		PollenDir:        pollenDir,
		TLSIdentityTTL:   time.Hour,
	})

	require.True(t, svc.attemptCertRenewal(),
		"root with no peers must self-renew via the local admin key")

	require.False(t, auth.IsCertExpired(creds.Cert(), time.Now()),
		"in-memory creds must reflect the freshly self-issued cert")

	loaded, err := auth.LoadNodeCredentials(identityDir)
	require.NoError(t, err)
	require.False(t, auth.IsCertExpired(loaded.Cert(), time.Now()),
		"persisted cert on disk must also be the freshly self-issued one")
}

// TestRootSelfRenewalRebuildsDelegationSigner pins the invariant that
// every renewal path refreshes the DelegationSigner. The signer
// captures its issuer cert by value, so a stale signer keeps signing
// SpecAuth and invite tokens with the about-to-expire publisher cert
// — peers reject those once the old TTL elapses, even though the
// node has long since rotated to a fresh cert.
func TestRootSelfRenewalRebuildsDelegationSigner(t *testing.T) {
	creds, pollenDir, nodePriv, nodePub := newRootCredentialsWithIdentity(t)

	staleSerial := creds.DelegationKey().IssuerCert().GetClaims().GetSerial()

	identityDir := auth.IdentityPath(pollenDir)
	adminPriv, _, err := auth.LoadAdminKey(identityDir)
	require.NoError(t, err)
	expired, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, auth.FullCapabilities(),
		time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	creds.SetCert(expired)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, creds))

	self := types.PeerKeyFromBytes(nodePub)
	svc := New(self, creds, newFakeNetwork(), newFakeClusterState(self), Config{
		Log:              zap.S(),
		TracerProvider:   tracenoop.NewTracerProvider(),
		NodeMetrics:      metrics.NewNodeMetrics(metricnoop.NewMeterProvider()),
		GossipInterval:   time.Hour,
		PeerTickInterval: time.Hour,
		Streams:          &fakeStreamOpener{},
		RTT:              &fakeRTTSource{},
		Certs:            newFakeCertManager(),
		PeerAddrs:        &fakePeerAddressSource{},
		SessionCloser:    newFakePeerSessionCloser(),
		RoutedSender:     noopRoutedSender{},
		NATDetector:      nat.NewDetector(),
		ReconnectWindow:  time.Hour,
		MembershipTTL:    time.Hour,
		SignPriv:         nodePriv,
		PollenDir:        pollenDir,
		TLSIdentityTTL:   time.Hour,
	})

	require.True(t, svc.attemptCertRenewal(), "root must self-renew")

	signerIssuer := creds.DelegationKey().IssuerCert()
	require.NotEqual(t, staleSerial, signerIssuer.GetClaims().GetSerial(),
		"signer must be rebuilt with the renewed cert; otherwise SpecAuth issuance still embeds the stale publisher")
	require.Equal(t, creds.Cert().GetClaims().GetSerial(), signerIssuer.GetClaims().GetSerial(),
		"signer's issuer cert must match the freshly-installed creds cert")
}

type recipientHarness struct {
	svc      *Service
	rootPriv ed25519.PrivateKey
	selfPub  ed25519.PublicKey
	closer   *fakePeerSessionCloser
	capTrans *fakeCapTransitioner
	network  *fakeNetwork
	state    *fakeClusterState
}

func newRecipientHarness(t *testing.T, startAsAdmin bool) *recipientHarness {
	t.Helper()
	rootPub, rootPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	selfPub, selfPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	initialCaps := auth.LeafCapabilities()
	if startAsAdmin {
		initialCaps = auth.FullCapabilities()
	}
	initialCert, err := auth.IssueDelegationCert(
		rootPriv, nil, selfPub, initialCaps,
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour), time.Time{})
	require.NoError(t, err)

	creds := auth.NewNodeCredentials(rootPub, initialCert)
	closer := newFakePeerSessionCloser()
	capTrans := &fakeCapTransitioner{}
	net := newFakeNetwork()
	self := types.PeerKeyFromBytes(selfPub)
	st := newFakeClusterState(self)

	svc := New(self, creds, net, st, Config{
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
		RoutedSender:     noopRoutedSender{},
		NATDetector:      nat.NewDetector(),
		ReconnectWindow:  time.Hour,
		MembershipTTL:    time.Hour,
		CapTransition:    capTrans,
		SignPriv:         selfPriv,
		PollenDir:        t.TempDir(),
		TLSIdentityTTL:   time.Hour,
	})
	return &recipientHarness{
		svc:      svc,
		rootPriv: rootPriv,
		selfPub:  selfPub,
		closer:   closer,
		capTrans: capTrans,
		network:  net,
		state:    st,
	}
}

func (h *recipientHarness) issueCert(t *testing.T, admin bool) *admissionv1.DelegationCert {
	t.Helper()
	caps := auth.LeafCapabilities()
	if admin {
		caps = auth.FullCapabilities()
	}
	cert, err := auth.IssueDelegationCert(
		h.rootPriv, nil, h.selfPub, caps,
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour), time.Time{})
	require.NoError(t, err)
	return cert
}

func newRootCredentialsWithIdentity(t *testing.T) (*auth.NodeCredentials, string, ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	return newRootCredentialsWithAttrs(t, nil)
}

func newRootCredentialsWithAttrs(t *testing.T, attrs *structpb.Struct) (*auth.NodeCredentials, string, ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	pollenDir := t.TempDir()
	identityDir := auth.IdentityPath(pollenDir)
	require.NoError(t, os.MkdirAll(identityDir, 0o700))

	adminPriv, adminPub, err := auth.EnsureAdminKey(identityDir)
	require.NoError(t, err)

	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	caps := auth.FullCapabilities()
	caps.Attributes = attrs
	seedCert, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, caps,
		time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)
	creds := auth.NewNodeCredentials(adminPub, seedCert)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, creds))

	signer, err := auth.NewDelegationSigner(identityDir, nodePriv)
	require.NoError(t, err)
	require.True(t, signer.IsRoot(), "signer must be root — admin key was persisted as root.pub")
	creds.SetDelegationKey(signer)
	return creds, pollenDir, nodePriv, nodePub
}

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

type noopRoutedSender struct{}

func (noopRoutedSender) SendMembershipDatagram(context.Context, types.PeerKey, []byte) error {
	return nil
}

func issueSubjectCert(t *testing.T, creds *auth.NodeCredentials, subjectPub ed25519.PublicKey, attrs *structpb.Struct) *admissionv1.DelegationCert {
	t.Helper()
	caps := auth.LeafCapabilities()
	caps.Attributes = attrs
	cert, err := creds.DelegationKey().IssueMemberCert(subjectPub, caps,
		time.Now().Add(-time.Minute), time.Now().Add(30*time.Minute), time.Time{})
	require.NoError(t, err)
	return cert
}
