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

// TestHandleCertPushSkipsRotationOnPromotionToAdmin: closing every
// peer session at the moment we become admin orphans the in-flight
// invite forwarding the mesh routes through us. Rotation buys
// nothing here — AdminCapable propagates via the SetAdmin gossip
// event, not TLS.
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

// TestHandleCertPushRotatesOnLeafCertPush is the symmetric pin: a
// non-admin cert push must still rotate sessions so peers re-handshake
// and pick up our updated subject.properties — that is the original
// motivation for the rotation.
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

// TestHandleCertPushRotatesOnAdminToAdminRegrant guards the
// predicate against over-skipping. Re-pushing an admin cert (e.g.
// attribute re-grant against an existing admin) must still rotate so
// peers' cached cert attrs refresh — attribute-keyed authz reads
// from that cache.
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

// TestAttemptCertRenewalRootSelfRenewsWithoutPeers pins the fix for the
// "root cannot renew its own cert" gap: a root node holds the admin key
// and must re-issue its self-signed cert from local material when expiry
// approaches, regardless of mesh connectivity. Pre-fix, attemptCertRenewal
// returned false on root because the peer-routed branch had no peers.
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

// recipientHarness wires a Service so it can apply pushed delegation
// certs from a known root. The service holds a real signPriv and
// pollenDir so applyNewCert can verify, regenerate the TLS identity,
// and persist the updated credentials.
type recipientHarness struct {
	svc      *Service
	rootPriv ed25519.PrivateKey
	selfPub  ed25519.PublicKey
	closer   *fakePeerSessionCloser
	capTrans *fakeCapTransitioner
	network  *fakeNetwork
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

// newRootCredentials builds NodeCredentials whose DelegationKey is a
// root signer. Root status is unlocked when the admin key on disk
// matches the persisted root.pub, so the helper makes admin==root and
// seeds the node credentials shape `pln init` produces when a cluster
// is bootstrapped.
func newRootCredentials(t *testing.T) *auth.NodeCredentials {
	t.Helper()
	creds, _, _, _ := newRootCredentialsWithIdentity(t)
	return creds
}

// newRootCredentialsWithIdentity returns the same root credentials plus the
// pollen dir, node identity private key, and node pub. Tests that exercise
// the full root self-renewal path (signing, applyNewCert) need these.
func newRootCredentialsWithIdentity(t *testing.T) (*auth.NodeCredentials, string, ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	pollenDir := t.TempDir()
	identityDir := auth.IdentityPath(pollenDir)
	require.NoError(t, os.MkdirAll(identityDir, 0o700))

	adminPriv, adminPub, err := auth.EnsureAdminKey(identityDir)
	require.NoError(t, err)

	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	seedCert, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, auth.FullCapabilities(),
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
