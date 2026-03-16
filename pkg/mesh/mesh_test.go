package mesh_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/traffic"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type testConsumer struct {
	seen map[string]struct{}
}

func (c *testConsumer) TryConsume(token *admissionv1.InviteToken, _ time.Time) (bool, error) {
	id := token.GetClaims().GetTokenId()
	if _, ok := c.seen[id]; ok {
		return false, nil
	}
	c.seen[id] = struct{}{}
	return true, nil
}

type meshHarness struct {
	mesh    mesh.Mesh
	peerKey types.PeerKey
	pubKey  ed25519.PublicKey
	port    int
	cancel  context.CancelFunc
}

type clusterAuth struct {
	adminPriv ed25519.PrivateKey
	trust     *admissionv1.TrustBundle
}

func newClusterAuth(t *testing.T) *clusterAuth {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &clusterAuth{adminPriv: priv, trust: auth.NewTrustBundle(pub)}
}

func (c *clusterAuth) credsFor(t *testing.T, subject ed25519.PublicKey) *auth.NodeCredentials {
	t.Helper()
	cert, err := auth.IssueDelegationCert(c.adminPriv, nil, c.trust.GetClusterId(), subject, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)
	return &auth.NodeCredentials{Trust: c.trust, Cert: cert}
}

func (c *clusterAuth) tokenFor(t *testing.T, subject ed25519.PublicKey, bootstrap *meshHarness) *admissionv1.JoinToken {
	t.Helper()
	token, err := auth.IssueJoinToken(c.adminPriv, c.trust, subject, []*admissionv1.BootstrapPeer{{
		PeerPub: bootstrap.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
	}}, time.Now(), time.Hour, config.CertTTLs{}.MembershipTTL(), time.Time{})
	require.NoError(t, err)
	return token
}

func (c *clusterAuth) signer(t *testing.T) *auth.DelegationSigner {
	t.Helper()
	adminPub, ok := c.adminPriv.Public().(ed25519.PublicKey)
	require.True(t, ok)

	issuer, err := auth.IssueDelegationCert(
		c.adminPriv,
		nil,
		c.trust.GetClusterId(),
		adminPub,
		auth.FullCapabilities(),
		time.Now().Add(-time.Minute),
		time.Now().Add(365*24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	return &auth.DelegationSigner{
		Priv:     c.adminPriv,
		Trust:    c.trust,
		Issuer:   issuer,
		Consumed: &testConsumer{seen: make(map[string]struct{})},
	}
}

func TestJoinWithTokenHappyPath(t *testing.T) {
	cluster := newClusterAuth(t)
	bootstrap := startMeshHarness(t, cluster)
	joiner := startMeshHarness(t, cluster)

	token := cluster.tokenFor(t, joiner.pubKey, bootstrap)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, joiner.mesh.JoinWithToken(joinCtx, token))

	require.Eventually(t, func() bool {
		_, ok := joiner.mesh.GetConn(bootstrap.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		_, ok := bootstrap.mesh.GetConn(joiner.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)
}

func TestConnectRejectsCrossClusterPeer(t *testing.T) {
	clusterA := newClusterAuth(t)
	clusterB := newClusterAuth(t)

	a := startMeshHarness(t, clusterA)
	b := startMeshHarness(t, clusterB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := a.mesh.Connect(ctx, b.peerKey, []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: b.port}})
	require.Error(t, err)

	_, ok := a.mesh.GetConn(b.peerKey)
	require.False(t, ok)
}

func TestJoinWithInviteHappyPath(t *testing.T) {
	cluster := newClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	bootstrapCreds := cluster.credsFor(t, bootstrapPub)
	signer := cluster.signer(t)
	bootstrapCreds.DelegationKey = signer
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	joiner := startMeshHarness(t, cluster)

	invite, err := auth.IssueInviteTokenWithSigner(
		cluster.signer(t),
		nil,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
		}},
		time.Now(),
		time.Hour,
		0,
	)
	require.NoError(t, err)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	joinToken, err := joiner.mesh.JoinWithInvite(joinCtx, invite)
	require.NoError(t, err)
	_, err = auth.VerifyJoinToken(joinToken, joiner.pubKey, time.Now())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, ok := joiner.mesh.GetConn(bootstrap.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		_, ok := bootstrap.mesh.GetConn(joiner.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)
}

func TestJoinWithInviteRejectsExpiredInviteTTL(t *testing.T) {
	cluster := newClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	bootstrapCreds := cluster.credsFor(t, bootstrapPub)
	signer := cluster.signer(t)
	bootstrapCreds.DelegationKey = signer
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	joiner := startMeshHarness(t, cluster)

	invite, err := auth.IssueInviteTokenWithSigner(
		cluster.signer(t),
		nil,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
		}},
		time.Now().Add(-2*time.Second),
		time.Second,
		0,
	)
	require.NoError(t, err)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = joiner.mesh.JoinWithInvite(joinCtx, invite)
	require.Error(t, err)
}

func TestConnectReturnsErrIdentityMismatch(t *testing.T) {
	cluster := newClusterAuth(t)

	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	// Generate a random key that differs from b's actual key.
	wrongPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	wrongKey := types.PeerKeyFromBytes(wrongPub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dial b's address but expect the wrong key.
	err = a.mesh.Connect(ctx, wrongKey, []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: b.port}})
	require.Error(t, err)
	require.True(t, errors.Is(err, mesh.ErrIdentityMismatch), "expected ErrIdentityMismatch, got: %v", err)
}

// notifyRouter implements Router with a Changed() channel so OpenStream's
// select loop can wake on route updates.
//
// lookupHook fires during Lookup (outside the lock) to inject concurrent route
// updates at a precise point in the OpenStream loop.
//
// staleOnce, when set, makes the first Lookup for the matching dest return
// staleOnce.next instead of the live routes map — even if the hook just updated
// the map. This lets tests force a stale return so that OpenStream must rely on
// the pre-snapshotted routeCh rather than observing the new route immediately.
type notifyRouter struct {
	mu         sync.Mutex
	routes     map[types.PeerKey]types.PeerKey
	changeCh   chan struct{}
	lookupHook func()
	lookupCh   chan struct{} // closed on first Lookup call
	staleOnce  *staleOnceEntry
}

type staleOnceEntry struct {
	dest types.PeerKey
	next types.PeerKey
}

func newNotifyRouter(routes map[types.PeerKey]types.PeerKey) *notifyRouter {
	return &notifyRouter{
		routes:   routes,
		changeCh: make(chan struct{}),
		lookupCh: make(chan struct{}),
	}
}

func (r *notifyRouter) Lookup(dest types.PeerKey) (types.PeerKey, bool) {
	r.mu.Lock()

	// Signal that Lookup has been entered.
	select {
	case <-r.lookupCh:
	default:
		close(r.lookupCh)
	}

	// Capture and clear one-shot fields.
	hook := r.lookupHook
	r.lookupHook = nil
	stale := r.staleOnce
	if stale != nil && stale.dest == dest {
		r.staleOnce = nil
	} else {
		stale = nil
	}
	r.mu.Unlock()

	if hook != nil {
		hook()
	}

	// Return the stale route for this call even though the hook may have
	// already updated r.routes.
	if stale != nil {
		return stale.next, true
	}

	r.mu.Lock()
	next, ok := r.routes[dest]
	r.mu.Unlock()
	return next, ok
}

func (r *notifyRouter) Changed() <-chan struct{} {
	r.mu.Lock()
	ch := r.changeCh
	r.mu.Unlock()
	return ch
}

func (r *notifyRouter) setLookupHook(fn func()) {
	r.mu.Lock()
	r.lookupHook = fn
	r.mu.Unlock()
}

func (r *notifyRouter) setStaleOnce(dest, next types.PeerKey) {
	r.mu.Lock()
	r.staleOnce = &staleOnceEntry{dest: dest, next: next}
	r.mu.Unlock()
}

func (r *notifyRouter) update(routes map[types.PeerKey]types.PeerKey) {
	r.mu.Lock()
	r.routes = routes
	close(r.changeCh)
	r.changeCh = make(chan struct{})
	r.mu.Unlock()
}

func TestOpenStreamRoutedWaitsForNextHop(t *testing.T) {
	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	// Fabricated unreachable peer C.
	cPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerC := types.PeerKeyFromBytes(cPub)

	// Router tells A: to reach C, go through B.
	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{peerC: b.peerKey})
	a.mesh.SetRouter(router)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, peerC)
		ch <- result{s, err}
	}()

	// Wait for OpenStream to enter the router path (first Lookup call).
	require.Eventually(t, func() bool {
		select {
		case <-router.lookupCh:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	// Connect A ↔ B — this should unblock the session wait.
	require.NoError(t, a.mesh.Connect(ctx, b.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: b.port},
	}))

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.stream != nil {
				_ = r.stream.Close()
			}
			// The call returned. It may succeed (stream opened to B with routing
			// header) or fail (B can't forward to C). Either way, it must NOT be
			// a deadline-exceeded hang waiting for a direct session to C.
			require.NotErrorIs(t, r.err, context.DeadlineExceeded)
			return true
		default:
			return false
		}
	}, 5*time.Second, 25*time.Millisecond, "OpenStream hung waiting for unreachable peer instead of next hop")
}

func TestOpenStreamRouteChurn(t *testing.T) {
	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	d := startMeshHarness(t, cluster)

	// Fabricated unreachable peers B and C.
	bPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerB := types.PeerKeyFromBytes(bPub)

	cPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerC := types.PeerKeyFromBytes(cPub)

	// Connect A ↔ D.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, a.mesh.Connect(ctx, d.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: d.port},
	}))
	require.Eventually(t, func() bool {
		_, ok := d.mesh.GetConn(a.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	// Router initially points peerC → B (B is not connected).
	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{peerC: peerB})
	a.mesh.SetRouter(router)

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, peerC)
		ch <- result{s, err}
	}()

	// Update route: peerC → D (D is already connected).
	router.update(map[types.PeerKey]types.PeerKey{peerC: d.peerKey})

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.stream != nil {
				_ = r.stream.Close()
			}
			require.NotErrorIs(t, r.err, context.DeadlineExceeded)
			return true
		default:
			return false
		}
	}, 5*time.Second, 25*time.Millisecond, "OpenStream hung on stale route instead of re-evaluating after route change")
}

func TestOpenStreamDirectSessionWhileRouting(t *testing.T) {
	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	c := startMeshHarness(t, cluster)

	// Fabricated unreachable peer B.
	bPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerB := types.PeerKeyFromBytes(bPub)

	// Router points C → B (B is not connected).
	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{c.peerKey: peerB})
	a.mesh.SetRouter(router)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, c.peerKey)
		ch <- result{s, err}
	}()

	// Connect A ↔ C directly — should unblock via the direct-session fast path.
	require.NoError(t, a.mesh.Connect(ctx, c.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: c.port},
	}))

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.stream != nil {
				_ = r.stream.Close()
			}
			require.NotErrorIs(t, r.err, context.DeadlineExceeded)
			return true
		default:
			return false
		}
	}, 5*time.Second, 25*time.Millisecond, "OpenStream hung on stale route instead of using direct session")
}

// TestOpenStreamRouteUpdateBetweenSnapshotAndLookup exercises the TOCTOU window:
// the route table updates after Changed() is snapshotted but during Lookup().
// Because routeCh was captured before reading state, the closed channel wakes
// the select and OpenStream re-evaluates with the new route.
func TestOpenStreamRouteUpdateBetweenSnapshotAndLookup(t *testing.T) {
	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	d := startMeshHarness(t, cluster)

	// Fabricated unreachable peers B and C.
	bPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerB := types.PeerKeyFromBytes(bPub)

	cPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerC := types.PeerKeyFromBytes(cPub)

	// Connect A ↔ D.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, a.mesh.Connect(ctx, d.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: d.port},
	}))
	require.Eventually(t, func() bool {
		_, ok := d.mesh.GetConn(a.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	// Router initially points peerC → B (not connected).
	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{peerC: peerB})
	a.mesh.SetRouter(router)

	// The hook fires during Lookup: it updates the route to peerC → D
	// (connected) and closes the old changeCh. But staleOnce forces this
	// first Lookup to still return peerB, so OpenStream cannot observe the
	// new route on this iteration. It must rely on the pre-snapshotted
	// routeCh (now closed) to wake the select and re-evaluate.
	router.setStaleOnce(peerC, peerB)
	router.setLookupHook(func() {
		router.update(map[types.PeerKey]types.PeerKey{peerC: d.peerKey})
	})

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, peerC)
		ch <- result{s, err}
	}()

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.stream != nil {
				_ = r.stream.Close()
			}
			require.NotErrorIs(t, r.err, context.DeadlineExceeded)
			return true
		default:
			return false
		}
	}, 5*time.Second, 25*time.Millisecond, "OpenStream missed route notification during TOCTOU window")
}

func TestRoutedDeliveryRejectsNonTunnel(t *testing.T) {
	const (
		typeRouted byte = 3
		typeClock  byte = 1
		typeTunnel byte = 2
		headerLen       = 66 // routeHeaderSize
		ttl        byte = 16
	)

	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, a.mesh.Connect(ctx, b.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: b.port},
	}))
	require.Eventually(t, func() bool {
		_, ok := b.mesh.GetConn(a.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	conn, ok := a.mesh.GetConn(b.peerKey)
	require.True(t, ok)

	// --- Negative: clock inner type must be rejected ---

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	var hdr [1 + headerLen]byte
	hdr[0] = typeRouted
	copy(hdr[1:33], b.peerKey[:])  // dest = B (local delivery)
	copy(hdr[33:65], a.peerKey[:]) // source = A
	hdr[65] = ttl
	hdr[66] = typeClock // rejected
	_, err = stream.Write(hdr[:])
	require.NoError(t, err)

	_ = stream.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = stream.Read(buf)
	require.Error(t, err, "expected stream reset for non-tunnel inner type")

	// --- Positive: tunnel inner type must be delivered ---

	stream2, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	var hdr2 [1 + headerLen]byte
	hdr2[0] = typeRouted
	copy(hdr2[1:33], b.peerKey[:])
	copy(hdr2[33:65], a.peerKey[:])
	hdr2[65] = ttl
	hdr2[66] = typeTunnel
	_, err = stream2.Write(hdr2[:])
	require.NoError(t, err)

	acceptCtx, acceptCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acceptCancel()

	rwc, stype, peerKey, err := b.mesh.AcceptAllStreams(acceptCtx)
	require.NoError(t, err)
	require.Equal(t, mesh.StreamTypeTunnel, stype)
	require.Equal(t, a.peerKey, peerKey)
	_ = rwc.Close()
	stream2.CancelRead(0)
	stream2.CancelWrite(0)
}

// TestSimultaneousDialConverges verifies that when two peers dial each other at
// the same time (creating two QUIC connections), both nodes converge on the same
// connection and neither peer gets kicked out. This is a regression test for a
// bug where the tie-break logic didn't consider connection direction, causing
// both nodes to hold different connections that the other side then closed.
func TestSimultaneousDialConverges(t *testing.T) {
	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Both sides dial simultaneously.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = a.mesh.Connect(ctx, b.peerKey, []*net.UDPAddr{
			{IP: net.IPv4(127, 0, 0, 1), Port: b.port},
		})
	}()
	go func() {
		defer wg.Done()
		_ = b.mesh.Connect(ctx, a.peerKey, []*net.UDPAddr{
			{IP: net.IPv4(127, 0, 0, 1), Port: a.port},
		})
	}()
	wg.Wait()

	// Both peers must see each other as connected and stay connected.
	require.Eventually(t, func() bool {
		_, aHasB := a.mesh.GetConn(b.peerKey)
		_, bHasA := b.mesh.GetConn(a.peerKey)
		return aHasB && bHasA
	}, 5*time.Second, 25*time.Millisecond, "peers should see each other after simultaneous dial")

	// Verify stability: the sessions should not die within a reasonable window.
	time.Sleep(500 * time.Millisecond)
	_, aHasB := a.mesh.GetConn(b.peerKey)
	_, bHasA := b.mesh.GetConn(a.peerKey)
	require.True(t, aHasB, "a lost connection to b after simultaneous dial")
	require.True(t, bHasA, "b lost connection to a after simultaneous dial")
}

func startMeshHarness(t *testing.T, cluster *clusterAuth) *meshHarness {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return startMeshHarnessWithCreds(t, priv, pub, cluster.credsFor(t, pub))
}

func startMeshHarnessWithCreds(
	t *testing.T,
	priv ed25519.PrivateKey,
	pub ed25519.PublicKey,
	creds *auth.NodeCredentials,
) *meshHarness {
	t.Helper()

	m, err := mesh.NewMesh(0, priv, creds, config.CertTTLs{}.TLSIdentityTTL(), config.CertTTLs{}.MembershipTTL(), config.CertTTLs{}.ReconnectWindowDuration(), 0, nil, metrics.NewMeshMetrics(nil))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.Start(ctx))

	h := &meshHarness{
		mesh:    m,
		peerKey: types.PeerKeyFromBytes(pub),
		pubKey:  pub,
		port:    m.ListenPort(),
		cancel:  cancel,
	}

	t.Cleanup(func() {
		h.cancel()
		_ = h.mesh.Close()
	})

	return h
}

func TestRoutedRelayTrafficAttribution(t *testing.T) {
	cluster := newClusterAuth(t)
	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)
	c := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Set up router on A: C→B and traffic tracker on B before connecting,
	// so acceptBidiStreams goroutines see fully-initialized state.
	routerA := newNotifyRouter(map[types.PeerKey]types.PeerKey{c.peerKey: b.peerKey})
	a.mesh.SetRouter(routerA)
	tracker := traffic.New()
	b.mesh.SetTrafficTracker(tracker)

	// Connect A↔B and B↔C (not A↔C).
	require.NoError(t, a.mesh.Connect(ctx, b.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: b.port},
	}))
	require.NoError(t, b.mesh.Connect(ctx, c.peerKey, []*net.UDPAddr{
		{IP: net.IPv4(127, 0, 0, 1), Port: c.port},
	}))

	// Wait for bidirectional sessions.
	require.Eventually(t, func() bool {
		_, ab := b.mesh.GetConn(a.peerKey)
		_, bc := c.mesh.GetConn(b.peerKey)
		return ab && bc
	}, 5*time.Second, 25*time.Millisecond)

	// A opens a routed stream to C.
	streamA, err := a.mesh.OpenStream(ctx, c.peerKey)
	require.NoError(t, err)

	// C accepts the stream.
	acceptCtx, acceptCancel := context.WithTimeout(ctx, 5*time.Second)
	defer acceptCancel()
	streamC, stype, peerKey, err := c.mesh.AcceptAllStreams(acceptCtx)
	require.NoError(t, err)
	require.Equal(t, mesh.StreamTypeTunnel, stype)
	require.Equal(t, a.peerKey, peerKey)

	// A→C: send payload.
	payload := []byte("hello from A to C")
	_, err = streamA.Write(payload)
	require.NoError(t, err)

	buf := make([]byte, 256)
	n, err := streamC.Read(buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf[:n])

	// C→A: send response.
	response := []byte("reply from C to A")
	_, err = streamC.Write(response)
	require.NoError(t, err)

	n, err = streamA.Read(buf)
	require.NoError(t, err)
	require.Equal(t, response, buf[:n])

	// Close streams.
	_ = streamA.Close()
	_ = streamC.Close()

	// Rotate the tracker to get the window snapshot.
	snap, changed := tracker.RotateAndSnapshot()
	require.True(t, changed)

	// Assert directionality on both legs.
	// Bytes B read from A (A's outbound data forwarded through B).
	require.Contains(t, snap, a.peerKey)
	require.Greater(t, snap[a.peerKey].BytesIn, uint64(0))
	// Bytes B wrote to A (C's response forwarded back through B).
	require.Greater(t, snap[a.peerKey].BytesOut, uint64(0))

	// Bytes B wrote to C (A's data forwarded to C).
	require.Contains(t, snap, c.peerKey)
	require.Greater(t, snap[c.peerKey].BytesOut, uint64(0))
	// Bytes B read from C (C's response).
	require.Greater(t, snap[c.peerKey].BytesIn, uint64(0))
}
