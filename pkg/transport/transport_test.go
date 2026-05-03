// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"maps"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/internal/testauth"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/transport"
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

func (c *testConsumer) Export() []*statev1.ConsumedInvite { return nil }

type trafficRecord struct {
	bytesIn  uint64
	bytesOut uint64
}

type testTrafficTracker struct {
	mu      sync.Mutex
	records map[types.PeerKey]trafficRecord
}

func (t *testTrafficTracker) Record(peer types.PeerKey, bytesIn, bytesOut uint64) {
	t.mu.Lock()
	r := t.records[peer]
	r.bytesIn += bytesIn
	r.bytesOut += bytesOut
	t.records[peer] = r
	t.mu.Unlock()
}

func (t *testTrafficTracker) snapshot() map[types.PeerKey]trafficRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[types.PeerKey]trafficRecord, len(t.records))
	maps.Copy(out, t.records)
	return out
}

type meshHarness struct {
	mesh    *transport.QUICTransport
	peerKey types.PeerKey
	pubKey  ed25519.PublicKey
	port    int
	cancel  context.CancelFunc
}

func (h *meshHarness) loopbackAddr() []netip.AddrPort {
	return []netip.AddrPort{netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), uint16(h.port))}
}

func TestJoinWithTokenHappyPath(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	bootstrap := startMeshHarness(t, cluster)
	joiner := startMeshHarness(t, cluster)

	token := cluster.TokenFor(t, joiner.pubKey, bootstrap.pubKey, net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port)))

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
	clusterA := testauth.NewClusterAuth(t)
	clusterB := testauth.NewClusterAuth(t)

	a := startMeshHarness(t, clusterA)
	b := startMeshHarness(t, clusterB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := a.mesh.Connect(ctx, b.peerKey, b.loopbackAddr())
	require.Error(t, err)

	_, ok := a.mesh.GetConn(b.peerKey)
	require.False(t, ok)
}

func TestJoinWithInviteHappyPath(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	bootstrapCreds := cluster.CredsFor(t, bootstrapPub)
	signer := cluster.Signer(t)
	bootstrapCreds.SetDelegationKey(signer)
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	joiner := startMeshHarness(t, cluster)

	invite, err := cluster.Signer(t).IssueInviteToken(
		nil,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
		}},
		time.Now(),
		time.Hour,
		0,
		nil,
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

func TestRedeemInviteRacesAddresses(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	bootstrapCreds := cluster.CredsFor(t, bootstrapPub)
	bootstrapCreds.SetDelegationKey(cluster.Signer(t))
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	blackhole, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	t.Cleanup(func() { _ = blackhole.Close() })
	blackholeAddr := blackhole.LocalAddr().(*net.UDPAddr) //nolint:forcetypeassert

	joinerPub, joinerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	invite, err := cluster.Signer(t).IssueInviteToken(
		joinerPub,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs: []string{
				blackholeAddr.String(),
				net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port)),
			},
		}},
		time.Now(),
		time.Hour,
		time.Hour,
		nil,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	joinToken, err := transport.RedeemInvite(ctx, joinerPriv, invite)
	require.NoError(t, err)
	_, err = auth.VerifyJoinToken(joinToken, joinerPub, time.Now())
	require.NoError(t, err)
}

func TestDiscoverPeerWakesDialer(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	a := startMeshHarness(t, cluster, transport.WithPeerTickInterval(time.Hour))
	b := startMeshHarness(t, cluster)

	a.mesh.DiscoverPeer(b.peerKey, []net.IP{net.IPv4(127, 0, 0, 1)}, b.port, nil, true, true)

	require.Eventually(t, func() bool {
		_, ok := a.mesh.GetConn(b.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond, "discover should wake the dialer rather than wait a full tick")
}

func TestJoinWithInviteRejectsExpiredInviteTTL(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	bootstrapCreds := cluster.CredsFor(t, bootstrapPub)
	signer := cluster.Signer(t)
	bootstrapCreds.SetDelegationKey(signer)
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	joiner := startMeshHarness(t, cluster)

	invite, err := cluster.Signer(t).IssueInviteToken(
		nil,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
		}},
		time.Now().Add(-2*time.Second),
		time.Second,
		0,
		nil,
	)
	require.NoError(t, err)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = joiner.mesh.JoinWithInvite(joinCtx, invite)
	require.Error(t, err)
}

func TestConnectReturnsErrIdentityMismatch(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)

	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	wrongPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	wrongKey := types.PeerKeyFromBytes(wrongPub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = a.mesh.Connect(ctx, wrongKey, b.loopbackAddr())
	require.Error(t, err)
	require.True(t, errors.Is(err, transport.ErrIdentityMismatch), "expected ErrIdentityMismatch, got: %v", err)
}

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

func (r *notifyRouter) NextHop(dest types.PeerKey) (types.PeerKey, bool) {
	r.mu.Lock()

	select {
	case <-r.lookupCh:
	default:
		close(r.lookupCh)
	}

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
	cluster := testauth.NewClusterAuth(t)
	b := startMeshHarness(t, cluster)

	cPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerC := types.PeerKeyFromBytes(cPub)

	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{peerC: b.peerKey})
	a := startMeshHarness(t, cluster, transport.WithRouter(router))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, peerC, transport.StreamTypeTunnel)
		ch <- result{s, err}
	}()

	require.Eventually(t, func() bool {
		select {
		case <-router.lookupCh:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, a.mesh.Connect(ctx, b.peerKey, b.loopbackAddr()))

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.err == nil {
				_ = r.stream.Close()
			}
			require.NotErrorIs(t, r.err, context.DeadlineExceeded)
			return true
		default:
			return false
		}
	}, 5*time.Second, 25*time.Millisecond, "OpenStream hung waiting for unreachable peer instead of next hop")
}

func TestOpenStreamRouteChurn(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)

	bPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerB := types.PeerKeyFromBytes(bPub)

	cPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerC := types.PeerKeyFromBytes(cPub)

	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{peerC: peerB})
	a := startMeshHarness(t, cluster, transport.WithRouter(router))
	d := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, a.mesh.Connect(ctx, d.peerKey, d.loopbackAddr()))
	require.Eventually(t, func() bool {
		_, ok := d.mesh.GetConn(a.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, peerC, transport.StreamTypeTunnel)
		ch <- result{s, err}
	}()

	router.update(map[types.PeerKey]types.PeerKey{peerC: d.peerKey})

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.err == nil {
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
	cluster := testauth.NewClusterAuth(t)
	c := startMeshHarness(t, cluster)

	bPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerB := types.PeerKeyFromBytes(bPub)

	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{c.peerKey: peerB})
	a := startMeshHarness(t, cluster, transport.WithRouter(router))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		stream io.ReadWriteCloser
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := a.mesh.OpenStream(ctx, c.peerKey, transport.StreamTypeTunnel)
		ch <- result{s, err}
	}()

	require.NoError(t, a.mesh.Connect(ctx, c.peerKey, c.loopbackAddr()))

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.err == nil {
				_ = r.stream.Close()
			}
			require.NotErrorIs(t, r.err, context.DeadlineExceeded)
			return true
		default:
			return false
		}
	}, 5*time.Second, 25*time.Millisecond, "OpenStream hung on stale route instead of using direct session")
}

func TestOpenStreamRouteUpdateBetweenSnapshotAndLookup(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)

	bPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerB := types.PeerKeyFromBytes(bPub)

	cPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerC := types.PeerKeyFromBytes(cPub)

	router := newNotifyRouter(map[types.PeerKey]types.PeerKey{peerC: peerB})
	a := startMeshHarness(t, cluster, transport.WithRouter(router))
	d := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, a.mesh.Connect(ctx, d.peerKey, d.loopbackAddr()))
	require.Eventually(t, func() bool {
		_, ok := d.mesh.GetConn(a.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

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
		s, err := a.mesh.OpenStream(ctx, peerC, transport.StreamTypeTunnel)
		ch <- result{s, err}
	}()

	require.Eventually(t, func() bool {
		select {
		case r := <-ch:
			if r.err == nil {
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
		typeDigest byte = 1
		typeTunnel byte = 2
		headerLen       = 66 // routeHeaderSize
		ttl        byte = 16
	)

	cluster := testauth.NewClusterAuth(t)
	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, a.mesh.Connect(ctx, b.peerKey, b.loopbackAddr()))
	require.Eventually(t, func() bool {
		_, ok := b.mesh.GetConn(a.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	conn, ok := a.mesh.GetConn(b.peerKey)
	require.True(t, ok)

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	var hdr [1 + headerLen]byte
	hdr[0] = typeRouted
	copy(hdr[1:33], b.peerKey[:])  // dest = B (local delivery)
	copy(hdr[33:65], a.peerKey[:]) // source = A
	hdr[65] = ttl
	hdr[66] = typeDigest
	_, err = stream.Write(hdr[:])
	require.NoError(t, err)

	_ = stream.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = stream.Read(buf)
	require.Error(t, err, "expected stream reset for non-tunnel inner type")

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

	rwc, stype, peerKey, err := b.mesh.AcceptStream(acceptCtx)
	require.NoError(t, err)
	require.Equal(t, transport.StreamTypeTunnel, stype)
	require.Equal(t, a.peerKey, peerKey)
	_ = rwc.Close()
	stream2.CancelRead(0)
	stream2.CancelWrite(0)
}

func TestSimultaneousDialConverges(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = a.mesh.Connect(ctx, b.peerKey, b.loopbackAddr())
	}()
	go func() {
		defer wg.Done()
		_ = b.mesh.Connect(ctx, a.peerKey, a.loopbackAddr())
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		_, aHasB := a.mesh.GetConn(b.peerKey)
		_, bHasA := b.mesh.GetConn(a.peerKey)
		return aHasB && bHasA
	}, 5*time.Second, 25*time.Millisecond, "peers should see each other after simultaneous dial")

	time.Sleep(500 * time.Millisecond)
	_, aHasB := a.mesh.GetConn(b.peerKey)
	_, bHasA := b.mesh.GetConn(a.peerKey)
	require.True(t, aHasB, "a lost connection to b after simultaneous dial")
	require.True(t, bHasA, "b lost connection to a after simultaneous dial")
}

func startMeshHarness(t *testing.T, cluster *testauth.ClusterAuth, opts ...transport.Option) *meshHarness {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return startMeshHarnessWithCreds(t, priv, pub, cluster.CredsFor(t, pub), opts...)
}

func startMeshHarnessWithCreds(
	t *testing.T,
	priv ed25519.PrivateKey,
	pub ed25519.PublicKey,
	creds *auth.NodeCredentials,
	extraOpts ...transport.Option,
) *meshHarness {
	t.Helper()

	peerKey := types.PeerKeyFromBytes(pub)
	opts := make([]transport.Option, 0, 5+len(extraOpts))
	opts = append(opts,
		transport.WithSigningKey(priv),
		transport.WithTLSIdentityTTL(config.DefaultTLSIdentityTTL),
		transport.WithMembershipTTL(config.DefaultMembershipTTL),
		transport.WithReconnectWindow(config.DefaultReconnectWindow),
		transport.WithInviteConsumer(&testConsumer{seen: make(map[string]struct{})}),
	)
	opts = append(opts, extraOpts...)
	m, err := transport.New(peerKey, creds, ":0", opts...)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.Start(ctx))

	h := &meshHarness{
		mesh:    m,
		peerKey: peerKey,
		pubKey:  pub,
		port:    m.ListenPort(),
		cancel:  cancel,
	}

	t.Cleanup(func() {
		h.cancel()
		_ = h.mesh.Stop()
	})

	return h
}

func TestRoutedRelayTrafficAttribution(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	b := startMeshHarness(t, cluster)
	c := startMeshHarness(t, cluster)

	routerA := newNotifyRouter(map[types.PeerKey]types.PeerKey{c.peerKey: b.peerKey})
	a := startMeshHarness(t, cluster, transport.WithRouter(routerA))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tracker := &testTrafficTracker{records: make(map[types.PeerKey]trafficRecord)}
	b.mesh.SetTrafficTracker(tracker)

	require.NoError(t, a.mesh.Connect(ctx, b.peerKey, b.loopbackAddr()))
	require.NoError(t, b.mesh.Connect(ctx, c.peerKey, c.loopbackAddr()))

	require.Eventually(t, func() bool {
		_, ab := b.mesh.GetConn(a.peerKey)
		_, bc := c.mesh.GetConn(b.peerKey)
		return ab && bc
	}, 5*time.Second, 25*time.Millisecond)

	streamA, err := a.mesh.OpenStream(ctx, c.peerKey, transport.StreamTypeTunnel)
	require.NoError(t, err)

	acceptCtx, acceptCancel := context.WithTimeout(ctx, 5*time.Second)
	defer acceptCancel()
	streamC, stype, peerKey, err := c.mesh.AcceptStream(acceptCtx)
	require.NoError(t, err)
	require.Equal(t, transport.StreamTypeTunnel, stype)
	require.Equal(t, a.peerKey, peerKey)

	payload := []byte("hello from A to C")
	_, err = streamA.Write(payload)
	require.NoError(t, err)

	buf := make([]byte, 256)
	n, err := streamC.Read(buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf[:n])

	response := []byte("reply from C to A")
	_, err = streamC.Write(response)
	require.NoError(t, err)

	n, err = streamA.Read(buf)
	require.NoError(t, err)
	require.Equal(t, response, buf[:n])

	_ = streamA.Close()
	_ = streamC.Close()

	snap := tracker.snapshot()

	require.Contains(t, snap, a.peerKey)
	require.Greater(t, snap[a.peerKey].bytesIn, uint64(0))
	require.Greater(t, snap[a.peerKey].bytesOut, uint64(0))

	require.Contains(t, snap, c.peerKey)
	require.Greater(t, snap[c.peerKey].bytesOut, uint64(0))
	require.Greater(t, snap[c.peerKey].bytesIn, uint64(0))
}
