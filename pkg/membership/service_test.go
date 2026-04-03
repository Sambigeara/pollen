package membership

import (
	"context"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

func testCreds() *auth.NodeCredentials {
	return auth.NewNodeCredentials(nil, &admissionv1.DelegationCert{
		Claims: &admissionv1.DelegationCertClaims{
			NotAfterUnix: time.Now().Add(24 * time.Hour).Unix(),
		},
	})
}

func newTestService(localID types.PeerKey) (*Service, *fakeNetwork, *fakeClusterState) {
	net := newFakeNetwork()
	st := newFakeClusterState(localID)
	svc := New(localID, testCreds(), net, st, Config{
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
		NATDetector:      nat.NewDetector(),
		ReconnectWindow:  time.Hour,
	})
	return svc, net, st
}

func TestStartAndStop(t *testing.T) {
	svc, _, _ := newTestService(peerKey(1))

	err := svc.Start(context.Background())
	require.NoError(t, err)

	err = svc.Stop()
	require.NoError(t, err)
}

func TestEventsChannelClosed(t *testing.T) {
	svc, _, _ := newTestService(peerKey(1))

	err := svc.Start(context.Background())
	require.NoError(t, err)

	require.NoError(t, svc.Stop())

	_, ok := <-svc.Events()
	require.False(t, ok, "events channel should be closed after Stop")
}

func TestDenyPeerEmitsEvent(t *testing.T) {
	svc, _, _ := newTestService(peerKey(1))
	t.Cleanup(func() {
		require.NoError(t, svc.Stop())
	})

	err := svc.Start(context.Background())
	require.NoError(t, err)

	require.NoError(t, svc.DenyPeer(peerKey(2)))

	select {
	case ev := <-svc.Events():
		_, ok := ev.(state.PeerDenied)
		require.True(t, ok, "expected PeerDenied event, got %T", ev)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for deny event")
	}
}

func TestControlMetricsReturnsCoord(t *testing.T) {
	svc, _, _ := newTestService(peerKey(1))

	svc.mu.Lock()
	svc.localCoord = coords.Coord{X: 42, Y: 7}
	svc.mu.Unlock()

	m := svc.ControlMetrics()
	require.InDelta(t, 42.0, m.LocalCoord.X, 0.01)
	require.InDelta(t, 7.0, m.LocalCoord.Y, 0.01)
}

func TestForwardEventsFiltersGossipApplied(t *testing.T) {
	svc, _, _ := newTestService(peerKey(1))

	svc.forwardEvents([]state.Event{
		state.PeerJoined{Key: peerKey(2)},
		state.GossipApplied{},
		state.PeerDenied{Key: peerKey(3)},
	})

	ev1 := <-svc.events
	_, ok := ev1.(state.PeerJoined)
	require.True(t, ok)

	ev2 := <-svc.events
	_, ok = ev2.(state.PeerDenied)
	require.True(t, ok)

	select {
	case ev := <-svc.events:
		t.Fatalf("unexpected event: %T", ev)
	default:
	}
}

func TestPeerEventConnectedUpdatesReachable(t *testing.T) {
	svc, net, st := newTestService(peerKey(1))
	t.Cleanup(func() {
		require.NoError(t, svc.Stop())
	})

	peer := peerKey(2)
	net.setConnectedPeers(peer)

	err := svc.Start(context.Background())
	require.NoError(t, err)

	net.peerEvents <- transport.PeerEvent{
		Key:  peer,
		Type: transport.PeerEventConnected,
	}

	require.Eventually(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.localReachable) > 0
	}, time.Second, 10*time.Millisecond)
}

func TestPeerEventDisconnectedUpdatesReachable(t *testing.T) {
	svc, net, st := newTestService(peerKey(1))
	t.Cleanup(func() {
		require.NoError(t, svc.Stop())
	})

	peer := peerKey(2)
	net.setConnectedPeers(peer)

	err := svc.Start(context.Background())
	require.NoError(t, err)

	net.peerEvents <- transport.PeerEvent{
		Key:  peer,
		Type: transport.PeerEventConnected,
	}
	require.Eventually(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.localReachable) > 0
	}, time.Second, 10*time.Millisecond)

	net.setConnectedPeers()
	net.peerEvents <- transport.PeerEvent{
		Key:  peer,
		Type: transport.PeerEventDisconnected,
	}
	require.Eventually(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.localReachable) == 0
	}, time.Second, 10*time.Millisecond)
}
