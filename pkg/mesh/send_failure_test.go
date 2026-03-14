package mesh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// newTestQUICConn creates a minimal loopback QUIC connection for tests that
// need a non-nil *quic.Conn without full mesh authentication.
func newTestQUICConn(t *testing.T) *quic.Conn {
	t.Helper()

	_, serverPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serverCert, err := GenerateIdentityCert(serverPriv, nil, time.Hour)
	require.NoError(t, err)

	serverUDP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	serverQT := &quic.Transport{Conn: serverUDP}

	ln, err := serverQT.Listen(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCert},
		NextProtos:   []string{"test"},
	}, &quic.Config{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			_ = c.CloseWithError(0, "ok")
		}
	}()

	clientUDP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	clientQT := &quic.Transport{Conn: clientUDP}

	conn, err := clientQT.Dial(ctx, serverUDP.LocalAddr(), &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{"test"},
	}, &quic.Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		cancel()
		_ = conn.CloseWithError(0, "test done")
		_ = ln.Close()
		_ = serverQT.Close()
		_ = clientQT.Close()
	})

	return conn
}

func TestHandleSendFailureRemovesSessionAndEmitsDisconnect(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	m := &impl{
		log:      zap.New(core).Sugar(),
		sessions: newSessionRegistry(nil),
		inCh:     make(chan peer.Input, 1),
		metrics:  &metrics.MeshMetrics{},
	}
	peerKey := testMeshPeerKey(1)
	session := &peerSession{conn: newTestQUICConn(t)}
	m.sessions.peers[peerKey] = session

	m.handleSendFailure(peerKey, session, &quic.ApplicationError{ErrorMessage: string(CloseReasonTopologyPrune)})

	_, ok := m.sessions.get(peerKey)
	require.False(t, ok)

	select {
	case in := <-m.inCh:
		disconnect, ok := in.(peer.PeerDisconnected)
		require.True(t, ok)
		require.Equal(t, peerKey, disconnect.PeerKey)
		require.Equal(t, peer.DisconnectTopologyPrune, disconnect.Reason)
	default:
		t.Fatal("expected disconnect event")
	}
}

func TestHandleSendFailureIgnoresUnknownErrors(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	m := &impl{
		log:      zap.New(core).Sugar(),
		sessions: newSessionRegistry(nil),
		inCh:     make(chan peer.Input, 1),
		metrics:  &metrics.MeshMetrics{},
	}
	peerKey := testMeshPeerKey(1)
	session := &peerSession{}
	m.sessions.peers[peerKey] = session

	m.handleSendFailure(peerKey, session, errors.New("boom"))

	_, ok := m.sessions.get(peerKey)
	require.True(t, ok)
	select {
	case <-m.inCh:
		t.Fatal("unexpected disconnect event")
	default:
	}
}

func testMeshPeerKey(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}
