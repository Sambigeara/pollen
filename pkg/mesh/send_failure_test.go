package mesh

import (
	"errors"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestHandleSendFailureRemovesSessionAndEmitsDisconnect(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	m := &impl{
		log:      zap.New(core).Sugar(),
		sessions: newSessionRegistry(),
		inCh:     make(chan peer.Input, 1),
	}
	peerKey := testMeshPeerKey(1)
	session := &peerSession{}
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
		sessions: newSessionRegistry(),
		inCh:     make(chan peer.Input, 1),
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
