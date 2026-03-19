package transport

import (
	"errors"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestHandleSendFailureRemovesSessionAndEmitsDisconnect(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	m := &impl{
		log:         zap.New(core).Sugar(),
		sessions:    newSessionRegistry(metrics.NewMeshMetrics(metricnoop.NewMeterProvider()).SessionsActive),
		peers:       newPeerStore(),
		peerEventCh: make(chan PeerEvent, 8),
		metrics:     metrics.NewMeshMetrics(metricnoop.NewMeterProvider()),
	}
	peerKey := testPeerKey(1)
	session := &peerSession{}
	m.sessions.peers[peerKey] = session

	m.handleSendFailure(peerKey, session, &quic.ApplicationError{ErrorMessage: string(closeReasonTopologyPrune)})

	_, ok := m.sessions.get(peerKey)
	require.False(t, ok)

	select {
	case ev := <-m.peerEventCh:
		require.Equal(t, PeerEventDisconnected, ev.Type)
		require.Equal(t, peerKey, ev.Key)
	default:
		t.Fatal("expected disconnect event on peer event channel")
	}
}

func TestHandleSendFailureIgnoresUnknownErrors(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	m := &impl{
		log:         zap.New(core).Sugar(),
		sessions:    newSessionRegistry(metrics.NewMeshMetrics(metricnoop.NewMeterProvider()).SessionsActive),
		peers:       newPeerStore(),
		peerEventCh: make(chan PeerEvent, 8),
		metrics:     metrics.NewMeshMetrics(metricnoop.NewMeterProvider()),
	}
	peerKey := testPeerKey(1)
	session := &peerSession{}
	m.sessions.peers[peerKey] = session

	m.handleSendFailure(peerKey, session, errors.New("boom"))

	_, ok := m.sessions.get(peerKey)
	require.True(t, ok)
	select {
	case <-m.peerEventCh:
		t.Fatal("unexpected disconnect event")
	default:
	}
}
