// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func testPeerKey(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func setupTestStore() *peerStore {
	dummyTrans := &QUICTransport{
		metrics: metrics.NewMeshMetrics(metricnoop.NewMeterProvider()),
	}
	return newPeerStore(dummyTrans)
}

func TestPeerDiscoveryLifecycle(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)
	ip := net.ParseIP("10.0.0.1")

	s.Discover(pk, []net.IP{ip}, 9000, nil, true, true)

	s.mu.RLock()
	p, ok := s.m[pk]
	s.mu.RUnlock()

	require.True(t, ok)
	require.Equal(t, peerStateDiscovered, p.State)
	require.Equal(t, connectStageDirect, p.Stage)

	s.MarkConnected(pk, ip, 9000)

	s.mu.RLock()
	p, ok = s.m[pk]
	s.mu.RUnlock()

	require.True(t, ok)
	require.Equal(t, peerStateConnected, p.State)
	require.Equal(t, 9000, p.ObservedPort)

	s.Disconnect(pk, disconnectIdleTimeout)

	s.mu.RLock()
	p, ok = s.m[pk]
	s.mu.RUnlock()

	require.True(t, ok)
	require.Equal(t, peerStateDiscovered, p.State)
	require.True(t, p.NextActionAt.After(time.Now()))
}

func TestPrivatePeerSkipsDirectStage(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, nil, false, false)

	s.mu.RLock()
	p, ok := s.m[pk]
	s.mu.RUnlock()

	require.True(t, ok)
	require.Equal(t, connectStagePunch, p.Stage)
}

func TestEagerRetryStageWithLastAddr(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)
	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, lastAddr, false, false)

	s.mu.RLock()
	p, ok := s.m[pk]
	s.mu.RUnlock()

	require.True(t, ok)
	require.Equal(t, connectStageEagerRetry, p.Stage)
}

func TestFailedEagerRetrySkipsDirectForPunchOnlyPeer(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)
	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, lastAddr, false, false)
	s.mu.Lock()
	s.m[pk].State = peerStateConnecting
	s.failConnectLocked(pk, time.Now())
	p := s.m[pk]
	s.mu.Unlock()

	require.Equal(t, connectStagePunch, p.Stage)
	require.Equal(t, 0, p.StageAttempts)
}

func TestDiscoverPrivateRouteClearsStaleLastAddr(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)
	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, lastAddr, false, false)
	s.Discover(pk, []net.IP{net.ParseIP("192.168.0.24")}, 9000, lastAddr, true, false)

	s.mu.RLock()
	p := s.m[pk]
	s.mu.RUnlock()

	require.Nil(t, p.LastAddr)
	require.Equal(t, connectStageDirect, p.Stage)
	require.Equal(t, 0, p.StageAttempts)
}

func TestNudgeResetsToCurrentReachabilityStage(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)
	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, lastAddr, false, false)
	s.mu.Lock()
	p := s.m[pk]
	p.State = peerStateUnreachable
	p.NextActionAt = time.Now().Add(time.Hour)
	p.StageAttempts = 1
	p.Stage = connectStagePunch
	s.mu.Unlock()

	s.Nudge(pk)

	s.mu.RLock()
	p = s.m[pk]
	s.mu.RUnlock()

	require.Equal(t, peerStateDiscovered, p.State, "nudge pulls peer out of unreachable wait")
	require.Equal(t, connectStageEagerRetry, p.Stage, "nudge uses the current discovered reachability")
	require.Equal(t, 0, p.StageAttempts)
	require.Equal(t, lastAddr, p.LastAddr)
	require.WithinDuration(t, time.Now(), p.NextActionAt, time.Second)
}

func TestNudgeIsNoopForConnectedPeer(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, nil, true, true)
	s.MarkConnected(pk, net.ParseIP("203.0.113.5"), 41234)
	originalNextAction := s.m[pk].NextActionAt

	s.Nudge(pk)

	s.mu.RLock()
	p := s.m[pk]
	s.mu.RUnlock()

	require.Equal(t, peerStateConnected, p.State)
	require.Equal(t, originalNextAction, p.NextActionAt, "nudge is a no-op for connected peers")
}

func TestFailConnectEscalation(t *testing.T) {
	s := setupTestStore()
	pk := testPeerKey(1)

	s.Discover(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9000, nil, true, true)

	s.mu.Lock()
	s.m[pk].State = peerStateConnecting
	s.mu.Unlock()

	// Direct attempt 1
	s.mu.Lock()
	s.failConnectLocked(pk, time.Now())
	s.mu.Unlock()

	require.Equal(t, connectStageDirect, s.m[pk].Stage)
	require.Equal(t, 1, s.m[pk].StageAttempts)

	s.mu.Lock()
	s.m[pk].State = peerStateConnecting
	s.mu.Unlock()

	// Direct attempt 2 -> Escalate to Punch
	s.mu.Lock()
	s.failConnectLocked(pk, time.Now())
	s.mu.Unlock()

	require.Equal(t, connectStagePunch, s.m[pk].Stage)
	require.Equal(t, 0, s.m[pk].StageAttempts)
}
