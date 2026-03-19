package transport

import (
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func testPeerKey(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func setupConnectedPeer(t *testing.T, s *peerStore, key types.PeerKey, now time.Time) {
	t.Helper()
	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})
	s.step(now, connectPeer{
		PeerKey:      key,
		IP:           net.ParseIP("10.0.0.1"),
		ObservedPort: 9000,
	})
}

func TestDisconnectReasonRetryDelays(t *testing.T) {
	tests := []struct {
		reason disconnectReason
		delay  time.Duration
	}{
		{disconnectIdleTimeout, idleTimeoutRetryInterval},
		{disconnectReset, resetRetryInterval},
		{disconnectGraceful, gracefulDisconnectRetryInterval},
		{disconnectTopologyPrune, unreachableRetryInterval},
		{disconnectDenied, unreachableRetryInterval},
		{disconnectUnknown, unknownDisconnectRetryInterval},
	}

	for _, tt := range tests {
		t.Run(tt.reason.String(), func(t *testing.T) {
			s := newPeerStore()
			key := testPeerKey(1)
			now := time.Now()

			setupConnectedPeer(t, s, key, now)

			s.step(now, peerDisconnected{PeerKey: key, Reason: tt.reason})

			p, ok := s.get(key)
			require.True(t, ok)
			require.Equal(t, peerStateDiscovered, p.State)
			require.Equal(t, connectStageEagerRetry, p.Stage)
			require.Equal(t, 0, p.StageAttempts)
			require.Equal(t, now.Add(tt.delay), p.NextActionAt)
		})
	}
}

func TestDisconnectTickReconnectCycle(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	setupConnectedPeer(t, s, key, now)

	s.step(now, peerDisconnected{PeerKey: key, Reason: disconnectIdleTimeout})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, peerStateDiscovered, p.State)

	outputs := s.step(now.Add(500*time.Millisecond), tick{})
	require.Empty(t, outputs)

	outputs = s.step(now.Add(2*time.Second), tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(attemptEagerConnect)
	require.True(t, ok)
	require.Equal(t, key, ec.PeerKey)
	require.NotNil(t, ec.Addr)

	p, _ = s.get(key)
	require.Equal(t, peerStateConnecting, p.State)

	s.step(now.Add(3*time.Second), connectPeer{PeerKey: key, IP: net.ParseIP("10.0.0.1"), ObservedPort: 9000})
	p, _ = s.get(key)
	require.Equal(t, peerStateConnected, p.State)
}

func TestStageEscalation(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	outputs := s.step(now, tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(attemptConnect)
	require.True(t, isAttempt)

	s.step(now, connectFailed{PeerKey: key})
	p, _ := s.get(key)
	require.Equal(t, connectStageDirect, p.Stage)
	require.Equal(t, 1, p.StageAttempts)

	outputs = s.step(now.Add(2*time.Second), tick{})
	require.Len(t, outputs, 1)
	s.step(now.Add(2*time.Second), connectFailed{PeerKey: key})
	p, _ = s.get(key)
	require.Equal(t, connectStagePunch, p.Stage)
	require.Equal(t, 0, p.StageAttempts)

	outputs = s.step(now.Add(3*time.Second), tick{})
	require.Len(t, outputs, 1)
	_, isPunch := outputs[0].(requestPunchCoordination)
	require.True(t, isPunch)
	s.step(now.Add(3*time.Second), connectFailed{PeerKey: key})
	p, _ = s.get(key)
	require.Equal(t, connectStagePunch, p.Stage)
	require.Equal(t, 1, p.StageAttempts)

	outputs = s.step(now.Add(5*time.Second), tick{})
	require.Len(t, outputs, 1)
	s.step(now.Add(5*time.Second), connectFailed{PeerKey: key})
	p, _ = s.get(key)
	require.Equal(t, peerStateUnreachable, p.State)
	require.Equal(t, now.Add(5*time.Second).Add(unreachableRetryInterval), p.NextActionAt)
}

func TestEagerRetryStage(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}
	s.step(now, discoverPeer{
		PeerKey:  key,
		Ips:      []net.IP{net.ParseIP("10.0.0.1")},
		Port:     9000,
		LastAddr: lastAddr,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageEagerRetry, p.Stage)

	outputs := s.step(now, tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(attemptEagerConnect)
	require.True(t, ok)
	require.Equal(t, key, ec.PeerKey)
	require.Equal(t, lastAddr, ec.Addr)

	s.step(now, connectFailed{PeerKey: key})
	p, _ = s.get(key)
	require.Equal(t, connectStageDirect, p.Stage)
	require.Equal(t, 0, p.StageAttempts)

	outputs = s.step(now.Add(2*time.Second), tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(attemptConnect)
	require.True(t, isAttempt)
}

func TestEagerRetrySkippedWhenNoLastAddr(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageDirect, p.Stage)

	outputs := s.step(now, tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(attemptConnect)
	require.True(t, isAttempt)
}

func TestEagerRetryWithOnlyLastAddr(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.8"), Port: 41000}
	s.step(now, discoverPeer{PeerKey: key, LastAddr: lastAddr})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageEagerRetry, p.Stage)

	outputs := s.step(now, tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(attemptEagerConnect)
	require.True(t, ok)
	require.Equal(t, key, ec.PeerKey)
	require.Equal(t, lastAddr, ec.Addr)
}

func TestForgetPeer(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	setupConnectedPeer(t, s, key, now)

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, peerStateConnected, p.State)

	s.step(now, peerDisconnected{PeerKey: key, Reason: disconnectCertExpired})
	s.step(now, forgetPeer{PeerKey: key})

	_, ok = s.get(key)
	require.False(t, ok, "peer should be removed from store after forgetPeer")

	outputs := s.step(now.Add(time.Minute), tick{})
	require.Empty(t, outputs)
}

func TestUnreachableRetryReturnsToEagerWithLastAddr(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 42000}
	s.step(now, discoverPeer{
		PeerKey:  key,
		Ips:      []net.IP{net.ParseIP("10.0.0.1")},
		Port:     9000,
		LastAddr: lastAddr,
	})

	s.step(now, tick{})
	s.step(now, connectFailed{PeerKey: key})
	s.step(now.Add(2*time.Second), tick{})
	s.step(now.Add(2*time.Second), connectFailed{PeerKey: key})
	s.step(now.Add(4*time.Second), tick{})
	s.step(now.Add(4*time.Second), connectFailed{PeerKey: key})

	s.step(now.Add(6*time.Second), tick{})
	s.step(now.Add(6*time.Second), connectFailed{PeerKey: key})
	outputs := s.step(now.Add(8*time.Second), tick{})
	require.Len(t, outputs, 1)
	_, isPunch := outputs[0].(requestPunchCoordination)
	require.True(t, isPunch)
	s.step(now.Add(8*time.Second), connectFailed{PeerKey: key})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, peerStateUnreachable, p.State)

	outputs = s.step(now.Add(8*time.Second).Add(unreachableRetryInterval), tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(attemptEagerConnect)
	require.True(t, ok)
	require.Equal(t, key, ec.PeerKey)
	require.Equal(t, lastAddr, ec.Addr)
}

func TestPrivatePeerSkipsDirectStage(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: false,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStagePunch, p.Stage)

	outputs := s.step(now, tick{})
	require.Len(t, outputs, 1)
	_, isPunch := outputs[0].(requestPunchCoordination)
	require.True(t, isPunch)
}

func TestPublicPeerStartsAtDirect(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageDirect, p.Stage)
}

func TestPrivatePeerWithLastAddrStartsEager(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}
	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		LastAddr:           lastAddr,
		PubliclyAccessible: false,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageEagerRetry, p.Stage)
}

func TestPrivatelyRoutablePeerStartsDirect(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.2.10")},
		Port:               9000,
		PrivatelyRoutable:  true,
		PubliclyAccessible: false,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageDirect, p.Stage)
}

func TestRediscoveredPrivatelyRoutablePeerRestagesToDirect(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.3.2.10")},
		Port:               9000,
		PubliclyAccessible: false,
	})

	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStagePunch, p.Stage)

	s.step(now.Add(time.Second), discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.3.2.10")},
		Port:               9000,
		PrivatelyRoutable:  true,
		PubliclyAccessible: false,
	})

	p, ok = s.get(key)
	require.True(t, ok)
	require.Equal(t, connectStageDirect, p.Stage)
	require.Zero(t, p.StageAttempts)
}

func TestConnectingTimeout(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	outputs := s.step(now, tick{})
	require.Len(t, outputs, 1)
	p, ok := s.get(key)
	require.True(t, ok)
	require.Equal(t, peerStateConnecting, p.State)

	outputs = s.step(now.Add(9*time.Second), tick{})
	require.Empty(t, outputs)
	p, _ = s.get(key)
	require.Equal(t, peerStateConnecting, p.State)

	outputs = s.step(now.Add(11*time.Second), tick{})
	require.Empty(t, outputs)
	p, _ = s.get(key)
	require.Equal(t, peerStateDiscovered, p.State)
	require.Equal(t, 1, p.StageAttempts)

	outputs = s.step(now.Add(12*time.Second), tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(attemptConnect)
	require.True(t, isAttempt)
}

func TestConnectingTimeoutDoesNotFireEarly(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: true,
	})

	s.step(now, tick{})

	s.step(now.Add(5*time.Second), connectPeer{PeerKey: key, IP: net.ParseIP("10.0.0.1"), ObservedPort: 9000})
	p, _ := s.get(key)
	require.Equal(t, peerStateConnected, p.State)

	outputs := s.step(now.Add(15*time.Second), tick{})
	require.Empty(t, outputs)
	p, _ = s.get(key)
	require.Equal(t, peerStateConnected, p.State)
}

func TestConnectingTimeoutPunchReachesUnreachable(t *testing.T) {
	s := newPeerStore()
	key := testPeerKey(1)
	now := time.Now()

	s.step(now, discoverPeer{
		PeerKey:            key,
		Ips:                []net.IP{net.ParseIP("10.0.0.1")},
		Port:               9000,
		PubliclyAccessible: false,
	})
	p, _ := s.get(key)
	require.Equal(t, connectStagePunch, p.Stage)

	s.step(now, tick{})
	p, _ = s.get(key)
	require.Equal(t, peerStateConnecting, p.State)

	t1 := now.Add(11 * time.Second)
	s.step(t1, tick{})
	p, _ = s.get(key)
	require.Equal(t, peerStateDiscovered, p.State)
	require.Equal(t, connectStagePunch, p.Stage)
	require.Equal(t, 1, p.StageAttempts)

	t2 := t1.Add(2 * time.Second)
	s.step(t2, tick{})
	p, _ = s.get(key)
	require.Equal(t, peerStateConnecting, p.State)

	t3 := t2.Add(11 * time.Second)
	s.step(t3, tick{})
	p, _ = s.get(key)
	require.Equal(t, peerStateUnreachable, p.State)
}
