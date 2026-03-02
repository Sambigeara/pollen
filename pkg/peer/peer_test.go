package peer

import (
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPeerKey(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func setupConnectedPeer(t *testing.T, s *Store, key types.PeerKey, now time.Time) {
	t.Helper()
	s.Step(now, DiscoverPeer{
		PeerKey: key,
		Ips:     []net.IP{net.ParseIP("10.0.0.1")},
		Port:    9000,
	})
	s.Step(now, ConnectPeer{
		PeerKey:      key,
		IP:           net.ParseIP("10.0.0.1"),
		ObservedPort: 9000,
	})
}

func TestDisconnectReasonRetryDelays(t *testing.T) {
	tests := []struct {
		reason DisconnectReason
		delay  time.Duration
	}{
		{DisconnectIdleTimeout, idleTimeoutRetryInterval},
		{DisconnectReset, resetRetryInterval},
		{DisconnectGraceful, gracefulDisconnectRetryInterval},
		{DisconnectUnknown, unknownDisconnectRetryInterval},
	}

	for _, tt := range tests {
		t.Run(tt.reason.String(), func(t *testing.T) {
			s := NewStore()
			key := testPeerKey(1)
			now := time.Now()

			setupConnectedPeer(t, s, key, now)

			s.Step(now, PeerDisconnected{PeerKey: key, Reason: tt.reason})

			p, ok := s.Get(key)
			require.True(t, ok)
			assert.Equal(t, PeerStateDiscovered, p.State)
			assert.Equal(t, ConnectStageEagerRetry, p.Stage)
			assert.Equal(t, 0, p.StageAttempts)
			assert.Equal(t, now.Add(tt.delay), p.NextActionAt)
		})
	}
}

func TestDisconnectTickReconnectCycle(t *testing.T) {
	s := NewStore()
	key := testPeerKey(1)
	now := time.Now()

	setupConnectedPeer(t, s, key, now)

	s.Step(now, PeerDisconnected{PeerKey: key, Reason: DisconnectIdleTimeout})

	p, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, PeerStateDiscovered, p.State)

	// Tick before retry delay elapses — no output.
	outputs := s.Step(now.Add(500*time.Millisecond), Tick{})
	assert.Empty(t, outputs)

	// Tick after retry delay — should emit AttemptEagerConnect (peer has LastAddr from prior connection).
	outputs = s.Step(now.Add(2*time.Second), Tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(AttemptEagerConnect)
	require.True(t, ok)
	assert.Equal(t, key, ec.PeerKey)
	assert.NotNil(t, ec.Addr)

	// Peer is now connecting.
	p, _ = s.Get(key)
	assert.Equal(t, PeerStateConnecting, p.State)

	// Reconnect succeeds.
	s.Step(now.Add(3*time.Second), ConnectPeer{PeerKey: key, IP: net.ParseIP("10.0.0.1"), ObservedPort: 9000})
	p, _ = s.Get(key)
	assert.Equal(t, PeerStateConnected, p.State)
}

func TestStageEscalation(t *testing.T) {
	s := NewStore()
	key := testPeerKey(1)
	now := time.Now()

	s.Step(now, DiscoverPeer{
		PeerKey: key,
		Ips:     []net.IP{net.ParseIP("10.0.0.1")},
		Port:    9000,
	})

	// First tick: attempt direct connect.
	outputs := s.Step(now, Tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(AttemptConnect)
	require.True(t, isAttempt)

	// Fail direct attempt 1.
	s.Step(now, ConnectFailed{PeerKey: key})
	p, _ := s.Get(key)
	assert.Equal(t, ConnectStageDirect, p.Stage)
	assert.Equal(t, 1, p.StageAttempts)

	// Tick + fail direct attempt 2 → escalate to punch.
	outputs = s.Step(now.Add(2*time.Second), Tick{})
	require.Len(t, outputs, 1)
	s.Step(now.Add(2*time.Second), ConnectFailed{PeerKey: key})
	p, _ = s.Get(key)
	assert.Equal(t, ConnectStagePunch, p.Stage)
	assert.Equal(t, 0, p.StageAttempts)

	// Tick + fail punch attempt 1.
	outputs = s.Step(now.Add(3*time.Second), Tick{})
	require.Len(t, outputs, 1)
	_, isPunch := outputs[0].(RequestPunchCoordination)
	require.True(t, isPunch)
	s.Step(now.Add(3*time.Second), ConnectFailed{PeerKey: key})
	p, _ = s.Get(key)
	assert.Equal(t, ConnectStagePunch, p.Stage)
	assert.Equal(t, 1, p.StageAttempts)

	// Tick + fail punch attempt 2 → unreachable.
	outputs = s.Step(now.Add(5*time.Second), Tick{})
	require.Len(t, outputs, 1)
	s.Step(now.Add(5*time.Second), ConnectFailed{PeerKey: key})
	p, _ = s.Get(key)
	assert.Equal(t, PeerStateUnreachable, p.State)
	assert.Equal(t, now.Add(5*time.Second).Add(unreachableRetryInterval), p.NextActionAt)
}

func TestEagerRetryStage(t *testing.T) {
	s := NewStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41234}
	s.Step(now, DiscoverPeer{
		PeerKey:  key,
		Ips:      []net.IP{net.ParseIP("10.0.0.1")},
		Port:     9000,
		LastAddr: lastAddr,
	})

	p, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, ConnectStageEagerRetry, p.Stage)

	// Tick should emit AttemptEagerConnect.
	outputs := s.Step(now, Tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(AttemptEagerConnect)
	require.True(t, ok)
	assert.Equal(t, key, ec.PeerKey)
	assert.Equal(t, lastAddr, ec.Addr)

	// Fail → should escalate to ConnectStageDirect.
	s.Step(now, ConnectFailed{PeerKey: key})
	p, _ = s.Get(key)
	assert.Equal(t, ConnectStageDirect, p.Stage)
	assert.Equal(t, 0, p.StageAttempts)

	// Next tick should emit AttemptConnect (normal direct).
	outputs = s.Step(now.Add(2*time.Second), Tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(AttemptConnect)
	require.True(t, isAttempt)
}

func TestEagerRetrySkippedWhenNoLastAddr(t *testing.T) {
	s := NewStore()
	key := testPeerKey(1)
	now := time.Now()

	s.Step(now, DiscoverPeer{
		PeerKey: key,
		Ips:     []net.IP{net.ParseIP("10.0.0.1")},
		Port:    9000,
	})

	p, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, ConnectStageDirect, p.Stage)

	outputs := s.Step(now, Tick{})
	require.Len(t, outputs, 1)
	_, isAttempt := outputs[0].(AttemptConnect)
	require.True(t, isAttempt)
}

func TestEagerRetryWithOnlyLastAddr(t *testing.T) {
	s := NewStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.8"), Port: 41000}
	s.Step(now, DiscoverPeer{PeerKey: key, LastAddr: lastAddr})

	p, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, ConnectStageEagerRetry, p.Stage)

	outputs := s.Step(now, Tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(AttemptEagerConnect)
	require.True(t, ok)
	assert.Equal(t, key, ec.PeerKey)
	assert.Equal(t, lastAddr, ec.Addr)
}

func TestUnreachableRetryReturnsToEagerWithLastAddr(t *testing.T) {
	s := NewStore()
	key := testPeerKey(1)
	now := time.Now()

	lastAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 42000}
	s.Step(now, DiscoverPeer{
		PeerKey:  key,
		Ips:      []net.IP{net.ParseIP("10.0.0.1")},
		Port:     9000,
		LastAddr: lastAddr,
	})

	// Eager attempt fails, then direct fails twice to punch.
	s.Step(now, Tick{})
	s.Step(now, ConnectFailed{PeerKey: key})
	s.Step(now.Add(2*time.Second), Tick{})
	s.Step(now.Add(2*time.Second), ConnectFailed{PeerKey: key})
	s.Step(now.Add(4*time.Second), Tick{})
	s.Step(now.Add(4*time.Second), ConnectFailed{PeerKey: key})

	// Punch fails twice -> unreachable.
	s.Step(now.Add(6*time.Second), Tick{})
	s.Step(now.Add(6*time.Second), ConnectFailed{PeerKey: key})
	outputs := s.Step(now.Add(8*time.Second), Tick{})
	require.Len(t, outputs, 1)
	_, isPunch := outputs[0].(RequestPunchCoordination)
	require.True(t, isPunch)
	s.Step(now.Add(8*time.Second), ConnectFailed{PeerKey: key})

	p, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, PeerStateUnreachable, p.State)

	// After unreachable retry interval, stage should return to eager retry.
	outputs = s.Step(now.Add(8*time.Second).Add(unreachableRetryInterval), Tick{})
	require.Len(t, outputs, 1)
	ec, ok := outputs[0].(AttemptEagerConnect)
	require.True(t, ok)
	assert.Equal(t, key, ec.PeerKey)
	assert.Equal(t, lastAddr, ec.Addr)
}
