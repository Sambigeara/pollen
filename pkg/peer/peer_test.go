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
			assert.Equal(t, ConnectStageDirect, p.Stage)
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

	// Tick after retry delay — should emit AttemptConnect.
	outputs = s.Step(now.Add(2*time.Second), Tick{})
	require.Len(t, outputs, 1)
	ac, ok := outputs[0].(AttemptConnect)
	require.True(t, ok)
	assert.Equal(t, key, ac.PeerKey)

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
