package peer

import (
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStore_DirectToPunchToUnreachable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FirstBackoff = 10 * time.Millisecond
	cfg.BaseBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 20 * time.Millisecond
	cfg.UnreachableRetryInterval = 25 * time.Millisecond

	store := NewStoreWithConfig(cfg)
	key := peerKey(1)
	now := time.Unix(0, 0)

	store.Step(now, DiscoverPeer{PeerKey: key, Ips: []net.IP{{127, 0, 0, 1}}, Port: 1})
	outputs := store.Step(now, Tick{})
	require.Len(t, outputs, 1)
	require.IsType(t, AttemptConnect{}, outputs[0])

	store.Step(now, ConnectFailed{PeerKey: key})
	peer := store.m[key]
	require.Equal(t, PeerStateDiscovered, peer.state)
	require.Equal(t, ConnectStagePunch, peer.stage)
	require.Equal(t, 0, peer.stageAttempts)
	require.Equal(t, now.Add(cfg.FirstBackoff), peer.nextActionAt)

	outputs = store.Step(now, Tick{})
	require.Empty(t, outputs)

	attemptTime := now.Add(cfg.FirstBackoff)
	outputs = store.Step(attemptTime, Tick{})
	require.Len(t, outputs, 1)
	_, ok := outputs[0].(RequestPunchCoordination)
	require.True(t, ok)

	store.Step(attemptTime, ConnectFailed{PeerKey: key})
	peer = store.m[key]
	require.Equal(t, PeerStateUnreachable, peer.state)
	require.Equal(t, attemptTime.Add(cfg.UnreachableRetryInterval), peer.nextActionAt)

	outputs = store.Step(attemptTime.Add(cfg.UnreachableRetryInterval), Tick{})
	require.Len(t, outputs, 1)
	require.IsType(t, AttemptConnect{}, outputs[0])
}

func TestStore_ConnectPeerResetsStage(t *testing.T) {
	store := NewStore()
	key := peerKey(2)
	now := time.Unix(0, 0)

	store.Step(now, DiscoverPeer{PeerKey: key, Ips: []net.IP{{10, 0, 0, 1}}, Port: 1})
	store.Step(now, Tick{})
	store.Step(now, ConnectFailed{PeerKey: key})

	outputs := store.Step(now, ConnectPeer{PeerKey: key, Ip: net.IP{10, 0, 0, 1}, ObservedPort: 2})
	require.Len(t, outputs, 1)
	require.IsType(t, PeerConnected{}, outputs[0])

	peer := store.m[key]
	require.Equal(t, PeerStateConnected, peer.state)
	require.Equal(t, ConnectStageDirect, peer.stage)
	require.Equal(t, 0, peer.stageAttempts)
	require.Equal(t, []string{"10.0.0.1"}, peer.ips)
	require.Equal(t, 2, peer.observedPort)
}

func TestStore_DisconnectSchedulesRetry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DisconnectedRetryInterval = 15 * time.Millisecond
	store := NewStoreWithConfig(cfg)
	key := peerKey(5)
	now := time.Unix(0, 0)

	store.Step(now, ConnectPeer{PeerKey: key, Ip: net.IP{10, 0, 0, 1}, ObservedPort: 1})
	store.Step(now, PeerDisconnected{PeerKey: key})
	peer := store.m[key]
	require.Equal(t, PeerStateDiscovered, peer.state)
	require.Equal(t, now.Add(cfg.DisconnectedRetryInterval), peer.nextActionAt)

	outputs := store.Step(now.Add(cfg.DisconnectedRetryInterval), Tick{})
	require.Len(t, outputs, 1)
	require.IsType(t, AttemptConnect{}, outputs[0])
}

func peerKey(seed byte) types.PeerKey {
	var b [32]byte
	for i := range b {
		b[i] = seed
	}
	return types.PeerKeyFromBytes(b[:])
}
