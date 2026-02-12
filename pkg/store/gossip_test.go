package store

import (
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestApplyNode_RebroadcastsOnSelfConflict(t *testing.T) {
	localID := peerKey(1)
	s := newTestStore(t, localID)

	initial := s.SetLocalNetwork([]string{"10.0.0.1"}, 60611)
	require.NotNil(t, initial)
	require.Equal(t, uint64(1), initial.GetCounter())

	result := s.ApplyNode(&statev1.GossipNode{
		PeerId:      localID.String(),
		Counter:     initial.GetCounter(),
		IdentityPub: append([]byte(nil), initial.GetIdentityPub()...),
		Ips:         []string{"10.0.0.9"},
		LocalPort:   initial.GetLocalPort(),
	})

	require.NotNil(t, result.Rebroadcast)
	require.Equal(t, uint64(2), result.Rebroadcast.GetCounter())
	require.Equal(t, []string{"10.0.0.1"}, result.Rebroadcast.GetIps())
}

func TestMissingFor_ReturnsOnlyNewerRecords(t *testing.T) {
	localID := peerKey(1)
	remoteID := peerKey(2)

	s := newTestStore(t, localID)
	local := s.SetLocalNetwork([]string{"10.0.0.1"}, 60611)
	require.NotNil(t, local)

	result := s.ApplyNode(&statev1.GossipNode{
		PeerId:      remoteID.String(),
		Counter:     3,
		IdentityPub: bytesOf(32, 9),
		Ips:         []string{"10.0.0.2"},
		LocalPort:   60612,
	})
	require.True(t, result.Updated)

	updates := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			localID.String():  local.GetCounter(),
			remoteID.String(): 2,
		},
	})
	require.Len(t, updates, 1)
	require.Equal(t, remoteID.String(), updates[0].GetPeerId())
	require.Equal(t, uint64(3), updates[0].GetCounter())
}

func newTestStore(t *testing.T, localID types.PeerKey) *Store {
	t.Helper()

	s, err := Load(t.TempDir(), localID, bytesOf(32, 7))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})

	return s
}

func peerKey(seed byte) types.PeerKey {
	var b [32]byte
	for i := range b {
		b[i] = seed
	}
	return types.PeerKeyFromBytes(b[:])
}

func bytesOf(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed
	}
	return b
}
