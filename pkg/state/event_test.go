package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Compile-time assertions: every Event variant satisfies the Event interface.
var (
	_ Event = PeerJoined{}
	_ Event = PeerLeft{}
	_ Event = PeerDenied{}
	_ Event = ServiceChanged{}
	_ Event = WorkloadChanged{}
	_ Event = TopologyChanged{}
	_ Event = GossipApplied{}
)

func TestDigest_NonZeroAfterMutation(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	snap := s.Snapshot()

	d := snap.Digest()
	require.NotNil(t, d.proto)
	require.NotEmpty(t, d.proto.GetPeers())
}

func TestDigest_DiffersAfterMutation(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	snap1 := s.Snapshot()

	s.upsertLocalService(8080, "web")
	snap2 := s.Snapshot()

	d1 := snap1.Digest()
	d2 := snap2.Digest()

	p1 := d1.proto.GetPeers()[pk.String()]
	p2 := d2.proto.GetPeers()[pk.String()]

	require.NotNil(t, p1)
	require.NotNil(t, p2)
	require.Greater(t, p2.GetMaxCounter(), p1.GetMaxCounter())
}
