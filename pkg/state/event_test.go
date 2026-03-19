package state

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
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

func TestClock_NonZeroAfterMutation(t *testing.T) {
	pub := genPub(t)
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	snap := s.Snapshot()

	clk := snap.Clock()
	require.NotNil(t, clk.digest)
	require.NotEmpty(t, clk.digest.GetPeers())
}

func TestClock_DiffersAfterMutation(t *testing.T) {
	pub := genPub(t)
	s := newTestStore(pub)

	snap1 := s.Snapshot()

	s.upsertLocalService(8080, "web")
	snap2 := s.Snapshot()

	c1 := snap1.Clock()
	c2 := snap2.Clock()

	localKey := types.PeerKeyFromBytes(pub)
	p1 := c1.digest.GetPeers()[localKey.String()]
	p2 := c2.digest.GetPeers()[localKey.String()]

	require.NotNil(t, p1)
	require.NotNil(t, p2)
	require.Greater(t, p2.GetMaxCounter(), p1.GetMaxCounter())
}
