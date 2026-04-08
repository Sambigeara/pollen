package placement

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPickP2C(t *testing.T) {
	self := types.PeerKey{0x01}
	peerA := types.PeerKey{0x02}
	peerB := types.PeerKey{0x03}
	hash := "deadbeef"

	claimants := func(pks ...types.PeerKey) map[types.PeerKey]struct{} {
		m := make(map[types.PeerKey]struct{}, len(pks))
		for _, pk := range pks {
			m[pk] = struct{}{}
		}
		return m
	}

	t.Run("bootstrap: self unknown, remote known prefers local", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(peerA, hash, 10)

		target, isLocal := pickP2C(self, true, claimants(self, peerA), lt, hash)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("both known, self faster prefers local", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(self, hash, 5)
		lt.Record(peerA, hash, 10)

		target, isLocal := pickP2C(self, true, claimants(self, peerA), lt, hash)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("both known, remote faster prefers remote", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(self, hash, 50)
		lt.Record(peerA, hash, 5)
		lt.Record(peerB, hash, 5)

		// With only one remote, the pick is deterministic.
		target, isLocal := pickP2C(self, true, claimants(self, peerA), lt, hash)
		require.Equal(t, peerA, target)
		require.False(t, isLocal)
	})

	t.Run("both unknown prefers local", func(t *testing.T) {
		lt := newLatencyTracker()

		target, isLocal := pickP2C(self, true, claimants(self, peerA), lt, hash)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("self known, remote unknown probes remote", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(self, hash, 10)

		target, isLocal := pickP2C(self, true, claimants(self, peerA), lt, hash)
		require.Equal(t, peerA, target)
		require.False(t, isLocal)
	})

	t.Run("not running locally routes to remote", func(t *testing.T) {
		lt := newLatencyTracker()

		target, isLocal := pickP2C(self, false, claimants(peerA), lt, hash)
		require.Equal(t, peerA, target)
		require.False(t, isLocal)
	})

	t.Run("running locally with no others prefers local", func(t *testing.T) {
		lt := newLatencyTracker()

		target, isLocal := pickP2C(self, true, claimants(self), lt, hash)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})
}
