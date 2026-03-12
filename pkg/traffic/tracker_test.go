package traffic

import (
	"sync"
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func peerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

func TestRecord_AccumulatesAcrossWindow(t *testing.T) {
	tr := New()
	pk := peerKey(1)

	// Tick 1: record some traffic.
	tr.Record(pk, 100, 200)
	snap1, changed1 := tr.RotateAndSnapshot()
	require.True(t, changed1)
	require.Equal(t, PeerTraffic{BytesIn: 100, BytesOut: 200}, snap1[pk])

	// Tick 2: record more traffic — window sums both ticks.
	tr.Record(pk, 50, 30)
	snap2, changed2 := tr.RotateAndSnapshot()
	require.True(t, changed2)
	require.Equal(t, PeerTraffic{BytesIn: 150, BytesOut: 230}, snap2[pk])
}

func TestDeadbandSuppression(t *testing.T) {
	tr := New()
	pk := peerKey(1)

	// Tick 1: record traffic.
	tr.Record(pk, 1000, 2000)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	// Tick 2: no new traffic, but window still has tick 1 data.
	// Window sum is unchanged from published → suppressed.
	_, changed = tr.RotateAndSnapshot()
	require.False(t, changed)
}

func TestEventualClearing(t *testing.T) {
	tr := New()
	pk := peerKey(1)

	// Tick 1: record traffic.
	tr.Record(pk, 100, 200)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Len(t, snap, 1)

	// Ticks 2 and 3: quiet. Window still contains tick 1 data → suppressed.
	_, changed = tr.RotateAndSnapshot()
	require.False(t, changed)
	_, changed = tr.RotateAndSnapshot()
	require.False(t, changed)

	// Tick 4: tick 1 data rotates out (ring slot overwritten). Window empty → changed.
	snap, changed = tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Empty(t, snap)
}

func TestDeadbandTriggersOnSignificantChange(t *testing.T) {
	tr := New()
	pk := peerKey(1)

	// Tick 1: baseline traffic.
	tr.Record(pk, 1000, 2000)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	// Tick 2: add traffic that causes >2% change in window sum.
	// Window will be 1000+50=1050 in (5% increase), which exceeds deadband.
	tr.Record(pk, 50, 100)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Equal(t, PeerTraffic{BytesIn: 1050, BytesOut: 2100}, snap[pk])
}

func TestConcurrentRecord(t *testing.T) {
	tr := New()
	pk := peerKey(1)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 1000 {
				tr.Record(pk, 1, 2)
			}
		})
	}
	wg.Wait()

	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Equal(t, uint64(100_000), snap[pk].BytesIn)
	require.Equal(t, uint64(200_000), snap[pk].BytesOut)
}

func TestPeerAppearanceAlwaysSignificant(t *testing.T) {
	tr := New()
	pkA := peerKey(1)
	pkB := peerKey(2)

	// Tick 1: only peer A.
	tr.Record(pkA, 1000, 2000)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	// Tick 2: peer A same traffic (stays in window), new peer B appears.
	// Peer set change is always significant regardless of deadband.
	tr.Record(pkA, 1000, 2000)
	tr.Record(pkB, 1, 1)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Contains(t, snap, pkA)
	require.Contains(t, snap, pkB)
}

func TestPeerDisappearanceAlwaysSignificant(t *testing.T) {
	tr := New()
	pkA := peerKey(1)
	pkB := peerKey(2)

	// Tick 1: both peers.
	tr.Record(pkA, 1000, 2000)
	tr.Record(pkB, 500, 600)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	// Tick 2: only peer A. Peer B still in window from tick 1.
	tr.Record(pkA, 1000, 2000)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Contains(t, snap, pkA)
	require.Contains(t, snap, pkB)

	// Tick 3: only peer A. Peer B still in window from tick 1 (3 slots).
	tr.Record(pkA, 1000, 2000)
	_, changed = tr.RotateAndSnapshot()
	// Peer A's values changed (accumulated), so this publishes.
	require.True(t, changed)

	// Tick 4: peer B's tick 1 data rotates out. Peer set changes → significant.
	tr.Record(pkA, 1000, 2000)
	snap, changed = tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Contains(t, snap, pkA)
	require.NotContains(t, snap, pkB)
}
