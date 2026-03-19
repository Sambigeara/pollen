package tunneling

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord_AccumulatesAcrossWindow(t *testing.T) {
	tr := newTracker()
	peer := pk(1)

	tr.Record(peer, 100, 200)
	snap1, changed1 := tr.RotateAndSnapshot()
	require.True(t, changed1)
	require.Equal(t, peerTraffic{BytesIn: 100, BytesOut: 200}, snap1[peer])

	tr.Record(peer, 50, 30)
	snap2, changed2 := tr.RotateAndSnapshot()
	require.True(t, changed2)
	require.Equal(t, peerTraffic{BytesIn: 150, BytesOut: 230}, snap2[peer])
}

func TestDeadbandSuppression(t *testing.T) {
	tr := newTracker()
	peer := pk(1)

	tr.Record(peer, 1000, 2000)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	_, changed = tr.RotateAndSnapshot()
	require.False(t, changed)
}

func TestEventualClearing(t *testing.T) {
	tr := newTracker()
	peer := pk(1)

	tr.Record(peer, 100, 200)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Len(t, snap, 1)

	_, changed = tr.RotateAndSnapshot()
	require.False(t, changed)
	_, changed = tr.RotateAndSnapshot()
	require.False(t, changed)

	snap, changed = tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Empty(t, snap)
}

func TestDeadbandTriggersOnSignificantChange(t *testing.T) {
	tr := newTracker()
	peer := pk(1)

	tr.Record(peer, 1000, 2000)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	tr.Record(peer, 50, 100)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Equal(t, peerTraffic{BytesIn: 1050, BytesOut: 2100}, snap[peer])
}

func TestConcurrentRecord(t *testing.T) {
	tr := newTracker()
	peer := pk(1)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 1000 {
				tr.Record(peer, 1, 2)
			}
		})
	}
	wg.Wait()

	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Equal(t, uint64(100_000), snap[peer].BytesIn)
	require.Equal(t, uint64(200_000), snap[peer].BytesOut)
}

func TestPeerAppearanceAlwaysSignificant(t *testing.T) {
	tr := newTracker()
	peerA := pk(1)
	peerB := pk(2)

	tr.Record(peerA, 1000, 2000)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	tr.Record(peerA, 1000, 2000)
	tr.Record(peerB, 1, 1)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Contains(t, snap, peerA)
	require.Contains(t, snap, peerB)
}

func TestPeerDisappearanceAlwaysSignificant(t *testing.T) {
	tr := newTracker()
	peerA := pk(1)
	peerB := pk(2)

	tr.Record(peerA, 1000, 2000)
	tr.Record(peerB, 500, 600)
	_, changed := tr.RotateAndSnapshot()
	require.True(t, changed)

	tr.Record(peerA, 1000, 2000)
	snap, changed := tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Contains(t, snap, peerA)
	require.Contains(t, snap, peerB)

	tr.Record(peerA, 1000, 2000)
	_, changed = tr.RotateAndSnapshot()
	require.True(t, changed)

	tr.Record(peerA, 1000, 2000)
	snap, changed = tr.RotateAndSnapshot()
	require.True(t, changed)
	require.Contains(t, snap, peerA)
	require.NotContains(t, snap, peerB)
}
