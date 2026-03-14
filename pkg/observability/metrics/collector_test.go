package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingSink struct {
	mu        sync.Mutex
	snapshots []Snapshot
}

func (s *recordingSink) Flush(snaps []Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots = append(s.snapshots, snaps...)
}

func (s *recordingSink) get() []Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Snapshot, len(s.snapshots))
	copy(out, s.snapshots)
	return out
}

func TestNilCollectorIsNoOp(t *testing.T) {
	var c *Collector
	ctr := c.Counter("test")
	g := c.Gauge("test")

	// All operations should be safe on nil.
	ctr.Inc()
	ctr.Add(5)
	g.Set(42.0)
	c.Close()
}

func TestCounterIncrement(t *testing.T) {
	sink := &recordingSink{}
	c := New(sink, Config{FlushInterval: time.Hour})
	defer c.Close()

	ctr := c.Counter("requests")
	ctr.Inc()
	ctr.Inc()
	ctr.Add(3)

	c.flush()

	snaps := sink.get()
	require.Len(t, snaps, 1)
	require.Equal(t, "requests", snaps[0].Name)
	require.Equal(t, KindCounter, snaps[0].Kind)
	require.Equal(t, 5.0, snaps[0].Value)
}

func TestCounterResetsAfterFlush(t *testing.T) {
	sink := &recordingSink{}
	c := New(sink, Config{FlushInterval: time.Hour})
	defer c.Close()

	ctr := c.Counter("requests")
	ctr.Add(10)
	c.flush()

	ctr.Add(3)
	c.flush()

	snaps := sink.get()
	require.Len(t, snaps, 2)
	require.Equal(t, 10.0, snaps[0].Value)
	require.Equal(t, 3.0, snaps[1].Value)
}

func TestZeroCounterNotFlushed(t *testing.T) {
	sink := &recordingSink{}
	c := New(sink, Config{FlushInterval: time.Hour})
	defer c.Close()

	c.Counter("idle")
	c.flush()

	require.Empty(t, sink.get())
}

func TestGaugeSetAndFlush(t *testing.T) {
	sink := &recordingSink{}
	c := New(sink, Config{FlushInterval: time.Hour})
	defer c.Close()

	g := c.Gauge("temperature")
	g.Set(36.6)
	c.flush()

	snaps := sink.get()
	require.Len(t, snaps, 1)
	require.Equal(t, "temperature", snaps[0].Name)
	require.Equal(t, KindGauge, snaps[0].Kind)
	require.Equal(t, 36.6, snaps[0].Value)
}

func TestSameNameReturnsSameCounter(t *testing.T) {
	sink := &recordingSink{}
	c := New(sink, Config{FlushInterval: time.Hour})
	defer c.Close()

	a := c.Counter("hits")
	b := c.Counter("hits")

	a.Inc()
	b.Inc()

	c.flush()

	snaps := sink.get()
	require.Len(t, snaps, 1)
	require.Equal(t, 2.0, snaps[0].Value)
}


func TestClosePerformsFinalFlush(t *testing.T) {
	sink := &recordingSink{}
	c := New(sink, Config{FlushInterval: time.Hour})

	c.Counter("final").Add(7)
	c.Close()

	snaps := sink.get()
	require.Len(t, snaps, 1)
	require.Equal(t, 7.0, snaps[0].Value)
}

func TestNilSinkCollectsWithoutFlush(t *testing.T) {
	c := New(nil, Config{})
	require.NotNil(t, c)
	c.Counter("x").Inc()
	c.Gauge("y").Set(3.14)
	c.Close()
}

func TestEWMAConvergence(t *testing.T) {
	e := NewEWMA(0.01)

	// Simulate a burst of stale events driving the EWMA to 1.0.
	for range 200 {
		e.Update(1.0)
	}
	require.Greater(t, e.Value(), 0.8, "EWMA should be high after stale burst")

	// Now simulate recovery: all events are applied (sample=0).
	for range 500 {
		e.Update(0.0)
	}
	require.Less(t, e.Value(), 0.05, "EWMA should recover after sustained applied events")
}

func TestEWMANilSafe(t *testing.T) {
	var e *EWMA
	e.Update(1.0)
	require.Equal(t, 0.0, e.Value())
}

func TestEWMAConcurrent(t *testing.T) {
	e := NewEWMA(0.01)
	var wg sync.WaitGroup
	const goroutines = 8
	const iterations = 1000

	for range goroutines {
		wg.Go(func() {
			for range iterations {
				e.Update(0.5)
			}
		})
	}
	wg.Wait()

	v := e.Value()
	require.Greater(t, v, 0.0, "value should be positive after updates")
	require.LessOrEqual(t, v, 1.0, "value should be <= 1.0")
}

func TestInstrumentBundles(t *testing.T) {
	t.Run("nil collector returns zero-value bundles", func(t *testing.T) {
		mm := NewMeshMetrics(nil)
		require.NotNil(t, mm)
		mm.DatagramsSent.Inc() // should not panic

		pm := NewPeerMetrics(nil)
		require.NotNil(t, pm)
		pm.Connections.Inc()

		gm := NewGossipMetrics(nil)
		require.NotNil(t, gm)
		gm.EventsApplied.Inc()

		tm := NewTopologyMetrics(nil)
		require.NotNil(t, tm)
		tm.VivaldiError.Set(0.5)

		nm := NewNodeMetrics(nil)
		require.NotNil(t, nm)
		nm.PunchAttempts.Inc()
	})

	t.Run("with collector registers instruments", func(t *testing.T) {
		sink := &recordingSink{}
		c := New(sink, Config{FlushInterval: time.Hour})
		defer c.Close()

		mm := NewMeshMetrics(c)
		mm.DatagramsSent.Add(10)
		mm.SessionConnects.Inc()

		c.flush()

		snaps := sink.get()
		require.GreaterOrEqual(t, len(snaps), 2)
	})
}
