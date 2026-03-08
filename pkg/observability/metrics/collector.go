package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Collector aggregates metrics in-memory and flushes them to a Sink.
// A nil *Collector is safe to use — all methods become no-ops.
type Collector struct {
	sink     Sink
	stopCh   chan struct{}
	counters map[metricKey]*counter
	gauges   map[metricKey]*gauge
	wg       sync.WaitGroup
	cfg      Config
	mu       sync.Mutex
}

type metricKey struct {
	name   string
	labels Labels
}

// New creates a Collector that flushes to sink. If sink is nil the collector
// still tracks metrics in memory but never flushes.
func New(sink Sink, cfg Config) *Collector {
	cfg = cfg.withDefaults()
	c := &Collector{
		sink:     sink,
		cfg:      cfg,
		stopCh:   make(chan struct{}),
		counters: make(map[metricKey]*counter),
		gauges:   make(map[metricKey]*gauge),
	}
	c.wg.Add(1)
	go c.flushLoop()
	return c
}

// Close stops the flush loop and performs a final flush.
func (c *Collector) Close() {
	if c == nil {
		return
	}
	close(c.stopCh)
	c.wg.Wait()
	c.flush()
}

// Counter returns a counter handle. Repeated calls with the same name+labels
// return the same underlying counter.
func (c *Collector) Counter(name string, labels Labels) *Counter {
	if c == nil {
		return nil
	}
	k := metricKey{name: name, labels: labels}
	c.mu.Lock()
	ctr, ok := c.counters[k]
	if !ok {
		ctr = &counter{}
		c.counters[k] = ctr
	}
	c.mu.Unlock()
	return &Counter{c: ctr}
}

// Gauge returns a gauge handle.
func (c *Collector) Gauge(name string, labels Labels) *Gauge {
	if c == nil {
		return nil
	}
	k := metricKey{name: name, labels: labels}
	c.mu.Lock()
	g, ok := c.gauges[k]
	if !ok {
		g = &gauge{}
		c.gauges[k] = g
	}
	c.mu.Unlock()
	return &Gauge{g: g}
}

func (c *Collector) flushLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.flush()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Collector) flush() {
	c.mu.Lock()
	snaps := make([]Snapshot, 0, len(c.counters)+len(c.gauges))
	for k, ctr := range c.counters {
		v := ctr.val.Swap(0)
		if v == 0 {
			continue
		}
		snaps = append(snaps, Snapshot{
			Name:   k.name,
			Labels: k.labels,
			Kind:   KindCounter,
			Value:  float64(v),
		})
	}
	for k, g := range c.gauges {
		if !g.dirty.Swap(false) {
			continue
		}
		snaps = append(snaps, Snapshot{
			Name:   k.name,
			Labels: k.labels,
			Kind:   KindGauge,
			Value:  g.load(),
		})
	}
	c.mu.Unlock()

	if len(snaps) > 0 && c.sink != nil {
		c.sink.Flush(snaps)
	}
}

// counter is the internal atomic accumulator.
type counter struct {
	val        atomic.Int64
	cumulative atomic.Int64
}

// gauge stores a float64 as atomic uint64 bits.
type gauge struct {
	bits  atomic.Uint64
	dirty atomic.Bool
}

func (g *gauge) store(v float64) {
	g.bits.Store(math.Float64bits(v))
	g.dirty.Store(true)
}

func (g *gauge) load() float64 {
	return math.Float64frombits(g.bits.Load())
}

// Counter is a monotonically increasing value. A nil Counter is a no-op.
type Counter struct {
	c *counter
}

// Add increments the counter by delta.
func (h *Counter) Add(delta int64) {
	if h == nil {
		return
	}
	h.c.val.Add(delta)
	h.c.cumulative.Add(delta)
}

// Inc increments the counter by 1.
func (h *Counter) Inc() {
	if h == nil {
		return
	}
	h.c.val.Add(1)
	h.c.cumulative.Add(1)
}

// Value returns the cumulative total of this counter. Returns 0 on a nil Counter.
func (h *Counter) Value() int64 {
	if h == nil {
		return 0
	}
	return h.c.cumulative.Load()
}

// EWMA is a lock-free exponentially-weighted moving average. A nil *EWMA is
// safe to use — Update is a no-op and Value returns 0.
type EWMA struct {
	bits  atomic.Uint64
	alpha float64
}

// NewEWMA creates an EWMA with the given smoothing factor, starting at 0.
func NewEWMA(alpha float64) *EWMA {
	return &EWMA{alpha: alpha}
}

// NewEWMAFrom creates an EWMA with the given smoothing factor and initial value.
func NewEWMAFrom(alpha, initial float64) *EWMA {
	e := &EWMA{alpha: alpha}
	e.bits.Store(math.Float64bits(initial))
	return e
}

func (e *EWMA) Update(sample float64) {
	if e == nil {
		return
	}
	for {
		old := e.bits.Load()
		oldVal := math.Float64frombits(old)
		newVal := e.alpha*sample + (1-e.alpha)*oldVal
		if e.bits.CompareAndSwap(old, math.Float64bits(newVal)) {
			return
		}
	}
}

func (e *EWMA) Value() float64 {
	if e == nil {
		return 0
	}
	return math.Float64frombits(e.bits.Load())
}

// Gauge is a value that can go up and down. A nil Gauge is a no-op.
type Gauge struct {
	g *gauge
}

// Set sets the gauge to value.
func (h *Gauge) Set(value float64) {
	if h == nil {
		return
	}
	h.g.store(value)
}

// Value returns the current gauge value. Returns 0 on a nil Gauge.
func (h *Gauge) Value() float64 {
	if h == nil {
		return 0
	}
	return h.g.load()
}
