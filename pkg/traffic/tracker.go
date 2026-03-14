package traffic

import (
	"maps"
	"sync"

	"github.com/sambigeara/pollen/pkg/types"
)

const (
	windowSize  = 3 // ring slots (completed ticks)
	deadbandPct = 2 // minimum % change to publish
)

// Recorder records per-peer byte counts. Implemented by *Tracker.
type Recorder interface {
	Record(peer types.PeerKey, bytesIn, bytesOut uint64)
}

// Noop is a Recorder that discards all byte counts.
var Noop Recorder = noopRecorder{}

type noopRecorder struct{}

func (noopRecorder) Record(types.PeerKey, uint64, uint64) {}

// PeerTraffic holds accumulated byte counts for a single peer.
type PeerTraffic struct {
	BytesIn  uint64
	BytesOut uint64
}

// Tracker accumulates per-peer byte counters with a sliding window and
// deadband suppression. Thread-safe.
type Tracker struct {
	ring      [windowSize]map[types.PeerKey]peerBytes
	current   map[types.PeerKey]*peerBytes
	published map[types.PeerKey]PeerTraffic
	pos       int
	mu        sync.Mutex
}

type peerBytes struct {
	in  uint64
	out uint64
}

// New creates a Tracker ready for use.
func New() *Tracker {
	return &Tracker{
		current: make(map[types.PeerKey]*peerBytes),
	}
}

// Record adds byte counts for a peer. Called from bridge goroutines.
func (t *Tracker) Record(peer types.PeerKey, bytesIn, bytesOut uint64) {
	t.mu.Lock()
	pb := t.current[peer]
	if pb == nil {
		pb = &peerBytes{}
		t.current[peer] = pb
	}
	pb.in += bytesIn
	pb.out += bytesOut
	t.mu.Unlock()
}

// RotateAndSnapshot rotates the current counters into the ring buffer,
// sums the sliding window, and compares against the last published snapshot.
// Returns the window snapshot and whether it changed significantly.
func (t *Tracker) RotateAndSnapshot() (map[types.PeerKey]PeerTraffic, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Rotate: snapshot current into ring[pos], advance pos, reset current.
	slot := make(map[types.PeerKey]peerBytes, len(t.current))
	for k, v := range t.current {
		slot[k] = peerBytes{in: v.in, out: v.out}
	}
	t.ring[t.pos] = slot
	t.pos = (t.pos + 1) % windowSize
	t.current = make(map[types.PeerKey]*peerBytes)

	// Sum all ring slots into window.
	window := make(map[types.PeerKey]PeerTraffic)
	for _, s := range t.ring {
		for k, v := range s {
			pt := window[k]
			pt.BytesIn += v.in
			pt.BytesOut += v.out
			window[k] = pt
		}
	}

	changed := t.hasChanged(window)
	if !changed {
		return nil, false
	}

	// Update published.
	if len(window) == 0 {
		t.published = nil
	} else {
		pub := make(map[types.PeerKey]PeerTraffic, len(window))
		maps.Copy(pub, window)
		t.published = pub
	}
	return window, true
}

// hasChanged compares window against published using deadband rules.
func (t *Tracker) hasChanged(window map[types.PeerKey]PeerTraffic) bool {
	// Peer set changes are always significant.
	if len(window) != len(t.published) {
		return true
	}
	for k := range window {
		if _, ok := t.published[k]; !ok {
			return true
		}
	}
	for k := range t.published {
		if _, ok := window[k]; !ok {
			return true
		}
	}

	// Same peer set — check values against deadband.
	for k, nw := range window {
		old := t.published[k]
		if beyondDeadband(old.BytesIn, nw.BytesIn) || beyondDeadband(old.BytesOut, nw.BytesOut) {
			return true
		}
	}
	return false
}

// beyondDeadband returns true if new differs from old by >= deadbandPct%.
func beyondDeadband(old, cur uint64) bool {
	if old == 0 {
		return cur > 0
	}
	if old == cur {
		return false
	}
	var diff uint64
	if cur > old {
		diff = cur - old
	} else {
		diff = old - cur
	}
	return diff*100/old >= deadbandPct
}
