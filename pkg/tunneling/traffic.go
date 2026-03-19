package tunneling

import (
	"io"
	"maps"
	"sync"

	"github.com/sambigeara/pollen/pkg/types"
)

const (
	windowSize  = 3
	deadbandPct = 2
)

type peerTraffic struct {
	BytesIn  uint64
	BytesOut uint64
}

// tracker accumulates per-peer byte counters with a sliding window and
// deadband suppression.
type tracker struct {
	ring      [windowSize]map[types.PeerKey]peerBytes
	current   map[types.PeerKey]*peerBytes
	published map[types.PeerKey]peerTraffic
	pos       int
	mu        sync.Mutex
}

type peerBytes struct {
	in  uint64
	out uint64
}

func newTracker() *tracker {
	return &tracker{
		current: make(map[types.PeerKey]*peerBytes),
	}
}

func (t *tracker) Record(peer types.PeerKey, bytesIn, bytesOut uint64) {
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

func (t *tracker) RotateAndSnapshot() (map[types.PeerKey]peerTraffic, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	slot := make(map[types.PeerKey]peerBytes, len(t.current))
	for k, v := range t.current {
		slot[k] = peerBytes{in: v.in, out: v.out}
	}
	t.ring[t.pos] = slot
	t.pos = (t.pos + 1) % windowSize
	t.current = make(map[types.PeerKey]*peerBytes)

	window := make(map[types.PeerKey]peerTraffic)
	for _, s := range t.ring {
		for k, v := range s {
			pt := window[k]
			pt.BytesIn += v.in
			pt.BytesOut += v.out
			window[k] = pt
		}
	}

	if !t.hasChanged(window) {
		return nil, false
	}
	t.published = maps.Clone(window)
	return window, true
}

func (t *tracker) hasChanged(window map[types.PeerKey]peerTraffic) bool {
	if len(window) != len(t.published) {
		return true
	}
	for k, nw := range window {
		old, ok := t.published[k]
		if !ok {
			return true
		}
		if beyondDeadband(old.BytesIn, nw.BytesIn) || beyondDeadband(old.BytesOut, nw.BytesOut) {
			return true
		}
	}
	return false
}

func beyondDeadband(old, cur uint64) bool {
	if old == 0 {
		return cur > 0
	}
	diff := max(cur, old) - min(cur, old)
	return diff*100/old >= deadbandPct
}

type countedStream struct {
	inner io.ReadWriteCloser
	tr    *tracker
	peer  types.PeerKey
}

func (s *countedStream) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	if n > 0 {
		s.tr.Record(s.peer, uint64(n), 0)
	}
	return n, err
}

func (s *countedStream) Write(p []byte) (int, error) {
	n, err := s.inner.Write(p)
	if n > 0 {
		s.tr.Record(s.peer, 0, uint64(n))
	}
	return n, err
}

func (s *countedStream) Close() error { return s.inner.Close() }

func (s *countedStream) CloseWrite() error {
	if cw, ok := s.inner.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return s.inner.Close()
}

func wrapStream(stream io.ReadWriteCloser, tr *tracker, peer types.PeerKey) io.ReadWriteCloser {
	return &countedStream{
		inner: stream,
		tr:    tr,
		peer:  peer,
	}
}
