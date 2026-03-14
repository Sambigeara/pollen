package route

import (
	"container/heap"
	"sync"

	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	defaultNilCoordWeight = 1e9
	minPositiveWeight     = 1e-6
)

// Route describes the next hop toward a destination.
type Route struct {
	NextHop  types.PeerKey
	Distance float64
	HopCount int
}

// NodeInfo is the routing-relevant subset of a peer's state.
type NodeInfo struct {
	Reachable map[types.PeerKey]struct{}
	Coord     *topology.Coord
}

// Table is a thread-safe routing table.
type Table struct {
	routes   map[types.PeerKey]Route
	changeCh chan struct{}
	mu       sync.RWMutex
	localKey types.PeerKey
}

// New creates an empty routing table.
func New(localKey types.PeerKey) *Table {
	return &Table{
		localKey: localKey,
		routes:   make(map[types.PeerKey]Route),
		changeCh: make(chan struct{}),
	}
}

// Changed returns a channel that is closed when the routing table is updated.
// Callers should snapshot this channel before reading state to avoid TOCTOU races.
func (t *Table) Changed() <-chan struct{} {
	t.mu.RLock()
	ch := t.changeCh
	t.mu.RUnlock()
	return ch
}

// Lookup returns the next hop for dest. Satisfies mesh.Router.
func (t *Table) Lookup(dest types.PeerKey) (types.PeerKey, bool) {
	t.mu.RLock()
	r, ok := t.routes[dest]
	t.mu.RUnlock()
	if !ok {
		return types.PeerKey{}, false
	}
	return r.NextHop, true
}

// Update atomically replaces the routing table contents.
func (t *Table) Update(routes map[types.PeerKey]Route) {
	t.mu.Lock()
	t.routes = routes
	close(t.changeCh)
	t.changeCh = make(chan struct{})
	t.mu.Unlock()
}

// Len returns the number of routes.
func (t *Table) Len() int {
	t.mu.RLock()
	n := len(t.routes)
	t.mu.RUnlock()
	return n
}

// Recompute runs Dijkstra from localKey over the reachability graph and returns
// routes for peers that are not directly connected. Direct peers are excluded
// because they already have QUIC sessions.
func Recompute(localKey types.PeerKey, localCoord *topology.Coord, directPeers map[types.PeerKey]struct{}, nodes map[types.PeerKey]NodeInfo) map[types.PeerKey]Route {
	// Collect coordinates for edge-weight calculation.
	coords := make(map[types.PeerKey]*topology.Coord, len(nodes)+1)
	coords[localKey] = localCoord
	for pk, info := range nodes {
		coords[pk] = info.Coord
	}

	edgeWeight := func(a, b types.PeerKey) float64 {
		ca, cb := coords[a], coords[b]
		if ca == nil || cb == nil {
			return defaultNilCoordWeight
		}
		d := topology.Distance(*ca, *cb)
		if d <= 0 {
			return minPositiveWeight
		}
		return d
	}

	// Build adjacency list from reachability. An edge A→B exists if A reports B
	// as reachable. We also add the reverse B→A to model the bidirectional
	// nature of QUIC sessions.
	type edge struct {
		to     types.PeerKey
		weight float64
	}
	adj := make(map[types.PeerKey][]edge)
	for pk, info := range nodes {
		for neighbor := range info.Reachable {
			w := edgeWeight(pk, neighbor)
			adj[pk] = append(adj[pk], edge{to: neighbor, weight: w})
			adj[neighbor] = append(adj[neighbor], edge{to: pk, weight: w})
		}
	}

	// Dijkstra from localKey.
	dist := map[types.PeerKey]float64{localKey: 0}
	first := make(map[types.PeerKey]types.PeerKey)
	hops := make(map[types.PeerKey]int)

	h := &minHeap{{dist: 0, node: localKey}}
	heap.Init(h)

	for h.Len() > 0 {
		cur := heap.Pop(h).(heapEntry) //nolint:forcetypeassert
		if d, ok := dist[cur.node]; ok && cur.dist > d {
			continue
		}
		for _, e := range adj[cur.node] {
			nd := cur.dist + e.weight
			if d, ok := dist[e.to]; ok && nd >= d {
				continue
			}
			dist[e.to] = nd
			if cur.node == localKey {
				first[e.to] = e.to
				hops[e.to] = 1
			} else {
				first[e.to] = first[cur.node]
				hops[e.to] = hops[cur.node] + 1
			}
			heap.Push(h, heapEntry{dist: nd, node: e.to})
		}
	}

	// Build routes for non-direct, non-local peers.
	routes := make(map[types.PeerKey]Route, len(dist))
	for pk, d := range dist {
		if pk == localKey {
			continue
		}
		if _, direct := directPeers[pk]; direct {
			continue
		}
		routes[pk] = Route{
			NextHop:  first[pk],
			Distance: d,
			HopCount: hops[pk],
		}
	}
	return routes
}

// --- min-heap for Dijkstra ---

type heapEntry struct {
	dist float64
	node types.PeerKey
}

type minHeap []heapEntry

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(heapEntry)) } //nolint:forcetypeassert

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
