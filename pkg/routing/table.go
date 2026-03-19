package routing

import (
	"container/heap"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	nilCoordWeight    = 1e9
	minPositiveWeight = 1e-6
)

// PeerTopology is the routing-relevant subset of a peer's state.
type PeerTopology struct {
	Reachable map[types.PeerKey]struct{}
	Coord     *coords.Coord
	Key       types.PeerKey
}

// Table is an immutable routing table produced by Build.
type Table struct {
	routes map[types.PeerKey]types.PeerKey
}

// NextHop returns the next hop toward dest.
func (t Table) NextHop(dest types.PeerKey) (types.PeerKey, bool) {
	next, ok := t.routes[dest]
	return next, ok
}

// Build computes a routing table via Dijkstra from self over the topology graph.
// Edge weights are Vivaldi distances between peers. Connected peers (direct QUIC
// sessions) are excluded from the result since they need no routing.
func Build(self types.PeerKey, topology []PeerTopology, connected []types.PeerKey) Table {
	coordOf := make(map[types.PeerKey]*coords.Coord, len(topology))
	for i := range topology {
		coordOf[topology[i].Key] = topology[i].Coord
	}

	adj := make(map[types.PeerKey][]types.PeerKey, len(topology))
	for i := range topology {
		t := &topology[i]
		for neighbor := range t.Reachable {
			adj[t.Key] = append(adj[t.Key], neighbor)
			adj[neighbor] = append(adj[neighbor], t.Key)
		}
	}

	dist := map[types.PeerKey]float64{self: 0}
	firstHop := make(map[types.PeerKey]types.PeerKey)
	h := &minHeap{{node: self}}

	for h.Len() > 0 {
		cur := heap.Pop(h).(heapEntry) //nolint:forcetypeassert
		if d, seen := dist[cur.node]; seen && cur.dist > d {
			continue
		}
		for _, neighbor := range adj[cur.node] {
			nd := cur.dist + edgeWeight(coordOf[cur.node], coordOf[neighbor])
			if d, seen := dist[neighbor]; seen && nd >= d {
				continue
			}
			dist[neighbor] = nd
			if cur.node == self {
				firstHop[neighbor] = neighbor
			} else {
				firstHop[neighbor] = firstHop[cur.node]
			}
			heap.Push(h, heapEntry{dist: nd, node: neighbor})
		}
	}

	for _, pk := range connected {
		delete(firstHop, pk)
	}
	return Table{routes: firstHop}
}

func edgeWeight(a, b *coords.Coord) float64 {
	if a == nil || b == nil {
		return nilCoordWeight
	}
	d := coords.Distance(*a, *b)
	if d <= 0 {
		return minPositiveWeight
	}
	return d
}

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
	v := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return v
}
