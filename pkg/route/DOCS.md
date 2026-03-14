# pkg/route

## Responsibilities
- Computes shortest-path routes via Dijkstra algorithm
- Uses Vivaldi coordinates for edge weighting
- Maintains thread-safe routing table with change notifications
- Provides route lookup for multi-hop mesh routing

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Table` | type | Thread-safe routing table container |
| `New` | func | Constructor |
| `Route` | type | Route descriptor (NextHop, Distance, HopCount) |
| `NodeInfo` | type | Routing peer state (Reachable, Coord) |
| `Recompute` | func | Dijkstra algorithm: compute routes, return map |
| `(*Table).Changed` | method | Channel closed on table update |
| `(*Table).Lookup` | method | Route lookup by destination; return next hop |
| `(*Table).Update` | method | Atomically replace routing table |
| `(*Table).Len` | method | Return route count |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/topology | `Coord`, `Distance` |
| pkg/types | `PeerKey` |

## Consumed by
- pkg/node (uses: `Table`, `New`, `Lookup`, `Update`, `NodeInfo`)

## Proposed Minimal API

### Exports kept

| Export | Consumers |
|--------|-----------|
| `Table`, `New` | node |
| `Route` | node (via `Lookup` return) |
| `NodeInfo` | node |
| `Recompute` | node |
| `(*Table).Changed`, `(*Table).Lookup`, `(*Table).Update` | node |

### Exports stripped (3)

| Export | Action | Reason |
|--------|--------|--------|
| `(*Table).Len` | deleted | No production callers |
| `(Route).Distance` | unexported | Only used within package for Dijkstra edge weighting |
| `(Route).HopCount` | unexported | Only used within package for route comparison |
