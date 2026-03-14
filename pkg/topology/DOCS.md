# pkg/topology

## Responsibilities
- Computes deterministic outbound connection targets (infrastructure backbone + k-nearest by Vivaldi + random long-links)
- Implements Vivaldi network coordinate system for latency prediction
- Manages NAT type filtering and LAN diversity caps
- Three-layer topology budget system with HMAC-based deterministic selection

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `PeerInfo` | type | Topology snapshot: coord, IPs, NAT type, key, accessibility |
| `Params` | type | Controls topology budget and selection |
| `DefaultInfraMax` | const | Default infrastructure peer limit (2) |
| `DefaultNearestK` | const | Default k-nearest limit (4) |
| `DefaultRandomR` | const | Default random link limit (2) |
| `EpochSeconds` | const | Epoch duration in seconds (300) |
| `PublishEpsilon` | const | Minimum coordinate movement before re-publishing (0.5) |
| `DefaultParams` | func | Create Params with default budgets |
| `ComputeTargetPeers` | func | Select bounded set of outbound targets (deterministic) |
| `Coord` | type | Vivaldi coordinate: X, Y, Height |
| `Sample` | type | Single RTT measurement with peer coordinate |
| `RandomCoord` | func | Coordinate uniformly distributed in init circle |
| `Distance` | func | Vivaldi distance: ‖a.pos - b.pos‖ + a.Height + b.Height |
| `MovementDistance` | func | Euclidean distance in 3D (x, y, height) space |
| `Update` | func | Apply RTT sample to local coordinate using Vivaldi algorithm |
| `SameObservedEgress` | func | Check if two observed egress IPs are equal |
| `InferPrivatelyRoutable` | func | Check if local and peer IPs share private site prefix |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/nat | `Type` |
| pkg/types | `PeerKey` |

## Consumed by
- pkg/node (uses: `ComputeTargetPeers`, `Coord`, `RandomCoord`, `Sample`, `Update`, `EpochSeconds`, `DefaultInfraMax`, `DefaultNearestK`, `DefaultRandomR`, `SameObservedEgress`, `InferPrivatelyRoutable`)
- pkg/route (uses: `Coord`, `Distance`)
- pkg/store (uses: `Coord`, `MovementDistance`, `PublishEpsilon`, `Distance`)
- pkg/scheduler (uses: `Distance`, `Coord`)

## Proposed Minimal API

No changes — API surface already minimal. All 17 exports have production callers across 4 consumers.
