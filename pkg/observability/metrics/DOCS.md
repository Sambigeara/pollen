# pkg/observability/metrics

## Responsibilities
- Lightweight opt-in metrics collection (no-ops when disabled via nil-safe Counter/Gauge)
- Counters and gauges tracked with atomic operations
- Periodic flushing to pluggable `Sink` interface
- Pre-registered instrument sets for mesh, peer, gossip, topology, and node layers
- EWMA support for smoothed values

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Sink` | interface | Receives metric snapshots (`Flush` method) |
| `Snapshot` | type | Point-in-time metric reading |
| `Kind` | type | Metric type discriminator |
| `KindCounter` | const | Monotonically increasing metric |
| `KindGauge` | const | Variable metric |
| `Config` | type | Collector configuration |
| `Collector` | type | In-memory metrics aggregator |
| `New` | func | Create collector with sink and config |
| `(*Collector).Counter` | method | Get/create counter handle |
| `(*Collector).Gauge` | method | Get/create gauge handle |
| `(*Collector).Close` | method | Stop flush loop and final flush |
| `Counter` | type | Monotonically increasing value (nil-safe) |
| `(*Counter).Add` | method | Increment by delta |
| `(*Counter).Inc` | method | Increment by 1 |
| `(*Counter).Value` | method | Get cumulative total |
| `Gauge` | type | Variable value (nil-safe) |
| `(*Gauge).Set` | method | Set gauge value |
| `(*Gauge).Value` | method | Get current value |
| `EWMA` | type | Lock-free exponentially-weighted moving average |
| `NewEWMA` | func | Create EWMA starting at 0 |
| `NewEWMAFrom` | func | Create EWMA with initial value |
| `(*EWMA).Update` | method | Update with new sample |
| `(*EWMA).Value` | method | Get current EWMA value |
| `MeshMetrics` | type | Pre-registered mesh layer instruments |
| `NewMeshMetrics` | func | Register mesh metrics on collector |
| `PeerMetrics` | type | Pre-registered peer state machine instruments |
| `NewPeerMetrics` | func | Register peer metrics on collector |
| `(*PeerMetrics).Enabled` | method | Check if any gauge is wired |
| `GossipMetrics` | type | Pre-registered gossip/store instruments |
| `NewGossipMetrics` | func | Register gossip metrics on collector |
| `TopologyMetrics` | type | Pre-registered topology instruments |
| `NewTopologyMetrics` | func | Register topology metrics on collector |
| `NodeMetrics` | type | Pre-registered node orchestrator instruments |
| `NewNodeMetrics` | func | Register node metrics on collector |
| `LogSink` | type | Zap logger sink implementation |
| `NewLogSink` | func | Create sink that logs to zap logger |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/mesh (uses: `MeshMetrics`, `Gauge`)
- pkg/node (uses: `Collector`, `New`, `Config`, `LogSink`, `NewLogSink`, all `New*Metrics` funcs, `EWMA`, `NewEWMAFrom`)
- pkg/peer (uses: `PeerMetrics`)
- pkg/store (uses: `GossipMetrics`)
- pkg/scheduler (uses: `Counter`)

## Proposed Minimal API

### Exports kept

All exports retained except `NewEWMA`. Key consumers:

| Export | Consumers |
|--------|-----------|
| `Collector`, `New`, `Config` | node |
| `Counter`, `Gauge` | mesh, node, peer, store, scheduler |
| `EWMA`, `NewEWMAFrom` | node |
| `MeshMetrics`, `NewMeshMetrics` | mesh, node |
| `PeerMetrics`, `NewPeerMetrics` | peer, node |
| `GossipMetrics`, `NewGossipMetrics` | store, node |
| `TopologyMetrics`, `NewTopologyMetrics` | node |
| `NodeMetrics`, `NewNodeMetrics` | node |
| `Sink`, `LogSink`, `NewLogSink` | node |

### Exports stripped (1)

| Export | Action | Reason |
|--------|--------|--------|
| `NewEWMA` | unexported | Only `NewEWMAFrom` is used externally; `NewEWMA` (starting at 0) had no production callers |
