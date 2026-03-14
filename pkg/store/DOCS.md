# pkg/store

## Responsibilities
- Manages cluster-wide CRDT gossip state: peer network/identity/services/reachability, workload specs/claims, traffic, coordinates
- Implements disk persistence and migration from legacy formats
- Provides query APIs for topology, scheduling, and service discovery
- Handles invite token consumption tracking and peer denial

## Files

| File | Content |
|------|---------|
| `store.go` | `Store` struct, constructor (`Load`), lifecycle (`Close`, `Save`), callback registration, `SetGossipMetrics`, `LocalID()` |
| `gossip.go` | CRDT log operations: `ApplyEvents`, `Clock`, `EagerSyncClock`, `MissingFor`, `LocalEvents` |
| `mutate.go` | All `SetLocal*` methods, `SetExternalPort`, `SetObservedExternalIP`, `SetLastAddr`, `DenyPeer`, `UpsertLocalService`, `RemoveLocalServices` |
| `query.go` | All read methods and exported view types (`RouteNodeInfo`, `LocalNodeView`, `KnownPeer`, `WorkloadSpecView`, `NodePlacementState`, `TrafficSnapshot`) |
| `workload.go` | `SetLocalWorkloadSpec`, `RemoveLocalWorkloadSpec`, `SetLocalWorkloadClaim`, workload spec conflict resolution, `ErrSpecOwnedRemotely` |
| `reachability.go` | `validNodesLocked` (deny/expiry filter), `liveComponentLocked` (BFS reachability component) |
| `record.go` | `nodeRecord` type, `clone()`, `attrKey`, `logEntry`, attribute kind constants, `computePeerHashLocked`, `tombstoneStaleAttrs` |
| `connections.go` | `Connection` struct, `DesiredConnections`, `AddDesiredConnection`, `RemoveDesiredConnection` |
| `invites.go` | `TryConsume` (implements `auth.InviteConsumer`), invite replay prevention |
| `disk.go` | Disk persistence (`openDisk`, `load`, `save`, `close`) |

## Consumer API

### Types

| Export | Kind | Description |
|--------|------|-------------|
| `Store` | type | Main state container |
| `Connection` | type | `{PeerID PeerKey, RemotePort, LocalPort uint32}` — comparable, usable as map key |
| `KnownPeer` | type | Full peer metadata for connection/dialling |
| `WorkloadSpecView` | type | Workload spec + publisher identity |
| `NodePlacementState` | type | Resource + Vivaldi + traffic data per node for scheduler |
| `TrafficSnapshot` | type | `{BytesIn, BytesOut uint64}` per-peer traffic counters |
| `RouteNodeInfo` | type | Per-node data for route table construction and status display |
| `LocalNodeView` | type | Read-only view of local node state |
| `ApplyResult` | type | `{Rebroadcast []*statev1.GossipEvent}` — events to rebroadcast |
| `ErrSpecOwnedRemotely` | var | Returned when a valid remote peer already publishes the same hash |

### Constructor & Lifecycle

| Method | Description |
|--------|-------------|
| `Load(pollenDir string, identityPub []byte) (*Store, error)` | Opens or creates the store, replaying disk state |
| `(*Store).Close() error` | Closes disk resources |
| `(*Store).Save() error` | Snapshots current state to disk |

### Configuration

| Method | Description |
|--------|-------------|
| `(*Store).LocalID() types.PeerKey` | Identity key of local node |
| `(*Store).SetGossipMetrics(m *metrics.GossipMetrics)` | Wire live metrics (default: no-op) |
| `(*Store).OnDenyPeer(fn func(types.PeerKey))` | Callback when peer is denied |
| `(*Store).OnRouteInvalidate(fn func())` | Callback when reachability or Vivaldi changes |
| `(*Store).OnWorkloadChange(fn func())` | Callback when workload spec/claim changes |
| `(*Store).OnTrafficChange(fn func())` | Callback when traffic heatmap changes |

### CRDT / Gossip

| Method | Description |
|--------|-------------|
| `(*Store).EagerSyncClock()` | Returns digest for eager gossip sync |
| `(*Store).Clock()` | Current CRDT digest for anti-entropy |
| `(*Store).MissingFor(digest)` | Events remote peer is missing |
| `(*Store).ApplyEvents(events, isPullResponse)` | Apply incoming gossip events |
| `(*Store).LocalEvents()` | All current local events |

### Mutations

All `SetLocal*` methods return `[]*statev1.GossipEvent` for rebroadcast (nil if nothing changed).

| Method | Description |
|--------|-------------|
| `SetLocalNetwork` | IPs and listen port |
| `SetExternalPort` | STUN-observed external port |
| `SetObservedExternalIP` | STUN-observed external IP |
| `SetLocalPubliclyAccessible` | Public accessibility flag |
| `SetLocalNatType` | NAT type |
| `SetLocalVivaldiCoord` | Vivaldi coordinate |
| `SetLocalResourceTelemetry` | CPU/memory telemetry |
| `SetLocalTrafficHeatmap` | Per-peer traffic counters |
| `SetLocalCertExpiry` | TLS certificate expiry |
| `SetLocalConnected` | Local reachability edge |
| `SetLastAddr` | Last observed address (local bookkeeping) |
| `DenyPeer` | Permanently deny a peer cluster-wide |
| `UpsertLocalService` | Announce a local service |
| `RemoveLocalServices` | Withdraw a local service |

### Workload

| Method | Description |
|--------|-------------|
| `SetLocalWorkloadSpec` | Publish workload spec; returns `ErrSpecOwnedRemotely` on conflict |
| `RemoveLocalWorkloadSpec` | Withdraw a workload spec |
| `SetLocalWorkloadClaim` | Assert or release a workload claim |

### Connections

| Method | Description |
|--------|-------------|
| `DesiredConnections` | Sorted list of desired peer connections |
| `AddDesiredConnection` | Record a desired connection |
| `RemoveDesiredConnection` | Remove matching connections (0 = wildcard) |

### Invites

| Method | Description |
|--------|-------------|
| `TryConsume` | Idempotent invite consumption; implements `auth.InviteConsumer` |

### Queries

| Method | Description |
|--------|-------------|
| `AllRouteInfo` | Per-node data for route table construction |
| `ExternalPort` | STUN-observed external port for a peer |
| `LocalRecord` | Read-only view of local node state |
| `KnownPeers` | Valid peers with connectable addresses |
| `AllPeerKeys` | Live peer keys (BFS reachable) |
| `AllWorkloadSpecs` | Merged workload specs across valid nodes |
| `AllWorkloadClaims` | Claims from live peers only |
| `AllTrafficHeatmaps` | Per-node traffic data |
| `AllNodePlacementStates` | Placement scoring data for live nodes |
| `ResolveWorkloadPrefix` | Prefix-match workload hash |
| `NodeIPs` | IPs for a peer |
| `IsConnected` | Whether source's reachability set contains target |
| `HasServicePort` | Whether peer advertises port |
| `IsPubliclyAccessible` | Whether peer is publicly reachable |
| `PeerVivaldiCoord` | Vivaldi coordinate for a peer |
| `NatType` | NAT type for a peer |
| `IdentityPub` | Identity public key bytes |
| `IsDenied` | Whether peer is in deny list |
| `LocalServices` | Clone of local service advertisements |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/auth | `InviteConsumer` (interface), `IsCertExpiredAt` |
| pkg/nat | `Type`, `Unknown` |
| pkg/observability/metrics | `GossipMetrics`, `NewGossipMetrics` |
| pkg/topology | `Coord`, `MovementDistance`, `PublishEpsilon`, `Distance` |
| pkg/types | `PeerKey`, `PeerKeyFromBytes`, `PeerKeyFromString` |

## Consumed by
- pkg/node (uses: `Store`, `Load`, all mutation/query methods, view types)
- pkg/scheduler (uses: `NodePlacementState`, `WorkloadSpecView`, `AllNodePlacementStates`, `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `SetLocalWorkloadClaim`)
- cmd/pln (uses: `Store`, `Load`)

## Proposed Minimal API

### Exports kept

~55 exports retained covering lifecycle, CRDT gossip, mutations, workload, connections, invites, and queries. Key consumers:

| Export | Consumers |
|--------|-----------|
| `Store`, `Load`, `Close`, `Save` | node, cmd/pln |
| View types (`KnownPeer`, `WorkloadSpecView`, `NodePlacementState`, `TrafficSnapshot`, `RouteNodeInfo`, `LocalNodeView`, `Connection`, `ApplyResult`) | node, scheduler |
| All `SetLocal*` mutations | node |
| All query methods (`AllRouteInfo`, `KnownPeers`, `AllPeerKeys`, etc.) | node, scheduler |
| Workload methods (`SetLocalWorkloadSpec`, `SetLocalWorkloadClaim`, etc.) | node, scheduler |
| `TryConsume` | mesh (via `auth.InviteConsumer` interface) |

### Exports stripped (4)

| Export | Action | Reason |
|--------|--------|--------|
| `NatType` | deleted | No production callers; NAT type accessed through `KnownPeer` |
| `IsConnected` | deleted | No production callers; reachability checked via `AllPeerKeys` BFS |
| `IsPubliclyAccessible` | deleted | No production callers; accessibility data embedded in `KnownPeer` |
| `ErrSpecOwnedRemotely` | unexported | Only returned/checked within package boundary |

### Proposed narrow consumer interfaces

`pkg/scheduler` already defines `SchedulerStore` covering exactly the methods it needs:
- `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `SetLocalWorkloadClaim`, `AllNodePlacementStates`

This pattern could be replicated for other narrow consumers (e.g., a `RouteStore` for pkg/route) as a follow-up.
