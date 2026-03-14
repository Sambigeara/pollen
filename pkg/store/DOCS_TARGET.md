# pkg/store — Target State

## Changes from Current

- [SAFE] Split `gossip.go` (2,259 LOC) into 8 focused files by concern
- [SAFE] Replace `Get()` and `AllNodes()` returning unexported `nodeRecord` with purpose-built query methods
- [SAFE] Delete dead `default` panic branches in `applyDeleteLocked` and `buildEventFromLog`
- [SAFE] Remove redundant nil guards in `applyValueLocked` (~20 LOC)
- [SAFE] Delete `GossipMetrics()` getter (write-once field, only used in one test)
- [SAFE] Replace `Connection.Key()` string formatting with `Connection` as direct map key
- [SAFE] Inline `sortConnections` into sole caller `DesiredConnections()`
- [SAFE] Expose `LocalID` via method `LocalID() PeerKey` instead of public field
- [BREAKING] Accept `store.NodePlacementState` directly from scheduler instead of wrapping it

## Target Exported API

### Removed exports

- `Get(peerID) (nodeRecord, bool)` — replaced by purpose-built queries
- `AllNodes() map[PeerKey]nodeRecord` — replaced by `AllRouteInfo()`
- `Connection.Key() string` — `Connection` used as map key directly
- `GossipMetrics() *metrics.GossipMetrics` — deleted (test accesses field directly)

### New exports

- `LocalID() PeerKey` — replaces direct field access on `Store.LocalID`
- `AllRouteInfo() map[PeerKey]RouteNodeInfo` — returns only route-relevant data (IPs, reachable peers, Vivaldi coord, publicly accessible) for `pkg/node` route table construction
- `ExternalPort(peerID PeerKey) (uint32, bool)` — replaces the `Get()` usage for external port lookup
- `LocalRecord() LocalNodeView` — returns a read-only view of the local node's state for the one call site that reads the full local record
- `RouteNodeInfo` — new exported struct with fields: `IPs []string`, `Reachable map[PeerKey]struct{}`, `Coord *topology.Coord`, `PubliclyAccessible bool`
- `LocalNodeView` — new exported struct with fields needed by the local-record call site

### Changed exports

- `Store.LocalID` field becomes unexported; access via `LocalID()` method

## Target File Layout

| File | Content | Est. LOC |
|------|---------|----------|
| `store.go` | `Store` struct, constructor (`Load`), lifecycle (`Close`, `Save`), callback registration, metrics setter, `LocalID()` | ~200 |
| `gossip.go` | CRDT log operations: `ApplyEvents`, `Clock`, `MissingFor`, `LocalEvents`, `EagerSyncClock` | ~500 |
| `mutate.go` | All `SetLocal*` methods, `DenyPeer`, `UpsertLocalService`, `RemoveLocalServices` | ~400 |
| `query.go` | All read methods: `AllRouteInfo`, `ExternalPort`, `LocalRecord`, `KnownPeers`, `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `AllTrafficHeatmaps`, `AllNodePlacementStates`, `ResolveWorkloadPrefix`, plus single-field queries (`NodeIPs`, `IsConnected`, etc.) | ~300 |
| `workload.go` | `SetLocalWorkloadSpec`, `SetLocalWorkloadClaim`, `RemoveLocalWorkloadSpec`, workload conflict resolution | ~200 |
| `reachability.go` | BFS reachability, `validNodesLocked`, deny filtering | ~150 |
| `record.go` | `nodeRecord` type, `clone()`, `attrKey`, `logEntry`, attribute kind definitions | ~200 |
| `connections.go` | `DesiredConnections`, `AddDesiredConnection`, `RemoveDesiredConnection`, `Connection` (now a direct map key) | ~70 |
| `invites.go` | `TryConsume`, `consumedInviteEntry`, invite replay prevention | ~60 |
| `disk.go` | Already separate — unchanged | ~278 |

## Deleted Items

| Item | Reason |
|------|--------|
| `Get(peerID) (nodeRecord, bool)` | Abstraction leak — returns unexported type. Replaced by `ExternalPort()` and `LocalRecord()` |
| `AllNodes() map[PeerKey]nodeRecord` | Abstraction leak. Replaced by `AllRouteInfo()` |
| `Connection.Key() string` | `Connection` struct is comparable; used as map key directly |
| `GossipMetrics() *metrics.GossipMetrics` | Write-once field, only read in one test |
| `sortConnections()` standalone function | Inlined into `DesiredConnections()` |
| `default` panic in `applyDeleteLocked` | Unreachable — `eventAttrKey` filters unknown types |
| `default` panic in `buildEventFromLog` | Same reason |
| Redundant nil guards in `applyValueLocked` | Proto getters handle nil safely; ~20 LOC |
