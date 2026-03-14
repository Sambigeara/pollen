# pkg/store

## Purpose

CRDT-style gossip state store: holds per-node attribute records (network info, services, reachability, workload specs/claims, Vivaldi coordinates, resource telemetry, traffic heatmaps, deny lists) and synchronizes them across the mesh via delta-state gossip with per-attribute Lamport counters and tombstones. Also manages persistent on-disk state (peer addresses, denied peers, consumed invites, workload specs).

## Files

| File | LOC | Role |
|------|-----|------|
| `gossip.go` | 2,259 | Core CRDT store: all types, state machine, local mutation methods, event application, digest/clock/delta sync, workload spec/claim conflict resolution, reachability graph BFS, persistence snapshots |
| `disk.go` | 278 | Disk persistence layer: flock-based exclusive locking, protobuf serialization, YAML-to-proto migration, consumed_invites.json migration |
| `gossip_test.go` | 2,892 | Comprehensive tests covering every attribute type, conflict resolution, round-trips, save/load, reachability filtering, stale ratio, traffic heatmaps, workload spec ownership |

## Exported API

### Types

- **`Store`** — Central gossip state store. Consumed by `pkg/node` (owns the instance) and `pkg/scheduler` (via interface).
- **`KnownPeer`** — Read-only snapshot of a remote peer's network info, Vivaldi coord, NAT type, etc. Used by `pkg/node` for topology computation.
- **`Connection`** — Desired connection descriptor (PeerID, RemotePort, LocalPort). Used by `pkg/node` to manage service connections.
  - `Connection.Key() string` — deterministic string key for map indexing.
- **`ApplyResult`** — Return type from `ApplyEvents`, containing `Rebroadcast []*statev1.GossipEvent`.
- **`WorkloadSpecView`** — Merged view: `Spec *statev1.WorkloadSpecChange` + `Publisher types.PeerKey`. Used by `pkg/scheduler`.
- **`NodePlacementState`** — Resource/coordinate/traffic data for scheduler placement. Used by `pkg/scheduler`.
- **`TrafficSnapshot`** — Per-peer traffic counters (`BytesIn`, `BytesOut`). Used by `pkg/node`.
- **`ErrSpecOwnedRemotely`** — Sentinel error returned by `SetLocalWorkloadSpec`.

### Constructor / Lifecycle

- **`Load(pollenDir string, identityPub []byte) (*Store, error)`** — Opens disk state, migrates legacy formats, initializes local node record with identity + tombstoned stale attrs, loads denied peers and workload specs into log.
- **`(*Store) Close() error`** — Releases flock.
- **`(*Store) Save() error`** — Snapshots in-memory state to disk under RLock.

### Gossip Protocol (used by `pkg/node`)

- **`Clock() *statev1.GossipStateDigest`** — Returns digest with per-peer max counter + state hash.
- **`EagerSyncClock() *statev1.GossipStateDigest`** — Returns empty digest when no remote state exists yet (forces full sync), otherwise real digest.
- **`MissingFor(digest) []*statev1.GossipEvent`** — Delta sync: returns events the remote is missing, based on hash comparison + counter gaps.
- **`ApplyEvents(events, isPullResponse) ApplyResult`** — Batch-applies incoming events; partitions self-events for conflict resolution; fires callbacks outside lock; updates stale ratio EWMA on pull responses.
- **`LocalEvents() []*statev1.GossipEvent`** — All current local log entries as events (for startup broadcast).

### Local Mutation Methods (return events for broadcasting)

- `SetLocalNetwork(ips []string, port uint32) []*statev1.GossipEvent`
- `SetExternalPort(port uint32) []*statev1.GossipEvent`
- `SetObservedExternalIP(ip string) []*statev1.GossipEvent`
- `SetLocalConnected(peerID, connected) []*statev1.GossipEvent`
- `SetLocalPubliclyAccessible(accessible bool) []*statev1.GossipEvent`
- `SetLocalNatType(natType nat.Type) []*statev1.GossipEvent`
- `SetLocalVivaldiCoord(coord topology.Coord) []*statev1.GossipEvent`
- `SetLocalCertExpiry(expiry int64) []*statev1.GossipEvent`
- `SetLocalResourceTelemetry(cpu, mem uint32, memTotal uint64, numCPU uint32) []*statev1.GossipEvent`
- `SetLocalTrafficHeatmap(rates map[PeerKey]TrafficSnapshot) []*statev1.GossipEvent`
- `UpsertLocalService(port uint32, name string) []*statev1.GossipEvent`
- `RemoveLocalServices(name string) []*statev1.GossipEvent`
- `DenyPeer(subjectPub []byte) []*statev1.GossipEvent`
- `SetLocalWorkloadSpec(hash, replicas, memoryPages, timeoutMs) ([]*statev1.GossipEvent, error)`
- `RemoveLocalWorkloadSpec(hash string) []*statev1.GossipEvent`
- `SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent`

### Query Methods

- `Get(peerID) (nodeRecord, bool)` — Returns cloned record. Note: `nodeRecord` is unexported but returned publicly.
- `NodeIPs(peerID) []string`
- `IsConnected(source, target) bool`
- `HasServicePort(peerID, port) bool`
- `KnownPeers() []KnownPeer`
- `IsPubliclyAccessible(peerID) bool`
- `PeerVivaldiCoord(peerID) (*topology.Coord, bool)`
- `NatType(peerID) nat.Type`
- `IdentityPub(peerID) ([]byte, bool)`
- `IsDenied(subjectPub []byte) bool`
- `LocalServices() map[string]*statev1.Service`
- `AllNodes() map[PeerKey]nodeRecord` — Returns valid (non-denied, non-expired) nodes.
- `AllPeerKeys() []PeerKey` — Valid + live (reachable from local via BFS).
- `AllWorkloadSpecs() map[string]WorkloadSpecView`
- `AllWorkloadClaims() map[string]map[PeerKey]struct{}`
- `AllTrafficHeatmaps() map[PeerKey]map[PeerKey]TrafficSnapshot`
- `AllNodePlacementStates() map[PeerKey]NodePlacementState`
- `ResolveWorkloadPrefix(prefix) (hash, ambiguous, found)`

### Desired Connections

- `AddDesiredConnection(peerID, remotePort, localPort)`
- `RemoveDesiredConnection(peerID, remotePort, localPort)`
- `DesiredConnections() []Connection`

### Callbacks

- `OnDenyPeer(fn func(PeerKey))` — Fired outside lock on new deny events.
- `OnRouteInvalidate(fn func())` — Fired on reachability or Vivaldi changes.
- `OnWorkloadChange(fn func())` — Fired on workload spec/claim or reachability changes.
- `OnTrafficChange(fn func())` — Fired on traffic heatmap changes.

### Metrics

- `SetGossipMetrics(m *metrics.GossipMetrics)`
- `GossipMetrics() *metrics.GossipMetrics`

### Interface Satisfaction

- `auth.InviteConsumer` — via `TryConsume(token, now) (bool, error)`

## Internal State

### Store struct fields

| Field | Type | Purpose |
|-------|------|---------|
| `disk` | `*disk` | Persistence handle (flock + file path) |
| `nodes` | `map[PeerKey]nodeRecord` | Per-node materialized view + log |
| `denied` | `map[PeerKey]struct{}` | Cluster-wide deny set |
| `consumedInvites` | `map[string]consumedInviteEntry` | Replay-prevention for invite tokens |
| `desiredConnections` | `map[string]Connection` | Service-level connection intents |
| `onDeny` | `func(PeerKey)` | Callback (single) |
| `onRouteInvalidate` | `func()` | Callback (single) |
| `onWorkloadChange` | `func()` | Callback (single) |
| `onTrafficChange` | `func()` | Callback (single) |
| `metrics` | `*metrics.GossipMetrics` | Gossip health metrics |
| `mu` | `sync.RWMutex` | Protects all mutable state |
| `LocalID` | `PeerKey` | Identity of the local node |

### nodeRecord fields

| Field | Type | Purpose |
|-------|------|---------|
| `log` | `map[attrKey]logEntry` | Per-attribute Lamport counter + tombstone flag |
| `maxCounter` | `uint64` | Highest counter seen for this node |
| `IPs`, `LocalPort`, `ExternalPort` | network primitives | Node's network configuration |
| `ObservedExternalIP` | `string` | STUN-discovered external IP |
| `IdentityPub` | `[]byte` | Ed25519 public key |
| `CertExpiry` | `int64` | Unix timestamp for cert expiry |
| `Services` | `map[string]*statev1.Service` | Named services with ports |
| `Reachable` | `map[PeerKey]struct{}` | Direct connectivity graph edges |
| `VivaldiCoord` | `*topology.Coord` | Network coordinate |
| `NatType` | `nat.Type` | NAT classification |
| `PubliclyAccessible` | `bool` | Whether node is publicly reachable |
| `CPUPercent`, `MemPercent`, `MemTotalBytes`, `NumCPU` | resource metrics | Resource telemetry |
| `WorkloadSpecs` | `map[string]*WorkloadSpecChange` | Workload specs published by this node |
| `WorkloadClaims` | `map[string]struct{}` | Workload hashes claimed by this node |
| `TrafficRates` | `map[PeerKey]trafficRate` | Per-peer traffic counters |
| `LastAddr` | `string` | Last known address (local-only, not gossiped) |

### disk struct fields

| Field | Type | Purpose |
|-------|------|---------|
| `lockFile` | `*os.File` | Exclusive flock handle |
| `statePath` | `string` | Path to `state.pb` |
| `mu` | `sync.Mutex` | Serializes disk writes |

### attrKey / attrKind

14 attribute kinds (`attrNetwork` through `attrTrafficHeatmap`), each with an `attrKey{kind, name, peer}` composite key for per-attribute counter tracking in the CRDT log.

## Concurrency Contract

### In

No channels. All work arrives via synchronous method calls from `pkg/node` goroutines (gossip handlers, periodic tickers, connection event handlers). The store is purely passive — it never spawns goroutines or owns channels.

### Out

Four optional callbacks (`onDeny`, `onRouteInvalidate`, `onWorkloadChange`, `onTrafficChange`) are captured under the write lock and invoked **after** the lock is released, so callbacks can safely acquire the store's read lock. Callbacks are single-slot (last-writer-wins registration).

### Internal

**No goroutines.** No channels. No background tickers.

Single `sync.RWMutex` protects all mutable state:
- **Write lock**: `ApplyEvents`, all `SetLocal*` methods, `DenyPeer`, `TryConsume`, `SetLastAddr`, `Add/RemoveDesiredConnection`
- **Read lock**: `Clock`, `EagerSyncClock`, `MissingFor`, `Get`, `NodeIPs`, `IsConnected`, `KnownPeers`, `AllWorkload*`, `AllNodes`, `AllPeerKeys`, `AllTrafficHeatmaps`, `AllNodePlacementStates`, `Save`, `LocalEvents`, `DesiredConnections`, `LocalServices`, all `Is*` queries

Disk writes (`disk.save`) use a separate `disk.mu` mutex so disk I/O doesn't block the main store lock.

**Shutdown**: No explicit shutdown ordering within the store. `Close()` releases the flock. `Save()` must be called before `Close()` by the owner (`pkg/node`).

## Dependencies

### Imports (what it uses from each internal package)

| Package | Used for |
|---------|----------|
| `api/genpb/pollen/admission/v1` | `InviteToken` (for `TryConsume`) |
| `api/genpb/pollen/state/v1` | All proto types: `GossipEvent`, `GossipStateDigest`, `PeerDigest`, `RuntimeState`, `PeerState`, `Service`, `*Change` messages |
| `pkg/auth` | `InviteConsumer` interface, `IsCertExpiredAt` |
| `pkg/nat` | `nat.Type`, `nat.Unknown`, `TypeFromUint32`, `ToUint32` |
| `pkg/observability/metrics` | `GossipMetrics` (EWMA, counters) |
| `pkg/topology` | `Coord`, `MovementDistance`, `PublishEpsilon` |
| `pkg/types` | `PeerKey`, `PeerKeyFromBytes`, `PeerKeyFromString` |
| `pkg/perm` | `EnsureDir`, `WriteGroupReadable`, `SetPrivate` (disk.go only) |

### Imported By (what consumers use from this package)

| Consumer | Symbols Used |
|----------|-------------|
| `pkg/node` | `Store`, `Load`, `KnownPeer`, `Connection`, `TrafficSnapshot` — uses nearly every exported method for gossip protocol handling, topology computation, service connections, NAT detection, workload management |
| `pkg/scheduler` | `WorkloadSpecView`, `NodePlacementState` — via interface methods `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `AllNodePlacementStates`, `SetLocalWorkloadClaim` |
| `pkg/node/reachability.go` | `*Store` — passed to `rankCoordinators` for connectivity queries |
| `pkg/node/topology_profile.go` | `KnownPeer` — input to `summarizeTopologyShape` |

## Cross-Boundary Contracts

- **`auth.InviteConsumer`**: `*Store` satisfies this interface via `TryConsume`. Verified by compile-time assertion `var _ auth.InviteConsumer = (*Store)(nil)`.
- **Scheduler store interface** (defined in `pkg/scheduler/reconciler.go`): `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `AllNodePlacementStates`, `SetLocalWorkloadClaim` — the scheduler depends on this package's exported types but accesses the store through an interface.
- **Callbacks**: Four `func` slots registered by `pkg/node` — called outside the lock to avoid deadlock. Single-writer semantics (no registration races because `pkg/node` registers before starting goroutines).

## Observations

1. **Monolithic file**: `gossip.go` at 2,259 LOC contains ~15 distinct concerns (CRDT log, state machine, local mutators, gossip protocol, workload conflict resolution, reachability BFS, persistence snapshots, query methods, desired connections, invite consumption, metrics). This is the primary split candidate.

2. **`nodeRecord` is unexported but leaks through `Get()` and `AllNodes()`**: These methods return `nodeRecord` values to external packages. Callers can access all fields including internal ones like `maxCounter` and `log`. This is an abstraction leak — the type should either be exported with a deliberate public API, or these methods should return a read-only view type.

3. **Desired connections are unrelated to CRDT gossip**: `AddDesiredConnection`, `RemoveDesiredConnection`, `DesiredConnections`, `Connection`, and `sortConnections` are purely local connection-intent management. They share the store's `mu` but have no interaction with the gossip log. They could live in a separate type.

4. **Consumed invites are unrelated to CRDT gossip**: `TryConsume`, `dropExpiredInvites`, `loadConsumedInvites`, `consumedInviteEntry`, and `consumedExpirySkew` implement invite replay prevention. This is persistence-layer state that shares the lock with gossip but has no gossip events. Could be a separate store or at minimum a separate file.

5. **`tombstoneStaleAttrs` iterates a hardcoded list**: The list of attrs to tombstone on restart (`publiclyAccessible`, `natType`, `observedExternalIP`, `resourceTelemetry`, `trafficHeatmap`) must be manually kept in sync with any new attr kinds. A forgotten entry means stale state leaks after restart.

6. **`validNodesLocked()` clones every node record**: Called by `KnownPeers`, `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `AllNodePlacementStates`, `AllTrafficHeatmaps`, `AllNodes`, `ResolveWorkloadPrefix`. Each call deep-clones all maps of all valid nodes. For frequent callers (scheduler reconciliation loop), this is O(N * M) allocations where M is the average map sizes per node.

7. **Single mutex for everything**: The store uses one `sync.RWMutex` for gossip state, desired connections, consumed invites, and callbacks. This is simple but means unrelated operations contend. The disk layer correctly has its own mutex.

8. **Callback registration is not goroutine-safe**: `OnDenyPeer`, `OnRouteInvalidate`, `OnWorkloadChange`, `OnTrafficChange` write to struct fields without holding the lock. This is safe only because `pkg/node` registers them before spawning goroutines (write-once pattern), but the code doesn't enforce this contract.

9. **`ensureNodeInit` has an implicit identity-from-peerkey fallback**: If `IdentityPub` is empty, it copies from the PeerKey bytes. This conflates the WireGuard public key (PeerKey) with an identity public key, which may be a legacy assumption.

10. **Per-attribute counter vs. global maxCounter**: The CRDT uses per-attribute Lamport counters (in the `log` map) AND a per-node `maxCounter`. Both must be tracked correctly. The `maxCounter` is bumped even for stale events (line 834-837) to keep clock synchronization accurate, but this subtlety is not documented.

## Deletion Candidates

1. **`default` cases in `applyDeleteLocked` and `buildEventFromLog` that `panic`** (~4 LOC): Since `eventAttrKey` is exhaustive and returns `false` for unknown types (preventing entry), the default panic branches in `applyDeleteLocked` (line 971) and `buildEventFromLog` (line 2255) are dead code. The `applyValueLocked` default (line 1054-1056) returns `true` which is also arguably dead. ~6 LOC total.

2. **`Connection.Key()` string formatting** (~5 LOC): Uses `fmt.Sprintf` to build a map key from structured data. The `Connection` struct itself could be used as a map key directly (all fields are comparable), eliminating the `Key()` method and the `desiredConnections map[string]Connection` in favor of `map[Connection]struct{}`. ~10 LOC including the method, key usages, and `sortConnections`.

3. **`GossipMetrics()` getter** (~3 LOC): Returns `s.metrics`, which is a write-once field set by `SetGossipMetrics` before goroutines start. Only used in one test (`TestStaleRatioBatchLevel`). The test could access `metrics` directly via the store. ~3 LOC.

4. **Redundant nil guards in `applyValueLocked`** (~20 LOC): Many cases check `if v.Network == nil`, `if v.ExternalPort != nil`, etc. — but the proto getter methods already handle nil safely (return zero values), and `eventAttrKey` already validates the change type. The nil payload case (e.g., `v.Network == nil`) would just set zero values which is the same as deletion semantics. Some of these guards prevent no-op writes, but they're inconsistent: some return `true` on nil (lines 980-982), others wrap the mutation in a nil check. Could be unified. ~20 LOC of guards.

5. **`AllNodes()` export** (~4 LOC): Returns `map[PeerKey]nodeRecord` (unexported type). The only consumer is `pkg/node/node.go` line 1556 for building route table info. This leaks internal state. Could be replaced with a purpose-built query method. ~4 LOC for the method itself, but the refactor has broader implications.

6. **`Get()` returning unexported `nodeRecord`** (~8 LOC): Same abstraction leak as `AllNodes()`. Used in `pkg/node` for external port lookup (line 1079) and local record access (line 880). Both could be served by dedicated query methods like the existing `NodeIPs`, `IsPubliclyAccessible`, etc. ~8 LOC for the method.

7. **YAML migration code** (~50 LOC): `migrateYAMLToProto`, `yamlDiskState`, `yamlDiskPeer` (lines 142-203 of disk.go). This is a one-time migration from an old format. If all nodes have been migrated, this is dead code. ~50 LOC. Depends on whether legacy nodes still exist.

8. **JSON consumed invites migration** (~60 LOC): `migrateConsumedInvitesJSON`, `jsonConsumedInviteRecord`, `jsonConsumedInviteState` (lines 206-278 of disk.go). Same rationale as YAML migration. ~60 LOC.

9. **`nodeRecord.clone()` copying `TrafficRates` map** (~4 LOC): The `TrafficRates` map is cloned in `clone()` but traffic data is transient (tombstoned on restart). The clone is only needed because `validNodesLocked` returns clones. If `validNodesLocked` were restructured to avoid full clones (e.g., returning a filtered key set instead), this and other clone work could be eliminated. Not strictly dead, but the clone cost is excessive for read-only queries.

10. **`sortConnections` as a standalone function** (~8 LOC): Only called from `DesiredConnections()`. Could be inlined. Minor. ~8 LOC.

**Total estimated deletable LOC: ~170 (conservative, excluding migrations) to ~280 (aggressive, including migrations).**
