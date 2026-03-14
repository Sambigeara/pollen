# pkg/store

## Purpose

Cluster-wide CRDT gossip store. Every node maintains a local copy of the full mesh state. `pkg/store` owns the in-memory data structures, CRDT log, disk persistence, and all query/mutation methods used by the rest of the daemon.

State is modelled as a per-peer attribute log where each attribute has a monotonically-increasing counter. Events are idempotent: applying the same event twice is a no-op. Anti-entropy (`MissingFor` / `ApplyEvents`) converges all nodes to the same state.

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

## Exported API

### Types

| Type | Description |
|------|-------------|
| `Store` | Main store; all methods are on `*Store` |
| `Connection` | `{PeerID PeerKey, RemotePort, LocalPort uint32}` — comparable struct used as map key directly |
| `KnownPeer` | Full peer metadata for connection/dialling |
| `WorkloadSpecView` | Workload spec merged with its winning publisher (`Spec`, `Publisher`) |
| `NodePlacementState` | Resource + Vivaldi + traffic data per node for scheduler placement scoring |
| `TrafficSnapshot` | `{BytesIn, BytesOut uint64}` accumulated traffic counters for one peer |
| `RouteNodeInfo` | Per-node data for route table construction and status display (`IPs`, `Reachable`, `Coord`, `Services`, ports, resource %s, `PubliclyAccessible`) |
| `LocalNodeView` | Read-only view of local node state (`IPs`, ports, resource %s, `WorkloadClaims`, `PubliclyAccessible`) |
| `ApplyResult` | `{Rebroadcast []*statev1.GossipEvent}` — events to rebroadcast after `ApplyEvents` |

### Errors

| Error | Description |
|-------|-------------|
| `ErrSpecOwnedRemotely` | Returned by `SetLocalWorkloadSpec` when a valid remote peer already publishes the same hash |

### Constructor & Lifecycle

| Method | Description |
|--------|-------------|
| `Load(pollenDir string, identityPub []byte) (*Store, error)` | Opens or creates the store, replaying disk state |
| `(*Store).Close() error` | Closes disk resources |
| `(*Store).Save() error` | Snapshots current in-memory state to disk |

### Configuration

| Method | Description |
|--------|-------------|
| `(*Store).LocalID() types.PeerKey` | Identity key of the local node |
| `(*Store).SetGossipMetrics(m *metrics.GossipMetrics)` | Wires live metric instruments (default: no-op) |
| `(*Store).OnDenyPeer(fn func(types.PeerKey))` | Callback fired outside the lock when a peer is denied |
| `(*Store).OnRouteInvalidate(fn func())` | Callback fired when reachability or Vivaldi state changes |
| `(*Store).OnWorkloadChange(fn func())` | Callback fired when workload spec/claim state changes |
| `(*Store).OnTrafficChange(fn func())` | Callback fired when traffic heatmap state changes |

### CRDT / Gossip

| Method | Description |
|--------|-------------|
| `(*Store).EagerSyncClock() *statev1.GossipStateDigest` | Returns empty digest if no remote state yet (forces full send from peer); otherwise returns the real digest |
| `(*Store).Clock() *statev1.GossipStateDigest` | Current CRDT digest for anti-entropy handshakes |
| `(*Store).MissingFor(digest) []*statev1.GossipEvent` | Events that the described peer is missing |
| `(*Store).ApplyEvents(events []*statev1.GossipEvent, isPullResponse bool) ApplyResult` | Applies a batch of incoming events under one lock; fires callbacks outside the lock |
| `(*Store).LocalEvents() []*statev1.GossipEvent` | All current local events (used at startup for full broadcast) |

### Mutations (local state → gossip events)

All methods return `[]*statev1.GossipEvent` for the caller to rebroadcast (nil if nothing changed).

| Method | Description |
|--------|-------------|
| `SetLocalNetwork(ips []string, port uint32)` | IPs and listen port |
| `SetExternalPort(port uint32)` | STUN-observed external port |
| `SetObservedExternalIP(ip string)` | STUN-observed external IP (empty string emits deletion) |
| `SetLocalPubliclyAccessible(accessible bool)` | Public accessibility flag |
| `SetLocalNatType(natType nat.Type)` | NAT type (`nat.Unknown` emits deletion) |
| `SetLocalVivaldiCoord(coord topology.Coord)` | Vivaldi coordinate (suppressed if movement < `topology.PublishEpsilon`) |
| `SetLocalResourceTelemetry(cpu, mem uint32, memTotal uint64, numCPU uint32)` | CPU/memory telemetry (suppressed within a ±2% deadband) |
| `SetLocalTrafficHeatmap(rates map[PeerKey]TrafficSnapshot)` | Per-peer traffic counters (zero-byte entries stripped; empty map emits deletion) |
| `SetLocalCertExpiry(expiry int64)` | TLS certificate expiry (embedded in identity event) |
| `SetLocalConnected(peerID PeerKey, connected bool)` | Local reachability edge to a peer |
| `SetLastAddr(peerID PeerKey, addr string)` | Last observed address for a peer (not gossiped; local bookkeeping only) |
| `DenyPeer(subjectPub []byte)` | Permanently deny a peer cluster-wide |
| `UpsertLocalService(port uint32, name string)` | Announce a local service |
| `RemoveLocalServices(name string)` | Withdraw a local service |

### Workload

| Method | Description |
|--------|-------------|
| `SetLocalWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) ([]*statev1.GossipEvent, error)` | Publish a workload spec; returns `ErrSpecOwnedRemotely` if a valid peer already owns the hash |
| `RemoveLocalWorkloadSpec(hash string) []*statev1.GossipEvent` | Withdraw a workload spec |
| `SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent` | Assert or release a workload execution claim |

### Connections

| Method | Description |
|--------|-------------|
| `DesiredConnections() []Connection` | Sorted list of currently desired peer connections |
| `AddDesiredConnection(peerID PeerKey, remotePort, localPort uint32)` | Record a desired connection |
| `RemoveDesiredConnection(peerID PeerKey, remotePort, localPort uint32)` | Remove matching connections (0 = wildcard for that port field) |

### Invites

| Method | Description |
|--------|-------------|
| `TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error)` | Idempotent invite consumption; persists to disk immediately. Implements `auth.InviteConsumer` |

### Queries

| Method | Description |
|--------|-------------|
| `AllRouteInfo() map[PeerKey]RouteNodeInfo` | Per-node data for route table construction; excludes denied/expired nodes |
| `ExternalPort(peerID PeerKey) (uint32, bool)` | STUN-observed external port for a peer |
| `LocalRecord() LocalNodeView` | Read-only view of local node state |
| `KnownPeers() []KnownPeer` | Valid peers with at least one connectable address, sorted by PeerKey |
| `AllPeerKeys() []PeerKey` | Live (reachable from local via BFS) peer keys |
| `AllWorkloadSpecs() map[string]WorkloadSpecView` | Merged workload specs across valid nodes; lowest PeerKey wins hash conflicts |
| `AllWorkloadClaims() map[string]map[PeerKey]struct{}` | Claims from live peers only (dead-peer claims excluded) |
| `AllTrafficHeatmaps() map[PeerKey]map[PeerKey]TrafficSnapshot` | Per-node traffic data for valid nodes with non-empty traffic |
| `AllNodePlacementStates() map[PeerKey]NodePlacementState` | Placement scoring data for live nodes |
| `ResolveWorkloadPrefix(prefix string) (hash string, ambiguous, found bool)` | Prefix-match a workload hash across valid nodes' specs |
| `NodeIPs(peerID PeerKey) []string` | IPs for a peer |
| `IsConnected(source, target PeerKey) bool` | Whether source's reachability set contains target |
| `HasServicePort(peerID PeerKey, port uint32) bool` | Whether a peer advertises a service on a given port |
| `IsPubliclyAccessible(peerID PeerKey) bool` | Whether a peer is publicly reachable |
| `PeerVivaldiCoord(peerID PeerKey) (*topology.Coord, bool)` | Vivaldi coordinate for a peer |
| `NatType(peerID PeerKey) nat.Type` | NAT type for a peer |
| `IdentityPub(peerID PeerKey) ([]byte, bool)` | Identity public key bytes for a peer |
| `IsDenied(subjectPub []byte) bool` | Whether a peer is in the cluster deny list |
| `LocalServices() map[string]*statev1.Service` | Clone of local service advertisements |
