# Package Boundary Audit

Generated 2026-03-14. Covers 20 packages in `pkg/` plus `cmd/pln`.

## Summary

### Dependency DAG (topological layers)

```
Layer 0 (leaves):  types, nat, perm, cas, wasm, observability/metrics, observability/traces
Layer 1:           auth (→perm), config (→perm), traffic (→types), topology (→nat,types), peer (→metrics,types)
Layer 2:           store (→nat,topology,types,auth,metrics), tunnel (→types,traffic), sock (→nat), route (→types,topology), workload (→cas,wasm)
Layer 3:           mesh (→auth,nat,metrics,traces,peer,sock,traffic,types)
Layer 4:           scheduler (→store,topology,types,wasm,workload)
Layer 5:           node (→auth,cas,mesh,nat,metrics,traces,peer,route,scheduler,store,topology,traffic,tunnel,types,wasm,workload)
Layer 6:           cmd/pln (→auth,config,node,peer,perm,store,types)
```

No cycles.

### Fan-in / Fan-out

| Package | Fan-in | Fan-out | Layer |
|---------|--------|---------|-------|
| `types` | 10 | 0 | 0 |
| `nat` | 5 | 0 | 0 |
| `perm` | 3 | 0 | 0 |
| `cas` | 2 | 0 | 0 |
| `wasm` | 3 | 0 | 0 |
| `observability/metrics` | 5 | 0 | 0 |
| `observability/traces` | 2 | 0 | 0 |
| `auth` | 4 | 1 | 1 |
| `config` | 1 | 1 | 1 |
| `traffic` | 3 | 1 | 1 |
| `topology` | 4 | 2 | 1 |
| `peer` | 3 | 2 | 1 |
| `store` | 3 | 5 | 2 |
| `tunnel` | 1 | 2 | 2 |
| `sock` | 1 | 1 | 2 |
| `route` | 1 | 2 | 2 |
| `workload` | 2 | 2 | 2 |
| `mesh` | 1 | 8 | 3 |
| `scheduler` | 1 | 5 | 4 |
| `node` | 1 | 16 | 5 |
| `cmd/pln` | 0 | 7 | 6 |

### Boundary Observations

1. **pkg/node fan-out (16)** — imports nearly every package. Expected for a top-level orchestrator but makes it the primary coupling bottleneck. Any package API change ripples through node.

2. **pkg/mesh ↔ pkg/peer tight coupling** — mesh consumes nearly all of peer's event types, state constants, and disconnect reasons. These two packages are semantically coupled; the boundary is more organizational than architectural.

3. **Fan-in-1 packages** — `tunnel` (only node), `sock` (only mesh), `config` (only cmd/pln), `route` (only node). These are effectively private implementation details of their single consumer. Consider whether they warrant separate packages.

4. **pkg/store wide API surface** — 40+ exported methods, most consumed only by pkg/node. The boundary is broad; narrowing with consumer-specific interfaces would reduce coupling.

5. **pkg/scheduler interface discipline** — all dependencies are accessed through narrow interfaces (SchedulerStore, WorkloadManager, ArtifactStore). Good testability pattern worth replicating elsewhere.

6. **pkg/types highest fan-in (10)** — healthy for a leaf identity primitive. No concerns.

7. **Observability packages used asymmetrically** — metrics has fan-in 5 (mesh, node, peer, store, scheduler) while traces has fan-in 2 (mesh, node). Tracing integration is incomplete or intentionally narrow.

---

## pkg/types

### Responsibilities
- Defines `PeerKey`, the canonical 32-byte ed25519 public key identity for every mesh node
- Provides constructors from bytes and hex strings, plus comparison/ordering utilities

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `PeerKey` | type | `[32]byte` ed25519 public key; comparable, usable as map key |
| `PeerKeyFromBytes` | func | Construct PeerKey from byte slice |
| `PeerKeyFromString` | func | Construct PeerKey from hex string |
| `(PeerKey).Bytes` | method | Raw 32-byte slice |
| `(PeerKey).String` | method | 64-char lowercase hex encoding |
| `(PeerKey).Short` | method | First 8 hex chars for logging |
| `(PeerKey).Less` | method | Lexicographic less-than |
| `(PeerKey).Compare` | method | Three-way comparison (-1, 0, +1) |

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/mesh (uses: `PeerKey`, `PeerKeyFromBytes`)
- pkg/node (uses: `PeerKey`, `PeerKeyFromBytes`)
- pkg/peer (uses: `PeerKey`)
- pkg/store (uses: `PeerKey`, `PeerKeyFromBytes`, `PeerKeyFromString`)
- pkg/topology (uses: `PeerKey`)
- pkg/traffic (uses: `PeerKey`)
- pkg/route (uses: `PeerKey`)
- pkg/tunnel (uses: `PeerKey`)
- pkg/scheduler (uses: `PeerKey`)
- cmd/pln (uses: `PeerKey`, `PeerKeyFromBytes`)

---

## pkg/nat

### Responsibilities
- Classifies NAT behavior as endpoint-independent ("easy") or endpoint-dependent ("hard")
- Provides NAT detection via observation collection from peer reports
- Supplies wire-protocol conversion (uint32)

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Type` | type | NAT classification enum |
| `Unknown` | const | Unclassified NAT |
| `Easy` | const | Endpoint-independent NAT (stable external port) |
| `Hard` | const | Endpoint-dependent/symmetric NAT (variable port) |
| `(Type).String` | method | String representation |
| `(Type).ToUint32` | method | Wire protocol conversion |
| `TypeFromUint32` | func | Reconstruct Type from uint32 |
| `Detector` | type | State machine for NAT type determination |
| `NewDetector` | func | Create a new NAT detector |
| `(*Detector).AddObservation` | method | Record port observation from peer |
| `(*Detector).Type` | method | Get current NAT classification |

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/mesh (uses: `Type`)
- pkg/node (uses: `Detector`, `NewDetector`, `Type`)
- pkg/store (uses: `Type`, `Unknown`)
- pkg/topology (uses: `Type`)
- pkg/sock (uses: `Type`, `Easy`)

---

## pkg/perm

### Responsibilities
- Manages file permissions for the pln data directory (Linux: pln:pln ownership; other platforms: no-ops)
- Provides atomic file writing with appropriate modes (private, group-readable, group-writable)
- Ensures directory creation with correct ownership

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `SetGroupDir` | func | Set directory mode to 0770 with pln:pln ownership |
| `SetGroupReadable` | func | Set file mode to 0640 with pln:pln ownership |
| `SetGroupSocket` | func | Set socket mode to 0660 with pln:pln ownership |
| `SetPrivate` | func | Set file mode to 0600 |
| `EnsureDir` | func | Create directory with parents; pln:pln ownership on Linux |
| `WritePrivate` | func | Atomically write with 0600 mode |
| `WriteGroupReadable` | func | Atomically write with 0640 mode |
| `WriteGroupWritable` | func | Atomically write with 0660 mode |

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/auth (uses: `EnsureDir`, `SetGroupReadable`, `WriteGroupReadable`)
- pkg/config (uses: `EnsureDir`, `WriteGroupWritable`)
- pkg/store (uses: `EnsureDir`, `SetPrivate`, `WriteGroupReadable`)

---

## pkg/cas

### Responsibilities
- Content-addressable storage for WASM artifacts indexed by SHA-256 hash
- Atomic store and retrieval backed by local filesystem
- Directory sharding by hash prefix for scalability

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `ErrNotFound` | var | Sentinel error when artifact not found |
| `Store` | type | Content-addressable artifact store |
| `New` | func | Create store rooted at pollenDir/cas |
| `(*Store).Put` | method | Write artifact from reader; return SHA-256 hex hash |
| `(*Store).Get` | method | Open artifact for reading by hash |
| `(*Store).Has` | method | Check if artifact exists |

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/node (uses: `Store`, `New`)
- pkg/workload (uses: `Store`, `Put`, `Get`)

---

## pkg/wasm

### Responsibilities
- WASM plugin runtime backed by Extism and wazero
- Compiles and caches modules by content hash
- Manages plugin instantiation with configurable resource limits and host functions
- Probes runtime config to detect mmap(PROT_EXEC) restrictions

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `ErrModuleMissing` | var | Sentinel error when compiled module not cached |
| `PluginConfig` | type | Per-workload resource limits (memory, timeout) |
| `Runtime` | type | Extism-backed WASM runtime with module cache |
| `NewRuntime` | func | Create runtime with host functions and concurrency limit |
| `(*Runtime).Compile` | method | Compile and cache WASM bytes by hash |
| `(*Runtime).Call` | method | Invoke function on cached module instance |
| `(*Runtime).DropCompiled` | method | Remove cached compiled module |
| `(*Runtime).Close` | method | Close all compiled plugins |
| `InvocationRouter` | interface | Inter-workload RPC through mesh (`RouteCall` method) |
| `NewHostFunctions` | func | Create host functions (pollen_log, pollen_call) |

**`InvocationRouter` consumed by:** pkg/node

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/node (uses: `Runtime`, `NewRuntime`, `NewHostFunctions`, `PluginConfig`, `InvocationRouter`)
- pkg/scheduler (uses: `PluginConfig`)
- pkg/workload (uses: `Runtime`, `PluginConfig`)

---

## pkg/observability/metrics

### Responsibilities
- Lightweight opt-in metrics collection (no-ops when disabled via nil-safe Counter/Gauge)
- Counters and gauges tracked with atomic operations
- Periodic flushing to pluggable `Sink` interface
- Pre-registered instrument sets for mesh, peer, gossip, topology, and node layers
- EWMA support for smoothed values

### Consumer API

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

**`Sink` consumed by:** pkg/node

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/mesh (uses: `MeshMetrics`, `Gauge`)
- pkg/node (uses: `Collector`, `New`, `Config`, `LogSink`, `NewLogSink`, all `New*Metrics` funcs, `EWMA`, `NewEWMAFrom`)
- pkg/peer (uses: `PeerMetrics`)
- pkg/store (uses: `GossipMetrics`)
- pkg/scheduler (uses: `Counter`)

---

## pkg/observability/traces

### Responsibilities
- Distributed tracing with TraceID (16-byte) and SpanID (8-byte) generation
- Span creation and attribute tracking
- Pluggable `Sink` interface for exporting completed spans
- Nil-safe tracer and span API

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Tracer` | type | Creates and exports spans (nil-safe) |
| `NewTracer` | func | Create tracer with sink |
| `(*Tracer).Start` | method | Begin root span |
| `(*Tracer).StartFromRemote` | method | Continue trace from remote envelope |
| `Attribute` | type | Key-value pair attached to span |
| `Span` | type | Unit of work within trace (nil-safe) |
| `(*Span).End` | method | Record end time and export |
| `(*Span).SetAttr` | method | Append attribute |
| `(*Span).TraceIDBytes` | method | Extract trace ID for wire embedding |
| `TraceID` | type | 16-byte trace identifier |
| `NewTraceID` | func | Generate random TraceID |
| `TraceIDFromBytes` | func | Construct from byte slice |
| `(TraceID).IsZero` | method | Check if zero value |
| `(TraceID).String` | method | Hex-encoded representation |
| `SpanID` | type | 8-byte span identifier |
| `NewSpanID` | func | Generate random SpanID |
| `(SpanID).String` | method | Hex-encoded representation |
| `ReadOnlySpan` | type | Snapshot of completed span for export |
| `Sink` | interface | Receives completed spans (`Export` method) |
| `LogSink` | type | Zap logger sink implementation |
| `NewLogSink` | func | Create sink that logs spans |

**`Sink` consumed by:** pkg/node

### Dependencies (internal)

None — leaf package.

### Consumed by
- pkg/mesh (uses: `Tracer`)
- pkg/node (uses: `Tracer`, `NewTracer`, `LogSink`, `NewLogSink`, `TraceIDFromBytes`)

---

## pkg/auth

### Responsibilities
- Manages node identity keys (ed25519 key pair generation/loading)
- Issues and verifies delegation certificates for role-based access control
- Creates and validates join tokens and invite tokens for node enrollment
- Manages trust bundles (cluster root public key and cluster ID)

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `NodeCredentials` | type | Trust bundle + delegation cert + signer key |
| `DelegationSigner` | type | Wraps signer key, issuer cert, trust, and invite consumer |
| `VerifiedToken` | type | Result of VerifyJoinToken with verified claims |
| `InviteConsumer` | interface | `TryConsume` method for tracking redeemed invite tokens |
| `FullCapabilities` | func | Returns admin capabilities (CanDelegate, CanAdmit, MaxDepth=255) |
| `LeafCapabilities` | func | Returns member capabilities (no delegation or admission) |
| `CertExpiresAt` | func | Extract expiration time from delegation cert |
| `IsCertExpiredAt` | func | Check if cert expired at time (with skew allowance) |
| `IsCertExpired` | func | Check if cert is expired at given time |
| `CertTTL` | func | Original TTL of cert (notAfter - notBefore) |
| `CertAccessDeadline` | func | Hard access deadline from cert claims |
| `IsCertWithinReconnectWindow` | func | Check if expired cert is within grace period |
| `LoadOrEnrollNodeCredentials` | func | Load existing credentials or enroll from join token |
| `EnsureLocalRootCredentials` | func | Ensure root credentials exist |
| `LoadExistingNodeCredentials` | func | Load and verify credentials |
| `SaveNodeCredentials` | func | Persist node credentials to disk |
| `LoadAdminKey` | func | Load root admin key from disk |
| `LoadOrCreateAdminKey` | func | Load or generate root admin key |
| `NewTrustBundle` | func | Create trust bundle from root public key |
| `IssueDelegationCert` | func | Issue signed delegation cert |
| `VerifyDelegationCertChain` | func | Verify proto validity, cluster scope, signature chain |
| `VerifyDelegationCert` | func | Verify delegation cert including time validity |
| `IssueJoinToken` | func | Admin issues join token for new node |
| `IssueJoinTokenWithIssuer` | func | Issue join token using delegated issuer cert |
| `VerifyJoinToken` | func | Verify join token signature and membership cert |
| `EncodeJoinToken` | func | Base64 encode join token |
| `DecodeJoinToken` | func | Base64 decode join token |
| `SaveDelegationCert` | func | Persist delegation cert to disk |
| `LoadDelegationSigner` | func | Load DelegationSigner from admin key or delegated cert |
| `IssueInviteTokenWithSigner` | func | Issue invite token using DelegationSigner |
| `VerifyInviteToken` | func | Verify invite token signature and claims |
| `EncodeInviteToken` | func | Base64 encode invite token |
| `DecodeInviteToken` | func | Base64 decode invite token |
| `MarshalDelegationCertBase64` | func | Encode delegation cert to base64 |
| `UnmarshalDelegationCertBase64` | func | Decode delegation cert from base64 |
| `GenIdentityKey` | func | Load or generate node ed25519 identity key pair |
| `ReadIdentityPub` | func | Read public key from disk |

**`InviteConsumer` consumed by:** pkg/store, pkg/mesh (via DelegationSigner)

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/perm | `EnsureDir`, `SetGroupReadable`, `WriteGroupReadable` |

### Consumed by
- pkg/mesh (uses: `DelegationSigner`, `NodeCredentials`, `CertExpiresAt`, `VerifyInviteToken`, `IssueJoinTokenWithIssuer`, `CertAccessDeadline`)
- pkg/node (uses: `NodeCredentials`, `IsCertExpired`, `IsCertWithinReconnectWindow`, `VerifyDelegationCert`, `SaveNodeCredentials`, `LoadOrEnrollNodeCredentials`, `EnsureLocalRootCredentials`, `LoadDelegationSigner`, `GenIdentityKey`, `ReadIdentityPub`)
- pkg/store (uses: `InviteConsumer`, `IsCertExpiredAt`)
- cmd/pln (uses: `VerifyJoinToken`, `NodeCredentials`, `LoadNodeCredentials`, `GenerateEdKeyAndInit`, `IssueAdminToken`, `IssueInviteToken`)

---

## pkg/config

### Responsibilities
- Loads/saves YAML configuration file for node state
- Manages bootstrap peer list with address normalization
- Tracks local services and outbound connections
- Stores certificate TTL settings

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `DefaultBootstrapPort` | const | Default bootstrap port (60611) |
| `BootstrapPeer` | type | YAML-serialized bootstrap peer (PeerPub hex, Addrs list) |
| `CertTTLs` | type | Membership, delegation, TLS identity, and reconnect TTLs |
| `Service` | type | Service name and port binding |
| `Connection` | type | Service, peer, remote port, and local port mapping |
| `Config` | type | Root configuration struct |
| `Load` | func | Load config from YAML; returns empty if not exists |
| `Save` | func | Save config to YAML with header |
| `RememberBootstrapPeer` | func | Add bootstrap peer and canonicalize |
| `ForgetBootstrapPeer` | func | Remove bootstrap peer by public key |
| `BootstrapProtoPeers` | func | Convert bootstrap list to proto format |
| `NormalizeRelayAddr` | func | Normalize relay address with default port |
| `AddService` | func | Add or update service by name/port |
| `RemoveService` | func | Remove service by name |
| `RemoveServiceByPort` | func | Remove service by port |
| `AddConnection` | func | Add outbound connection tracking |
| `RemoveConnection` | func | Remove connections (wildcard matching) |
| `(CertTTLs).MembershipTTL` | method | Return membership TTL or default |
| `(CertTTLs).DelegationTTL` | method | Return delegation TTL or default |
| `(CertTTLs).TLSIdentityTTL` | method | Return TLS identity TTL or default |
| `(CertTTLs).ReconnectWindowDuration` | method | Return reconnect window or default |

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/perm | `EnsureDir`, `WriteGroupWritable` |

### Consumed by
- cmd/pln (uses: `Load`, `Save`, `Config`, `RememberBootstrapPeer`, `ForgetBootstrapPeer`, `BootstrapProtoPeers`, `AddService`, `RemoveService`, `AddConnection`, `RemoveConnection`, TTL methods)

---

## pkg/traffic

### Responsibilities
- Records per-peer inbound/outbound byte counts from bidirectional streams
- Accumulates traffic with sliding window (3-slot ring buffer)
- Suppresses publication when changes are below deadband threshold (2%)
- Provides noop implementation for optional traffic tracking

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Recorder` | interface | `Record` method for per-peer byte count tracking |
| `Noop` | var | No-op Recorder implementation |
| `PeerTraffic` | type | Accumulated BytesIn and BytesOut for a peer |
| `Tracker` | type | Thread-safe accumulator with sliding window and deadband |
| `New` | func | Create new Tracker |
| `(*Tracker).Record` | method | Add byte counts for a peer |
| `(*Tracker).RotateAndSnapshot` | method | Rotate window, sum, compare against published |
| `WrapStream` | func | Wrap bidirectional stream with traffic counting |

**`Recorder` consumed by:** pkg/mesh, pkg/tunnel

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/types | `PeerKey` |

### Consumed by
- pkg/mesh (uses: `Recorder`, `Noop`, `WrapStream`)
- pkg/node (uses: `New`, `Tracker`, `RotateAndSnapshot`)
- pkg/tunnel (uses: `Recorder`, `Noop`, `WrapStream`)

---

## pkg/topology

### Responsibilities
- Computes deterministic outbound connection targets (infrastructure backbone + k-nearest by Vivaldi + random long-links)
- Implements Vivaldi network coordinate system for latency prediction
- Manages NAT type filtering and LAN diversity caps
- Three-layer topology budget system with HMAC-based deterministic selection

### Consumer API

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

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/nat | `Type` |
| pkg/types | `PeerKey` |

### Consumed by
- pkg/node (uses: `ComputeTargetPeers`, `Coord`, `RandomCoord`, `Sample`, `Update`, `EpochSeconds`, `DefaultInfraMax`, `DefaultNearestK`, `DefaultRandomR`, `SameObservedEgress`, `InferPrivatelyRoutable`)
- pkg/route (uses: `Coord`, `Distance`)
- pkg/store (uses: `Coord`, `MovementDistance`, `PublishEpsilon`, `Distance`)
- pkg/scheduler (uses: `Distance`, `Coord`)

---

## pkg/peer

### Responsibilities
- Maintains state machine for peer connection lifecycle (Discovered → Connecting → Connected / Unreachable)
- Manages connection staging (direct dial → eager retry → NAT punch)
- Emits events for attempted connections and disconnections
- Thread-safe store with per-peer state counts and metrics

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `State` | type | Peer connection state enum |
| `Discovered` | const | Known but not yet connected |
| `Connecting` | const | Connection attempt in progress |
| `Connected` | const | Successfully connected |
| `Unreachable` | const | Failed attempts exhausted |
| `ConnectStage` | type | Connection attempt stage enum |
| `ConnectStageDirect` | const | Direct dial attempt |
| `ConnectStageEagerRetry` | const | Retry on previously-known address |
| `ConnectStagePunch` | const | NAT punch coordination |
| `Peer` | type | Peer record (ID, state, addresses, stage) |
| `Input` | interface | Marker interface for input events |
| `DiscoverPeer` | type | Event: add/update peer with addresses |
| `Tick` | type | Event: evaluate all peers for pending actions |
| `ConnectPeer` | type | Event: signal successful connection |
| `ConnectFailed` | type | Event: signal failed connection attempt |
| `PeerDisconnected` | type | Event: signal disconnection with reason |
| `RetryPeer` | type | Event: retry connection for a peer |
| `ForgetPeer` | type | Event: remove peer from state machine |
| `DisconnectReason` | type | Reason enum for disconnection |
| `DisconnectUnknown` | const | Unknown |
| `DisconnectIdleTimeout` | const | Idle timeout |
| `DisconnectReset` | const | Stateless reset |
| `DisconnectGraceful` | const | Clean app-level close |
| `DisconnectTopologyPrune` | const | Topology pruned edge |
| `DisconnectDenied` | const | Peer denied session/membership |
| `DisconnectCertRotation` | const | Forced reconnection for cert rotation |
| `DisconnectCertExpired` | const | Membership cert expired |
| `Output` | interface | Marker interface for output effects |
| `PeerConnected` | type | Output: peer transitioned to connected |
| `AttemptConnect` | type | Output: attempt direct connection |
| `AttemptEagerConnect` | type | Output: attempt on known address |
| `RequestPunchCoordination` | type | Output: request NAT punch coordination |
| `Store` | type | Thread-safe peer state machine store |
| `NewStore` | func | Create new Store |
| `(*Store).SetPeerMetrics` | method | Wire metrics instruments |
| `(*Store).Step` | method | Process input event, return output effects |
| `(*Store).Get` | method | Get peer state by key |
| `(*Store).InState` | method | Check if peer is in given state |
| `(*Store).GetAll` | method | Get all peers in given state |
| `StateCounts` | type | Per-state peer counts |
| `(*Store).StateCounts` | method | Return current state counts |

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/observability/metrics | `PeerMetrics` |
| pkg/types | `PeerKey` |

### Consumed by
- pkg/mesh (uses: `Input`, `Output`, all event types, all state constants, all disconnect reasons)
- pkg/node (uses: `Store`, `NewStore`, all event types, all output types, all disconnect reasons)
- cmd/pln (uses: `Store`)

---

## pkg/store

### Responsibilities
- Manages cluster-wide CRDT gossip state: peer network/identity/services/reachability, workload specs/claims, traffic, coordinates
- Implements disk persistence and migration
- Provides query APIs for topology, scheduling, and service discovery
- Handles invite token consumption tracking and peer denial

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Store` | type | Main state container |
| `Load` | func | Open or create store, replaying disk state |
| `Connection` | type | Service connection descriptor |
| `KnownPeer` | type | Peer metadata for connectivity |
| `LocalNodeView` | type | Read-only snapshot of local node state |
| `RouteNodeInfo` | type | Routing-relevant peer state |
| `NodePlacementState` | type | Scheduler state snapshot |
| `TrafficSnapshot` | type | Per-peer traffic counters |
| `WorkloadSpecView` | type | Workload spec + publisher identity |
| `ApplyResult` | type | Result of gossip event application |
| `ErrSpecOwnedRemotely` | var | Error: spec hash owned by another node |

Full method list: 40+ methods covering lifecycle, CRDT gossip, mutations, workload, connections, invites, and queries. See `pkg/store/DOCS.md` for complete reference.

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/auth | `InviteConsumer` (interface), `IsCertExpiredAt` |
| pkg/nat | `Type`, `Unknown` |
| pkg/observability/metrics | `GossipMetrics`, `NewGossipMetrics` |
| pkg/topology | `Coord`, `MovementDistance`, `PublishEpsilon`, `Distance` |
| pkg/types | `PeerKey`, `PeerKeyFromBytes`, `PeerKeyFromString` |

### Consumed by
- pkg/node (uses: `Store`, `Load`, all mutation/query methods, `Connection`, `KnownPeer`, `RouteNodeInfo`, `TrafficSnapshot`)
- pkg/scheduler (uses: `NodePlacementState`, `WorkloadSpecView`, `AllNodePlacementStates`, `AllWorkloadSpecs`, `AllWorkloadClaims`, `AllPeerKeys`, `SetLocalWorkloadClaim`)
- cmd/pln (uses: `Store`, `Load`)

---

## pkg/tunnel

### Responsibilities
- Manages service tunneling over peer QUIC streams
- Handles incoming stream acceptance and port header parsing
- Maintains service registrations and remote connections
- Bridges tunnel connections to local services and clients

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Manager` | type | Service tunnel orchestrator |
| `New` | func | Constructor |
| `StreamTransport` | interface | QUIC stream provider (`OpenStream`, `AcceptStream`) |
| `ConnectionInfo` | type | Active connection metadata |
| `(*Manager).SetTrafficTracker` | method | Inject traffic recorder |
| `(*Manager).Start` | method | Start stream acceptance loop |
| `(*Manager).RegisterService` | method | Register local service port handler |
| `(*Manager).UnregisterService` | method | Unregister service; close streams |
| `(*Manager).ConnectService` | method | Open tunnel to remote service; return local port |
| `(*Manager).ListConnections` | method | Return active connections |
| `(*Manager).DisconnectLocalPort` | method | Close connection by local port |
| `(*Manager).DisconnectPeer` | method | Close all connections to peer |
| `(*Manager).DisconnectRemoteService` | method | Close specific remote service connection |
| `(*Manager).Close` | method | Gracefully close all tunnels and services |

**`StreamTransport` consumed by:** pkg/mesh (implements it)

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/traffic | `Recorder`, `Noop`, `WrapStream` |
| pkg/types | `PeerKey` |

### Consumed by
- pkg/node (uses: `Manager`, `New`, all methods)

---

## pkg/sock

### Responsibilities
- Manages UDP socket connections for NAT hole punching
- Implements probe-based NAT traversal (STUN-like protocol)
- Tracks active sockets and deduplicates connections
- Handles main socket probe responses from remote peers

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `SockStore` | interface | Socket and punch management |
| `Conn` | type | UDP connection with peer address and ref-counted close |
| `ProbeWriter` | type | Function type: writes probe packets to address |
| `ErrUnreachable` | var | Peer unreachable error sentinel |
| `NewSockStore` | func | Constructor |
| `(*Conn).Peer` | method | Return peer address |
| `(*Conn).Close` | method | Close connection (ref-counted) |

**`SockStore` consumed by:** pkg/mesh

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/nat | `Type`, `Easy` |

### Consumed by
- pkg/mesh (uses: `SockStore`, `NewSockStore`, `Conn`, `ErrUnreachable`)

---

## pkg/route

### Responsibilities
- Computes shortest-path routes via Dijkstra algorithm
- Uses Vivaldi coordinates for edge weighting
- Maintains thread-safe routing table with change notifications
- Provides route lookup for multi-hop mesh routing

### Consumer API

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

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/topology | `Coord`, `Distance` |
| pkg/types | `PeerKey` |

### Consumed by
- pkg/node (uses: `Table`, `New`, `Lookup`, `Update`, `NodeInfo`)

---

## pkg/workload

### Responsibilities
- Manages WASM workload lifecycle (seed/compile/invoke/unseed)
- Orchestrates artifact storage and compilation
- Provides workload execution context and result marshaling
- Implements prefix resolution for hash lookups

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Manager` | type | Workload lifecycle manager |
| `New` | func | Constructor |
| `Status` | type | Workload status enum |
| `StatusRunning` | const | Workload running state |
| `Summary` | type | Workload snapshot (Hash, Status, CompiledAt) |
| `(*Manager).Seed` | method | Store WASM and compile; return hash |
| `(*Manager).SeedFromCAS` | method | Compile from CAS artifact |
| `(*Manager).IsRunning` | method | Check if workload compiled |
| `(*Manager).Call` | method | Invoke function on compiled workload |
| `(*Manager).Unseed` | method | Remove workload and drop module |
| `(*Manager).ResolvePrefix` | method | Resolve hash prefix; return full hash |
| `(*Manager).List` | method | Return all workloads as summaries |
| `(*Manager).Close` | method | Drop all compiled modules |
| `ErrAlreadyRunning` | var | Workload already running |
| `ErrNotRunning` | var | Workload not running |
| `ErrAmbiguousPrefix` | var | Hash prefix ambiguous |
| `ErrStore` | var | CAS storage error |
| `ErrCompile` | var | Compilation error |

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/cas | `Store`, `Put`, `Get` |
| pkg/wasm | `Runtime`, `PluginConfig`, `Compile`, `Call`, `DropCompiled`, `ErrModuleMissing` |

### Consumed by
- pkg/node (uses: `Manager`, `New`, `Seed`, `SeedFromCAS`, `IsRunning`, `Call`, `Unseed`, `List`, `Close`, `Summary`, `ErrNotRunning`, `ErrCompile`, `ErrAlreadyRunning`)
- pkg/scheduler (uses: `ErrNotRunning`; via interface: `SeedFromCAS`, `Unseed`, `IsRunning`)

---

## pkg/mesh

### Responsibilities
- Manages peer-to-peer QUIC connections with TLS-based identity and delegation certificates
- Handles message routing: direct, routed (multi-hop), and specialized streams (tunnel, artifact, workload, clock)
- Provides NAT traversal via connection punching and address discovery
- Manages peer session lifecycle including addition, removal, and certificate rotation

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Mesh` | interface | Core mesh operations (24 methods: sessions, streams, datagrams, connections) |
| `Router` | interface | Multi-hop routing lookup |
| `CloseReason` | type | Named string type for session close reasons |
| `CloseReasonDenied` | const | Peer denied access |
| `CloseReasonTopologyPrune` | const | Topology-driven disconnection |
| `CloseReasonCertExpired` | const | Peer cert expired |
| `CloseReasonCertRotation` | const | Cert rotation trigger |
| `CloseReasonDisconnect` | const | Graceful disconnect |
| `CloseReasonDuplicate` | const | Duplicate connection closed |
| `CloseReasonReplaced` | const | Session replaced by newer |
| `CloseReasonDisconnected` | const | Session terminated |
| `CloseReasonShutdown` | const | Mesh shutdown |
| `Packet` | type | Envelope + sender peer key |
| `NewMesh` | func | Constructor |
| `GenerateIdentityCert` | func | Create identity TLS cert from delegation cert |
| `RedeemInvite` | func | Redeem invite token without active mesh (CLI join flow) |
| `GetAdvertisableAddrs` | func | Discover public/private IPs for advertisement |
| `DefaultExclusions` | var | IP ranges to exclude from advertisement |
| `ErrIdentityMismatch` | var | Peer identity mismatch sentinel |

**`Mesh` consumed by:** pkg/node
**`Router` consumed by:** pkg/node (injects route.Table)

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/auth | `DelegationSigner`, `NodeCredentials`, `VerifyInviteToken`, `IssueJoinTokenWithIssuer`, `CertExpiresAt`, `CertAccessDeadline` |
| pkg/nat | `Type` |
| pkg/observability/metrics | `MeshMetrics`, `Gauge` |
| pkg/observability/traces | `Tracer` |
| pkg/peer | `Input`, `ConnectPeer`, `PeerDisconnected`, `ForgetPeer`, `DisconnectReason` |
| pkg/sock | `SockStore`, `Conn` |
| pkg/traffic | `Recorder`, `WrapStream`, `Noop` |
| pkg/types | `PeerKey` |

### Consumed by
- pkg/node (uses: `Mesh`, `NewMesh`, `GenerateIdentityCert`, `CloseReason*`, `ErrIdentityMismatch`, `GetAdvertisableAddrs`, `DefaultExclusions`)

---

## pkg/scheduler

### Responsibilities
- Distributed workload placement: ranks peers by capacity, traffic affinity, and network proximity
- Manages workload lifecycle: seeding, artifact fetching, claim lifecycle, eviction with cooldowns
- Artifact fetching from mesh streams with fallback to multiple peers
- Workload function invocation over mesh streams with wire protocol

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `SchedulerStore` | interface | Query gossip store for specs, claims, peers, placement state |
| `WorkloadManager` | interface | Manage local workload compilation and execution |
| `ArtifactStore` | interface | Check local WASM artifact availability |
| `ArtifactFetcher` | interface | Fetch WASM from peers |
| `CASReader` | interface | Read artifacts from CAS |
| `CASWriter` | interface | Write artifacts into local CAS |
| `MeshStreamOpener` | interface | Open artifact streams to peers |
| `WorkloadInvoker` | interface | Execute function on locally compiled workload |
| `GossipPublisher` | type | Function type for publishing gossip events |
| `Reconciler` | type | Main reconciliation loop manager |
| `NewReconciler` | func | Constructor |
| `(*Reconciler).Run` | method | Main reconciliation loop (blocks until ctx cancelled) |
| `(*Reconciler).Signal` | method | Trigger reconciliation cycle (non-blocking) |
| `(*Reconciler).SignalTraffic` | method | Trigger traffic-driven reconciliation (non-blocking) |
| `HandleArtifactStream` | func | Server-side handler for artifact fetch requests |
| `HandleWorkloadStream` | func | Server-side handler for workload invocation requests |
| `InvokeOverStream` | func | Client-side workload function invocation over stream |
| `NewArtifactFetcher` | func | Create fetcher that pulls artifacts over mesh streams |

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/store | `WorkloadSpecView`, `NodePlacementState`, `TrafficSnapshot` |
| pkg/topology | `Distance`, `Coord` |
| pkg/types | `PeerKey` |
| pkg/wasm | `PluginConfig` |
| pkg/workload | `ErrNotRunning` |

### Consumed by
- pkg/node (uses: `Reconciler`, `NewReconciler`, `HandleArtifactStream`, `HandleWorkloadStream`, `InvokeOverStream`, `NewArtifactFetcher`)

---

## pkg/node

### Responsibilities
- Orchestrates entire node lifecycle and peer connectivity management
- Coordinates topology decisions using Vivaldi coordinates and network metrics
- Implements gRPC control plane (GetStatus, ConnectService, DisconnectService, SeedWorkload, CallWorkload, etc.)
- Manages gossip propagation, clock synchronization, certificate renewal, event broadcasting
- Service connection forwarding via tunnel manager
- Routes workload invocations to local or remote claimants

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Config` | type | Node configuration |
| `BootstrapPeer` | type | Bootstrap peer descriptor |
| `Node` | type | Main node orchestrator |
| `New` | func | Constructor |
| `(*Node).Start` | method | Main event loop (blocks until ctx cancelled) |
| `(*Node).Ready` | method | Channel closed when node is ready |
| `(*Node).ListenPort` | method | Get mesh listen port |
| `(*Node).GetConnectedPeers` | method | Get connected peer list |
| `(*Node).ConnectService` | method | Connect to remote service; return local port |
| `(*Node).DisconnectService` | method | Disconnect from service by local port |
| `(*Node).RouteCall` | method | Route workload invocation locally or to remote claimant |
| `NodeService` | type | gRPC service handler |
| `NewNodeService` | func | Constructor for gRPC service |
| `MaxDatagramPayload` | const | Safe QUIC datagram size |
| `ErrCertExpired` | var | Cert expired beyond reconnect window |

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/auth | `NodeCredentials`, `CertExpiresAt`, `IsCertExpired`, `IsCertWithinReconnectWindow`, `VerifyDelegationCert`, `SaveNodeCredentials` |
| pkg/cas | `Store` |
| pkg/mesh | `Mesh`, `NewMesh`, `GenerateIdentityCert`, `CloseReason*`, `ErrIdentityMismatch`, `GetAdvertisableAddrs`, `DefaultExclusions` |
| pkg/nat | `Detector`, `Type` |
| pkg/observability/metrics | `Collector`, all `New*Metrics` funcs, `EWMA`, `NewEWMAFrom` |
| pkg/observability/traces | `Tracer`, `NewTracer`, `LogSink`, `NewLogSink`, `TraceIDFromBytes` |
| pkg/peer | `Store`, all event/output types, all state/disconnect constants |
| pkg/route | `Table`, `NodeInfo` |
| pkg/scheduler | `Reconciler`, `NewReconciler`, `HandleArtifactStream`, `HandleWorkloadStream`, `InvokeOverStream`, `NewArtifactFetcher` |
| pkg/store | `Store`, `Load`, all mutation/query methods, view types |
| pkg/topology | `Coord`, `RandomCoord`, `Update`, `Sample`, `Distance`, `EpochSeconds`, `ComputeTargetPeers`, `InferPrivatelyRoutable`, `SameObservedEgress` |
| pkg/traffic | `Tracker`, `New`, `WrapStream` |
| pkg/tunnel | `Manager`, `New`, all methods |
| pkg/types | `PeerKey`, `PeerKeyFromBytes` |
| pkg/wasm | `Runtime`, `NewRuntime`, `NewHostFunctions`, `InvocationRouter`, `PluginConfig` |
| pkg/workload | `Manager`, `New`, `Status`, `ErrNotRunning`, `ErrCompile`, `ErrAlreadyRunning` |

### Consumed by
- cmd/pln (uses: `New`, `Node`, `Config`, `BootstrapPeer`, `NewNodeService`, `ErrCertExpired`)

---

## cmd/pln

### Responsibilities
- CLI entry point for user interactions (initialize, connect, join, invite, manage workloads)
- Daemon lifecycle management (start, stop, restart, upgrade)
- IPC communication with background daemon via gRPC over Unix socket
- Admin operations (invite generation, peer denial)
- Workload operations (seed, unseed, call)

### Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `main` | func | Entry point |

### Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/auth | `VerifyJoinToken`, `NodeCredentials`, `LoadNodeCredentials`, `GenIdentityKey`, `IssueInviteToken` |
| pkg/config | `Config`, `Load`, `Save`, bootstrap/service/connection methods |
| pkg/node | `New`, `Config`, `BootstrapPeer`, `Node`, `NewNodeService` |
| pkg/peer | `Store` |
| pkg/perm | socket file permissions |
| pkg/store | `Store`, `Load` |
| pkg/types | `PeerKey`, `PeerKeyFromBytes` |

### Consumed by

None — top-level binary.
