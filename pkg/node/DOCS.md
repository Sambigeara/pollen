# pkg/node

## Responsibilities
- Orchestrates entire node lifecycle and peer connectivity management
- Coordinates topology decisions using Vivaldi coordinates and network metrics
- Implements gRPC control plane (GetStatus, ConnectService, DisconnectService, SeedWorkload, CallWorkload, etc.)
- Manages gossip propagation, clock synchronization, certificate renewal, event broadcasting
- Service connection forwarding via tunnel manager
- Routes workload invocations to local or remote claimants

## File Layout

| File | Content | Est. LOC |
|------|---------|----------|
| `node.go` | `Node` struct, `New`, `Start` (main event loop), `shutdown`, `handlePeerInput`, `handleOutputs`, `handleDatagram`, `tick`, `recomputeRoutes`, debounce logic | ~900 |
| `gossip.go` | `broadcastGossipBatches`, `queueGossipEvents`, `batchEvents`, `gossip`, `handleClockStream`, `sendClockViaStream`, `acceptClockStreamsLoop` | ~200 |
| `punch.go` | `requestPunchCoordination` (synchronous), `handlePunchCoordRequest` (sequential), `handlePunchCoordTrigger`, `punchLoop` | ~80 |
| `cert.go` | `checkCertExpiry`, `attemptCertRenewal`, `applyCertRenewal`, `handleCertRenewalRequest` | ~130 |
| `jitter.go` | `JitterTicker` helper, unexported | ~50 |
| `sysinfo.go` | System info sampling, unexported | ~20 |
| `svc.go` | Node service implementation | ~652 |
| `reachability.go` | Reachability checking | ~117 |
| `topology_profile.go` | Topology profiling | ~64 |

## Consumer API

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

## Dependencies (internal)

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

## Consumed by
- cmd/pln (uses: `New`, `Node`, `Config`, `BootstrapPeer`, `NewNodeService`, `ErrCertExpired`)

## Proposed Minimal API

### Exports kept

| Export | Consumers |
|--------|-----------|
| `Config`, `BootstrapPeer` | cmd/pln |
| `Node`, `New` | cmd/pln |
| `(*Node).Start` | cmd/pln |
| `(*Node).RouteCall` | cmd/pln (via `wasm.InvocationRouter`) |
| `NodeService`, `NewNodeService` | cmd/pln |

### Exports stripped (7)

| Export | Action | Reason |
|--------|--------|--------|
| `MaxDatagramPayload` | unexported | Only used within package for QUIC datagram sizing |
| `ErrCertExpired` | unexported | Only checked within package; gRPC status codes used at boundary |
| `(*Node).ConnectService` | unexported | Accessed through gRPC `NodeService`, not direct calls |
| `(*Node).DisconnectService` | unexported | Accessed through gRPC `NodeService`, not direct calls |
| `(*Node).Ready` | unexported | Only used in tests and internal coordination |
| `(*Node).ListenPort` | unexported | Only used in tests and internal coordination |
| `(*Node).GetConnectedPeers` | unexported | Only used in tests; status exposed through gRPC |
