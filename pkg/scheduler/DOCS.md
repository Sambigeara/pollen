# pkg/scheduler

## Responsibilities
- Distributed workload placement: ranks peers by capacity, traffic affinity, and network proximity
- Manages workload lifecycle: seeding, artifact fetching, claim lifecycle, eviction with cooldowns
- Artifact fetching from mesh streams with fallback to multiple peers
- Workload function invocation over mesh streams with wire protocol

## Consumer API

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

## Concurrency Contract

`Run` calls `wg.Wait()` before returning, ensuring all background `executeClaim` goroutines complete before the caller assumes the reconciler is stopped. This prevents `store.SetLocalWorkloadClaim()` from racing with `store.Close()` during shutdown.

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/store | `WorkloadSpecView`, `NodePlacementState`, `TrafficSnapshot` |
| pkg/topology | `Distance`, `Coord` |
| pkg/types | `PeerKey` |
| pkg/wasm | `PluginConfig` |
| pkg/workload | `ErrNotRunning` |

## Consumed by
- pkg/node (uses: `Reconciler`, `NewReconciler`, `HandleArtifactStream`, `HandleWorkloadStream`, `InvokeOverStream`, `NewArtifactFetcher`)

## Proposed Minimal API

No changes — API surface already minimal (gold standard). All dependencies accessed through narrow interfaces (`SchedulerStore`, `WorkloadManager`, `ArtifactStore`, etc.).
