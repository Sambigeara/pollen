# pkg/scheduler — Target State

## Changes from Current

- [SAFE] Delete `NodeState` and `ClusterState` shadow types — use `store.NodePlacementState` directly
- [SAFE] Delete `Spec` wrapper struct — use `map[string]uint32` (replica counts) in `evaluate`
- [BREAKING] Unexport `Evaluate`, `Action`, `ActionKind`, `Spec`, `NodeState`, `ClusterState` — all internal-only
- [SAFE] Remove dead `hash string` parameter (named `_`) from `suitabilityScore`
- [SAFE] Remove `now()` nil guard — `nowFunc` always set by constructor
- [RISKY] Add `sync.WaitGroup` to track in-flight `executeClaim` goroutines; `Run` waits on it before returning

## Target Exported API

### Unexported (were exported, only used internally)

- `Evaluate` -> `evaluate`
- `Action` -> `action`
- `ActionKind` -> `actionKind`
- `ActionClaim` -> `actionClaim`
- `ActionRelease` -> `actionRelease`
- `Spec` -> deleted entirely (replaced by `map[string]uint32`)
- `NodeState` -> deleted entirely (replaced by `store.NodePlacementState`)
- `ClusterState` -> deleted entirely (replaced by `map[types.PeerKey]store.NodePlacementState`)

### Unchanged exports

`Reconciler`, `NewReconciler`, `Signal`, `SignalTraffic`, `Run`, `SchedulerStore`, `WorkloadManager`, `ArtifactStore`, `ArtifactFetcher`, `MeshStreamOpener`, `CASWriter`, `CASReader`, `WorkloadInvoker`, `GossipPublisher`, `NewArtifactFetcher`, `HandleArtifactStream`, `HandleWorkloadStream`, `InvokeOverStream`.

## Target Concurrency Contract

- `Run` now calls `wg.Wait()` before returning, ensuring all background `executeClaim` goroutines complete before the caller assumes the reconciler is stopped. This prevents `store.SetLocalWorkloadClaim()` from racing with `store.Close()` during shutdown.

## Deleted Items

| Item | Reason |
|------|--------|
| `NodeState` struct | Near-clone of `store.NodePlacementState`; use directly |
| `ClusterState` struct | Thin wrapper around `map[PeerKey]NodeState`; use `map[PeerKey]store.NodePlacementState` |
| `buildClusterState()` method | Field-by-field copy eliminated by using `store.NodePlacementState` directly |
| `Spec` struct | Wraps single `uint32`; replaced by `map[string]uint32` |
| `suitabilityScore` `hash string` parameter | Named `_`, never used |
| `now()` nil guard | `nowFunc` always set by constructor |
