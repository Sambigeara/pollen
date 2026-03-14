# Consolidation Diff

Dependency-ordered task list for the Pollen codebase consolidation. Changes are ordered so that independent changes come first, followed by changes that depend on earlier ones. Within each tier, changes are grouped by owner.

---

## Tier 1: Independent Foundation Changes (no dependencies)

### Change 1: Delete `types.Envelope`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/types`
- **Owner**: core-state
- **Estimated effort**: Small
- **Description**: Delete the dead `Envelope` struct from `types.go`. The codebase uses `meshv1.Envelope` from protobuf instead. This struct is never referenced outside the file.
- **Success criteria**: `go build ./...` passes. No references to `types.Envelope` in the codebase.

### Change 2: Delete `pkg/observability/logging`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/observability/logging`, `cmd/pln`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Delete the entire `pkg/observability/logging` package (23 LOC). `Init()` is never called. Remove any import from `cmd/pln`.
- **Success criteria**: `go build ./...` passes. Package directory deleted.

### Change 3: Delete dead traces fields (`parentSpanID` and friends)
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/observability/traces`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Delete `Span.parentSpanID` field, `ReadOnlySpan.ParentSpanID` field, `LogSink.Export` parent branch, `SpanID.IsZero()` method, and inline `TraceID.Bytes()` into `Span.TraceIDBytes()`. No child span API exists, so `parentSpanID` is never set.
- **Success criteria**: `go test ./pkg/observability/traces/...` passes. No remaining references to `parentSpanID` or `ParentSpanID`.

### Change 4: Delete `metrics.Labels` and all label plumbing
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/observability/metrics`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Delete the `Labels` type, remove `Snapshot.Labels` field, remove label iteration in `LogSink.Flush`, remove `labels` from `metricKey`, and remove the `labels Labels` parameter from `Collector.Counter()` and `Collector.Gauge()`. No metric uses labels — every registration passes `Labels{}`. Update all callers to remove the `Labels{}` argument.
- **Success criteria**: `go test ./...` passes. No references to `metrics.Labels` remain.

### Change 5: Add noop `traffic.Recorder`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/traffic`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Add `var Noop Recorder = noopRecorder{}` where `noopRecorder` implements `Record` as a no-op. This follows the project's "noop implementations for optional features" principle. Delete the unreachable zero-entry removal loop in `RotateAndSnapshot`.
- **Success criteria**: `go test ./pkg/traffic/...` passes. `traffic.Noop` is usable as a default.

### Change 6: Delete `util.Bump()` and bump channel
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/util`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Delete the `Bump()` method, the `bump` channel field, and the `case <-bump` branch in the JitterTicker goroutine's select loop. None are called anywhere.
- **Success criteria**: `go test ./pkg/util/...` and `go build ./...` pass.

### Change 7: Delete dead code in `pkg/sock`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/sock`
- **Owner**: transport
- **Estimated effort**: Small
- **Description**: Delete `sockStore.log` field (never read), move `probe()` to test file, delete redundant `Conn.onClose` nil check, delete redundant `Conn.UDPConn` nil check.
- **Success criteria**: `go test ./pkg/sock/...` passes.

### Change 8: Delete dead code in `pkg/cas`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/cas`
- **Owner**: workloads
- **Estimated effort**: Small
- **Description**: Delete unreachable short-hash guard in `path()` (`if len(hash) < 2`). SHA-256 hashes are always 64 characters.
- **Success criteria**: `go test ./pkg/cas/...` passes.

### Change 9: Unexport internal-only constants in `pkg/topology`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/topology`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Unexport `NearestHysteresis`, `MinHysteresisDistance`, `CcDefault`, `CeDefault`, `MinHeight`, `MaxCoord`, `MinRTTFloor`. Replace `clamp()` function with `max(lo, min(hi, v))` builtins.
- **Success criteria**: `go test ./pkg/topology/...` passes. No external references to the unexported constants.

### Change 10: Unexport internal-only constants in `pkg/config`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/config`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Unexport `DefaultMembershipTTL`, `DefaultDelegationTTL`, `DefaultTLSIdentityTTL`, `DefaultReconnectWindow`. Replace `ed25519PublicKeyBytes` with `crypto/ed25519.PublicKeySize`. Delete nil-receiver guards on `BootstrapProtoPeers` and `RememberBootstrapPeer`.
- **Success criteria**: `go test ./pkg/config/...` passes. `go build ./...` passes.

### Change 11: Clean up `pkg/wasm` exports
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/wasm`
- **Owner**: workloads
- **Estimated effort**: Small
- **Description**: Delete `IsCompiled` method (only called from tests). Tests should verify compilation through `Call` or use package-internal access. Use `mu.RLock` for read-only map lookups.
- **Success criteria**: `go test ./pkg/wasm/...` passes.

### Change 12: Clean up `pkg/workload` exports
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/workload`
- **Owner**: workloads
- **Estimated effort**: Small
- **Description**: Delete `StatusStopped` and `StatusErrored` enum values (never assigned). Unexport `IsRunning` (only called internally by `Call`).
- **Success criteria**: `go test ./pkg/workload/...` passes. `go build ./...` passes (update `pkg/node/svc.go` proto mapping if needed).

### Change 13: Clean up `pkg/auth` exports
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/auth`
- **Owner**: infra
- **Estimated effort**: Small
- **Description**: Unexport `EnsureNodeCredentialsFromToken` (only called by `LoadOrEnrollNodeCredentials`). Unexport `IsCapSubset` (only used internally + tests). Delete `VerifiedInviteToken` struct (return value discarded by all callers) — change `VerifyInviteToken` return to just `error`. Simplify `VerifyDelegationCertChain` wrapper.
- **Success criteria**: `go test ./pkg/auth/...` passes. `go build ./...` passes (update `pkg/mesh/invite.go` to not capture the deleted return value).

---

## Tier 2: File Splits and Reorganization (depend on Tier 1 dead code removal)

### Change 14: Split `store/gossip.go` into focused files
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 1 (types cleanup)
- **Packages touched**: `pkg/store`
- **Owner**: core-state
- **Estimated effort**: Medium
- **Description**: Split the 2,259-LOC `gossip.go` into 8 files: `store.go` (struct, constructor, lifecycle), `gossip.go` (CRDT log ops), `mutate.go` (all `SetLocal*`), `query.go` (all read methods), `workload.go` (workload spec/claim), `reachability.go` (BFS), `record.go` (nodeRecord type), `connections.go` (desired connections), `invites.go` (invite replay). Delete dead `default` panic branches and redundant nil guards during the split.
- **Success criteria**: `go test ./pkg/store/...` passes. All tests still pass with identical behavior. File count matches plan.

### Change 15: Split `mesh.go` into focused files
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 7 (sock cleanup)
- **Packages touched**: `pkg/mesh`
- **Owner**: transport
- **Estimated effort**: Medium
- **Description**: Split the 1,397-LOC `mesh.go` into `mesh.go` (interface, constructor, lifecycle), `session.go` (per-session management), `stream.go` (stream operations), `connect.go` (connection establishment), `cert.go` (cert management). During the split, unify the three `Open*Stream` methods into a single `openStreamWaitLoop` private method and unify `peerKeyFromRawCert`/`peerKeyFromConn`.
- **Success criteria**: `go test ./pkg/mesh/...` passes. Total LOC reduced by ~60 from deduplication.

### Change 16: Split `node.go` by concern
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 6 (util cleanup)
- **Packages touched**: `pkg/node`
- **Owner**: orchestrator
- **Estimated effort**: Medium
- **Description**: Extract from the 1,680-LOC `node.go`: `gossip.go` (gossip batching, clock streams), `punch.go` (NAT punch coordination), `cert.go` (cert lifecycle). Make `requestPunchCoordination` synchronous and `handlePunchCoordRequest` sequential (saves 3 goroutine spawns per coordination event).
- **Success criteria**: `go test ./pkg/node/...` passes. `node.go` reduced to ~900 LOC.

---

## Tier 3: API Narrowing and Abstraction Fixes (depend on Tier 2 splits)

### Change 17: Replace `store.Get()` and `AllNodes()` with purpose-built queries
- **Type**: SAFE
- **Risk**: Medium
- **Dependencies**: Change 14 (store split)
- **Packages touched**: `pkg/store`, `pkg/node`
- **Owner**: core-state
- **Estimated effort**: Medium
- **Description**: Delete `Get()` and `AllNodes()` which return the unexported `nodeRecord` type. Replace with `AllRouteInfo()` (returns only route-relevant data), `ExternalPort()`, and `LocalRecord()` (returns a read-only view). Expose `LocalID` via method instead of public field. Replace `Connection.Key()` with direct struct map keys. Update all call sites in `pkg/node`.
- **Success criteria**: `go test ./pkg/store/... ./pkg/node/...` passes. No remaining references to `nodeRecord` outside `pkg/store`.

### Change 18: Delete `mesh.PeerCertExpiresAt`, `RedeemInvite`, unexport `Stream`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 15 (mesh split)
- **Packages touched**: `pkg/mesh`
- **Owner**: transport
- **Estimated effort**: Small
- **Description**: Remove `PeerCertExpiresAt` from `Mesh` interface and impl (zero callers). Delete `RedeemInvite` function (zero callers). Unexport `Stream` to `stream`. Remove `JoinWithInvite` from the interface (keep impl for tests). Unexport `TailscaleCGNAT` and `TailscaleULA`. Delete redundant nil guards in `closeSession` and `Close`.
- **Success criteria**: `go test ./pkg/mesh/...` passes. `go build ./...` passes.

### Change 19: Rename `peer.PeerState*` to `peer.State*` (stutter removal)
- **Type**: BREAKING
- **Risk**: Medium
- **Dependencies**: none (independent but grouped here for logical ordering)
- **Packages touched**: `pkg/peer`, `pkg/node`, `pkg/mesh`, `cmd/pln`
- **Owner**: transport
- **Estimated effort**: Medium
- **Description**: Rename `PeerState` to `State`, `PeerStateDiscovered` to `Discovered`, `PeerStateConnecting` to `Connecting`, `PeerStateConnected` to `Connected`, `PeerStateUnreachable` to `Unreachable`, `PeerStateCounts` to `StateCounts`. Delete `ConnectStageUnspecified`. Delete unreachable `DisconnectReason.String()` default branch. Unexport test-only `Peer` fields (`Stage`, `StageAttempts`, `ConnectingAt`).
- **Success criteria**: `go test ./...` passes. No remaining references to old names.

### Change 20: Unexport scheduler internal types
- **Type**: BREAKING
- **Risk**: Low
- **Dependencies**: none (independent but grouped here)
- **Packages touched**: `pkg/scheduler`
- **Owner**: workloads
- **Estimated effort**: Small
- **Description**: Unexport `Evaluate`, `Action`, `ActionKind`, `Spec`, `NodeState`, `ClusterState`. Delete `NodeState` and `ClusterState` (use `store.NodePlacementState` directly). Delete `Spec` (use `map[string]uint32`). Remove dead `hash string` parameter from `suitabilityScore`. Remove `now()` nil guard.
- **Success criteria**: `go test ./pkg/scheduler/...` passes. Public API surface reduced to runtime types only.

---

## Tier 4: Package Merging and Extraction (depend on Tier 3 API changes)

### Change 21: Move identity key I/O from `pkg/node` to `pkg/auth`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 13 (auth cleanup), Change 16 (node split)
- **Packages touched**: `pkg/auth`, `pkg/node`, `cmd/pln`
- **Owner**: orchestrator + infra
- **Estimated effort**: Medium
- **Description**: Move `GenIdentityKey`, `ReadIdentityPub`, `loadIdentityKey`, `decodePubKeyPEM`, and PEM path constants from `pkg/node/node.go` to a new `pkg/auth/identity.go`. Update `cmd/pln` imports to reference `auth.GenIdentityKey` and `auth.ReadIdentityPub`.
- **Success criteria**: `go test ./pkg/auth/... ./pkg/node/... ./cmd/pln/...` passes. No identity key I/O remains in `pkg/node`.

### Change 22: Inline `pkg/util` into `pkg/node`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 6 (util Bump cleanup), Change 16 (node split)
- **Packages touched**: `pkg/util` (deleted), `pkg/node`
- **Owner**: orchestrator
- **Estimated effort**: Small
- **Description**: Move JitterTicker to `pkg/node/jitter.go` as unexported types. Delete `pkg/util` entirely. Remove the `pkg/util` import from `pkg/node`.
- **Success criteria**: `go build ./...` passes. `pkg/util` directory deleted.

### Change 23: Inline `pkg/sysinfo` into `pkg/node`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 16 (node split)
- **Packages touched**: `pkg/sysinfo` (deleted), `pkg/node`
- **Owner**: orchestrator
- **Estimated effort**: Small
- **Description**: Move `sysinfo.Sample` to `pkg/node/sysinfo.go` as an unexported function. Delete `pkg/sysinfo` entirely.
- **Success criteria**: `go build ./...` passes. `pkg/sysinfo` directory deleted.

### Change 24: Inline `pkg/server` into `cmd/pln`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/server` (deleted), `cmd/pln`
- **Owner**: orchestrator
- **Estimated effort**: Small
- **Description**: Move the `Start` logic from `pkg/server/server.go` into `cmd/pln/main.go` as a local function. Merge stale-socket detection with the existing `nodeSocketActive` check. Delete `GrpcServer` struct (it only carries a logger). Delete `pkg/server` entirely.
- **Success criteria**: `go build ./cmd/pln/...` passes. `pkg/server` directory deleted.

### Change 25: Inline `pkg/workspace` into `cmd/pln`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: none
- **Packages touched**: `pkg/workspace` (deleted), `cmd/pln`
- **Owner**: orchestrator
- **Estimated effort**: Small
- **Description**: Move `EnsurePollenDir` logic into `cmd/pln/main.go` as a local function (10 lines: `os.MkdirAll` + error formatting). Delete `pkg/workspace` entirely.
- **Success criteria**: `go build ./cmd/pln/...` passes. `pkg/workspace` directory deleted.

### Change 26: Inline `sock.connList` into `sockStore`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 7 (sock dead code cleanup)
- **Packages touched**: `pkg/sock`
- **Owner**: transport
- **Estimated effort**: Small
- **Description**: Inline the `connList` type (36 LOC in `list.go`) into `sockStore` as direct map access with the existing `probeMu` or a dedicated inline mutex. Delete `list.go`.
- **Success criteria**: `go test ./pkg/sock/...` passes. `list.go` deleted.

---

## Tier 5: Concurrency Fixes (depend on splits and API changes)

### Change 27: Wire noop `traffic.Recorder` in `pkg/mesh` and `pkg/tunnel`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 5 (noop Recorder), Change 15 (mesh split)
- **Packages touched**: `pkg/mesh`, `pkg/tunnel`
- **Owner**: transport
- **Estimated effort**: Small
- **Description**: Initialize `trafficTracker` to `traffic.Noop` in `mesh.impl` and `tunnel.Manager` constructors. Remove all nil checks on `trafficTracker` at call sites. `SetTrafficTracker` overrides the noop with the real implementation.
- **Success criteria**: `go test ./pkg/mesh/... ./pkg/tunnel/...` passes. No `nil` checks on `trafficTracker` remain.

### Change 28: Make `mesh.inCh` sends blocking with ctx.Done
- **Type**: RISKY
- **Risk**: High
- **Dependencies**: Change 15 (mesh split)
- **Packages touched**: `pkg/mesh`
- **Owner**: transport
- **Estimated effort**: Medium
- **Description**: Replace the 5s-timeout drop semantics on `inCh` sends with `select { case m.inCh <- event: case <-ctx.Done(): }`. This eliminates the most dangerous drop point where `PeerConnected`/`PeerDisconnected` events could be permanently lost, causing the node's peer state machine to go permanently out of sync. The risk is that a slow consumer could block per-session goroutines; this is mitigated by the 256-slot buffer and the fact that the main loop processes events quickly.
- **Success criteria**: `go test ./pkg/mesh/...` passes. Integration tests demonstrate no stuck goroutines under load. Manual verification that shutdown does not hang.

### Change 29: Add `sync.WaitGroup` to scheduler for in-flight claims
- **Type**: RISKY
- **Risk**: Medium
- **Dependencies**: Change 20 (scheduler cleanup)
- **Packages touched**: `pkg/scheduler`
- **Owner**: workloads
- **Estimated effort**: Small
- **Description**: Add a `sync.WaitGroup` to the `Reconciler`. Each `startClaim` goroutine increments it; `Run()` calls `wg.Wait()` before returning after context cancellation. This ensures all `executeClaim` goroutines complete before shutdown proceeds, preventing races where `store.SetLocalWorkloadClaim()` runs after `store.Close()`.
- **Success criteria**: `go test ./pkg/scheduler/...` passes. Verify with `-race` that no data race exists between claim goroutines and shutdown.

---

## Tier 6: Cross-Cutting Polish (after all structural changes)

### Change 30: Delete dead nil guards across codebase
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Changes 14-18 (all splits and API narrowing complete)
- **Packages touched**: `pkg/store`, `pkg/mesh`, `pkg/scheduler`, `pkg/config`
- **Owner**: all
- **Estimated effort**: Small
- **Description**: Final sweep to delete all remaining redundant nil guards identified in the analysis: `applyValueLocked` proto nil checks (~20 LOC in store), `closeSession`/`Close` nil guards (~9 LOC in mesh), `Reconciler.now()` nil guard (~3 LOC in scheduler), config nil-receiver guards (~6 LOC). Most of these should already be addressed by earlier changes; this is a verification pass.
- **Success criteria**: `go test ./...` passes. `just lint` passes.

### Change 31: Unify test helpers in `pkg/node`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Change 16 (node split)
- **Packages touched**: `pkg/node` (test files only)
- **Owner**: orchestrator
- **Estimated effort**: Small
- **Description**: Unify `findNonTargetPeer` and `findNonTargetPublicPeer` with a predicate parameter. Extract `knownPeersToPeerInfos` adapter to share between production code and tests. These are test-only changes.
- **Success criteria**: `go test ./pkg/node/...` passes.

### Change 32: Extract `mustPollenPath` helper in `cmd/pln`
- **Type**: SAFE
- **Risk**: Low
- **Dependencies**: Changes 24, 25 (server and workspace inlined)
- **Packages touched**: `cmd/pln`
- **Owner**: orchestrator
- **Estimated effort**: Small
- **Description**: Extract a `mustPollenPath(cmd) string` helper to deduplicate the repeated `pollenDir, err := pollenPath(cmd)` boilerplate across multiple CLI commands (~40 LOC saved). Also clean up the `formatWorkloadStatus` and `certExpiryFooter` unreachable trailing returns, and the `formatStatus` default branch.
- **Success criteria**: `go build ./cmd/pln/...` passes. `just lint` passes.

---

## Summary

| Tier | Changes | Risk | Estimated Total Effort |
|------|---------|------|----------------------|
| 1: Foundation | 1-13 | Low | Small (13 independent tasks) |
| 2: File splits | 14-16 | Low | Medium (3 tasks) |
| 3: API narrowing | 17-20 | Low-Medium | Medium (4 tasks) |
| 4: Package merging | 21-26 | Low | Small-Medium (6 tasks) |
| 5: Concurrency | 27-29 | Low-High | Small-Medium (3 tasks) |
| 6: Polish | 30-32 | Low | Small (3 tasks) |

**Total: 32 changes across 6 tiers.**

### Packages deleted (4)
- `pkg/observability/logging` (Change 2)
- `pkg/util` (Change 22)
- `pkg/sysinfo` (Change 23)
- `pkg/server` (Change 24)
- `pkg/workspace` (Change 25)

### Estimated LOC reduction: ~650 LOC (from dead code, deduplication, and wrapper elimination)
### Estimated LOC movement: ~290 LOC (identity key I/O, package inlining)
### Net package count reduction: 5 packages eliminated (26 -> 21)
