# Architectural Analysis (Phase 2)

Comprehensive analysis of the Pollen codebase based on Phase 1 documentation across all 26 packages (17,102 production LOC, 10,727 test LOC).

---

## Goroutine Architecture

### Should `recvLoop` merge into the main select loop?

**No. Keep it separate.** `mesh.Recv()` calls `conn.ReceiveDatagram()` which is a blocking QUIC read. It cannot be placed directly inside a `select` because Go's `select` requires channel cases. The current design -- a dedicated goroutine reading datagrams into `recvCh` (buffer 256), with the main loop consuming from `recvCh` -- is the correct Go pattern for bridging blocking I/O into a select-based event loop.

However, the current architecture has the `recvLoop` goroutine call `handleDatagram` directly, which processes the envelope and mutates node state. This means datagram handling runs outside the main event loop goroutine, violating the "single-goroutine owner of all mutable state" invariant documented in `concurrency.md`. Either `recvLoop` should only enqueue raw packets for the main loop to process (adding a case for `recvCh` in the main select), or the handleDatagram call must be documented as thread-safe. **[RISKY]** -- needs careful audit of what `handleDatagram` touches.

### Are 3 fixed punch workers correctly sized?

**Probably too few at scale, but fine for current deployment sizes.** Each punch attempt blocks for up to `punchTimeout` (3s). With 3 workers and a buffer-32 `punchCh`, the system can handle at most 3 concurrent punches. Drops are silent (`select/default`). At target scale (50-100 nodes joining simultaneously), punch requests will be dropped silently.

Better design: a worker pool sized to `min(GOMAXPROCS, maxConcurrentPunches)` or a semaphore-gated goroutine-per-request model. The fixed worker count is an artifact of the current small-cluster deployment. **[RISKY]** -- changing concurrency model.

### Per-session goroutine count: appropriate at target scale?

Each connected peer costs 2 persistent goroutines (`recvDatagrams` + `acceptBidiStreams`) plus ephemeral goroutines for stream handling. For N peers, that's 2N persistent goroutines. At 100 peers, 200 goroutines is trivial for Go. At 1,000 peers, 2,000 goroutines is still manageable but the channel buffer sizes (256 for `recvCh`, `inCh`, `streamCh`, `clockStreamCh`) become the bottleneck. **Current design is appropriate for the 100-peer target.** Beyond that, the per-session goroutine pair should be evaluated against a multiplexed reader model. **[SAFE]** -- no immediate action needed.

### 256 goroutines per punch attempt in `sock`: is this sane?

**No. This is the most aggressive concurrency point in the codebase.** Each `Punch()` call spawns up to 256 ephemeral goroutines, each opening a UDP socket. For 3 concurrent punches, that's 768 goroutines and 768 file descriptors. On a system with concurrent punches during cluster join (N nodes joining simultaneously), this scales to `3 * 256 = 768` goroutines per node.

Problems:
1. **File descriptor exhaustion**: 256 ephemeral sockets per punch. Systems with `ulimit -n 1024` will fail after 4 concurrent punches.
2. **Port exhaustion**: Random ports in [1024, 65535] with 256 probes per punch. Low probability of collision but no dedup.
3. **Socket leak window**: When the first probe succeeds and `cancel()` fires, the remaining 255 goroutines must timeout on their 1s read deadline before closing sockets. During this window, all sockets remain open.

Better design: a fixed pool of N ephemeral sockets (e.g., 16-32) that send multiple probes each, with port rotation handled by `sendto()` on a single socket rather than opening new sockets. Alternatively, use the main socket for all probing (the scatter-probe-main path already does this for easy-NAT). **[RISKY]** -- fundamental redesign of punch path.

### Scheduler `executeClaim` goroutines with no WaitGroup: risk analysis

**Medium risk.** When `Run()` returns on context cancellation, background `executeClaim` goroutines may still be running. They will eventually exit when their fetch contexts are cancelled, but there is no `sync.WaitGroup` to ensure completion. Consequences:

1. During shutdown, `store.Save()` and `store.Close()` may execute while an `executeClaim` goroutine is calling `store.SetLocalWorkloadClaim()`. The store's mutex protects against data corruption, but the logical sequencing is wrong -- a claim could be persisted after shutdown begins.
2. `workload.Manager.Close()` drops compiled modules while an `executeClaim` goroutine might be calling `SeedFromCAS()`.

Fix: add a `sync.WaitGroup` to the `Reconciler` and `Wait()` in `Run()` before returning. **[RISKY]** -- touches shutdown ordering.

### Fire-and-forget goroutines in node: acceptable?

**Mixed.** Inventory:

| Goroutine | Risk | Assessment |
|-----------|------|------------|
| Eager sync (`go func() { sendClockViaStream(...) }()`) | Low | 5s timeout context. Failure is logged. No state mutation on failure. |
| `requestPunchCoordination` | Low | Fire-and-forget `mesh.Send` with `context.Background()`. If it fails, the punch just doesn't happen. No state corruption. |
| `handlePunchCoordRequest` (2x) | Low | Same pattern. Two sends, fire-and-forget. |
| `doConnect` | **Medium** | Blocking send on `localPeerEvents` (buffer 64). If buffer is full, this goroutine blocks indefinitely until the main loop drains. Not a leak (context cancellation will unblock `mesh.Connect`), but could delay shutdown. |

The `context.Background()` usage in fire-and-forget sends means these goroutines survive node context cancellation. They will eventually fail when `mesh.Close()` closes the QUIC transport, but there is a window between `ctx.Done()` and transport close where they are orphaned. **[RISKY]** to fix (need to thread the node context through).

---

## Channel Correctness

### Audit: `select { case ch <- x: default: }` -- can dropped events cause state divergence?

| Channel | Drop semantics | Divergence risk |
|---------|---------------|-----------------|
| `gossipEvents` (buf 64) | Gossip events dropped on full | **Medium**. Dropped rebroadcasts mean some peers learn state changes later (via the next clock sync pull). CRDT semantics ensure eventual consistency, but convergence is delayed. Acceptable for gossip. |
| `punchCh` (buf 32) | Punch requests dropped on full | **Low**. Dropped punches mean a peer stays unreachable until the next topology tick rediscovers them. No state corruption. |
| `localPeerEvents` (buf 64, only from `OnDenyPeer` callback) | Deny event dropped on full | **Medium**. If the deny event is dropped, the main loop won't immediately close the session of a denied peer. However, the deny state is persisted in the store and will be enforced on the next topology tick or gossip round. Delayed enforcement, not permanent divergence. |
| `mesh.inCh` (buf 256) | Peer events dropped after 5s timeout | **High**. If a `PeerConnected` or `PeerDisconnected` event is dropped, the node's peer state machine will be permanently out of sync with the actual QUIC session state. The 5s timeout mitigates this (events are rarely dropped), but a dropped `PeerConnected` means the node never transitions the peer to Connected state, breaking gossip and topology. This is the most dangerous drop point. |
| `mesh.streamCh` (buf 256) | Tunnel streams dropped immediately | **Low for correctness, high for user experience.** Dropped streams mean tunnel connections fail silently. The TCP client will see a connection reset. No state divergence. |
| `mesh.clockStreamCh` (buf 256) | Clock streams dropped immediately | **Low**. Dropped clock streams mean one gossip round is skipped. The next tick retries. |
| `mesh.renewalCh` (buf 1) | Renewal response dropped | **Medium**. See dedicated section below. |

**Recommendation**: The `inCh` 5s-timeout drop is the most dangerous. Consider making it blocking with a longer timeout or converting to a blocking send with `ctx.Done()` fallback. **[RISKY]**

### Buffer sizes: justified?

| Channel | Buffer | Justification | Verdict |
|---------|--------|---------------|---------|
| `gossipEvents` | 64 | Batches of gossip events. Multiple producers (store mutations, scheduler publish). 64 batches handles burst after a large state sync. | Appropriate. |
| `mesh.recvCh` | 256 | One entry per received datagram. With N peers sending gossip at ~1Hz, buffer needs to hold at least N entries. 256 handles bursts up to 256 concurrent datagrams. | Appropriate for <256 peers. |
| `mesh.inCh` | 256 | Peer lifecycle events. One event per connect/disconnect. 256 handles mass reconnect scenarios. | Appropriate. |
| `mesh.streamCh` | 256 | One entry per incoming tunnel stream. 256 concurrent tunnel connections is generous. | Appropriate. |
| `mesh.clockStreamCh` | 256 | One entry per incoming clock stream. N peers sending clocks at gossip interval. | Appropriate. |
| `localPeerEvents` | 64 | Local peer events from doConnect, punchLoop, deny callback. 64 handles burst from topology tick discovering many new peers. | Appropriate. |
| `punchCh` | 32 | Punch requests. With 3 workers, 32 buffer handles burst discovery of many NAT'd peers. | Appropriate. |
| `routeInvalidate` | 1 | Coalescing signal. Only needs 1 pending. | Correct by design. |
| `scheduler.triggerCh` | 1 | Coalescing signal. | Correct by design. |
| `scheduler.trafficTriggerCh` | 1 | Coalescing signal. | Correct by design. |

### `routeInvalidate` (buffer 1) -- can rapid invalidations lose state?

**No.** The channel carries `struct{}` -- it is a pure signal with no payload. Multiple rapid invalidations coalesce into a single pending signal. When the main loop reads it, it recomputes routes from the full current state (not from the event). This is correct coalescing semantics. The debounce timer (100ms / 1s max) ensures recomputation isn't thrashed. **[SAFE]**

### `mesh.renewalCh` (buffer 1, no request correlation) -- concurrent renewal race?

**Yes, there is a race.** `renewalCh` is a single-slot rendezvous channel. `RequestCertRenewal` iterates connected peers and sends a renewal request, then waits on `renewalCh` for the response. If two goroutines call `RequestCertRenewal` concurrently:

1. Goroutine A sends request to Peer 1, blocks on `<-renewalCh`
2. Goroutine B sends request to Peer 2, blocks on `<-renewalCh`
3. Peer 2's response arrives first, `acceptBidiStreams` does non-blocking send to `renewalCh`
4. Goroutine A reads Peer 2's response -- wrong cert!

**In practice this doesn't happen** because `RequestCertRenewal` is only called from `attemptCertRenewal` which runs synchronously in the main loop's cert-check ticker. But the design is fragile. Fix: use a request/response correlation ID or a per-request channel. **[RISKY]** if the calling pattern ever changes.

### `gossipEvents` drop-on-full -- what happens to dropped gossip events?

Dropped gossip events are events that should be rebroadcast to peers but are silently discarded. The CRDT's per-attribute Lamport counters ensure that peers will eventually learn the correct state via the next clock-based pull sync. The gossip protocol has two propagation paths:

1. **Push (rebroadcast via `gossipEvents`)**: Fast but lossy (drop-on-full).
2. **Pull (clock sync every `GossipInterval`)**: Slow but reliable (delta sync fills gaps).

Dropped push events increase convergence latency but do not cause permanent divergence. The system self-heals on the next gossip tick. At default gossip interval (~5s with jitter), the worst-case convergence delay from a dropped event is ~5s. **[SAFE]** -- acceptable for the gossip protocol.

---

## Abstraction Leaks

### `mesh.Mesh` has 25+ methods -- decompose into narrow interfaces?

**Yes.** The 25-method interface exists only because `pkg/node` uses everything. Two consumers already define narrow local interfaces:

- `tunnel.StreamTransport`: `OpenStream` + `AcceptStream` (2 methods)
- `scheduler.MeshStreamOpener`: `OpenArtifactStream` (1 method)

Proposed decomposition:

| Interface | Methods | Consumer |
|-----------|---------|----------|
| `MeshLifecycle` | `Start`, `Close` | `node` |
| `MeshSender` | `Send`, `Recv`, `Events` | `node` |
| `StreamTransport` | `OpenStream`, `AcceptStream` | `tunnel` |
| `ClockTransport` | `OpenClockStream`, `AcceptClockStream` | `node` |
| `ArtifactTransport` | `OpenArtifactStream`, `SetArtifactHandler` | `scheduler`, `node` |
| `WorkloadTransport` | `OpenWorkloadStream`, `SetWorkloadHandler` | `node` |
| `PeerManager` | `Connect`, `Punch`, `ClosePeerSession`, `ConnectedPeers`, `BroadcastDisconnect`, `IsOutbound`, `GetActivePeerAddress`, `PeerDelegationCert`, `ListenPort` | `node` |
| `CertManager` | `UpdateMeshCert`, `RequestCertRenewal` | `node` |
| `MeshConfig` | `SetTracer`, `SetRouter`, `SetTrafficTracker` | `node` |

`pkg/node` would accept the concrete `*mesh.impl` and use it through these narrow interfaces for self-documentation. The concrete type satisfies all interfaces. **[BREAKING]** -- changes the mesh API.

### `scheduler` imports concrete `store` types

The scheduler defines its own `SchedulerStore` interface (good), but it imports `store.WorkloadSpecView` and `store.NodePlacementState` as concrete data types. This means the scheduler has a compile-time dependency on `pkg/store` despite using an interface for method calls.

Fix: move the data types to the scheduler package (since they are only used by the scheduler) or to a shared `pkg/types` expansion. Alternatively, accept that these are stable data transfer types and the dependency is acceptable. **[SAFE]** -- pure refactor.

### `node` accesses `store.LocalID` as public field

`store.Store.LocalID` is a `PeerKey` field accessed directly by `pkg/node`. This is a write-once field set at construction, so there is no correctness risk. However, it breaks the "clean package APIs" principle -- a `LocalID()` method would be more idiomatic. **[SAFE]** -- trivial rename.

### `nodeRecord` leaks through `store.Get()` and `store.AllNodes()`

`nodeRecord` is an unexported type returned from exported methods. External callers (all in `pkg/node`) can access all fields including internal ones like `maxCounter` and `log`. This is the biggest abstraction leak in the codebase.

Fix: replace `Get()` with purpose-built query methods (the store already has `NodeIPs()`, `IsPubliclyAccessible()`, etc.). Replace `AllNodes()` with a method returning route-relevant data only (the sole consumer uses it for route table info). **[SAFE]** -- narrowing API.

### `Peer` struct fields exported but only read in tests

`peer.Peer` exports `Stage`, `StageAttempts`, `ConnectingAt` which are only inspected in test code. The production code only reads `State`, `LastAddr`, `Ips`, `ObservedPort`, `NextActionAt`, `ConnectedAt`, `ID`. Unexport the test-only fields and adjust tests to use dedicated query methods. **[SAFE]** -- narrowing API.

---

## Package Boundaries

### `node` imports 20 packages -- what can be extracted?

The `node` package is a 2,513-LOC god object importing 20 internal packages. Extractable subsystems:

| Subsystem | Estimated LOC | Target | Risk |
|-----------|---------------|--------|------|
| Identity key I/O (`GenIdentityKey`, `ReadIdentityPub`, `loadIdentityKey`, `decodePubKeyPEM`) | ~100 | `pkg/auth` (extend existing) | **[SAFE]** |
| Certificate lifecycle (`checkCertExpiry`, `attemptCertRenewal`, `applyCertRenewal`, `handleCertRenewalRequest`) | ~130 | `pkg/auth/renewal` or inline in `pkg/auth` | **[SAFE]** |
| NAT punch coordination (`requestPunchCoordination`, `handlePunchCoordRequest`, `handlePunchCoordTrigger`, `punchLoop`) | ~100 | New file `node/punch.go` (file split, not package split) | **[SAFE]** |
| Gossip batching + broadcast (`broadcastGossipBatches`, `queueGossipEvents`, `batchEvents`, `gossip`, `handleClockStream`, `sendClockViaStream`) | ~120 | New file `node/gossip.go` | **[SAFE]** |
| Clock stream handling (`acceptClockStreamsLoop`, `handleClockStream`, `sendClockViaStream`, clock gossip in `gossip()`) | ~80 | Part of `node/gossip.go` | **[SAFE]** |
| gRPC service handler (`svc.go`) | 652 | Already separate file; could be separate `internal` package | **[SAFE]** |
| Topology/peer selection logic from `syncPeersFromState` | ~140 | Extend `pkg/topology` with a selection orchestrator | **[SAFE]** |
| `GetStatus` BFS + response assembly | ~280 | `internal/nodestatus` package | **[SAFE]** |

Priority: Identity key I/O into `pkg/auth` is the highest-value, lowest-risk extraction.

### `store/gossip.go` (2,259 LOC) -- how to split?

By concern:

| File | Content | Est. LOC |
|------|---------|----------|
| `store.go` | `Store` struct, constructor (`Load`), lifecycle (`Close`, `Save`), callback registration, metrics setter | ~200 |
| `gossip.go` | CRDT log operations, `ApplyEvents`, `Clock`, `MissingFor`, `LocalEvents`, `EagerSyncClock` | ~500 |
| `mutate.go` | All `SetLocal*` methods, `DenyPeer`, `UpsertLocalService`, `RemoveLocalServices` | ~400 |
| `query.go` | All read methods: `Get`, `AllNodes`, `KnownPeers`, `AllPeerKeys`, `AllWorkloadSpecs`, etc. | ~300 |
| `workload.go` | `SetLocalWorkloadSpec`, `SetLocalWorkloadClaim`, `RemoveLocalWorkloadSpec`, workload conflict resolution | ~200 |
| `reachability.go` | BFS reachability, `validNodesLocked`, deny filtering | ~150 |
| `record.go` | `nodeRecord` type, `clone()`, `attrKey`, `logEntry`, attribute kind definitions | ~200 |
| `connections.go` | `DesiredConnections`, `AddDesiredConnection`, `RemoveDesiredConnection`, `Connection` | ~80 |
| `invites.go` | `TryConsume`, `consumedInviteEntry`, invite replay prevention | ~60 |
| `disk.go` | Already separate (278 LOC) | 278 |

Total: ~2,370 LOC across 10 files. **[SAFE]** -- pure file reorganization within the same package.

### `mesh.go` (1,397 LOC) -- split candidates?

| File | Content | Est. LOC |
|------|---------|----------|
| `mesh.go` | `Mesh` interface, constructor, `Start`, `Close`, `BroadcastDisconnect` | ~200 |
| `session.go` | `addPeer`, `closeSession`, `handleSendFailure`, `recvDatagrams`, `acceptBidiStreams`, `sessionReaper` | ~400 |
| `stream.go` | `OpenStream`, `OpenClockStream`, `OpenArtifactStream`, `OpenWorkloadStream`, `openTypedStream`, `AcceptStream`, `AcceptClockStream`, `bridgeStreams`, `Stream` type | ~300 |
| `connect.go` | `Connect`, `Punch`, `JoinWithToken`, `JoinWithInvite`, `raceDirectDial` | ~200 |
| `cert.go` | `UpdateMeshCert`, `RequestCertRenewal`, `handleCertRenewal` | ~100 |
| `identity.go` | Already separate (303 LOC) | 303 |
| `discovery.go` | Already separate (187 LOC) | 187 |
| `invite.go` | Already separate (84 LOC) | 84 |
| `session_registry.go` | Already separate (151 LOC) | 151 |

**[SAFE]** -- file reorganization within the same package.

### Should `server` be inlined into `cmd/pln`?

**Yes.** `pkg/server` is 69 LOC with a single consumer (`cmd/pln/main.go`). The `GrpcServer` struct carries only a logger. The stale-socket detection is duplicated with `nodeSocketActive` in `main.go`. Inlining eliminates a package boundary that adds no value.

Concrete plan: move `Start` logic into `runNode` as a local function. The `conc/pool` pattern stays the same. **[SAFE]** -- ~69 LOC reduction in package count.

### Should `workspace`, `logging` be deleted entirely?

**`logging`**: Yes, delete immediately. 23 LOC, zero consumers. The function `Init()` is not called anywhere. Logger initialization happens elsewhere. **[SAFE]** -- 23 LOC deleted.

**`workspace`**: The DOCS_CURRENT.md says no consumers, but `cmd/pln` imports it. Checking: `cmd/pln` does import `pkg/workspace` for `EnsurePollenDir`. However, this is a trivial 10-line function (`os.MkdirAll` + error formatting). It can be inlined into `cmd/pln`. **[SAFE]** -- 26 LOC deleted, logic inlined.

---

## Missing Abstractions

### No explicit lifecycle manager

Startup and shutdown ordering is inline in `Node.Start()` via `defer n.shutdown()`. The ordering is correct (documented in `concurrency.md`) but fragile -- adding a new subsystem requires modifying two places (startup initialization AND shutdown defer ordering).

A lifecycle manager would formalize the ordering:

```go
type Lifecycle struct {
    startFuncs []func(ctx context.Context) error
    stopFuncs  []func()
}
```

However, the current system has only 8 subsystems to start/stop. A lifecycle manager is over-engineering for this scale. The real fix is to extract subsystem initialization into a separate method per subsystem and ensure each returns a cleanup function, so startup and shutdown are co-located. **[SAFE]** -- moderate refactor.

### No formalized event bus

The system uses three separate event delivery mechanisms:

1. **Channels**: `gossipEvents`, `localPeerEvents`, `punchCh`, `routeInvalidate`, `mesh.Events()`
2. **Callbacks**: `store.OnDenyPeer`, `store.OnRouteInvalidate`, `store.OnWorkloadChange`, `store.OnTrafficChange`
3. **Direct method calls**: `sched.Signal()`, `sched.SignalTraffic()`

This is pragmatic and appropriate for the current size. A unified event bus would add complexity without benefit -- the three mechanisms serve different purposes (async data delivery, synchronous notification, coalescing triggers). **No action recommended.**

### `internal/` packages -- where would they enforce boundaries?

| Proposed internal package | Content | Purpose |
|---------------------------|---------|---------|
| `node/internal/status` | `GetStatus` BFS + response assembly | Prevent `svc.go` status logic from leaking into other packages |
| `store/internal/record` | `nodeRecord`, `attrKey`, `logEntry` | Enforce that record internals don't leak through public API |
| `mesh/internal/session` | `peerSession`, `sessionRegistry` | Enforce session management encapsulation |

These would add value if the packages are split, but are not strictly necessary with the current single-consumer architecture. **[SAFE]** -- defer until package splits create real boundary enforcement needs.

### No noop `traffic.Recorder`

`pkg/tunnel` and `pkg/mesh` store a `traffic.Recorder` field that starts as nil and is set later via `SetTrafficTracker`. The callers must nil-check before calling `WrapStream`. Per the project's "noop implementations for optional features" principle (CLAUDE.md), a noop `Recorder` should be provided and used as the default. Fix: add `var Noop Recorder = noopRecorder{}` to `pkg/traffic` and initialize fields to it. **[SAFE]** -- 5 LOC.

---

## Idiomatic Go Audit

### Hand-rolled helpers that stdlib now covers

| Package | Current code | Replacement | LOC saved |
|---------|-------------|-------------|-----------|
| `topology` | `clamp(v, lo, hi float64) float64` | `max(lo, min(hi, v))` (Go 1.21 builtins) | 7 |
| `config` | `ed25519PublicKeyBytes = 32` constant | `crypto/ed25519.PublicKeySize` | 1 |

No instances of `slices.Contains` or `maps.Keys` replacements found -- the codebase already uses modern Go patterns in most places.

### Manual error string concatenation vs `%w` wrapping

The codebase generally follows the `fmt.Errorf("%w")` pattern correctly. No instances of string concatenation for errors found.

### Index loops where range loops suffice

No significant instances found. The codebase uses range loops idiomatically.

### Deep nesting instead of early returns

No systemic issues. The `applyValueLocked` method in `store/gossip.go` has deep nesting per attribute kind, but this is structural (switch + nil checks per case) rather than avoidable deep nesting.

### Stuttered names

| Current | Proposed | Package |
|---------|----------|---------|
| `config.Config` | Already acceptable (the package IS the config) | `config` |
| `peer.PeerState` | `peer.State` | `peer` |
| `peer.PeerStateDiscovered` | `peer.Discovered` | `peer` |
| `peer.PeerStateCounts` | `peer.StateCounts` | `peer` |

The `peer.PeerState*` stutter is the most significant instance. **[BREAKING]** -- renames exported types.

### Type assertions without `errors.As`/`errors.Is`

The `mesh` package uses `classifyQUICError` which type-asserts QUIC errors directly. This is acceptable because the QUIC library uses concrete error types, not sentinel errors. No instances of `errors.Is` misuse found.

### Named returns used without reason

| Package | Function | Issue |
|---------|----------|-------|
| `sysinfo` | `Sample()` | Named returns for 4 values -- acceptable for documentation |
| `store` | `ResolveWorkloadPrefix` | Named returns `(hash string, ambiguous bool, found bool)` -- acceptable for clarity |

No instances of named returns used for naked returns in non-trivial functions.

---

## Concurrency Minimization

### Goroutines that could be synchronous calls

| Current goroutine | Could be synchronous? | Assessment |
|-------------------|----------------------|------------|
| `requestPunchCoordination` | Yes | Single `mesh.Send` call. Could be called synchronously from `handleOutputs`. Fire-and-forget pattern is premature optimization -- `Send` is already non-blocking (QUIC datagram). |
| Per-peer eager sync | No | Opens a QUIC stream with 5s timeout. Must not block the main loop. |
| `handlePunchCoordRequest` (2x) | Partially | Sends two messages to different peers. Could be sequential instead of concurrent -- the latency cost of serializing two `mesh.Send` calls is negligible. |
| Per-peer clock gossip | No | Opens streams to multiple peers concurrently. Sequential would be too slow for the gossip interval. |

**Recommendation**: Make `requestPunchCoordination` and `handlePunchCoordRequest` synchronous. Saves 3 goroutine spawns per coordination event. **[SAFE]** -- the `mesh.Send` calls are non-blocking.

### Channels used where mutex + condition is simpler

| Channel | Alternative | Assessment |
|---------|-------------|------------|
| `route.changeCh` (barrier pattern) | `sync.Cond` | Barrier pattern (close + replace) is idiomatic Go and integrates with `select`. Keep as-is. |
| `mesh.sessions.changeCh` | `sync.Cond` | Same barrier pattern. Keep as-is. |
| `scheduler.triggerCh` (coalescing signal) | `atomic.Bool` + `sync.Cond` | Channel is simpler and integrates with `select`. Keep as-is. |
| `wasm.Runtime.sem` (semaphore) | `sync.Semaphore` (not in stdlib) | Channel-based semaphore is the standard Go pattern. Keep as-is. |
| `util.JitterTicker.bump` | N/A | Never called. Delete. |

**No channels need replacement.** The codebase uses channels appropriately.

### Defensive locks on write-once fields

| Package | Field | Issue |
|---------|-------|-------|
| `sock` | `mainWrite ProbeWriter` | Write-once, no synchronization. Safe by happens-before (set before `Punch`). Documented. |
| `mesh` | `SetRouter`, `SetTracer`, `SetTrafficTracker`, `SetArtifactHandler`, `SetWorkloadHandler` | Five write-once setters with no locks. Safe by initialization order but not enforced. |
| `tunnel` | `trafficTracker` | Write-once, no synchronization. |

Per CLAUDE.md: "Don't add mutexes around write-once fields." These are correct as-is. **[SAFE]** -- no action needed, but document the write-once contract on each field.

### Background tickers that could be demand-driven

| Ticker | Package | Interval | Could be demand-driven? |
|--------|---------|----------|------------------------|
| Peer tick | `node` | 1s | No -- drives the peer state machine which needs regular evaluation. |
| Gossip tick | `node` | Jittered ~5s | No -- gossip must be periodic for anti-entropy. |
| IP refresh | `node` | 5m | Yes -- could be triggered by network change events. But the platform detection for network change events is complex. Keep as ticker. |
| Cert check | `node` | 5m (30s on failure) | Partially -- could be triggered by clock-based notification (wake at `certExpiry - renewalWindow`). But the timer approach is simpler and handles clock skew. |
| State save | `node` | 30s | Could be demand-driven (save on significant state change). But periodic is safer for crash recovery. |
| Session reaper | `mesh` | 5m | Could be demand-driven (check age on session add). But the ticker is simpler. |
| Metrics flush | `metrics` | 10s | No -- periodic flush is the standard pattern for metrics. |

**No tickers should be converted.** The periodic approach is appropriate for all cases.

### `util.JitterTicker.Bump()` -- never called

Confirmed dead code. The `Bump()` method, the `bump` channel (buffer 1), and the `case <-bump` branch in the goroutine's select loop are all dead. Delete. **[SAFE]** -- 10 LOC.

---

## Deletion Roadmap

### 1. Entire Dead Packages

| Package | LOC | Rationale |
|---------|-----|-----------|
| `pkg/observability/logging` | 23 | Zero consumers. `Init()` is never called. Logger initialization happens in `cmd/pln`. |
| `pkg/workspace` | 26 | Single trivial function, inlineable into `cmd/pln`. |
| `pkg/server` | 69 | Single consumer, trivial wrapper. Inline into `cmd/pln`. |

**Total: 118 LOC**

### 2. Dead Exported Symbols

| Symbol | Package | LOC | Rationale |
|--------|---------|-----|-----------|
| `Envelope` struct | `types` | 3 | Never referenced. Codebase uses `meshv1.Envelope`. |
| `PeerCertExpiresAt` method | `mesh` | 8 | On `Mesh` interface, zero external callers. |
| `RedeemInvite` function | `mesh` | 22 | Zero callers outside the package. |
| `TailscaleCGNAT`, `TailscaleULA` vars | `mesh` | 3 | Only consumed via `DefaultExclusions`. |
| `JoinWithInvite` on `Mesh` interface | `mesh` | 0 | Only called from test code. |
| `Stream` type export | `mesh` | 6 | Never referenced by name outside `pkg/mesh`. |
| `probe()` function | `sock` | 4 | Only called from tests. |
| `sockStore.log` field | `sock` | 2 | Created but never used (no `s.log.*` calls). |
| `IsCompiled` method | `wasm` | 6 | Only called from tests. |
| `IsRunning` method | `workload` | 5 | Only called internally by `Call`. |
| `StatusStopped`, `StatusErrored` enum values | `workload` | 10 | Never assigned by the workload manager. |
| `VerifiedInviteToken` struct | `auth` | 5 | Return value discarded by all callers. |
| `IsCapSubset` (exported) | `auth` | 10 | Only used internally + tests. Could be unexported. |
| `EnsureNodeCredentialsFromToken` (exported) | `auth` | 30 | Only called by `LoadOrEnrollNodeCredentials` in same file. |
| `Evaluate`, `Action`, `ActionKind`, `Spec`, `NodeState`, `ClusterState` (exported) | `scheduler` | 0 (rename) | All exported but only used within the package. |
| `GossipMetrics()` getter | `store` | 3 | Write-once field, only used in one test. |
| `WritePrivate` | `perm` | 3 | No external consumers. Only tested. |
| `SetGroupDir` (exported) | `perm` | 1 | Only called internally by `EnsureDir`. |
| `NearestHysteresis`, `MinHysteresisDistance`, `CcDefault`, `CeDefault`, `MinHeight`, `MaxCoord`, `MinRTTFloor` (exported constants) | `topology` | 8 | Only used internally. Could be unexported. |
| `DefaultMembershipTTL`, `DefaultDelegationTTL`, `DefaultTLSIdentityTTL`, `DefaultReconnectWindow` (exported constants) | `config` | 4 | Never referenced outside the package. |
| `Connection.Key()` method | `store` | 5 | `Connection` could be used as map key directly. |
| `AllNodes()` returning unexported type | `store` | 4 | Abstraction leak. Replace with purpose-built query. |
| `Get()` returning unexported type | `store` | 8 | Same abstraction leak. |

**Total: ~150 LOC deletable, ~40 LOC renameable (unexport)**

### 3. Dead Internal Code

| Item | Package | LOC | Rationale |
|------|---------|-----|-----------|
| `Span.parentSpanID` field + `ReadOnlySpan.ParentSpanID` + `LogSink.Export` parent branch | `traces` | 8 | No child span API. parentSpanID is never set. |
| `SpanID.IsZero()` method | `traces` | 1 | Only used for dead parentSpanID check. |
| `TraceID.Bytes()` method | `traces` | 1 | Trivial wrapper, only called in one place. |
| `default` panic cases in `applyDeleteLocked` and `buildEventFromLog` | `store` | 6 | Unreachable -- `eventAttrKey` filters unknown types. |
| Redundant nil guards in `applyValueLocked` | `store` | 20 | Proto getters already handle nil safely. |
| `Conn.onClose` nil check | `sock` | 2 | Always set when `UDPConn` is non-nil. |
| `Conn.UDPConn` nil check | `sock` | 2 | Same reasoning. |
| Redundant nil guards in `mesh.Close` | `mesh` | 6 | `listener` and `mainQT` always set after `Start`. |
| Redundant nil guard in `mesh.closeSession` | `mesh` | 3 | `s.conn` always non-nil. |
| `suitabilityScore` dead `hash string` parameter | `scheduler` | 1 | Named `_`, never used. |
| `Reconciler.now()` nil guard | `scheduler` | 3 | `nowFunc` always set by constructor. |
| `os.ErrExist` guard in `server.go` | `server` | 3 | Dead branch -- Unix Listen never returns this. |
| Zero-entry removal loop in `traffic.RotateAndSnapshot` | `traffic` | 4 | Unreachable -- entries always have nonzero counts. |
| Short-hash branch in `cas.path()` | `cas` | 2 | SHA-256 hashes are always 64 chars. |
| `formatWorkloadStatus` trailing return | `cmd/pln` | 2 | Unreachable after exhaustive switch. |
| `certExpiryFooter` trailing return | `cmd/pln` | 2 | Unreachable after exhaustive switch. |
| `formatStatus` default branch | `cmd/pln` | 2 | Conflates UNSPECIFIED with offline. |
| `DisconnectReason.String()` default branch | `peer` | 1 | All enum values covered in switch. |
| `ConnectStageUnspecified` constant | `peer` | 3 | Always treated identically to `ConnectStageDirect`. |
| `Labels` type and all label plumbing | `metrics` | 25 | No metric uses labels. Every registration passes `Labels{}`. |
| `BootstrapProtoPeers` nil-receiver guard | `config` | 3 | `Config` is never nil from `Load`. |
| `RememberBootstrapPeer` nil-receiver guard | `config` | 3 | Same. |
| `ed25519PublicKeyBytes` constant | `config` | 1 | Duplicates `crypto/ed25519.PublicKeySize`. |
| `clusterStatePaths` legacy entries | `cmd/pln` | 2 | TODOs say they should be removed. |

**Total: ~105 LOC**

### 4. Inlineable Single-Consumer Packages

| Package | LOC | Consumer | Inline target |
|---------|-----|----------|---------------|
| `pkg/server` | 69 | `cmd/pln` | Inline into `cmd/pln/main.go` |
| `pkg/util` | 66 | `pkg/node` | Inline into `pkg/node` as unexported helper |
| `pkg/workspace` | 26 | `cmd/pln` | Inline into `cmd/pln/main.go` |
| `pkg/sysinfo` | 23 | `pkg/node` | Inline into `pkg/node` as unexported function |
| `pkg/observability/logging` | 23 | None | Delete entirely |

**Total: 207 LOC in 5 packages** (138 LOC inlined, 23 LOC deleted, 46 LOC net reduction from eliminated package boilerplate)

### 5. Duplicated Logic

| Duplication | Package(s) | LOC per dedup |
|-------------|-----------|---------------|
| Three nearly-identical `Open*Stream` methods | `mesh` | ~50 (extract `openStreamWaitLoop`) |
| `peerKeyFromRawCert` vs `peerKeyFromConn` | `mesh` | ~12 (unify) |
| `NodeState` / `ClusterState` / `buildClusterState` duplicating `store.NodePlacementState` | `scheduler` | ~25 (remove shadow types) |
| `PeerInfo` construction duplicated in `node.go` and `topology_targets_test.go` | `node` | ~14 (extract adapter) |
| Stale-socket detection in `runNode` + `server.Start` | `cmd/pln` + `server` | ~15 (unified in inlined version) |
| `findNonTargetPeer` vs `findNonTargetPublicPeer` in tests | `node` tests | ~10 (unify with predicate) |
| Repeated `pollenDir, err := pollenPath(cmd)` boilerplate | `cmd/pln` | ~40 (extract `mustPollenPath`) |

**Total: ~166 LOC saved from deduplication**

### 6. Over-Abstracted Wrappers

| Wrapper | Package | LOC | Assessment |
|---------|---------|-----|------------|
| `GrpcServer` struct wrapping only a logger | `server` | 5 | Inline -- the struct adds nothing. |
| `connList` (mutex-protected map with 2 methods) | `sock` | 36 | Inline into `sockStore`. |
| `Spec` struct wrapping single `uint32` | `scheduler` | 5 | Replace with `map[string]uint32`. |
| `VerifyDelegationCertChain` exported wrapper over unexported function | `auth` | 4 | Export the inner function directly. |
| `newControlClient` (wraps `newControlClientWithTimeout` with default) | `cmd/pln` | 3 | Marginal. Keep or inline. |
| `sortConnections` standalone function | `store` | 8 | Inline into sole caller. |
| `ttlOrDefault` helper | `config` | 4 | Inline into 4 one-liner methods. |

**Total: ~65 LOC**

### Total Estimated LOC Savings

| Category | LOC |
|----------|-----|
| Dead packages | 118 |
| Dead exported symbols | 150 |
| Dead internal code | 105 |
| Inlineable packages (net savings) | 46 |
| Deduplication | 166 |
| Over-abstracted wrappers | 65 |
| **Total** | **~650 LOC** |

This represents approximately **3.8% of the 17,102 production LOC**. Additionally, the store migration code (`migrateYAMLToProto` ~50 LOC, `migrateConsumedInvitesJSON` ~60 LOC) is potentially deletable if all nodes have been migrated, adding another ~110 LOC.

---

## Recommendations

Ordered by impact/risk ratio (highest first):

### Tier 1: High Impact, Low Risk (do first)

| # | Action | Category | LOC impact | Risk |
|---|--------|----------|------------|------|
| 1 | Delete `pkg/observability/logging` | Dead package | -23 | **[SAFE]** |
| 2 | Delete `types.Envelope` | Dead symbol | -3 | **[SAFE]** |
| 3 | Delete `Span.parentSpanID` + related dead fields | Dead code | -10 | **[SAFE]** |
| 4 | Delete `metrics.Labels` and all label plumbing | Dead code | -25 | **[SAFE]** |
| 5 | Delete `util.Bump()` + bump channel + select branch | Dead code | -10 | **[SAFE]** |
| 6 | Delete `sock.probe()`, `sockStore.log` | Dead symbols | -6 | **[SAFE]** |
| 7 | Inline `pkg/workspace` into `cmd/pln` | Inlineable | -26 | **[SAFE]** |
| 8 | Inline `pkg/server` into `cmd/pln` | Inlineable | -69 | **[SAFE]** |
| 9 | Inline `pkg/util` into `pkg/node` | Inlineable | -66 | **[SAFE]** |
| 10 | Inline `pkg/sysinfo` into `pkg/node` | Inlineable | -23 | **[SAFE]** |
| 11 | Extract `mustPollenPath` helper in `cmd/pln` | Dedup | -40 | **[SAFE]** |
| 12 | Unify `Open*Stream` methods in mesh | Dedup | -50 | **[SAFE]** |
| 13 | Remove dead nil guards across codebase | Dead code | -30 | **[SAFE]** |
| 14 | Unexport internal-only symbols (scheduler, topology, config, peer, perm) | API cleanup | 0 (rename) | **[BREAKING]** |
| 15 | Delete `sock.connList`, inline into `sockStore` | Wrapper | -36 | **[SAFE]** |

### Tier 2: Medium Impact, Medium Risk

| # | Action | Category | LOC impact | Risk |
|---|--------|----------|------------|------|
| 16 | Split `store/gossip.go` into 8-9 files | Readability | 0 (reorg) | **[SAFE]** |
| 17 | Split `mesh.go` into 4-5 files | Readability | 0 (reorg) | **[SAFE]** |
| 18 | Move identity key I/O from `node` to `auth` | Boundary | -100 from node | **[SAFE]** |
| 19 | Replace `nodeRecord` leaks with query methods | Abstraction | -12 + new methods | **[SAFE]** |
| 20 | Remove scheduler shadow types (`NodeState`, `ClusterState`) | Dedup | -25 | **[SAFE]** |
| 21 | Rename `peer.PeerState*` to `peer.State*` | Stutter | 0 (rename) | **[BREAKING]** |
| 22 | Add noop `traffic.Recorder` | Missing abstraction | +5 | **[SAFE]** |
| 23 | Add `sync.WaitGroup` to scheduler for in-flight claims | Correctness | +10 | **[RISKY]** |
| 24 | Make `requestPunchCoordination` synchronous | Concurrency simplification | -5 | **[SAFE]** |

### Tier 3: High Impact, High Risk (plan carefully)

| # | Action | Category | LOC impact | Risk |
|---|--------|----------|------------|------|
| 25 | Redesign 256-goroutine punch to use socket pool | Concurrency | ~-100 | **[RISKY]** |
| 26 | Make `mesh.inCh` sends blocking with ctx.Done | Correctness | ~5 | **[RISKY]** |
| 27 | Add request correlation to `renewalCh` | Correctness | ~20 | **[RISKY]** |
| 28 | Decompose `mesh.Mesh` into narrow interfaces | Architecture | 0 (reorg) | **[BREAKING]** |
| 29 | Extract cert lifecycle from `node` | Architecture | -130 from node | **[SAFE]** but large |
| 30 | Extract `GetStatus` into internal package | Architecture | -280 from svc.go | **[SAFE]** but large |
| 31 | Thread node context through fire-and-forget goroutines | Correctness | ~10 | **[RISKY]** |
| 32 | Delete YAML/JSON migration code (if all nodes migrated) | Dead code | -110 | **[SAFE]** if confirmed |
