# Boundary Refactor — Design Spec

**Date:** 2026-03-16
**Goal:** Clean separation, safe concurrency, and behavioral backward compatibility through a phased refactor of component boundaries — starting from the store and mesh, working inward to the node orchestrator and control plane.

**Context:** The refactor attempted in v0.1.0-alpha.129–139 (+38k/–26k lines across 204 files) introduced regressions by changing too much at once. This spec takes the valuable architectural ideas from that attempt and applies them incrementally, with rigorous per-phase verification. The integration test suite has been ported back to the v0.1.0-alpha.128 baseline and mutation-tested.

**Non-goals:** Package renames, umbrella directory restructuring, CLI behavioral changes. Structure follows naturally from clean boundaries; CLI changes get their own spec if needed.

---

## Core Principles

These four rules govern every phase. Any change that violates one gets rejected.

1. **One owner per goroutine.** Every piece of mutable state has exactly one goroutine that writes to it. Other goroutines communicate via channels or method calls that internally serialize. No shared mutexes across component boundaries.

2. **Value types cross boundaries, references don't.** All inter-component data is passed as immutable snapshots or value copies. No `*Store` passed to the scheduler. No `*Mesh` embedded in the node. Consumer-defined interfaces with value-type method signatures.

3. **Blocking backpressure, never silent drops.** Every channel send either blocks (with context deadline) or returns an explicit error. No `select { default: }` drops. If a consumer is slow, the producer waits or the system signals overload — never silently loses state.

4. **Consumer-defined interfaces.** Each package defines the narrowest interface it needs from its dependencies. The provider satisfies the interface without knowing about it. No concrete type imports across package boundaries except foundation types (`PeerKey`, `Envelope`, proto messages).

---

## Phase 1: Store Boundary

The store has the widest blast radius: its `RWMutex` is touched by the node event loop, scheduler callbacks, tunnel handlers, and gossip processing.

### Current Problems

- `store.Store` is a concrete type passed directly to scheduler, node, and others.
- `RWMutex` held by consumers during reads — contention under load.
- Callbacks (`OnDenyPeer`, `OnRouteInvalidate`, `OnWorkloadChange`, `OnTrafficChange`) fire synchronously in the caller's goroutine after the store unlocks. A slow callback blocks the caller (e.g., the node's recv loop or clock-stream handler) from returning to process more datagrams/streams, coupling the store's reaction path to the caller's throughput.
- Consumers reach into store internals to query per-node records.

### Target State

#### a) Immutable snapshots for reads

Instead of consumers taking `RLock` and walking internal maps, the store exposes a `Snapshot()` method that returns a frozen, read-only copy of cluster state. The snapshot is a value type — consumers can hold it as long as they want without blocking writes. The store rebuilds the snapshot on each mutation (cheap for the CRDT's append-only nature — or copy-on-write if profiling shows otherwise).

#### b) Typed event channel for reactions

The four callbacks become a single `Events() <-chan StoreEvent` channel. `StoreEvent` is a sum type (interface with sealed variants: `DenyEvent`, `RouteInvalidateEvent`, `WorkloadChangeEvent`, `TrafficChangeEvent`, plus `GossipApplied` for general state changes). The store owns the send side, consumers own the receive side. Sends block with a context deadline — if the consumer is dead, the store's context is cancelled and it shuts down cleanly rather than silently dropping.

#### c) Consumer-defined read interfaces

The scheduler doesn't need the full store — it needs specs, claims, and cluster state:

```go
// In pkg/scheduler
type ClusterView interface {
    Snapshot() ClusterSnapshot  // specs, claims, node states
}
```

The node needs peer state, services, reachability:

```go
// In pkg/node
type StateView interface {
    Snapshot() NodeSnapshot  // peers, services, reachability, coordinates
}
```

The store satisfies both without knowing about either. Under the hood, there is a single internal snapshot type — `ClusterSnapshot` and `NodeSnapshot` are projections (type aliases or thin wrappers) over the same data. The store builds one snapshot per mutation; consumers see the projection their interface defines.

#### d) Mutation stays internal

Only the store's own goroutine mutates state. `ApplyEvents()` and `LocalEvents()` become channel-sends into the store, not direct method calls under lock. The store's internal loop processes mutations sequentially — no mutex needed because there's only one writer.

`ApplyEvents` currently returns `ApplyResult` (containing `Rebroadcast` events that the caller uses immediately for gossip propagation). In the new model, the store handles rebroadcast internally: after applying events, the store emits a `GossipApplied` event through the event channel containing the events to rebroadcast. The node receives this event and pushes to peers. This changes the gossip propagation path from synchronous (apply → return rebroadcast → push) to asynchronous (apply → emit event → node pulls → push), but the latency is negligible (in-process channel read) and it cleanly separates store mutation from gossip transport.

#### e) Eager event propagation

State changes push eagerly on mutation: when the store processes a mutation, it emits a `GossipApplied` event that the node uses to immediately push state to peers. The periodic gossip tick (10s) becomes a consistency fallback, not the primary propagation path.

### What Changes

- `pkg/store` exports `Snapshot()`, `Events()`, and mutation channels instead of `RWMutex`-guarded methods + callbacks.
- `pkg/scheduler` depends on a `ClusterView` interface, not `*store.Store`.
- `pkg/node` depends on a `StateView` interface, not `*store.Store`.
- Disk persistence moves inside the store's own loop (it already saves on a ticker — now it's the store's goroutine doing it, not the node's).

### What Doesn't Change

- CRDT merge semantics (same `ApplyEvents` logic internally).
- Gossip wire format (proto messages unchanged).
- On-disk format (same `state.pb`).
- Integration test assertions (they test observable behavior, not store internals).

### Risk

Snapshot copy cost. Mitigation: the CRDT state is small (dozens of peers x ~10 attributes each). Profile before optimizing. If needed, use structural sharing (immutable tree) but don't build it speculatively.

---

## Phase 2: Mesh/Transport Boundary

The mesh owns the most goroutines and the most dangerous silent-drop channels. It conflates transport (QUIC sessions, streams) with peer management (connect/disconnect events, routing, punch coordination).

### Current Problems

- `streamCh` / `clockStreamCh` — per-session goroutines write with `select { default: }`, immediately dropping streams if buffer full.
- `inCh` — peer events dropped after 5s timeout.
- Per-session goroutine pair (`recvDatagrams` + `acceptBidiStreams`) multiplexes all stream types through shared buffered channels. One slow consumer creates head-of-line blocking, which the code "solves" by dropping.
- `Mesh` is a concrete type — the node imports mesh internals.
- Stream type dispatch leaks conceptually into every consumer that knows about stream type constants.

### Target State

#### a) Consumer-defined transport interface

The node defines what it needs:

```go
// In pkg/node
type Transport interface {
    Connect(ctx context.Context, key PeerKey, addrs []netip.AddrPort) error
    PeerEvents() <-chan PeerEvent  // blocking sends from transport
    Send(ctx context.Context, dest PeerKey, data []byte) error
    Recv(ctx context.Context) (Packet, error)
    AcceptStream(ctx context.Context) (Stream, StreamType, PeerKey, error)
    OpenStream(ctx context.Context, peer PeerKey, st StreamType) (Stream, error)
}
```

No more separate accept channels per stream type. One blocking call, consumer dispatches. QUIC's own accept queue provides the buffer.

#### b) Eliminate per-type buffered channels

`streamCh`, `clockStreamCh`, and `inCh` disappear. (`recvCh` already blocks correctly and is retained.) The per-session `acceptBidiStreams` goroutine reads the type byte and hands the stream directly to a dispatcher, which blocks until the consumer pulls via `AcceptStream`. `renewalCh` (buffer 1, `select { default: }` drop) is converted to a blocking send — a stale renewal response blocking briefly is acceptable; silently losing a renewal response is not.

#### c) Blocking peer events

`inCh` becomes a blocking channel with context deadline. If the node's event loop is too slow to process peer events, that's a real problem to surface, not hide.

#### d) Reduce per-session goroutines

Currently 2 per peer. Evaluate whether a single per-session goroutine suffices. If not, keep two but with blocking sends instead of drops.

#### e) Routing moves out of mesh

The mesh calls through an interface for multi-hop forwarding:

```go
// In pkg/mesh
type Router interface {
    NextHop(dest PeerKey) (PeerKey, bool)
}
```

The route package satisfies this. The mesh no longer imports `pkg/route` directly.

### What Changes

- `pkg/mesh` exports a transport interface, not concrete `*impl`.
- Stream type dispatch moves from mesh internals to the node's accept loop.
- 4 drop-channels fixed: `streamCh`, `clockStreamCh`, `inCh` eliminated; `renewalCh` converted to blocking.
- `pkg/node` depends on `Transport` interface.
- `pkg/route` satisfies `Router` interface.
- Tunnel manager receives streams from the node's dispatch, not directly from mesh.

### What Doesn't Change

- QUIC as the transport (same wire protocol).
- Stream type byte encoding (same format).
- NAT punch mechanics (same UDP probing).
- Session reaping logic (behind the interface).
- Integration tests (they use `TestNode` which wraps the full stack).

### Risk

Single `AcceptStream` blocking call means one slow stream type blocks acceptance of all types. Mitigation: the node's dispatch loop reads the type and hands off to handler goroutines immediately. The blocking point moves from "mesh internal channel" to "handler goroutine availability," which is visible and debuggable.

---

## Phase 3: Node Orchestrator

With clean store and mesh boundaries, the node event loop simplifies as a consequence.

### Current Problems

- `node.Start()` select loop has 10+ cases mixing peer management, gossip, scheduling signals, cert renewal, state persistence, route debouncing.
- Ephemeral goroutines (`doConnect`, `requestPunchCoordination`, per-peer clock gossip, eager sync) spawned fire-and-forget with no concurrency bound.
- `localPeerEvents` channel exists because multiple goroutines need to feed peer state back to the main loop — a symptom of unclear ownership.
- The node directly reaches into store and mesh internals rather than reacting to events.

### Target State

#### a) Simplified select loop

```go
for {
    select {
    case <-ctx.Done():
        return nil
    case ev := <-transport.PeerEvents():
        // peer connected/disconnected — update topology, trigger gossip
    case ev := <-store.Events():
        // route invalidation, workload change, deny, traffic
        // RouteInvalidateEvent starts a debounce timer (routeDebounceT)
    case <-routeDebounceT.C:
        // debounced route recomputation (100ms debounce, 1s max delay — same as today)
    case <-peerTicker.C:
        // evaluate target peers, connect/disconnect
    case <-gossipTicker.C:
        // push state to random peers (consistency fallback)
    case <-certTicker.C:
        // check cert expiry
    case <-ipRefreshTicker.C:
        // refresh external IP for nodes behind dynamic IPs
    }
}
```

Eight cases. `localPeerEvents`, `punchCh`, `gossipEvents`, and `stateSaveTicker` eliminated. `routeInvalidate` channel replaced by `RouteInvalidateEvent` via `store.Events()`, with the debounce timer retained as a select case. `ipRefreshTicker` retained — it's a real operational concern for dynamic-IP nodes.

#### b) Stream dispatch as a separate goroutine

```go
for {
    stream, stype, peer, err := transport.AcceptStream(ctx)
    if err != nil { return }
    switch stype {
    case StreamTypeClock:    go n.handleClockStream(ctx, stream, peer)
    case StreamTypeTunnel:   n.tunnels.Handle(stream, peer)
    case StreamTypeArtifact: go n.handleArtifact(ctx, stream, peer)
    case StreamTypeWorkload: go n.handleWorkload(ctx, stream, peer)
    default:
        n.log.Warn("unknown stream type", zap.Uint8("type", uint8(stype)))
        stream.Close()
    }
}
```

Exhaustive switch with explicit unknown-type handling. `StreamTypeRouted` is handled internally by the transport layer (forwarded at the mesh level before reaching `AcceptStream`), so it never appears here. Each handler is short-lived and bounded.

#### c) Bounded worker pool for ephemeral work

Connect attempts, punch coordination, eager syncs, and per-peer clock gossip submit to a bounded worker pool (semaphore channel + WaitGroup). No fire-and-forget, no unbounded goroutine growth. `wg.Wait()` in shutdown guarantees clean teardown.

#### d) `localPeerEvents` eliminated

Connect results come through `transport.PeerEvents()`. Deny events come through `store.Events()`. The channel is unnecessary.

#### e) Punch coordination simplified

Three persistent `punchLoop` goroutines consuming from buffered `punchCh` with drops replaced by worker pool submissions. No dedicated goroutines, no drop channel.

### What Changes

- `node.Start()` event loop: 10+ cases → 8.
- `localPeerEvents`, `punchCh`, `gossipEvents` channels eliminated. `routeInvalidate` channel replaced by store event + debounce timer.
- Ephemeral goroutines managed through bounded worker pool.
- Stream dispatch becomes its own goroutine.
- Punch loops replaced by worker pool submissions.

### What Doesn't Change

- Peer tick / gossip tick intervals.
- Topology evaluation logic (pure function).
- Cert renewal logic.
- Shutdown ordering intent (drain active work before closing transport). The mechanism changes: fire-and-forget goroutines become explicit `wg.Wait()` in the worker pool, making shutdown deterministic rather than best-effort.
- Integration tests.

### Risk

Fewer select cases processing fewer event sources per iteration. Mitigation: the current loop processes one case per select iteration anyway — some events now arrive as sub-variants of `store.Events()`, which is fewer cases to evaluate (faster).

---

## Phase 4: Control Plane (Internal Only)

No CLI changes. No lifecycle changes. Purely internal: the control service depends on interfaces instead of `*Node`.

### Target State

The gRPC control service (`NodeService`) depends on narrow interfaces:

```go
type ServiceRegistry interface {
    Register(ctx context.Context, port uint16, name string) error
    Unregister(ctx context.Context, name string) error
}

type WorkloadRunner interface {
    Seed(ctx context.Context, binary []byte, replicas int) (hash string, err error)
    Unseed(ctx context.Context, hash string) error
    Call(ctx context.Context, hash string, fn string, input []byte) ([]byte, error)
}

type StatusProvider interface {
    Status(ctx context.Context) (*StatusResponse, error)
    Metrics(ctx context.Context) (*MetricsResponse, error)
}
```

Additional methods (`Shutdown`, `DenyPeer`, `ConnectPeer`, `GetStatus`, `GetMetrics`) are covered by extending these interfaces or adding narrowly-scoped ones as needed. The control service becomes a thin translation layer: gRPC request → interface method → gRPC response. Error mapping to gRPC status codes stays here.

### What Changes

- `NodeService` holds interfaces, not `*Node`.
- Node satisfies the interfaces.

### What Doesn't Change

- gRPC proto definitions (same `control.proto`).
- Unix socket transport.
- CLI command surface, flags, defaults.
- Status output format, columns, colors, library.
- Metrics names and labels.

---

## Verification Protocol

### Pre-Phase: Behavioral Inventory

Before any code changes, produce a committed inventory document cataloguing every observable behavior. Nothing in the inventory may regress at any phase gate.

| Category | What to capture |
|----------|----------------|
| **CLI surface** | Every command, subcommand, flag, default — from `--help` output |
| **Status output** | Exact format of `pln status`, `pln status --wide`, `pln status --all` — columns, ordering, colors (library), alignment, truncation |
| **Logging** | Every log line emitted during: startup, peer connection/disconnection, gossip sync, service registration, workload seed/unseed/call, cert renewal, shutdown. Source location, level, structured fields |
| **Metrics** | Every Prometheus metric name, label set, type from `GetMetrics` |
| **Wire format** | Proto field numbers, gossip event types, digest format, QUIC stream type bytes |
| **On-disk format** | `state.pb` schema, config file format, key file locations |

### Tier 1 — Automated (every commit)

1. **Integration tests pass.** `just test-integration` — all scenarios green. Test interface adaptations committed separately from implementation so they can be verified against old code first.
2. **New code covered by tests.** Any new interface, event type, or behavioral path gets a dedicated test.
3. **Mutation testing.** Each test exercising changed code is mutation-tested: break the code in the expected way, verify test fails with minimal timeout, reinstate.
4. **Lint.** `just lint` green.
5. **Race detector.** `-race` on integration tests and new concurrency unit tests.

### Tier 2 — Hetzner Cluster (phase gate)

All nodes start with fully purged state. Fresh binary via existing Ansible playbooks. All commands use the current CLI surface exactly as it exists.

**Cluster formation:**

1. Local node as root. Initialize and start with current flow. Verify: startup logs match inventory, `pln status` shows self.
2. First Hetzner node via `pln bootstrap ssh root@<ip>`. Verify: both nodes see each other in `pln status`, status format matches inventory, logs match inventory.
3. Second Hetzner node via `pln invite` / `pln join <token>`. Verify: node discovers existing peers eagerly, all three visible in status on all three, logs show expected connection sequence.
4. Third Hetzner node via `pln id` + `pln invite --subject <id>` + `pln join <token>`. Verify: all four nodes fully meshed.

**Convergence:**

5. `pln status --wide` on all nodes. Vivaldi error decreases, samples increase. Diagnostics section present and correctly formatted. By end of refactor, convergence should be near-instant due to eager gossip propagation.

**Services:**

6. `pln serve 8080 test-svc` on one node. All others show it in status within seconds (near-instant by end of refactor).
7. `pln connect test-svc` on another node. Verify tunnel works end-to-end.

**Workloads:**

8. Full `pln seed` → `pln unseed` → `pln seed --replicas 2` cycle. Status shows correct replica counts, claims, uptime.
9. `pln call <hash> handle "hello"` from all four nodes. Correct response from each.
10. 1000 concurrent calls from two non-seed nodes. All correct responses. No timeouts or errors.

**Workload migration:**

11. With 2 replicas running, `pln down` one of the seed-holding nodes. Verify: the remaining cluster detects the loss and a different node claims the workload, restoring the desired replica count. Check via `pln status` on surviving nodes and logs showing the claim. This is the final manual check — cluster state resets on next iteration.

**Cross-cutting at every step:**

- Logs checked against inventory — no missing lines, no changed levels, no lost structured fields.
- `pln status` / `--wide` / `--all` output format identical to inventory.
- Metrics values sensible.

### Verification is scripted

Each tier 2 step becomes a reproducible just recipe. The script purges state, deploys fresh binary, runs each step, asserts expected output, fails loudly on deviation.

---

## Success Criteria

By completion of all four phases:

- **Zero behavioral regressions.** Every item in the behavioral inventory produces identical output.
- **No silent drops.** Every channel in the system either blocks with backpressure or returns an error. `grep -r 'select {' | grep 'default:'` across channel sends returns zero hits (outside of tests).
- **Clean boundaries.** No concrete type imports across package boundaries except foundation types. `go vet` and manual review confirm.
- **Eager convergence.** State changes propagate immediately on mutation. Hetzner verification steps that previously took seconds complete near-instantly.
- **Goroutine reduction.** Fewer persistent goroutines, bounded ephemeral goroutines via worker pool. Measurable via `runtime.NumGoroutine()` in tests.
- **Integration tests green with race detector.** All existing scenarios pass, plus new scenarios covering the changed boundaries.
