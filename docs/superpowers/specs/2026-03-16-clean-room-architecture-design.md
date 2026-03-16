# Clean Room Architecture Design

**Date:** 2026-03-16
**Status:** Approved

## Context

Between v0.1.0-alpha.128 and alpha.141, two refactor attempts were made:

**alpha.129 (failed):** A big-bang rewrite — 188 files, +34k/-26k lines. Replaced entire packages (`store`→`state`, `mesh`→`transport`, `node`→`orchestrator`), renamed CLI commands, restructured directories. Eleven follow-up releases (alpha.130–139) chased cascading regressions: broken identity keys, missing join flow, lost admin commands, broken peer detection, missing clock gossip. Rolled back at alpha.140.

**alpha.141 (succeeded):** The same *ideas* applied surgically — 52 files, same package names, interfaces added in place. Store snapshots, event channels, consumer-defined interfaces, bounded worker pool, unified stream dispatch — all delivered without breaking behavioral compatibility.

**Key lesson:** The architectural ideas were sound. The delivery mechanism (wholesale replacement) was the failure mode. This design defines the *target architecture* as if building from scratch. The codebase will be incrementally converged toward it through controlled steps with strict verification gates.

## Goals

1. **Optimal component separation** — each package has one clear purpose, communicates through well-defined interfaces, and can be understood and tested independently.
2. **Clean, minimal consumer interfaces** — each package exposes a narrow API; consumers define their own narrower subset interfaces.
3. **Agent independence** — different agents (AI or human) can work on a component without breaking others. A change inside one domain service touches zero files outside that package.
4. **Clean leaf dependencies** — foundation packages have zero or minimal internal dependencies.
5. **Incremental migration** — the current codebase converges toward this target through many controlled steps, each verified by integration tests and Hetzner cluster validation.

## Layer Architecture

```
Layer 5: Composition
  cmd/pln          CLI (Cobra commands, flag parsing, output formatting)
  control          gRPC translation layer (CLI <-> domain services)
  supervisor       lifecycle, shutdown ordering, wiring, signals

Layer 4: Domain Services (each owns its event loop, independent of siblings)
  membership       join/deny, certs, gossip schedule, peer selection
  placement        scheduling, workload exec, CAS fetch, WASM runtime
  tunneling        service bridging, connection lifecycle, traffic, relay

Layer 3: Routing (pure computation, no goroutines)
  routing          topology snapshot -> Dijkstra table -> NextHop(dest)

Layer 2: Core (siblings -- neither imports the other)
  state            CRDT merge, snapshots, events, delta encode/decode
                   typed projections: MembershipView, PlacementView,
                   TopologyView, TrafficView
                   zero I/O, zero goroutines -- caller owns the thread

  transport        QUIC sessions, peer FSM, stream mux, NAT punch,
                   UDP sockets, hole-punching
                   knows nothing about gossip, workloads, or services
                   all operations are direct-peer only

Layer 1: Foundation (leaf packages, zero or minimal internal deps)
  types            PeerKey, Envelope
  auth             credentials, delegation certs, trust bundles, join tokens
  coords           Vivaldi math (pure functions: Update, Distance)
  nat              NAT type enum (Easy, Hard, Unknown)
  config           YAML loading, defaults, persistence
  cas              content-addressable store (filesystem, SHA-256)
  wasm             WASM runtime (Extism/wazero wrapper)
  observability    metrics, traces, logging (noop pattern)
```

### Dependency Rules

- Each layer may only import from layers below it.
- Siblings at the same layer never import each other.
- Consumer-defined interfaces at every boundary — each package defines the narrowest interface it needs from its dependencies.
- Value types cross boundaries, references don't. Exception: stream handles (`io.ReadWriteCloser`) are inherently reference-typed and are passed across boundaries by design. The stream is the boundary — once handed off, the receiver owns it.

## Package APIs

### State

State is a pure data structure with zero I/O. No goroutines, no channels, no scheduling. Thread-safe via internal serialization (mutations are mutex-protected; `Snapshot()` is lock-free via `atomic.Pointer[Snapshot]`).

```go
package state

type Store struct { ... }
func New(self types.PeerKey) *Store

// Reads -- lock-free, callable from any goroutine
func (s *Store) Snapshot() Snapshot

// Gossip support -- called by membership service
func (s *Store) ApplyDelta(from types.PeerKey, data []byte) ([]Event, error)
func (s *Store) EncodeDelta(since Clock) []byte
func (s *Store) FullState() []byte
```

**Snapshot projections** — typed, read-only views off an immutable snapshot:

```go
func (s Snapshot) Peers() []PeerInfo
func (s Snapshot) Peer(key) (PeerInfo, bool)
func (s Snapshot) Services() []ServiceInfo
func (s Snapshot) Workloads() []WorkloadInfo
func (s Snapshot) DeniedPeers() []PeerKey
func (s Snapshot) Self() PeerKey
func (s Snapshot) Clock() Clock
```

`PeerInfo` bundles: addresses, NAT type, Vivaldi coord, reachability set, resource telemetry, traffic heatmap.

**Typed mutations** — state knows the attribute schema (must, for CRDT resolution), but nothing about I/O, timing, or transport:

```go
// Membership-relevant
func (s *Store) DenyPeer(key PeerKey) []Event
func (s *Store) SetLocalAddresses(addrs []netip.AddrPort) []Event
func (s *Store) SetLocalNAT(t nat.Type) []Event
func (s *Store) SetLocalCoord(c coords.Coord) []Event
func (s *Store) SetLocalReachable(peers []PeerKey) []Event

// Placement-relevant
func (s *Store) SetWorkloadSpec(hash string, replicas int, ...) []Event
func (s *Store) ClaimWorkload(hash string) []Event
func (s *Store) ReleaseWorkload(hash string) []Event
func (s *Store) SetLocalResources(cpu, mem float64) []Event

// Tunneling-relevant
func (s *Store) SetService(port uint32, name string) []Event
func (s *Store) RemoveService(name string) []Event
func (s *Store) SetLocalTraffic(peer PeerKey, in, out uint64) []Event
```

Every mutation method returns `[]Event` so local changes trigger the same reactive paths as remote gossip.

**Events** — returned synchronously, not via channel. The caller decides what to do with them:

```go
type Event interface { stateEvent() }  // sealed sum type

type PeerJoined      struct { Key PeerKey }
type PeerLeft        struct { Key PeerKey }
type PeerDenied      struct { Key PeerKey }
type ServiceChanged  struct { Peer PeerKey; Name string }
type WorkloadChanged struct { Hash string }
type TopologyChanged struct { Peer PeerKey }
type GossipApplied   struct {}
```

**Consumer interface pattern** — each domain service defines the narrowest interface it needs:

```go
// In package membership:
type ClusterState interface {
    Snapshot() state.Snapshot
    ApplyDelta(from types.PeerKey, data []byte) ([]state.Event, error)
    EncodeDelta(since state.Clock) []byte
    DenyPeer(key types.PeerKey) []state.Event
    SetLocalAddresses([]netip.AddrPort) []state.Event
    SetLocalCoord(coords.Coord) []state.Event
    SetLocalNAT(nat.Type) []state.Event
    SetLocalReachable([]types.PeerKey) []state.Event
}

// In package placement:
type WorkloadState interface {
    Snapshot() state.Snapshot
    SetWorkloadSpec(hash string, replicas int, ...) []state.Event
    ClaimWorkload(hash string) []state.Event
    ReleaseWorkload(hash string) []state.Event
    SetLocalResources(cpu, mem float64) []state.Event
}

// In package tunneling:
type ServiceState interface {
    Snapshot() state.Snapshot
    SetService(port uint32, name string) []state.Event
    RemoveService(name string) []state.Event
    SetLocalTraffic(peer types.PeerKey, in, out uint64) []state.Event
}
```

### Event Fan-Out Model

Membership is the **sole caller** of `ApplyDelta` during steady-state operation (it owns the gossip lifecycle end-to-end: recv, merge, send). Supervisor calls `ApplyDelta` once during startup to hydrate from disk, before membership starts. When `ApplyDelta` returns events, membership processes the ones it cares about (peer join/leave, topology changes for peer selection) and forwards all events on an output channel:

```go
// Membership exposes:
func (s *Service) Events() <-chan state.Event
```

Supervisor reads this channel and dispatches to other consumers:
- `TopologyChanged` -> rebuild routing table, inject into tunneling
- `WorkloadChanged` -> notify placement (or placement polls snapshots on a ticker)
- `ServiceChanged` -> notify tunneling

This makes membership the single gossip writer and supervisor the event router. No other domain service calls `ApplyDelta` or `Recv`. Placement and tunneling react to state changes either via supervisor-forwarded events or by reading snapshots on their own tickers.

### State Persistence

State has zero I/O — it doesn't read from or write to disk. Persistence is supervisor's responsibility:
- **On startup:** supervisor reads the state file from disk, calls `state.ApplyDelta()` to hydrate.
- **On shutdown:** supervisor calls `state.FullState()` and writes it to disk.
- **Periodically:** supervisor snapshots state to disk on a timer (e.g., every 30s).

### Multi-Hop Relay and Routed Streams

Transport is direct-peer only. Multi-hop routing is composed at a higher layer.

**Originating a routed stream:** When a domain service (e.g., placement fetching an artifact, tunneling connecting a service) needs to reach a peer that isn't directly connected:

1. Try `transport.OpenStream(ctx, peer, st)` — succeeds if directly connected.
2. If that fails, look up `routing.NextHop(peer)`.
3. Open a stream to the next hop with `StreamTypeRouted`, write the destination peer key and TTL into the stream header.
4. The next hop's tunneling service receives this routed stream via `HandleRoutedStream`, reads the header, looks up the next hop, and bridges forward.

Supervisor provides a **routed stream opener** (internal to the supervisor package) that wraps this logic:

```go
// Internal to supervisor — wiring, not domain logic
type routedOpener struct {
    transport *transport.QUICTransport
    routing   func() routing.Table  // reads current table
}

func (r *routedOpener) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (transport.Stream, error) {
    // Try direct first
    if stream, err := r.transport.OpenStream(ctx, peer, st); err == nil {
        return stream, nil
    }
    // Fall back to routed
    nextHop, ok := r.routing().NextHop(peer)
    if !ok {
        return nil, errors.New("no route to peer")
    }
    stream, err := r.transport.OpenStream(ctx, nextHop, StreamTypeRouted)
    if err != nil {
        return nil, err
    }
    // Write route header (destination + TTL)
    writeRouteHeader(stream, peer, defaultTTL)
    return stream, nil
}
```

Tunneling and placement receive this wrapper (which satisfies their `StreamTransport` consumer interface) instead of raw transport. They don't know whether streams are direct or routed — the abstraction is transparent.

### Datagram Protocol

Datagrams (`Send`/`Recv`) are used exclusively for gossip. Cert renewal uses a dedicated stream type (`StreamTypeCertRenewal`), not datagrams. This makes membership the sole consumer of `Recv` — no multiplexing needed.

### Transport

Transport handles QUIC connections, stream multiplexing, and NAT traversal. All operations are direct-peer only. Data bytes are opaque.

```go
package transport

type StreamType byte
type PeerEventType int // Connected | Disconnected

type Packet    struct { From types.PeerKey; Data []byte }
type PeerEvent struct { Key types.PeerKey; Type PeerEventType; Addrs []netip.AddrPort }

type QUICTransport struct { ... }
func New(self types.PeerKey, creds auth.NodeCredentials, listenAddr string) *QUICTransport

// Lifecycle
func (t *QUICTransport) Start(ctx context.Context) error
func (t *QUICTransport) Stop() error

// Peer management -- callers decide who to connect to
func (t *QUICTransport) Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
func (t *QUICTransport) Disconnect(key types.PeerKey) error
func (t *QUICTransport) PeerEvents() <-chan PeerEvent
func (t *QUICTransport) ConnectedPeers() []types.PeerKey

// Datagrams -- direct peers only, opaque bytes
func (t *QUICTransport) Send(ctx context.Context, peer types.PeerKey, data []byte) error
func (t *QUICTransport) Recv(ctx context.Context) (Packet, error)

// Streams -- direct peers only, type-agnostic
func (t *QUICTransport) OpenStream(ctx context.Context, peer types.PeerKey, st StreamType) (Stream, error)
func (t *QUICTransport) AcceptStream(ctx context.Context) (Stream, StreamType, types.PeerKey, error)
```

**Internal concerns** (not exposed):

- QUIC session management (quic-go)
- Peer connection FSM (connecting -> connected -> backoff -> reconnecting)
- NAT detection (UDP probes)
- Hole-punching (ephemeral sockets) — `Connect` handles punch coordination internally when NAT traversal is needed; callers don't trigger punches explicitly
- TLS mutual auth (using auth.NodeCredentials)

**Design decisions:**

1. **No routing in transport.** `Send` and `OpenStream` fail if the peer isn't directly connected. Multi-hop relay is handled by the tunneling service, which opens a new stream to the next hop and bridges the two streams.
2. **Stream types are opaque.** Transport reads/writes the `StreamType` byte for framing but doesn't interpret it. Constants are defined in transport as wire protocol values; semantics belong to the domain services.
3. **No gossip awareness.** `Send`/`Recv` move opaque byte slices.
4. **Connect is caller-driven.** Membership's peer selection algorithm decides who to connect to; transport executes.

**Consumer interface examples:**

```go
// In package membership:
type Network interface {
    Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
    Disconnect(key types.PeerKey) error
    PeerEvents() <-chan transport.PeerEvent
    ConnectedPeers() []types.PeerKey
    Send(ctx context.Context, peer types.PeerKey, data []byte) error
    Recv(ctx context.Context) (transport.Packet, error)
}

// In package tunneling:
type StreamTransport interface {
    OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (transport.Stream, error)
}
```

### Domain Services

Each domain service owns its own event loop, runs in its own goroutine, and communicates with state and transport through narrow consumer-defined interfaces. Siblings never import each other.

#### Membership

Owns: join/deny, gossip schedule, peer selection, Vivaldi exchange, cert renewal, NAT tracking, address refresh.

```go
package membership

type Service struct { ... }
func New(self types.PeerKey, creds auth.NodeCredentials, net Network, cluster ClusterState, opts ...Option) *Service

func (s *Service) Start(ctx context.Context) error
func (s *Service) Stop() error

// Control plane
func (s *Service) DenyPeer(key types.PeerKey) error
func (s *Service) Invite(subject string) (string, error)

// Stream dispatch
func (s *Service) HandleClockStream(stream transport.Stream, peer types.PeerKey)
func (s *Service) HandleCertRenewalStream(stream transport.Stream, peer types.PeerKey)

// Event output -- supervisor reads this to dispatch to other services
func (s *Service) Events() <-chan state.Event
```

**Event loop:**

- `transport.PeerEvents()` -> update reachability, trigger eager gossip
- `transport.Recv()` -> `state.ApplyDelta()` -> process returned events
- Gossip ticker -> `state.EncodeDelta()` -> `transport.Send()` to selected peers
- Peer selection ticker -> evaluate topology -> `transport.Connect/Disconnect`
- Cert renewal ticker -> check expiry, renew via stream
- IP refresh ticker -> re-detect external address

#### Placement

Owns: scheduling algorithm, seed/unseed lifecycle, WASM compilation, workload invocation, CAS storage, artifact fetching.

```go
package placement

type Service struct { ... }
func New(self types.PeerKey, workloads WorkloadState, cas cas.Store, runtime wasm.Runtime, opts ...Option) *Service

func (s *Service) Start(ctx context.Context) error
func (s *Service) Stop() error

// Control plane
func (s *Service) Seed(hash string, binary []byte) error
func (s *Service) Unseed(hash string) error
func (s *Service) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
func (s *Service) Status() []WorkloadSummary

// Stream dispatch
func (s *Service) HandleArtifactStream(stream transport.Stream, peer types.PeerKey)
func (s *Service) HandleWorkloadStream(stream transport.Stream, peer types.PeerKey)
```

**Event loop:**

- Scheduling ticker -> read snapshot -> pure `Evaluate()` -> claim/release via state
- React to workload spec changes -> fetch missing artifacts from peers
- Resource telemetry ticker -> report CPU/mem via state

#### Tunneling

Owns: service exposure, connection lifecycle, traffic tracking, multi-hop relay.

```go
package tunneling

type Service struct { ... }
func New(self types.PeerKey, services ServiceState, streams StreamTransport, router RoutingTable, opts ...Option) *Service

func (s *Service) Start(ctx context.Context) error
func (s *Service) Stop() error

// Control plane
func (s *Service) Connect(ctx context.Context, service, peer string) error
func (s *Service) Disconnect(service string) error
func (s *Service) ExposeService(port uint32, name string) error
func (s *Service) UnexposeService(name string) error
func (s *Service) ListConnections() []ConnectionInfo

// Stream dispatch
func (s *Service) HandleTunnelStream(stream transport.Stream, peer types.PeerKey)
func (s *Service) HandleRoutedStream(stream transport.Stream, peer types.PeerKey)
```

**Event loop:**

- Accept tunnel streams -> bridge to local port
- Accept routed streams -> look up next hop via `router.NextHop()` -> open stream to next hop -> bridge
- Traffic ticker -> record bytes via state

### Routing

Pure computation, no goroutines, no state:

```go
package routing

func Build(self types.PeerKey, topology []PeerTopology, connected []types.PeerKey) Table

type Table struct { ... }
func (t Table) NextHop(dest types.PeerKey) (next types.PeerKey, ok bool)
```

Rebuilt by supervisor whenever `TopologyChanged` events arrive from state. The new table is injected into tunneling via a setter or atomic swap.

### Supervisor

Composition root. Zero domain logic.

```go
package supervisor

type Supervisor struct { ... }
func New(cfg config.Config, creds auth.NodeCredentials) (*Supervisor, error)
func (s *Supervisor) Run(ctx context.Context) error
```

**Responsibilities:**

1. Creates state, transport, membership, placement, tunneling, routing, control.
2. Wires them together — passes consumer interfaces, no component knows its siblings.
3. Starts in order: state (load from disk) -> transport -> domain services -> control plane.
4. Runs the stream dispatch loop (routes accepted streams to domain services by type).
5. Reacts to `TopologyChanged` events -> rebuilds routing table -> injects into tunneling.
6. Shuts down in reverse: stop control -> stop domain services (drain) -> flush observability -> stop transport -> persist state to disk.
7. Handles OS signals (SIGTERM, SIGINT).

**Stream dispatch** — the one piece of domain-aware glue:

```go
go func() {
    for {
        stream, st, peer, err := transport.AcceptStream(ctx)
        switch st {
        case StreamTypeClock:
            membership.HandleClockStream(stream, peer)
        case StreamTypeCertRenewal:
            membership.HandleCertRenewalStream(stream, peer)
        case StreamTypeTunnel:
            tunneling.HandleTunnelStream(stream, peer)
        case StreamTypeRouted:
            tunneling.HandleRoutedStream(stream, peer)
        case StreamTypeArtifact:
            placement.HandleArtifactStream(stream, peer)
        case StreamTypeWorkload:
            placement.HandleWorkloadStream(stream, peer)
        }
    }
}()
```

**State persistence loop:**

```go
go func() {
    ticker := time.NewTicker(30 * time.Second)
    for {
        select {
        case <-ctx.Done():
            return
        case ev := <-membership.Events():
            // dispatch to routing, placement, tunneling as needed
        case <-ticker.C:
            writeStateToDisk(state.FullState())
        }
    }
}()
```

### Control Plane

gRPC translation layer. Each domain service exposes a narrow control interface:

```go
package control

type Server struct { ... }
func New(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, state StateReader) *Server
func (s *Server) Start(socketPath string) error
func (s *Server) Stop() error

type MembershipControl interface {
    DenyPeer(key types.PeerKey) error
    Invite(subject string) (string, error)
}

type PlacementControl interface {
    Seed(hash string, binary []byte) error
    Unseed(hash string) error
    Call(ctx context.Context, hash, fn string, input []byte) ([]byte, error)
    Status() []placement.WorkloadSummary
}

type TunnelingControl interface {
    Connect(ctx context.Context, service, peer string) error
    Disconnect(service string) error
    ExposeService(port uint32, name string) error
    UnexposeService(name string) error
    ListConnections() []tunneling.ConnectionInfo
}

type StateReader interface {
    Snapshot() state.Snapshot
}
```

Handlers are one-liners: unmarshal request -> call interface method -> marshal response -> map error to gRPC status code.

## Package Migration Map

| Current Package | Target Package | Change Type |
|----------------|---------------|-------------|
| `types` | `types` | unchanged |
| `auth` | `auth` | unchanged |
| `config` | `config` | unchanged |
| `nat` | `nat` | trim to enum only; detection moves to transport |
| `topology` | `coords` (math) + `membership` (peer selection) | split |
| `route` | `routing` | rename, simplified API |
| `store` | `state` | rename, remove I/O, add typed projections |
| `mesh` | `transport` | rename, remove routing/gossip awareness |
| `peer` | `transport` (internal) | fold in, unexport |
| `sock` | `transport` (internal) | fold in, unexport |
| `traffic` | `tunneling` (internal) | fold in |
| `tunnel` | `tunneling` | rename, absorb relay |
| `scheduler` | `placement` (internal) | fold in |
| `workload` | `placement` (internal) | fold in |
| `cas` | `cas` | unchanged |
| `wasm` | `wasm` | unchanged |
| `node` | `supervisor` + `membership` + `placement` + `tunneling` | split |
| `server` | `control` | rename, thin translation only |
| `observability` | `observability` | unchanged |
| `sysinfo` | `placement` (internal) or foundation | fold in or keep |
| `perm` | `config` (internal) or foundation | fold in or keep |
| `util` | foundation or domain services (internal) | `JitterTicker` moves to foundation; other helpers inline |
| `integration` | `integration` | unchanged (test harness) |
| `workspace` | `config` or `supervisor` | fold in |

## Migration Methodology

### Code Movement, Not Rewriting

Each migration step is primarily a **code-shovelling exercise**: copy/paste functioning production code into its new package, adapt signatures to match the new interfaces, and verify behavior is preserved. Implementations stay; signatures change. This is explicitly NOT a rewrite — the code that runs today runs tomorrow, just in a different package.

### Tests Are Never Removed

Tests are only **adapted or added**, never removed. If a test's signature breaks because an API moved, adapt the test to call the new API. If a migration step creates a new boundary, add tests for that boundary. The test count monotonically increases across the entire migration.

### No Behavioral Regressions

The following must remain identical across every migration step:

- **Status output** — columns, ordering, colors (ANSI codes), alignment, truncation
- **Logging verbosity** — every structured log line during startup, peer connect/disconnect, gossip sync, workload operations, cert renewal, shutdown (see `docs/behavioral-inventory.md`)
- **Metrics collection** — Prometheus metric names, label sets, types
- **Wire format** — stream type bytes, routed stream headers, QUIC config, proto field numbers
- **On-disk format** — directory layout, `state.pb` schema, `config.yaml`, key file formats, permissions
- **CLI surface** — all commands, flags, defaults

## Testing Strategy

### Test Tiers

| Tier | Scope | Method |
|------|-------|--------|
| Unit | Each package in isolation | State: pure data structure, zero mocks. Routing: pure function, zero mocks. Domain services: fake state + fake transport via consumer interfaces. |
| Integration | Multi-node cluster in-process | VirtualSwitch (UDP sim) -> real QUICTransport -> real state -> real domain services. TestNode, Builder, scenario assertions. |
| Hetzner | Real cluster on real hardware | Terraform + Ansible + verification script. Run at **every** verification stage, not just phase gates. |

### Verification Cycle

Every migration step follows this cycle — no exceptions:

```
1. Port code into target package (copy/paste, adapt signatures)
2. Port/adapt unit tests (signatures change, assertions don't)
3. Adapt integration tests to new APIs (NEVER remove, only adapt or add)
4. Add new integration tests for new boundaries introduced
5. Run mutation tests (see below)
6. Run full integration test suite
7. Run lint + race detector
8. Run Hetzner cluster verification
9. Commit only after all gates pass
```

### Mutation Testing Protocol

At each verification stage, every integration test must **earn its place**. For each test:

1. **Break the production code** that the test is supposed to guard — introduce a deliberate, targeted mutation (e.g., swap a condition, drop a message, return early, corrupt a field).
2. **Verify the test fails** with the mutation in place.
3. **Verify the test passes** with the mutation reverted.
4. If a test passes despite the mutation, the test is ineffective — fix the test to actually guard the behavior, then repeat.

This applies to both existing integration tests and newly added ones. A test that can't detect a real bug is worse than no test — it provides false confidence.

### What to Test at Each Boundary

When a package is extracted or a boundary is created:

- **Interface compliance** — does the concrete type satisfy the consumer-defined interface? (compile-time check via `var _ Interface = (*Concrete)(nil)`)
- **Event fidelity** — do the same state mutations produce the same events before and after the migration step?
- **Snapshot equivalence** — does `Snapshot()` return the same data before and after?
- **Wire compatibility** — does gossip between a pre-migration node and a post-migration node converge?
- **Behavioral equivalence** — for every integration scenario, does the observable behavior (logs, metrics, status output) remain identical?

### Hetzner Cluster Verification

The Hetzner verification script runs at every verification stage (not just phase gates). It validates:

- Nodes start with fully purged state
- Cluster formation (bootstrap SSH, invite flows, dynamic join)
- Convergence verification (Vivaldi error decreases, samples increase)
- Services (expose locally, tunnel remotely)
- Workloads (seed/unseed/call cycle, 1000 concurrent calls, multi-node success)
- Workload migration (down a seed-holding node, remaining cluster auto-recovers)

## Wiring Diagram

```
cmd/pln --gRPC--> control
                     |
                     | narrow interfaces
                     v
              supervisor.Run(ctx)
               |    |    |    |
         +-----+    |    |    +------+
         v          v    v           v
    membership  placement  tunneling  stream dispatch
         |          |        |  ^          |
         |          |        |  | routing  |
         |          |        |  |  .Build  |
         v          v        v  |          |
    +---------+  +------------+ |          |
    |  state  |  | transport  |<+----------+
    | (pure)  |  |  (QUIC)    |
    +----+----+  +-----+------+
         |             |
    +----+-------------+------+
    |      foundation         |
    | types auth coords nat   |
    | config cas wasm obs     |
    +-------------------------+
```
