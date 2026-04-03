# Architecture Specification

## Principles

1. **Optimal component separation** — each package has one clear purpose, communicates through well-defined interfaces, and can be understood and tested independently.
2. **Clean, minimal consumer interfaces** — each package exposes a narrow API; consumers define their own narrower subset interfaces.
3. **Agent independence** — different agents (AI or human) can work on a component without breaking others. A change inside one domain service touches zero files outside that package.
4. **Clean leaf dependencies** — foundation packages have zero or minimal internal dependencies.

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
                   typed projections: Peers, Services, Workloads
                   zero I/O, zero goroutines -- caller owns the thread

  transport        QUIC sessions, peer FSM, stream mux, NAT punch,
                   UDP sockets, hole-punching
                   knows nothing about gossip, workloads, or services
                   all operations are direct-peer only

Layer 1: Foundation (leaf packages, zero or minimal internal deps)
  types            PeerKey, Envelope
  auth             credentials, delegation certs, join tokens
  coords           Vivaldi math (pure functions: Update, Distance)
  nat              NAT type enum + detector (Easy, Hard, Unknown)
  plnfs            atomic writes, dir creation, pln group ownership
  config           YAML loading, defaults, persistence
  cas              content-addressable store (filesystem, SHA-256)
  wasm             WASM runtime (Extism/wazero wrapper)
  sysinfo          CPU/memory/numCPU sampling (gopsutil)
  observability    metrics, traces, logging (noop pattern)
```

### Dependency Rules

- Each layer may only import from layers below it.
- Siblings at the same layer never import each other.
- Consumer-defined interfaces at every boundary — each package defines the narrowest interface it needs from its dependencies.
- Value types cross boundaries, references don't. Exception: stream handles (`io.ReadWriteCloser`) are inherently reference-typed and are passed across boundaries by design. The stream is the boundary — once handed off, the receiver owns it.

### Interface Boundary Enforcement

Every component package declares two types of interfaces:

1. **Producer-side interface** — the complete public API (`StateStore`, `TransportAPI`, `MembershipAPI`, `PlacementAPI`, `TunnelingAPI`), declared in the producing package's main file with a compile-time assertion: `var _ Interface = (*ConcreteType)(nil)`.

2. **Consumer-side interfaces** — narrow subsets declared in each consuming package (`ClusterState`, `Network`, `WorkloadState`, `ServiceState`, `StreamTransport`, `RoutingTable`, etc.).

Supervisor stores all component references as interface types, never concrete types. Compile-time assertions in `pkg/supervisor/interface_check.go` verify that concrete types satisfy all consumer interfaces across every boundary. This means any component can be swapped by providing a different implementation of the same interface.

## Package APIs

### State

State is a pure data structure with zero I/O. No goroutines, no disk access, no network operations. Thread-safe via internal serialization (mutations are mutex-protected; `Snapshot()` is lock-free via `atomic.Pointer[Snapshot]`). Mutations buffer their gossip events in-memory and signal a notification channel so membership can eagerly push them without polling. The channel is a coordination primitive, not I/O — state never reads from it, only non-blocking-sends a signal.

**Producer-side interface** — the complete public API, declared in the state package. Consumer-defined interfaces (`ClusterState`, `WorkloadState`, `ServiceState`) are narrower subsets.

```go
package state

type StateStore interface {
    Snapshot() Snapshot
    ApplyDelta(from types.PeerKey, data []byte) ([]Event, []byte, error)
    EncodeDelta(since Clock) []byte
    EncodeFull() []byte
    PendingNotify() <-chan struct{}
    FlushPendingGossip() []*statev1.GossipEvent

    DenyPeer(key types.PeerKey) []Event
    SetLocalAddresses(addrs []netip.AddrPort) []Event
    SetLocalNAT(t nat.Type) []Event
    SetLocalCoord(c coords.Coord) []Event
    SetLocalReachable(peers []types.PeerKey) []Event
    SetLocalObservedExternalIP(ip string) []Event

    SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []Event
    ClaimWorkload(hash string) []Event
    ReleaseWorkload(hash string) []Event
    SetLocalResources(cpu, mem float64) []Event

    SetService(port uint32, name string) []Event
    RemoveService(name string) []Event
    SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event

    SetBootstrapPublic()
}

var _ StateStore = (*Store)(nil)

type Store struct { ... }
func New(self types.PeerKey) *Store
```

`ApplyDelta` returns three values: domain events, a rebroadcast payload (non-stale gossip events for eager propagation), and an error. Every mutation method enqueues gossip events internally and signals `PendingNotify()` so membership can flush and broadcast them immediately.

**Snapshot projections** — typed, read-only views off an immutable snapshot:

```go
func (s Snapshot) Peers() []PeerInfo
func (s Snapshot) Peer(key types.PeerKey) (PeerInfo, bool)
func (s Snapshot) Services() []ServiceInfo
func (s Snapshot) Workloads() []WorkloadInfo
func (s Snapshot) DeniedPeers() []types.PeerKey
func (s Snapshot) Self() types.PeerKey
func (s Snapshot) Clock() Clock
// Field: LocalID types.PeerKey
```

`PeerInfo` bundles: addresses, NAT type, Vivaldi coord, reachability set, resource telemetry, traffic heatmap.

**Events** — returned synchronously from mutations and `ApplyDelta`. The caller decides what to do with them:

```go
type Event interface { stateEvent() }  // sealed sum type

type PeerJoined      struct { Key types.PeerKey }
type PeerDenied      struct { Key types.PeerKey }
type ServiceChanged  struct { Name string; Peer types.PeerKey }
type WorkloadChanged struct { Hash string }
type TopologyChanged struct { Peer types.PeerKey }
type GossipApplied   struct {}
```

**Consumer interface pattern** — each domain service defines the narrowest interface it needs:

```go
// In package membership:
type ClusterState interface {
    Snapshot() state.Snapshot
    ApplyDelta(from types.PeerKey, data []byte) ([]state.Event, []byte, error)
    EncodeDelta(since state.Clock) []byte
    EncodeFull() []byte
    PendingNotify() <-chan struct{}
    FlushPendingGossip() []*statev1.GossipEvent
    DenyPeer(key types.PeerKey) []state.Event
    SetLocalAddresses([]netip.AddrPort) []state.Event
    SetLocalCoord(coords.Coord) []state.Event
    SetLocalNAT(nat.Type) []state.Event
    SetLocalReachable([]types.PeerKey) []state.Event
    SetLocalObservedExternalIP(string) []state.Event
}

// In package placement:
type WorkloadState interface {
    Snapshot() state.Snapshot
    SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []state.Event
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

Membership is the **sole caller** of `ApplyDelta` during steady-state operation (it owns the gossip lifecycle end-to-end: recv, merge, send). Supervisor calls `ApplyDelta` once during startup to hydrate from disk, before membership starts. `ApplyDelta` returns three values: domain events, a rebroadcast payload (the subset of incoming gossip events that were non-stale), and an error. The rebroadcast payload enables eager propagation — membership immediately forwards new events to other peers rather than waiting for the next gossip tick. The staleness filtering is a natural byproduct of CRDT resolution inside state, so returning it avoids duplicating that logic in membership. When `ApplyDelta` returns events, membership processes the ones it cares about (peer join/leave, topology changes for peer selection) and forwards all events on an output channel:

```go
// Membership exposes:
func (s *Service) Events() <-chan state.Event
```

Supervisor reads this channel and dispatches to other consumers:
- `PeerDenied` -> disconnect peer, clean up tunneling connections, forget peer
- `TopologyChanged` -> rebuild routing table, signal placement re-evaluation
- `WorkloadChanged` -> signal placement re-evaluation
- `ServiceChanged` -> tunneling reconciles via periodic snapshot poll

This makes membership the single gossip writer and supervisor the event router. No other domain service calls `ApplyDelta` or `Recv`. Placement reacts to supervisor `Signal()` calls and its own reconciliation ticker. Tunneling reconciles desired connections on its own ticker.

### State Persistence

State has zero I/O — it doesn't read from or write to disk. Persistence is supervisor's responsibility:
- **On startup:** supervisor reads the state file from disk, calls `state.ApplyDelta()` to hydrate.
- **On shutdown:** supervisor calls `state.EncodeFull()` and writes it to disk.
- **Periodically:** supervisor snapshots state to disk on a timer (e.g., every 30s).

### Multi-Hop Relay and Routed Streams

Transport handles routed stream forwarding internally. When it accepts a `StreamTypeRouted` stream, it reads the route header (destination peer key + TTL), checks if the destination is local, and either delivers the inner stream via `AcceptStream` or forwards to the next hop. Callers of `AcceptStream` never see `StreamTypeRouted` — they receive the inner stream type transparently.

Supervisor provides a **stream open adapter** (internal to the supervisor package) that wraps transport and adapts `OpenStream` to return `io.ReadWriteCloser` (satisfying tunneling's `StreamTransport` interface):

```go
type streamOpenAdapter struct {
    transport Transport
}

func (a *streamOpenAdapter) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
    return a.transport.OpenStream(ctx, peer, st)
}
```

Tunneling and placement receive this adapter instead of raw transport.

### Datagram Protocol

Datagrams (`Send`/`Recv`) carry gossip events, cert renewal request/response, observed address notifications, and punch coordination — all via `Envelope` proto oneof variants. Membership is the sole consumer of `Recv()` and demultiplexes internally. `StreamTypeCertRenewal` exists as a stream type constant for future migration of cert renewal to streams.

### Transport

Transport handles QUIC connections, stream multiplexing, and NAT traversal. Data bytes are opaque. Transport also handles routed stream forwarding internally — when an accepted stream is `StreamTypeRouted`, transport reads the route header, forwards across hops, and delivers the inner stream type via the accept channel. Callers never see `StreamTypeRouted`.

**Producer-side interface** — the complete public API:

```go
package transport

type Transport interface {
    Start(ctx context.Context) error
    Stop() error

    Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
    PeerEvents() <-chan PeerEvent
    SupervisorEvents() <-chan PeerEvent
    ConnectedPeers() []types.PeerKey
    GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
    IsOutbound(peer types.PeerKey) bool

    Send(ctx context.Context, peer types.PeerKey, data []byte) error
    Recv(ctx context.Context) (Packet, error)

    OpenStream(ctx context.Context, peer types.PeerKey, st StreamType) (Stream, error)
    AcceptStream(ctx context.Context) (Stream, StreamType, types.PeerKey, error)

    DiscoverPeer(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool)
    ForgetPeer(pk types.PeerKey)
    ConnectFailed(pk types.PeerKey)
    IsPeerConnected(pk types.PeerKey) bool
    IsPeerConnecting(pk types.PeerKey) bool
    ClosePeerSession(peer types.PeerKey, reason DisconnectReason)

    Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error

    ListenPort() int
    GetConn(peer types.PeerKey) (*quic.Conn, bool)
    PeerStateCounts() PeerStateCounts

    UpdateMeshCert(cert tls.Certificate)
    RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
    PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)

    JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)
}

var _ Transport = (*QUICTransport)(nil)
```

The concrete type has additional methods beyond the interface (e.g. construction-time setters like `SetPeerMetrics`, internal FSM methods like `HasPeer`/`RetryPeer`/`MarkPeerConnected`) that are not part of the public API. Each consumer package defines its own narrow subset of `Transport` — see consumer-side interfaces below.

**Stream type constants:**

```go
StreamTypeDigest        StreamType = 1
StreamTypeTunnel        StreamType = 2
StreamTypeRouted        StreamType = 3  // handled internally by transport
StreamTypeArtifact      StreamType = 4
StreamTypeWorkload      StreamType = 5
StreamTypeCertRenewal   StreamType = 6
```

**Internal concerns** (not exposed via `TransportAPI`):

- QUIC session management (quic-go)
- Peer connection FSM (connecting -> connected -> backoff -> reconnecting)
- NAT detection (UDP probes)
- Hole-punching — `Connect` handles punch coordination internally
- TLS mutual auth (using auth.NodeCredentials)
- Routed stream forwarding — multi-hop relay with TTL
- Cert renewal request/response via datagrams
- Join/invite flows

**Design decisions:**

1. **Routed streams are transparent.** Transport intercepts `StreamTypeRouted` internally, reads the route header, forwards across hops, and delivers the final-destination stream to `AcceptStream` as the inner stream type. Callers never see `StreamTypeRouted`.
2. **Stream types are opaque to callers.** Transport reads/writes the `StreamType` byte for framing but doesn't interpret it beyond routing. Constants are defined in transport as wire protocol values; semantics belong to the domain services.
3. **No gossip awareness.** `Send`/`Recv` move opaque byte slices.
4. **Connect is caller-driven.** Membership's peer selection algorithm decides who to connect to; transport executes.

**Consumer interface examples:**

```go
// In package membership:
type Network interface {
    Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
    Disconnect(key types.PeerKey) error
    Send(ctx context.Context, peer types.PeerKey, data []byte) error
    Recv(ctx context.Context) (transport.Packet, error)
    ConnectedPeers() []types.PeerKey
    PeerEvents() <-chan transport.PeerEvent
}

// Membership also defines narrow interfaces for supplementary transport features:
type StreamOpener interface {
    OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (transport.Stream, error)
}
type RTTSource interface {
    GetConn(peer types.PeerKey) (*quic.Conn, bool)
}
type CertManager interface {
    UpdateMeshCert(cert tls.Certificate)
    RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
    PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)
}
type PeerAddressSource interface {
    GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
}
type PeerSessionCloser interface {
    ClosePeerSession(peer types.PeerKey, reason string)
}

// In package tunneling:
type StreamTransport interface {
    OpenStream(ctx context.Context, peerID types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}
```

**Type details:**

```go
type StreamType byte
type PeerEventType int // Connected | Disconnected

type Packet    struct { From types.PeerKey; Data []byte }
type PeerEvent struct { Key types.PeerKey; Type PeerEventType; Addrs []netip.AddrPort }

type PeerStateCounts struct {
    Connected  uint32
    Connecting uint32
    Backoff    uint32
}
```

### Domain Services

Each domain service owns its own event loop, runs in its own goroutine, and communicates with state and transport through narrow consumer-defined interfaces. Siblings never import each other.

#### Membership

Owns: join/deny, gossip schedule, peer selection, Vivaldi exchange, cert renewal, NAT tracking, address refresh.

```go
package membership

type MembershipAPI interface {
    Start(ctx context.Context) error
    Stop() error

    DenyPeer(key types.PeerKey) error
    Invite(subject string) (string, error)

    HandleDigestStream(ctx context.Context, stream transport.Stream, peer types.PeerKey)
    HandleCertRenewalStream(stream transport.Stream, peer types.PeerKey)

    Events() <-chan state.Event
    ControlMetrics() ControlMetrics
}

var _ MembershipAPI = (*Service)(nil)

type Service struct { ... }
func New(self types.PeerKey, creds auth.NodeCredentials, net Network, cluster ClusterState, opts ...Option) *Service
```

`ControlMetrics()` returns a struct with Vivaldi coordinates, smoothed error, sample counts, eager sync counts, and cert expiry.

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

type PlacementAPI interface {
    Start(ctx context.Context) error
    Stop() error

    Seed(hash string, binary []byte, replicas, memoryPages, timeoutMs uint32) error
    Unseed(hash string) error
    Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
    Status() []WorkloadSummary

    HandleArtifactStream(stream io.ReadWriteCloser, peer types.PeerKey)
    HandleWorkloadStream(stream io.ReadWriteCloser, peer types.PeerKey)

    Signal()
}

var _ PlacementAPI = (*Service)(nil)

type Service struct { ... }
func New(self types.PeerKey, workloads WorkloadState, cas cas.Store, runtime wasm.Runtime, opts ...Option) *Service
```

`Signal()` triggers immediate scheduling re-evaluation. Supervisor calls it on `WorkloadChanged` and `TopologyChanged` events to avoid waiting for the reconciliation poll ticker.

**Event loop:**

- Reconciliation ticker (+ `Signal()` trigger) -> read snapshot -> pure `evaluate()` -> claim/release via state
- React to workload spec changes -> fetch missing artifacts from peers
- Resource telemetry ticker -> report CPU/mem via state

#### Tunneling

Owns: service exposure, connection lifecycle, traffic tracking, desired connection management.

```go
package tunneling

type TunnelingAPI interface {
    Start(ctx context.Context) error
    Stop() error

    Connect(ctx context.Context, service, peer string) error
    Disconnect(service string) error
    ExposeService(port uint32, name string) error
    UnexposeService(name string) error
    ListConnections() []ConnectionInfo

    HandleTunnelStream(stream transport.Stream, peer types.PeerKey)
    HandleRoutedStream(stream transport.Stream, peer types.PeerKey)

    HandlePeerDenied(peerID types.PeerKey)
    DesiredPeers() []types.PeerKey
    SeedDesiredConnection(pk types.PeerKey, remotePort, localPort uint32)
    ListDesiredConnections() []ConnectionInfo
    TrafficRecorder() transport.TrafficRecorder
}

var _ TunnelingAPI = (*Service)(nil)

type Service struct { ... }
func New(self types.PeerKey, services ServiceState, streams StreamTransport, router RoutingTable, opts ...Option) *Service
```

`HandlePeerDenied` combines disconnect + connection cleanup into one call for supervisor's deny handler. `DesiredPeers` returns peer keys from desired connections for topology target building. `TrafficRecorder` exposes the per-peer byte tracker for transport to call into.

**Event loop:**

- Accept tunnel streams -> bridge to local port
- Accept routed streams -> dispatch to local handler
- Reconcile desired connections on ticker
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

Composition root. Zero domain logic. All component fields are interface-typed — no concrete component type crosses a package boundary. Any component can be swapped by providing a different implementation of the same interface.

```go
package supervisor

type Supervisor struct {
    store      state.StateStore
    membership membership.MembershipAPI
    placement  placement.PlacementAPI
    tunneling  tunneling.TunnelingAPI
    mesh       transport.Transport
    // ...
}

func New(opts Options, creds *auth.NodeCredentials, inviteConsumer auth.InviteConsumer) (*Supervisor, error)
func (s *Supervisor) Run(ctx context.Context) error
```

**Responsibilities:**

1. Creates state, transport, membership, placement, tunneling, routing, control.
2. Wires them together — passes consumer interfaces, no component knows its siblings.
3. Starts in order: state (load from disk) -> transport -> domain services -> control plane.
4. Runs the stream dispatch loop (routes accepted streams to domain services by type).
5. Dispatches events from `membership.Events()`: `TopologyChanged` -> rebuild routing + signal placement, `WorkloadChanged` -> signal placement, `PeerDenied` -> disconnect + cleanup.
6. Shuts down in reverse: stop control -> stop domain services (drain) -> flush observability -> stop transport -> persist state to disk.
7. Handles OS signals (SIGTERM, SIGINT).

**Stream dispatch** — transport intercepts `StreamTypeRouted` internally and delivers the inner stream type. Supervisor never sees routed streams:

```go
switch st {
case StreamTypeDigest:
    go membership.HandleDigestStream(ctx, stream, peer)
case StreamTypeCertRenewal:
    go membership.HandleCertRenewalStream(stream, peer)
case StreamTypeTunnel:
    tunneling.HandleTunnelStream(stream, peer)
case StreamTypeArtifact:
    go placement.HandleArtifactStream(stream, peer)
case StreamTypeWorkload:
    go placement.HandleWorkloadStream(stream, peer)
}
```

**Main event loop** — combines event dispatch with periodic state persistence:

```go
for {
    select {
    case <-ctx.Done():
        return
    case ev := <-membership.Events():
        dispatchEvents(ev)  // routing, placement, tunneling
    case <-saveTicker.C:
        writeStateToDisk(state.EncodeFull())
    }
}
```

### Control Plane

gRPC translation layer. The four core domain interfaces define the primary control surface. Optional narrow interfaces provide supplementary data for status/metrics RPCs and operational commands.

```go
package control

type Service struct { ... }
func NewService(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, state StateReader, opts ...Option) *Service
func (s *Service) Start(socketPath string) error
func (s *Service) Stop() error
```

**Core domain interfaces** — the primary control surface. Each domain service implements exactly one:

```go
type MembershipControl interface {
    DenyPeer(key types.PeerKey) error
    Invite(subject string) (string, error)
}

type PlacementControl interface {
    Seed(hash string, binary []byte, replicas, memoryPages, timeoutMs uint32) error
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

**Supplementary interfaces** — injected via options. These provide data for status/metrics RPCs and operational commands that require information beyond what the four core interfaces expose:

```go
type TransportInfo interface {
    PeerStateCounts() PeerStateCounts
    GetActivePeerAddress(types.PeerKey) (*net.UDPAddr, bool)
    PeerRTT(types.PeerKey) (time.Duration, bool)
    ReconnectWindowDuration() time.Duration
}

type MetricsSource interface {
    ControlMetrics() Metrics
}

type MeshConnector interface {
    Connect(ctx context.Context, pk types.PeerKey, addrs []netip.AddrPort) error
}
```

**Options** — dependency injection for supplementary interfaces and operational hooks:

```go
type Option func(*Service)

func WithShutdown(fn func()) Option
func WithCredentials(c *auth.NodeCredentials) Option
func WithTransportInfo(t TransportInfo) Option
func WithMetricsSource(m MetricsSource) Option
func WithMeshConnector(c MeshConnector) Option
```

**Metrics** — aggregated node metrics returned by the GetMetrics RPC:

```go
type Metrics struct {
    CertExpirySeconds  float64
    CertRenewals       uint64
    CertRenewalsFailed uint64
    PunchAttempts      uint64
    PunchFailures      uint64
    GossipApplied      uint64
    GossipStale        uint64
    SmoothedVivaldiErr float64
    VivaldiSamples     uint64
    EagerSyncs         uint64
    EagerSyncFailures  uint64
}

type PeerStateCounts struct {
    Connected  uint32
    Connecting uint32
    Backoff    uint32
}
```

Handlers are one-liners: unmarshal request -> call interface method -> marshal response -> map error to gRPC status code.

## Invariants

The following must remain stable across all changes:

- **Status output** — columns, ordering, colors (ANSI codes), alignment, truncation
- **Logging verbosity** — every structured log line during startup, peer connect/disconnect, gossip sync, workload operations, cert renewal, shutdown
- **Metrics collection** — OTel metric names (dot-separated `pollen.*` convention), label sets, types
- **Wire format** — stream type bytes, routed stream headers, QUIC config, proto field numbers
- **On-disk format** — directory layout, `state.pb` schema, `config.yaml`, key file formats, permissions
- **CLI surface** — all commands, flags, defaults

## Testing Strategy

### Test Tiers

| Tier | Scope | Method |
|------|-------|--------|
| Unit | Each package in isolation | State: pure data structure, zero mocks. Routing: pure function, zero mocks. Domain services: fake state + fake transport via consumer interfaces. |
| Integration | Multi-node cluster in-process | VirtualSwitch (UDP sim) -> real QUICTransport -> real state -> real domain services. TestNode, Builder, scenario assertions. |
| Hetzner | Real cluster on real hardware | Terraform + Ansible + verification script. |

### Boundary Testing

Every package boundary must verify:

- **Interface compliance** — does the concrete type satisfy the consumer-defined interface? (compile-time check via `var _ Interface = (*Concrete)(nil)`)
- **Event fidelity** — do the same state mutations produce the same events?
- **Snapshot equivalence** — does `Snapshot()` return the expected data?
- **Wire compatibility** — does gossip between nodes converge?
- **Behavioral equivalence** — does the observable behavior (logs, metrics, status output) remain correct?

### Mutation Testing

Every integration test must **earn its place**:

1. **Break the production code** that the test guards — introduce a deliberate, targeted mutation (e.g., swap a condition, drop a message, return early, corrupt a field).
2. **Verify the test fails** with the mutation in place.
3. **Verify the test passes** with the mutation reverted.
4. If a test passes despite the mutation, the test is ineffective — fix it.

A test that can't detect a real bug is worse than no test — it provides false confidence.

### Hetzner Cluster Verification

The Hetzner verification script validates:

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
