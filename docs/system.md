# Pollen System Overview

## What is Pollen?

Pollen is a decentralized mesh-networking platform that connects nodes into a peer-to-peer overlay network with automatic NAT traversal, cryptographic cluster membership, and distributed workload execution. Nodes discover each other through gossip-based state synchronization, establish direct QUIC connections (with UDP hole-punching for NAT'd hosts), and maintain a Vivaldi coordinate system for latency-aware topology management. The system supports TCP service tunneling over the mesh, content-addressed WASM workload distribution, and a distributed scheduler that places workloads across the cluster based on capacity, traffic affinity, and network proximity.

The architecture follows a layered composition model. Foundation packages (`types`, `nat`, `perm`, `cas`, `topology`, `traffic`) provide pure value types, algorithms, and utilities with zero or minimal internal dependencies. Infrastructure packages (`store`, `mesh`, `peer`, `sock`, `auth`) implement the core subsystems: CRDT gossip state, QUIC transport, peer lifecycle state machines, NAT hole-punching, and cryptographic trust chains. Middleware packages (`scheduler`, `tunnel`, `route`, `workload`, `wasm`) compose infrastructure into business-logic features: distributed placement, TCP forwarding, multi-hop routing, and WASM execution. Integration packages (`node`, `server`, `cmd/pln`) wire everything together: `node` is the central orchestrator with the main event loop, `server` exposes a gRPC control plane, and `cmd/pln` provides the CLI entry point.

Data flows through three primary paths. Gossip state (network info, services, Vivaldi coordinates, workload specs/claims, traffic heatmaps) propagates via CRDT delta-sync over QUIC datagrams, with per-attribute Lamport counters and tombstones for conflict resolution. Tunnel traffic flows from local TCP listeners through mesh QUIC streams to remote service endpoints, with bidirectional byte-counting for traffic-aware scheduling. Workload artifacts flow from the seeding node through content-addressed storage, are fetched over mesh artifact streams by claiming nodes, compiled via the Extism/wazero WASM runtime, and invoked either locally or remotely through mesh workload streams.

## Package Dependency Graph

```mermaid
graph TD
    subgraph Foundation
        types["types"]
        nat["nat"]
        perm["perm"]
        cas["cas"]
        sysinfo["sysinfo"]
        util["util"]
        workspace["workspace"]
        logging["observability/logging"]
    end

    subgraph Infrastructure
        metrics["observability/metrics"]
        traces["observability/traces"]
        topology["topology"]
        traffic["traffic"]
        peer["peer"]
        sock["sock"]
        auth["auth"]
        config["config"]
        store["store"]
        mesh["mesh"]
    end

    subgraph Middleware
        route["route"]
        tunnel["tunnel"]
        wasm["wasm"]
        workload["workload"]
        scheduler["scheduler"]
    end

    subgraph Integration
        node["node"]
        server["server"]
        pln["cmd/pln"]
    end

    %% Foundation dependencies (none internal)

    %% Infrastructure dependencies
    topology --> types
    topology --> nat
    traffic --> types
    peer --> types
    peer --> metrics
    sock --> nat
    auth --> perm
    config --> perm
    store --> types
    store --> nat
    store --> perm
    store --> auth
    store --> metrics
    store --> topology
    mesh --> types
    mesh --> nat
    mesh --> sock
    mesh --> peer
    mesh --> auth
    mesh --> metrics
    mesh --> traces
    mesh --> traffic
    mesh --> config

    %% Middleware dependencies
    route --> types
    route --> topology
    tunnel --> types
    tunnel --> traffic
    wasm -.-> |no internal deps| wasm
    workload --> cas
    workload --> wasm
    scheduler --> types
    scheduler --> topology
    scheduler --> store
    scheduler --> wasm
    scheduler --> workload

    %% Integration dependencies
    node --> types
    node --> nat
    node --> perm
    node --> cas
    node --> sysinfo
    node --> util
    node --> metrics
    node --> traces
    node --> topology
    node --> traffic
    node --> peer
    node --> auth
    node --> store
    node --> mesh
    node --> route
    node --> tunnel
    node --> wasm
    node --> workload
    node --> scheduler
    server --> node
    server --> perm
    pln --> types
    pln --> auth
    pln --> config
    pln --> node
    pln --> server
    pln --> store
    pln --> peer
    pln --> mesh
    pln --> logging
    pln --> workspace
```

## Data Flow Diagrams

### Gossip Flow: Network Receipt -> Store -> Rebroadcast

```mermaid
sequenceDiagram
    participant Remote as Remote Peer
    participant QUIC as mesh (QUIC Datagram)
    participant RecvLoop as node.recvLoop
    participant MainLoop as node.Start (main loop)
    participant Store as store.Store (CRDT)
    participant GossipCh as node.gossipEvents chan
    participant Broadcast as node.broadcastGossipBatches

    Remote->>QUIC: SendDatagram(Envelope)
    QUIC->>RecvLoop: Recv() -> Packet
    RecvLoop->>MainLoop: recvCh (buffered 256)
    MainLoop->>MainLoop: handleDatagram()

    alt GossipEventBatch
        MainLoop->>Store: ApplyEvents(events, isPullResponse)
        Store->>Store: Per-attribute Lamport counter check
        Store->>Store: Merge winners into nodeRecord
        Store-->>Store: Fire callbacks (onRouteInvalidate, onWorkloadChange, etc.)
        Store-->>MainLoop: ApplyResult.Rebroadcast []GossipEvent
        MainLoop->>GossipCh: queueGossipEvents(rebroadcast)
    end

    alt GossipStateDigest (clock)
        MainLoop->>Store: MissingFor(digest)
        Store-->>MainLoop: []GossipEvent (delta)
        MainLoop->>Remote: Send(GossipEventBatch with deltas)
    end

    Note over MainLoop,GossipCh: Periodic gossip tick
    MainLoop->>Store: Clock()
    Store-->>MainLoop: GossipStateDigest
    MainLoop->>Remote: Send clock via OpenClockStream to each peer

    GossipCh->>MainLoop: events := <-gossipEvents (drain all pending)
    MainLoop->>Broadcast: broadcastGossipBatches(events)
    Broadcast->>Remote: mesh.Send(GossipEventBatch) to each connected peer
```

### Tunnel Flow: CLI Connect -> Node -> Mesh -> Remote Peer

```mermaid
sequenceDiagram
    participant CLI as pln connect
    participant gRPC as NodeService (gRPC)
    participant Node as node.Node
    participant TunMgr as tunnel.Manager
    participant TCPLn as Local TCP Listener
    participant Mesh as mesh.Mesh (QUIC)
    participant RemotePeer as Remote Peer
    participant RemoteTun as Remote tunnel.Manager
    participant LocalSvc as Remote Local Service

    CLI->>gRPC: ConnectService(service, peer, remotePort, localPort)
    gRPC->>Node: ConnectService(peerID, remotePort, localPort)
    Node->>TunMgr: ConnectService(peerID, remotePort, localPort)
    TunMgr->>TCPLn: net.Listen("tcp", ":localPort")
    TunMgr-->>CLI: bound localPort

    Note over TCPLn: User connects to localhost:localPort
    TCPLn->>TunMgr: Accept() -> net.Conn
    TunMgr->>Mesh: OpenStream(ctx, peerID)
    Mesh->>Mesh: Session lookup or wait/route
    Mesh->>RemotePeer: QUIC BidiStream (type byte: tunnel)
    RemotePeer->>RemoteTun: AcceptStream() -> (peerKey, stream)
    RemoteTun->>RemoteTun: Read 4-byte port header
    RemoteTun->>LocalSvc: net.Dial("tcp", "localhost:remotePort")

    Note over TunMgr,RemoteTun: Bidirectional bridge (64KB pooled buffers)
    TunMgr->>TunMgr: bridge(tcpConn, meshStream)
    RemoteTun->>RemoteTun: bridge(meshStream, localConn)

    Note over TunMgr,RemoteTun: Half-close propagation via CloseWrite
```

### Workload Flow: Spec Seed -> Gossip -> Scheduler -> Claim -> Fetch -> Execute

```mermaid
sequenceDiagram
    participant CLI as pln seed
    participant gRPC as NodeService
    participant WM as workload.Manager
    participant CAS as cas.Store
    participant WASM as wasm.Runtime
    participant Store as store.Store (CRDT)
    participant Gossip as Gossip Broadcast
    participant Sched as scheduler.Reconciler
    participant Fetcher as scheduler.meshFetcher
    participant RemoteCAS as Remote CAS (via mesh)

    CLI->>gRPC: SeedWorkload(wasmBytes, config)
    gRPC->>WM: Seed(wasmBytes, config)
    WM->>CAS: Put(wasmBytes) -> hash
    WM->>WASM: Compile(wasmBytes, hash, config)
    WM-->>gRPC: hash

    gRPC->>Store: SetLocalWorkloadSpec(hash, replicas, memPages, timeoutMs)
    Store->>Store: Create GossipEvent with Lamport counter
    Store-->>gRPC: []GossipEvent
    gRPC->>Gossip: queueGossipEvents(events)
    Gossip->>Gossip: Broadcast spec to all connected peers

    Note over Store,Sched: OnWorkloadChange callback fires
    Store-->>Sched: Signal()

    Note over Sched: Debounced reconciliation (200ms / 2s max)
    Sched->>Sched: Run loop wakes on triggerCh
    Sched->>Store: AllWorkloadSpecs(), AllWorkloadClaims(), AllPeerKeys(), AllNodePlacementStates()
    Sched->>Sched: Evaluate(localID, peers, specs, claims, cluster, isRunning)

    Note over Sched: Pure deterministic scoring
    Sched->>Sched: suitabilityScore(capacity + traffic + Vivaldi)
    Sched->>Sched: Sorted peer list -> top N = target claimants

    alt ActionClaim (this node should run workload)
        Sched->>Sched: startClaim(hash) -> background goroutine

        alt Artifact not in local CAS
            Sched->>Fetcher: Fetch(ctx, hash, peersThatHaveIt)
            Fetcher->>RemoteCAS: OpenArtifactStream(ctx, peer)
            RemoteCAS-->>Fetcher: WASM bytes over stream
            Fetcher->>CAS: Put(wasmBytes)
        end

        Sched->>WM: SeedFromCAS(hash, config)
        WM->>CAS: Get(hash) -> wasmBytes
        WM->>WASM: Compile(wasmBytes, hash, config)

        Sched->>Store: SetLocalWorkloadClaim(hash, true)
        Store-->>Sched: []GossipEvent
        Sched->>Gossip: publish(events)
    end

    alt ActionRelease (this node should stop workload)
        Sched->>WM: Unseed(hash)
        WM->>WASM: DropCompiled(hash)
        Sched->>Store: SetLocalWorkloadClaim(hash, false)
        Store-->>Sched: []GossipEvent
        Sched->>Gossip: publish(events)
    end
```

## Package Tier Table

| Tier | Package | Production LOC | Test LOC | Role |
|------|---------|---------------|----------|------|
| **Foundation** | types | 53 | 0 | Core `PeerKey` identity type (32-byte ed25519 public key) |
| **Foundation** | nat | 110 | 129 | NAT type classification (Easy/Hard) from observed addresses |
| **Foundation** | perm | 195 | 196 | File/directory permissions with Linux pln-group enforcement |
| **Foundation** | cas | 88 | 67 | Content-addressable filesystem store for WASM artifacts |
| **Foundation** | sysinfo | 23 | 0 | System resource sampling (CPU, memory) via gopsutil |
| **Foundation** | util | 66 | 0 | Jittered ticker for gossip interval randomization |
| **Foundation** | workspace | 26 | 0 | Data directory creation with permission error hints |
| **Foundation** | observability/logging | 23 | 0 | Global zap logger initialization (dead code) |
| **Infrastructure** | observability/metrics | 462 | 242 | Atomic counters, gauges, EWMA, pluggable sink flush loop |
| **Infrastructure** | observability/traces | 212 | 106 | Minimal distributed tracing with TraceID/SpanID propagation |
| **Infrastructure** | topology | 560 | 818 | Vivaldi coordinates and topology-aware peer selection (3-layer budget) |
| **Infrastructure** | traffic | 203 | 158 | Per-peer byte-counting with sliding-window ring buffer |
| **Infrastructure** | peer | 561 | 611 | Deterministic peer connection lifecycle state machine |
| **Infrastructure** | sock | 339 | 182 | UDP NAT hole-punching with nonce-based probe protocol |
| **Infrastructure** | auth | 1107 | 700 | Ed25519 delegation certs, join/invite tokens, credential persistence |
| **Infrastructure** | config | 399 | 150 | YAML config persistence for bootstrap peers, services, connections |
| **Infrastructure** | store | 2537 | 2892 | CRDT gossip state store with per-attribute Lamport counters |
| **Infrastructure** | mesh | 2122 | 1008 | QUIC peer connections, datagram/stream transport, TLS identity |
| **Middleware** | route | 190 | 194 | Dijkstra shortest-path routing over Vivaldi-weighted graph |
| **Middleware** | tunnel | 463 | 0 | TCP-over-mesh service tunneling with bidirectional bridging |
| **Middleware** | wasm | 310 | 192 | Extism/wazero WASM compilation cache and execution engine |
| **Middleware** | workload | 229 | 117 | Workload lifecycle orchestration (seed/call/unseed) |
| **Middleware** | scheduler | 1116 | 1206 | Distributed placement scoring, debounced reconciler, artifact fetch |
| **Integration** | node | 2513 | 1398 | Central orchestrator: main event loop, all subsystem coordination |
| **Integration** | server | 69 | 0 | gRPC Unix-socket transport for control plane |
| **Integration** | cmd/pln | 3126 | 361 | CLI entry point: all `pln` subcommands and daemon lifecycle |

### Tier Summary

| Tier | Packages | Total Prod LOC | Total Test LOC |
|------|----------|---------------|----------------|
| Foundation | 8 | 584 | 392 |
| Infrastructure | 10 | 8,502 | 6,867 |
| Middleware | 5 | 2,308 | 1,709 |
| Integration | 3 | 5,708 | 1,759 |
| **Total** | **26** | **17,102** | **10,727** |
