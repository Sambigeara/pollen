# Pollen Architecture & Orchestration Model

**Vision:** A decentralized, local-first WebAssembly (WASM) runtime and orchestration layer, operating over a self-forming, zero-trust QUIC mesh network. 

Pollen replaces centralized orchestrators (like Kubernetes) and external artifact registries with emergent swarm intelligence. Nodes independently calculate network topography and pull workloads based on local deterministic rules.

## 1. Network Topology & Control Plane
Pollen utilizes a **Sparse "Small World" Mesh** driven by an eventually consistent CRDT state.
* **Minimal Persistent Gossip:** Nodes maintain a low number of persistent connections for the CRDT control plane to prevent gossip floods.
* **Anti-Partitioning Rules:** Nodes are mandated to maintain `N` connections across network boundaries (e.g., different subnets or high-latency peers) to bridge local islands into a global mesh, preventing split-brain partitions.
* **Lazy Data Plane:** Direct, high-bandwidth connections for intra-WASM RPC are punched on-demand (lazily) and torn down when idle.
* **Latency Awareness (Vivaldi):** Nodes do not ping the entire cluster. Instead, they maintain a geometric network coordinate (e.g., Vivaldi coordinates) updated via standard gossip. This allows any node to calculate the estimated latency to any other node mathematically, without active probing.

## 2. Workload Orchestration (The "Pull" Model)
Pollen eliminates the centralized scheduler. Workload placement is an emergent property of nodes acting in their own self-interest.
* **State Declaration:** The desired state (e.g., "Run 3 replicas of Workload X") is injected into the gossiped CRDT.
* **Local Evaluation:** Nodes continuously evaluate the CRDT against a hierarchy of local rules:
  1. *Capacity:* Do I have available CPU/Memory?
  2. *Caching:* Do I already have this WASM binary on disk?
  3. *Proximity:* Am I geometrically close (via Vivaldi coordinates) to the source of the traffic demand?
* **Workload Claiming:** Nodes that score highly modify the CRDT to claim the workload. If conflicts occur (too many nodes claim the same replica), deterministic conflict resolution (e.g., lowest Peer ID or highest compute score) forces the losers to gracefully terminate their instances.

## 3. Execution Environment
* **Pure Go WebAssembly (`wazero`):** Workloads are executed using `wazero`, ensuring Pollen remains a single, CGO-free, statically compiled binary capable of running on highly constrained edge devices.
* **Transparent Intra-WASM RPC:** WASM modules are abstracted away from network complexity. They communicate via simple host functions, while the Pollen daemon seamlessly routes the RPCs over the multiplexed, mTLS-encrypted QUIC mesh.

## 4. Artifact Distribution (P2P CAS)
Pollen nodes act as an implicit, peer-to-peer Content Delivery Network (CDN) for their own executable code.
* **Content-Addressable Storage (CAS):** Workloads are requested strictly by their cryptographic hash (SHA-256), providing mathematical proof of integrity upon download.
* **Implicit Registries:** Nodes with excess storage implicitly act as artifact registries by caching the WASM binaries they run or observe.
* **P2P Retrieval:** When a node claims a workload it doesn't have, it requests the binary from the cluster.
* **Direct vs. Relay Transfer:** Binaries are pulled via direct QUIC streams from the closest caching peer. If symmetric NATs block a direct connection, the transfer is automatically routed over the shortest relay path within the mesh. *(Note: While chunked, multi-peer pulling is supported in theory, whole-file transfers are prioritized for V1 due to the naturally small footprint of WASM modules).*

## 5. WASM Host Integration & Resource Telemetry

Pollen acts as a highly specialized, self-aware WebAssembly host, providing a custom Application Binary Interface (ABI) and strict resource boundaries.
* **Custom Host Functions:** Rather than relying on standard WASI sockets (which require complex, heavy networking logic inside the module), Pollen injects custom host functions. WASM modules simply execute high-level RPC calls, and the Pollen host seamlessly bridges them into the underlying QUIC mesh.
* **Telemetry-Driven Bidding:** The daemon continuously monitors real-time system health (CPU load, available memory) using cross-platform, pure-Go telemetry (e.g., `gopsutil`).
* **Suitability Scoring:** When evaluating the CRDT for new workload requests, nodes calculate a local "Suitability Score" combining their dynamic hardware availability, geographic proximity, and local artifact cache.
* **Resource Sandboxing:** To protect the host routing daemon from rogue workloads, `wazero` enforces strict context-cancellation timeouts and memory allocation limits on every executing module, preventing noisy-neighbor and host OOM scenarios.

## 6. Package Architecture

```
cmd/pln              CLI (Cobra commands, flag parsing, output formatting)
pkg/control          gRPC translation layer (CLI <-> domain services)
pkg/supervisor       lifecycle, shutdown ordering, event loop, stream dispatch

pkg/membership       gossip schedule, Vivaldi exchange, cert renewal, NAT, IP refresh
pkg/placement        workload scheduling, seed/unseed, CAS fetch, WASM execution
pkg/tunneling        service bridging, connection lifecycle, traffic tracking, relay

pkg/routing          Dijkstra routing table (pure computation)

pkg/state            CRDT state store, immutable snapshots, typed projections, events
pkg/transport        QUIC sessions, peer FSM, stream mux, NAT punch, UDP sockets

pkg/types            PeerKey, Envelope (foundational identity types)
pkg/auth             credentials, delegation certs, trust bundles, join tokens
pkg/coords           Vivaldi coordinate math (pure functions)
pkg/nat              NAT type detection
pkg/config           YAML config loading, defaults, persistence
pkg/cas              content-addressable store (filesystem, SHA-256)
pkg/wasm             WASM runtime (Extism/wazero wrapper)
pkg/observability    metrics, traces, logging (noop pattern)
pkg/traffic          per-peer traffic counting and heatmap
pkg/topology         peer selection algorithm (Vivaldi-based)
```

**Dependency rules:** Each layer imports only from layers below. Siblings (state, transport) never import each other. Domain services define consumer interfaces for their dependencies.

**Design spec:** `docs/superpowers/specs/2026-03-16-clean-room-architecture-design.md`
