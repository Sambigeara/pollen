# Phase 4 Completion Checklist: State API Alignment

Source of truth: `TARGET_ARCHITECTURE_SPEC.md`
Gap analysis: `GAP_ANALYSIS.md`

---

## Spec Target

The spec says state exports:
- `New(self types.PeerKey) *Store` (constructor)
- `Snapshot() Snapshot` (lock-free read)
- `ApplyDelta(from types.PeerKey, data []byte) ([]Event, error)`
- `EncodeDelta(since Clock) []byte`
- `FullState() []byte`
- Typed mutations: DenyPeer, SetLocalAddresses, SetLocalNAT, SetLocalCoord, SetLocalReachable, SetWorkloadSpec, ClaimWorkload, ReleaseWorkload, SetLocalResources, SetService, RemoveService, SetLocalTraffic
- Snapshot projections: Peers, Peer, Services, Workloads, DeniedPeers, Self, Clock
- Events: PeerJoined, PeerLeft, PeerDenied, ServiceChanged, WorkloadChanged, TopologyChanged, GossipApplied
- Clock type with Marshal/UnmarshalClock

---

## Constructor
- [x] `New(self types.PeerKey) *Store` — matches (store.go:235)

## Core Methods — Verify Exported
- [x] `Snapshot() Snapshot` — exported (store.go:1588)
- [x] `ApplyDelta(from types.PeerKey, data []byte) ([]Event, error)` — exported (delta.go:12)
- [x] `EncodeDelta(since Clock) []byte` — exported (delta.go:24)
- [x] `FullState() []byte` — exported (delta.go:33)

## Typed Mutations — Verify All Exported with Correct Signatures
Each must return `[]Event`:
- [x] `DenyPeer(key types.PeerKey) []Event` — mutations.go:13
- [x] `SetLocalAddresses(addrs []netip.AddrPort) []Event` — mutations.go:22
- [x] `SetLocalNAT(t nat.Type) []Event` — mutations.go:40
- [x] `SetLocalCoord(c coords.Coord) []Event` — mutations.go:49
- [x] `SetLocalReachable(peers []types.PeerKey) []Event` — mutations.go:58
- [x] `SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) ([]Event, error)` — mutations.go:104
- [x] `ClaimWorkload(hash string) []Event` — mutations.go:116
- [x] `ReleaseWorkload(hash string) []Event` — mutations.go:125
- [x] `SetLocalResources(cpu, mem float64) []Event` — mutations.go:135
- [x] `SetService(port uint32, name string) []Event` — mutations.go:144
- [x] `RemoveService(name string) []Event` — mutations.go:153
- [x] `SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event` — mutations.go:169

## Snapshot Projections — Verify Present
- [x] `Peers() []PeerInfo` — snapshot.go:84
- [x] `Peer(key) (PeerInfo, bool)` — snapshot.go:97
- [x] `Services() []ServiceInfo` — snapshot.go:106
- [x] `Workloads() []WorkloadInfo` — snapshot.go:121
- [x] `DeniedPeers() []types.PeerKey` — snapshot.go:182
- [x] `Self() types.PeerKey` — snapshot.go:74
- [x] `Clock() Clock` — snapshot.go:79

## Extra Snapshot Methods — Assess
- [x] `KnownPeers() []KnownPeer` — KEEP. Used by supervisor (knownPeersFromSnapshot in supervisor.go:789) and integration tests. Required for topology target computation.
- [x] `HasServicePort(peerID, port) bool` — KEEP. Used by integration tests (scenario_test.go, assert.go) for service propagation verification.

## Event Types — Verify Present
- [x] `Event` interface (sealed) — event.go:10
- [x] `PeerJoined` struct — event.go:15
- [x] `PeerLeft` struct — event.go:16
- [x] `PeerDenied` struct — event.go:17
- [x] `ServiceChanged` struct — event.go:18
- [x] `WorkloadChanged` struct — event.go:25
- [x] `TopologyChanged` struct — event.go:26
- [x] `GossipApplied` struct — event.go:27

## Extra Event Types — Assess
- [x] `TrafficChanged` — KEEP. Used by supervisor (supervisor.go:455: `case state.TrafficChanged: n.placement.SignalTraffic()`). Required for traffic-aware placement signaling.

## GossipApplied Signature
- [x] **FIXED.** Removed `Rebroadcast []*statev1.GossipEvent` field. No caller reads `.Rebroadcast` from the Event interface — membership reads it from `ApplyResult.Rebroadcast` directly (service.go:599). The field on GossipApplied was set but never consumed.

## Extra Methods on *Store — Assess Each
- [x] `FlushPendingGossip()` — KEEP. Called by membership (service.go:407) to drain pending local mutations for eager broadcast.
- [x] `LocalEvents()` — KEEP. Called by membership (service.go:269) during startup to broadcast initial local state.
- [x] `PendingNotify()` — KEEP. Called by membership (service.go:406) to wake event loop when mutations accumulate.
- [x] `SetGossipMetrics(m)` — KEEP. Called by supervisor (supervisor.go:169) to wire metrics instruments.
- [x] `SetLastAddr(peerID, addr)` — KEEP. Called by supervisor (supervisor.go:466) to record last-known peer address for reconnection.
- [x] `SetLocalPubliclyAccessible(bool)` — KEEP. Called by supervisor (supervisor.go:105) when node is a bootstrap peer.
- [x] `ApplyEvents(events, isPullResponse) ApplyResult` — KEEP. This is the proto-level gossip API that `ApplyDelta` delegates to. Membership calls it directly (service.go:598) for richer result data (Rebroadcast, NewPeers, DeniedPeers).

## Extra Types — Assess Each
- [x] `NodeView` — KEEP. Embedded in exported `PeerInfo` and used as value type in exported `Snapshot.Nodes` field. Accessed by tunneling, supervisor, membership, control, and placement packages.
- [x] `Connection` — KEEP. Used by tunneling (service.go:108,418,669) and supervisor (supervisor.go:778,947) for desired peer connections.
- [x] `TrafficSnapshot` — KEEP. Used as value type in exported `NodeView.TrafficRates` field. Also used in `SetLocalTraffic` (mutations.go:170).
- [x] `NodePlacementState` — KEEP. Used by placement (reconciler.go:176) for traffic-aware scheduling. Exposed via `Snapshot.Placements`.
- [x] `ApplyResult` — KEEP. Used by membership (service.go:73,598) as return type from `ApplyEvents`.
- [x] `KnownPeer` — KEEP. Used by supervisor (supervisor.go:491,789) for topology target computation.

## Extra Package Functions
- [x] `UnmarshalClock(data []byte) (Clock, error)` — KEEP. Inverse of `Clock.Marshal()`, needed by membership for gossip pull requests.

---

## Verification Gates

- [x] `go build ./...` — passes
- [x] `go test -count=1 ./...` — all unit tests pass
- [x] `go test -tags integration -count=1 -timeout 120s ./pkg/integration/cluster/...` — passes (first run hit pre-existing flaky panic in membership.sendEvent; second run clean)
- [x] `just lint` — 0 issues
- [ ] `cd infra && just verify-hetzner` — steps 0-10 pass
