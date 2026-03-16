# Integration Test Suite Restoration

Restore and expand the integration test coverage that came out of the failed
refactor, adapted to the current production APIs.

## Context

The recent refactor (see `incremental-rollback`) produced a solid integration
test suite, but the refactor itself was rolled back. The test harness
infrastructure (VirtualSwitch, ClusterAuth, Builder, TestNode) has been ported
to the current codebase, but scenario coverage dropped from ~26 test cases to 4.

The old suite also had a design flaw: many tests operated at the wrong
abstraction layer, calling internal APIs (store CRDT apply, scheduler evaluate)
directly. This meant integration seams like invite flows were never exercised.

## Design Principles

1. **Full-stack black-box testing.** Every scenario exercises complete `TestNode`
   instances through the cluster harness. No direct calls to internal subsystem
   APIs (store event application, scheduler evaluation, routing pipeline).
   Observe behavior from the outside.

2. **Actions through the node, observations through the store.** Tests perform
   mutations through `node.Node` public methods (which handle gossip event
   queueing and broadcast — the real production path). Tests observe state
   through `Store()` read methods for assertions. This ensures tests exercise
   the same code paths as production, including eager broadcast. If `node.Node`
   doesn't expose a needed action, add it.

3. **Single scenario file.** All scenarios live in `scenario_test.go`, organized
   by topology (PublicMesh, RelayRegions) then behavior. Split into domain files
   later if it grows unwieldy.

4. **Failing tests are acceptable.** The system may not be fast enough to pass
   all scenarios. Tests that fail tell us something real and serve as a forcing
   function for optimization work.

## Assertion Helpers

Existing (unchanged):
- `RequireConverged(t)` — all nodes see all other nodes via gossip
- `RequireHealthy(t)` — every node has ≥1 connected peer
- `RequirePeerVisible(t, name)` — named peer visible to all others
- `RequireConnectedPeers(t, name, n)` — node has ≥n connected peers
- `RequireEventually(t, pred, timeout)` — poll predicate
- `RequireNever(t, pred, duration)` — assert predicate stays false

New:
- `RequireWorkloadReplicas(t, hash, count)` — n nodes across cluster have
  claimed a workload hash (checked via `Store().AllWorkloadClaims()`)
- `RequireWorkloadClaimedBy(t, hash, nodeNames)` — specific named nodes claim
  the workload (claim-based, not runtime-based)
- `RequireServiceVisible(t, nodeName, port)` — all other nodes see a service
  published by the named node (checked via `Store().HasServicePort()`)

All assertions are methods on `*Cluster` with 25ms poll / 10s default timeout.

Partition isolation and other one-off checks use `RequireNever` or
`RequireEventually` with inline predicates.

## Scenario Coverage

### PublicMesh (using `PublicMesh` preset)

**TestPublicMesh_GossipConvergence** (4 nodes)
- `AllNodesSeeSamePeers` — `RequireConverged()`
- `StateConvergesAfterMutation` — register service on one node, verify all
  nodes see it

**TestPublicMesh_TopologyStabilization** (6 nodes)
- `VivaldiCoordinatesPropagated` — all nodes have non-zero Vivaldi coords
  visible to all other nodes (tests gossip propagation of initial random
  coordinates, not actual Vivaldi convergence)
- `PeerSelectionSettles` — every node has ≥2 connections after convergence

**TestPublicMesh_WorkloadPlacement** (4 nodes)
- `PropagatesViaGossip` — seed workload on one node, spec propagates to all
- `ClaimTracking` — replicas claimed by expected number of nodes

**TestPublicMesh_NodeJoinLeave** (3 nodes + 1 dynamic)
- `JoinReceivesOngoingGossip` — new node via `AddNodeAndStart` converges
- `LeaveTriggersDisconnection` — stopped node drops from connected peers

**TestPublicMesh_ServiceExposure** (4 nodes)
- `VisibleClusterWide` — `RequireServiceVisible()` after upsert
- `ConnectFromRemoteNode` — desired connection added via `node.Node` public
  API (add if needed), verify tunnel manager establishes it. Full data-flow
  verification is out of scope for now.

**TestPublicMesh_InviteFlow** (2 nodes + 1 joining)

*Prerequisites: `node.Node` does not currently expose invite/join methods and
the internal `mesh.Mesh` is unexported. This scenario requires adding a public
`JoinWithInvite(ctx, token)` method to `node.Node` that delegates to the mesh
layer. Add this as part of the implementation.*

- `JoinWithValidToken` — new node joins cluster via invite token
- `JoinWithInvalidToken` — invalid token is rejected
- `TokenSingleUse` — consumed token cannot be reused
- `BootstrapSSHDelegation` — root node delegates admin to second node via
  the bootstrap SSH flow, verify second node holds admin delegation
- `InvitedNodeConverges` — root generates invite, third node joins via
  invite, verify full gossip convergence across all three nodes afterwards

**TestPublicMesh_DenyPeer** (3 nodes)

*Mechanism: Node A denies B through `node.Node`'s public API (add if needed),
which stores the deny event AND broadcasts it eagerly. Remote nodes apply
the deny event, their `OnDenyPeer` callback fires and closes the mesh session
to B. The test asserts from B's perspective: B observes its connections
dropping.*

- `DeniedPeerDisconnected` — denied peer loses connections
- `DeniedPeerCannotReconnect` — denied peer stays excluded (`RequireNever`)

### RelayRegions (using `RelayRegions` preset)

**TestRelayRegions_GossipConvergence** (2 regions, 2 nodes/region)
- `ConvergesAcrossRegions` — `RequireConverged()`
- `PrivateNodesDiscoverViaRelay` — `RequirePeerVisible()` for cross-region
  private nodes

**TestRelayRegions_NATpunchthrough** (2 regions, 2 nodes/region)

*Prerequisites: Currently all TestNodes set `DisableNATPunch: true`. This
scenario requires a `DisableNATPunch` field on `TestNodeConfig` so private
nodes can opt in to punch. The node's punch coordination path
(`requestPunchCoordination`, `handlePunchCoordRequest`, `punchLoop`) must be
active and the VirtualSwitch's NAT gating simulates the real behavior. Add
harness support for this as part of the implementation.*

- `PrivateToPrivateCrossRegion` — private nodes in different regions establish
  direct connection after punch
- `CoordinationViaRelay` — relay mediates the punch coordination

**TestRelayRegions_PartitionAndHeal** (2 regions, 1 node/region)
- `PartitionBlocksPropagation` — after partition, new state on one side does
  NOT appear on the other (`RequireNever` ~2s)
- `HealedNetworkPropagatesNewEvents` — after heal, cluster re-converges and
  new state propagates

**TestRelayRegions_WorkloadPlacement** (2 regions, 2 nodes/region)
- `CrossRegionSpecVisibility` — workload spec propagates across regions
- `CrossRegionClaimTracking` — claims visible from all regions

## Test Mechanics

- **Timeouts:** 30s context per scenario, 120s global (`just integration-test`)
- **Polling:** 25ms interval, 10s default assertion timeout
- **Partition isolation:** `RequireNever` with ~2s window before healing
- **Build tag:** `//go:build integration` on all test files
- **Gossip propagation:** Tests mutate state through `node.Node` public
  methods, which call the store AND queue the returned gossip events for
  immediate broadcast — the same path production uses. Tests never call
  `Store()` mutation methods directly, because that bypasses event queueing
  and creates a false propagation model. If `node.Node` doesn't expose a
  needed mutation, add a public method that delegates to the store and queues
  events. `Store()` is used only for read/observation in assertions.
- **Workload seeding:** Tests publish workload specs through `node.Node` (add
  a public method if needed). The scheduler reconciler running inside each
  node evaluates and claims. Assertions use `Store().AllWorkloadClaims()` to
  check claims. No WASM compilation needed — placement is purely a
  scheduling/gossip concern.
- **Invite flow:** `TestNode` construction must wire `*store.Store` as the
  `auth.InviteConsumer` on `DelegationSigner.Consumed`. Currently this field
  is nil and would panic during invite redemption. Fix in `node.go` TestNode
  construction.

## File Changes

- `pkg/integration/cluster/assert.go` — add `RequireWorkloadReplicas`,
  `RequireWorkloadClaimedBy`, `RequireServiceVisible`
- `pkg/integration/cluster/scenario_test.go` — replace current 4 scenarios with
  full 11-test / 25-subtest suite. All tests ship, even if prerequisites aren't
  fully wired yet — failing tests are the forcing function.
- `pkg/integration/cluster/node.go` — wire `Store` as `InviteConsumer` on
  `DelegationSigner.Consumed`; add `DisableNATPunch` field to `TestNodeConfig`
  so NAT punch scenarios can opt in
- `pkg/node/node.go` — add public methods for actions tests need:
  `JoinWithInvite`, `UpsertService`, `RemoveService`, `SetWorkloadSpec`,
  `DenyPeer`, `AddDesiredConnection`, `RemoveDesiredConnection`, and any
  others discovered during implementation. Each delegates to the appropriate
  subsystem and queues gossip events for broadcast.

## Out of Scope

- Splitting scenarios into multiple files (follow-up if needed)
- Performance optimization of test timing
- Expanding beyond old suite coverage (follow-up)
- Unit-style pipeline tests (replaced by full-stack scenarios)
- Tunnel data-flow verification (desired connections only, not payload)
- WASM compilation/invocation testing (workload placement is gossip-level only)
