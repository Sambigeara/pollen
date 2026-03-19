# Integration Test Mutation Map

Mutation testing results for the integration test suite in `pkg/integration/cluster/`.
Each test was subjected to a targeted production-code mutation to verify it catches the defect.

## Summary

| Test | Result |
|------|--------|
| TestPublicMesh_GossipConvergence | PASS (kills mutation) |
| TestPublicMesh_TopologyStabilization | PASS (kills vivaldi publish mutation); NOTE: survives vivaldi.Update no-op |
| TestPublicMesh_WorkloadPlacement | PASS (kills mutation) |
| TestPublicMesh_NodeJoinLeave | PASS (kills mutation) |
| TestPublicMesh_ServiceExposure | PASS (kills mutation) |
| TestPublicMesh_InviteFlow | PASS (kills mutation) |
| TestPublicMesh_DenyPeer | PASS (kills mutation) |
| TestPublicMesh_EagerGossipPropagation | PASS (kills mutation) |
| TestRelayRegions_GossipConvergence | PASS (kills mutation) |
| TestRelayRegions_NATpunchthrough | PASS (kills mutation) |
| TestRelayRegions_PartitionAndHeal | PASS (kills mutation) |
| TestRelayRegions_WorkloadPlacement | PASS (kills mutation) |
| TestVirtualPacketConn_WriteAndRead | PASS (kills mutation) |
| TestVirtualPacketConn_ReadDeadline | PASS (kills mutation) |
| TestVirtualPacketConn_DeadlineWakesBlockedRead | PASS (kills mutation) |
| TestVirtualPacketConn_CloseUnblocksRead | PASS (kills mutation) |
| TestVirtualSwitch_Latency | PASS (kills mutation) |
| TestVirtualSwitch_Partition | PASS (kills mutation) |
| TestVirtualSwitch_NATGating | PASS (kills mutation) |
| TestCluster_Builder | PASS (kills mutation) |
| TestCluster_PublicMeshPreset | PASS (kills mutation) |
| TestCluster_RelayRegionsPreset | PASS (kills mutation) |
| TestTestNode_StartsAndStops | PASS (kills mutation) |
| TestClusterAuth_NodeCredentials | PASS (kills mutation) |
| TestClusterAuth_TwoNodesSameTrust | PASS (kills mutation) |

## Detailed Results

### TestPublicMesh_GossipConvergence

- **Guards:** Gossip state convergence across all nodes; eager push of local mutations.
- **Production file:** `pkg/state/store.go` :: `ApplyEvents`
- **Mutation:** Skip applying remote events (`continue` at top of `otherEvents` loop).
- **Result:** FAIL as expected -- both `AllNodesSeeSamePeers` and `StateConvergesAfterMutation` subtests fail.

### TestPublicMesh_TopologyStabilization

- **Guards:** Vivaldi coordinate propagation; peer selection establishing additional connections in a sparse chain.
- **Production file:** `pkg/state/store.go` :: `SetLocalVivaldiCoord`
- **Mutation:** Early return `nil` to skip coordinate publishing.
- **Result:** FAIL as expected -- `VivaldiCoordinatesPropagated` subtest fails.
- **Note:** A secondary mutation (`topology.Update` returning input unchanged) was **survived** because the test checks that coordinates are non-zero, and `RandomCoord()` seeds non-zero initial coordinates. The test guards *propagation*, not *computation*. This is acceptable behavior for an integration test -- Vivaldi computation correctness is tested by unit tests in `pkg/topology/vivaldi_test.go`.

### TestPublicMesh_WorkloadPlacement

- **Guards:** Workload spec gossip propagation and scheduler claim tracking.
- **Production file:** `pkg/scheduler/scheduler.go` :: `Evaluate`
- **Mutation:** Early return `nil` to skip all claim/release evaluation.
- **Result:** FAIL as expected -- `ClaimTracking` subtest fails (0 claims instead of 2).

### TestPublicMesh_NodeJoinLeave

- **Guards:** Peer connect event emission; late-joiner gossip convergence; graceful leave detection.
- **Production file:** `pkg/mesh/mesh.go` :: `addPeer`
- **Mutation:** Suppress `ConnectPeer` event emission to `inCh`.
- **Result:** FAIL as expected -- cluster never converges because `handleOutputs` never processes PeerConnected.

### TestPublicMesh_ServiceExposure

- **Guards:** Service attribute gossip propagation; desired connection recording.
- **Production file:** `pkg/state/store.go` :: `UpsertLocalService`
- **Mutation:** Early return `nil` to skip service event creation and publishing.
- **Result:** FAIL as expected -- `VisibleClusterWide` subtest fails within eager timeout (500ms).

### TestPublicMesh_InviteFlow

- **Guards:** Invite token validation (subject binding, single-use), join-with-invite flow, bootstrap SSH delegation, post-invite convergence.
- **Production file:** `pkg/auth/invite.go` :: `VerifyInviteToken`
- **Mutation:** Skip subject mismatch check (remove `SubjectPub` vs `expectedSubject` comparison).
- **Result:** FAIL as expected -- `JoinWithInvalidToken` subtest expects an error but gets nil.

### TestPublicMesh_DenyPeer

- **Guards:** Deny peer disconnection and reconnection prevention.
- **Production file:** `pkg/node/node.go` :: `DenyPeer`
- **Mutation:** Early return to skip all deny logic (gossip, tunnel disconnect, session close).
- **Result:** FAIL as expected -- both `DeniedPeerDisconnected` (peer keeps connections) and `DeniedPeerCannotReconnect` (peer reconnects) subtests fail.

### TestPublicMesh_EagerGossipPropagation

- **Guards:** Eager push of local mutations (within 500ms, well under the 1s gossip tick).
- **Production file:** `pkg/node/node.go` :: `broadcastEvents`
- **Mutation:** Replace body with no-op (skip broadcasting to connected peers).
- **Result:** FAIL as expected -- service not visible within eager timeout.

### TestRelayRegions_GossipConvergence

- **Guards:** Cross-region gossip convergence via relay nodes; private node discovery.
- **Production file:** `pkg/state/store.go` :: `ApplyEvents`
- **Mutation:** Skip applying remote events (same as PublicMesh variant).
- **Result:** FAIL as expected -- both `ConvergesAcrossRegions` and `PrivateNodesDiscoverViaRelay` fail.

### TestRelayRegions_NATpunchthrough

- **Guards:** Direct connection between private nodes across regions via NAT punch coordination.
- **Production file:** `pkg/mesh/mesh.go` :: `Punch`
- **Mutation:** Always return error ("NAT punch disabled").
- **Result:** FAIL as expected -- `PrivateToPrivateCrossRegion` and `CoordinationViaRelay` subtests fail because private nodes in different 10.x subnets are not privately routable, so the peer state machine enters `ConnectStagePunch` and relies on punch coordination. With `Punch` disabled, the coordination fails and nodes never connect.
- **Fix:** Private nodes are now assigned to different 10.x subnets via `SetAddr` (10.1.0.1 vs 10.2.0.1), and an additional relay introduction ensures a coordinator is available. Previously all nodes shared the 10.0.x.y prefix, making `InferPrivatelyRoutable` return true and bypassing punch coordination entirely.

### TestRelayRegions_PartitionAndHeal

- **Guards:** Network partition blocks gossip propagation; heal restores it.
- **Production file:** `pkg/state/store.go` :: `HasServicePort`
- **Mutation:** Always return `true` (pretend every peer has every service).
- **Result:** FAIL as expected -- `PartitionBlocksPropagation` subtest fails because `RequireNever` sees the condition satisfied immediately.

### TestRelayRegions_WorkloadPlacement

- **Guards:** Cross-region workload spec visibility and claim tracking.
- **Production file:** `pkg/scheduler/scheduler.go` :: `Evaluate`
- **Mutation:** Early return `nil` (same as PublicMesh variant).
- **Result:** FAIL as expected -- `CrossRegionClaimTracking` fails (0 claims instead of 2).

## Harness Tests

### TestVirtualPacketConn_WriteAndRead

- **Guards:** Basic packet delivery between two VirtualPacketConns through the VirtualSwitch.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `WriteTo`
- **Mutation:** Skip `c.sw.deliver()` call -- WriteTo returns success but never routes the packet.
- **Result:** FAIL as expected -- ReadFrom blocks forever (test times out at 30s).

### TestVirtualPacketConn_ReadDeadline

- **Guards:** ReadFrom returns `os.ErrDeadlineExceeded` when deadline is already in the past.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `ReadFrom`
- **Mutation:** When `remaining <= 0`, set remaining to `time.Hour` instead of returning `os.ErrDeadlineExceeded`.
- **Result:** FAIL as expected -- ReadFrom blocks forever (test times out at 30s).

### TestVirtualPacketConn_DeadlineWakesBlockedRead

- **Guards:** SetReadDeadline wakes a goroutine blocked in ReadFrom via the `deadlineNotify` channel.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `SetReadDeadline`
- **Mutation:** Remove the `deadlineNotify <- struct{}{}` send -- deadline is stored but blocked ReadFrom is never signaled.
- **Result:** FAIL as expected -- `require.Eventually` condition never satisfied (blocked ReadFrom never wakes).

### TestVirtualPacketConn_CloseUnblocksRead

- **Guards:** Closing a VirtualPacketConn unblocks a goroutine blocked in ReadFrom.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `Close`
- **Mutation:** Remove `close(c.closedCh)` -- unregister still happens but closedCh stays open.
- **Result:** FAIL as expected -- `require.Eventually` condition never satisfied (blocked ReadFrom never wakes).

### TestVirtualSwitch_Latency

- **Guards:** VirtualSwitch applies configured latency to packet delivery.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `computeDelay`
- **Mutation:** Always return 0 regardless of base latency and jitter.
- **Result:** FAIL as expected -- elapsed time is ~91us, assertion requires >= 40ms.

### TestVirtualSwitch_Partition

- **Guards:** VirtualSwitch drops packets between partitioned nodes; Heal restores delivery.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `isPartitioned`
- **Mutation:** Always return `false` -- partitions have no effect.
- **Result:** FAIL as expected -- packet arrives despite partition; `ErrorIs(err, os.ErrDeadlineExceeded)` fails because err is nil.

### TestVirtualSwitch_NATGating

- **Guards:** Private-to-private traffic is blocked without punch; simultaneous open enables delivery.
- **Harness file:** `pkg/integration/cluster/switch.go` :: `deliver`
- **Mutation:** Skip the NAT gating block entirely (`if false && fromRole == Private`).
- **Result:** FAIL as expected -- private-to-private packet arrives immediately; `ErrorIs(err, os.ErrDeadlineExceeded)` fails because err is nil.

### TestCluster_Builder

- **Guards:** Builder creates and wires nodes correctly; Cluster.Nodes() and Cluster.Node() return expected state.
- **Harness file:** `pkg/integration/cluster/cluster.go` :: `Nodes`
- **Mutation:** Return `nil` instead of `c.ordered`.
- **Result:** FAIL as expected -- `require.Len(c.Nodes(), 2)` fails (0 items instead of 2).

### TestCluster_PublicMeshPreset

- **Guards:** PublicMesh factory creates the correct number of nodes in a full mesh.
- **Harness file:** `pkg/integration/cluster/cluster.go` :: `PublicMesh`
- **Mutation:** Only add 1 node instead of n (`range 1` instead of `range n`).
- **Result:** FAIL as expected -- `mustNode` fatals on missing node "node-1" during introduction.

### TestCluster_RelayRegionsPreset

- **Guards:** RelayRegions factory creates relays and private nodes in correct quantities.
- **Harness file:** `pkg/integration/cluster/cluster.go` :: `RelayRegions`
- **Mutation:** Skip the private-node creation loop entirely.
- **Result:** FAIL as expected -- `require.Len(c.Nodes(), 4)` fails (2 items instead of 4).

### TestTestNode_StartsAndStops

- **Guards:** NewTestNode produces a running node with correct role, name, store, and non-zero PeerKey.
- **Harness file:** `pkg/integration/cluster/node.go` :: `NewTestNode`
- **Mutation (role):** Always set role to `Private` regardless of config.
- **Result:** FAIL as expected -- `require.Equal(Public, tn.Role())` fails (expected 0, got 1).
- **Mutation (peerKey):** Set `peerKey` to `types.PeerKey{}` (zero value).
- **Result:** FAIL as expected -- `require.NotEqual(types.PeerKey{}, tn.PeerKey())` catches the zero PeerKey. Previously used `[32]byte{}` which never matched due to type mismatch.

### TestClusterAuth_NodeCredentials

- **Guards:** NodeCredentials issues a valid TLS cert and delegation cert bound to the correct node public key.
- **Harness file:** `pkg/integration/cluster/auth.go` :: `NodeCredentials`
- **Mutation:** Issue delegation cert to a freshly generated random key instead of the node's actual public key.
- **Result:** FAIL as expected -- `auth.VerifyDelegationCert` returns "delegation cert subject mismatch".

### TestClusterAuth_TwoNodesSameTrust

- **Guards:** Two nodes issued credentials from the same ClusterAuth share a trust root and can verify each other.
- **Harness file:** `pkg/integration/cluster/auth.go` :: `TrustBundle`
- **Mutation:** Return a fresh trust bundle (new random root key) on each call instead of the shared one.
- **Result:** FAIL as expected -- `auth.VerifyDelegationCert` returns "delegation cert not scoped to cluster".

## State Package Boundary Tests

Black-box tests (`package state_test`) exercising the public API of `pkg/state`.

| Test | Result |
|------|--------|
| TestStore_SnapshotIsImmutable | PASS (kills mutation) |
| TestStore_ApplyEventsReturnsEvents | PASS (kills mutation) |
| TestStore_DenyPeerAppearsInSnapshot | PASS (kills mutation) |
| TestStore_ServiceAppearsInSnapshot | PASS (kills mutation) |
| TestStore_WorkloadSpecAppearsInSnapshot | PASS (kills mutation) |
| TestStore_EventChannelEmitsOnMutation | PASS (kills mutation) |
| TestSnapshot_ProjectionConsistency | PASS (kills mutation) |

### TestStore_SnapshotIsImmutable

- **Guards:** Snapshot isolation -- a snapshot taken at time T1 is not affected by mutations at T2.
- **Production file:** `pkg/state/store.go` :: `Snapshot`
- **Mutation:** `Snapshot()` returns `Snapshot{}` (empty).
- **Result:** FAIL as expected -- `snap1.Peer(snap1.Self())` returns `ok=false`.
- **Note:** The snapshot immutability is structurally guaranteed by Go value semantics (Snapshot is returned by value from an atomic pointer). The test guards against regressions where someone might change the return type to a pointer or share mutable maps.

### TestStore_ApplyEventsReturnsEvents

- **Guards:** Cross-store gossip propagation via `ApplyEvents`.
- **Production file:** `pkg/state/store.go` :: `ApplyEvents`
- **Mutation:** Early return `ApplyResult{}` (skip all event processing).
- **Result:** FAIL as expected -- `result.Rebroadcast` is empty.

### TestStore_DenyPeerAppearsInSnapshot

- **Guards:** Deny list population via `DenyPeer` and visibility through `Snapshot().DeniedPeers()`.
- **Production file:** `pkg/state/snapshot.go` :: `DeniedPeers`
- **Mutation:** Return `nil` instead of `s.DeniedKeys`.
- **Result:** FAIL as expected -- `require.Len(denied, 1)` fails (0 items).

### TestStore_ServiceAppearsInSnapshot

- **Guards:** Service registration via `UpsertLocalService` and visibility through `Snapshot().Services()`.
- **Production file:** `pkg/state/snapshot.go` :: `Services`
- **Mutation:** Return `nil` (skip all service aggregation).
- **Result:** FAIL as expected -- `require.Len(svcs, 1)` fails (0 items).

### TestStore_WorkloadSpecAppearsInSnapshot

- **Guards:** Workload spec registration via `SetLocalWorkloadSpec` and visibility through `Snapshot().Workloads()`.
- **Production file:** `pkg/state/snapshot.go` :: `Workloads`
- **Mutation:** Return `nil` (skip all workload aggregation).
- **Result:** FAIL as expected -- `require.Len(workloads, 1)` fails (0 items).

### TestStore_EventChannelEmitsOnMutation

- **Guards:** Event channel emission on local state mutations.
- **Production file:** `pkg/state/store.go` :: `emitEvent`
- **Mutation:** No-op `emitEvent` (suppress all events).
- **Result:** FAIL as expected -- test times out waiting for `LocalMutationApplied` event.

### TestSnapshot_ProjectionConsistency

- **Guards:** Consistency between `Peers()`, `PeerKeys`, `Self()`, `Services()`, `Workloads()`, and `Nodes` map.
- **Production file:** `pkg/state/snapshot.go` :: `Peers`
- **Mutation:** Return `nil` from `Peers()`.
- **Result:** FAIL as expected -- `len(snap.PeerKeys) != len(snap.Peers())` (2 vs 0).
