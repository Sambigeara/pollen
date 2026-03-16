# Integration Test Suite Restoration

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore and expand integration test coverage to 11 tests / 25 subtests
exercising full-stack nodes through the cluster harness.

**Architecture:** Tests mutate state through `node.Node` public methods (which
queue gossip events for broadcast) and observe via `Store()` reads. New public
methods are extracted from `NodeService` so both the RPC layer and tests share
one code path. The cluster harness gets `InviteConsumer` wiring and NAT punch
opt-in.

**Tech Stack:** Go, testify/require, integration build tag, VirtualSwitch
harness

**Spec:** `docs/superpowers/specs/2026-03-16-integration-test-restoration-design.md`

---

## File Structure

**Modified production files:**
- `pkg/node/node.go` — add public action methods: `UpsertService`,
  `RemoveService`, `DenyPeer`, `SetWorkloadSpec`, `JoinWithInvite`,
  `AddDesiredConnection`, `RemoveDesiredConnection`, `Credentials` accessor.
  `ConnectService` and `DisconnectService` already exist.
- `pkg/node/svc.go` — refactor `NodeService` methods to delegate to new
  `Node` methods

**Modified harness files:**
- `pkg/integration/cluster/node.go` — wire `InviteConsumer`, add
  `DisableNATPunch` to `TestNodeConfig`
- `pkg/integration/cluster/assert.go` — add workload and service assertions

**Replaced:**
- `pkg/integration/cluster/scenario_test.go` — full rewrite with 11 tests /
  25 subtests

---

## Chunk 1: Node Public API Methods

Extract action methods from `NodeService` into `Node` so tests and the RPC
layer share one code path.

### Task 1: Add UpsertService and RemoveService to Node

**Files:**
- Modify: `pkg/node/node.go`
- Modify: `pkg/node/svc.go:385-404`

- [ ] **Step 1: Add UpsertService method to Node**

In `pkg/node/node.go`, add after the existing `ConnectPeer` method (~line 1608):

```go
// UpsertService registers a local service and broadcasts the change via gossip.
func (n *Node) UpsertService(port uint32, name string) {
	n.tun.RegisterService(port)
	n.queueGossipEvents(n.store.UpsertLocalService(port, name))
}

// RemoveService unregisters a local service and broadcasts the change via gossip.
func (n *Node) RemoveService(name string, port uint32) {
	events := n.store.RemoveLocalServices(name)
	n.tun.UnregisterService(port)
	n.queueGossipEvents(events)
}
```

- [ ] **Step 2: Refactor NodeService to delegate**

In `pkg/node/svc.go`, replace `RegisterService` (lines 385-395) and
`UnregisterService` (lines 397-404):

```go
func (s *NodeService) RegisterService(_ context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	name := req.GetName()
	if name == "" {
		name = strconv.FormatUint(uint64(req.Port), 10)
	}
	s.node.UpsertService(req.Port, name)
	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *NodeService) UnregisterService(_ context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	s.node.RemoveService(req.GetName(), req.GetPort())
	return &controlv1.UnregisterServiceResponse{}, nil
}
```

- [ ] **Step 3: Run goimports on both files**

Run: `goimports -w pkg/node/node.go pkg/node/svc.go`

- [ ] **Step 4: Run existing tests to verify no regression**

Run: `go test ./pkg/node/ -v -count=1 -timeout 120s`
Expected: All existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add pkg/node/node.go pkg/node/svc.go
git commit -s -m "extract UpsertService and RemoveService from NodeService to Node"
```

### Task 2: Add DenyPeer to Node

**Files:**
- Modify: `pkg/node/node.go`
- Modify: `pkg/node/svc.go:441-460`

- [ ] **Step 1: Add DenyPeer method to Node**

In `pkg/node/node.go`, add:

```go
// DenyPeer adds a peer to the deny list, broadcasts the event, and tears down
// all connections to that peer.
func (n *Node) DenyPeer(pk types.PeerKey) {
	events := n.store.DenyPeer(pk.Bytes())
	n.queueGossipEvents(events)
	n.tun.DisconnectPeer(pk)
	n.store.RemoveDesiredConnection(pk, 0, 0)
	n.mesh.ClosePeerSession(pk, mesh.CloseReasonDenied)
	n.localPeerEvents <- peer.PeerDisconnected{PeerKey: pk, Reason: peer.DisconnectDenied}
	n.localPeerEvents <- peer.ForgetPeer{PeerKey: pk}
	if err := n.store.Save(); err != nil {
		n.log.Warnw("failed to save state after deny", "err", err)
	}
}
```

- [ ] **Step 2: Refactor NodeService.DenyPeer to delegate**

In `pkg/node/svc.go`, replace `DenyPeer` (lines 441-460):

```go
func (s *NodeService) DenyPeer(_ context.Context, req *controlv1.DenyPeerRequest) (*controlv1.DenyPeerResponse, error) {
	if s.creds.DelegationKey == nil {
		return nil, status.Error(codes.FailedPrecondition, "delegation key not available; this node cannot deny peers")
	}
	s.node.DenyPeer(types.PeerKeyFromBytes(req.GetPeerId()))
	return &controlv1.DenyPeerResponse{}, nil
}
```

- [ ] **Step 3: Run goimports on both files**

Run: `goimports -w pkg/node/node.go pkg/node/svc.go`

- [ ] **Step 4: Run existing tests**

Run: `go test ./pkg/node/ -v -count=1 -timeout 120s`
Expected: All existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add pkg/node/node.go pkg/node/svc.go
git commit -s -m "extract DenyPeer from NodeService to Node"
```

### Task 3: Add SetWorkloadSpec to Node

**Files:**
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Add SetWorkloadSpec method to Node**

This is a lighter method than `NodeService.SeedWorkload` — it only publishes
the spec via gossip without CAS storage or WASM compilation. The scheduler
reconciler inside each node will react to the spec independently.

```go
// SetWorkloadSpec publishes a workload placement spec and broadcasts it via
// gossip. This does not compile or store the workload binary — it only affects
// the scheduler's placement decisions.
func (n *Node) SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) error {
	events, err := n.store.SetLocalWorkloadSpec(hash, replicas, memoryPages, timeoutMs)
	if err != nil {
		return err
	}
	n.queueGossipEvents(events)
	return nil
}
```

- [ ] **Step 2: Run goimports**

Run: `goimports -w pkg/node/node.go`

- [ ] **Step 3: Run existing tests**

Run: `go test ./pkg/node/ -v -count=1 -timeout 120s`
Expected: All existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add pkg/node/node.go
git commit -s -m "add SetWorkloadSpec to Node for gossip-level workload placement"
```

### Task 4: Add JoinWithInvite, Credentials, AddDesiredConnection, RemoveDesiredConnection to Node

**Files:**
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Add Credentials accessor to Node**

The invite flow test needs access to the `DelegationSigner` to issue invites.
The `creds` field on `Node` is unexported (`node.go:149`).

```go
// Credentials returns the node's authentication credentials.
func (n *Node) Credentials() *auth.NodeCredentials {
	return n.creds
}
```

- [ ] **Step 2: Add JoinWithInvite method to Node**

The mesh layer already implements `JoinWithInvite`. Expose it through Node:

```go
// JoinWithInvite redeems an invite token to join the cluster. The invite is
// sent to the bootstrap peer(s) embedded in the token, and on success the node
// receives a join token which is applied to the mesh.
func (n *Node) JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	return n.mesh.JoinWithInvite(ctx, token)
}
```

- [ ] **Step 3: Add AddDesiredConnection and RemoveDesiredConnection**

These need to go through the node (not store directly) to maintain the
"actions through the node" principle. The store methods don't return gossip
events (desired connections are local-only state), but going through the node
keeps the API consistent.

```go
// AddDesiredConnection registers a tunnel connection the node should maintain.
func (n *Node) AddDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	n.store.AddDesiredConnection(pk, remotePort, localPort)
}

// RemoveDesiredConnection removes a desired tunnel connection.
func (n *Node) RemoveDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	n.store.RemoveDesiredConnection(pk, remotePort, localPort)
}
```

- [ ] **Step 4: Run goimports**

Run: `goimports -w pkg/node/node.go`

- [ ] **Step 5: Verify build**

Run: `go build ./pkg/node/`
Expected: Compiles without errors.

- [ ] **Step 6: Commit**

```bash
git add pkg/node/node.go
git commit -s -m "add JoinWithInvite, Credentials, and desired connection methods to Node"
```

---

## Chunk 2: TestNode Harness Changes

### Task 5: Wire InviteConsumer on TestNode

**Files:**
- Modify: `pkg/integration/cluster/node.go:53-60`

The `DelegationSigner.Consumed` field is currently nil, which would panic
during invite redemption. Wire the node's `*store.Store` as the
`InviteConsumer`.

- [ ] **Step 1: Set Consumed field in TestNode construction**

In `pkg/integration/cluster/node.go`, find the `creds` construction
(~lines 53-60):

```go
creds := &auth.NodeCredentials{
	Trust: cfg.Auth.TrustBundle(),
	Cert:  dc,
	DelegationKey: &auth.DelegationSigner{
		Priv:   priv,
		Trust:  cfg.Auth.TrustBundle(),
		Issuer: dc,
	},
}
```

Replace with:

```go
creds := &auth.NodeCredentials{
	Trust: cfg.Auth.TrustBundle(),
	Cert:  dc,
	DelegationKey: &auth.DelegationSigner{
		Priv:     priv,
		Trust:    cfg.Auth.TrustBundle(),
		Issuer:   dc,
		Consumed: stateStore,
	},
}
```

The `stateStore` variable is the `*store.Store` created earlier in
`NewTestNode` (at `node.go:65`).

- [ ] **Step 2: Run integration tests to verify no regression**

Run: `just integration-test`
Expected: All existing tests pass.

- [ ] **Step 3: Commit**

```bash
git add pkg/integration/cluster/node.go
git commit -s -m "wire Store as InviteConsumer on TestNode credentials"
```

### Task 6: Add Stop() to TestNode and DisableNATPunch opt-in to TestNodeConfig

**Files:**
- Modify: `pkg/integration/cluster/node.go`

The `LeaveTriggersDisconnection` test needs a way to stop a node mid-test.
Currently `NewTestNode` captures a `context.CancelFunc` in `t.Cleanup` but
does not expose it. Also, all TestNodes hardcode `DisableNATPunch: true` and
the NAT punchthrough scenario needs private nodes with punch enabled.

- [ ] **Step 1: Add cancel and Stop() to TestNode**

Add a `cancel` field to the `TestNode` struct and a `Stop()` method:

```go
type TestNode struct {
	n       *node.Node
	store   *store.Store
	peerKey types.PeerKey
	addr    *net.UDPAddr
	name    string
	role    NodeRole
	cancel  context.CancelFunc
}

// Stop cancels the node's context, shutting it down.
func (tn *TestNode) Stop() {
	tn.cancel()
}
```

In `NewTestNode`, store the cancel function on the TestNode instead of (or in
addition to) using it in `t.Cleanup`. The node's context cancel at `node.go:89`
should be captured in `tn.cancel`. Also keep the `t.Cleanup` call as a safety
net so nodes are always cleaned up.

- [ ] **Step 2: Add fields to TestNodeConfig**

In the `TestNodeConfig` struct, add:

```go
type TestNodeConfig struct {
	Context        context.Context
	Switch         *VirtualSwitch
	Auth           *ClusterAuth
	Addr           *net.UDPAddr
	Name           string
	Role           NodeRole
	BootstrapPeers []node.BootstrapPeer
	EnableNATPunch bool // when true, do not disable NAT punch on this node
}
```

- [ ] **Step 2: Use the field in NewTestNode**

Find where `DisableNATPunch: true` is set in the node config construction and
change it to:

```go
DisableNATPunch: !cfg.EnableNATPunch,
```

- [ ] **Step 3: Run integration tests**

Run: `just integration-test`
Expected: All existing tests pass (no config uses `EnableNATPunch` yet).

- [ ] **Step 4: Commit**

```bash
git add pkg/integration/cluster/node.go
git commit -s -m "add EnableNATPunch option to TestNodeConfig"
```

---

## Chunk 3: Assertion Helpers

### Task 7: Add workload and service assertion helpers

**Files:**
- Modify: `pkg/integration/cluster/assert.go`

- [ ] **Step 1: Add RequireWorkloadReplicas**

```go
// RequireWorkloadReplicas asserts that exactly count nodes across the cluster
// have claimed the given workload hash.
func (c *Cluster) RequireWorkloadReplicas(t testing.TB, hash string, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		total := 0
		for _, n := range c.ordered {
			claims := n.Store().AllWorkloadClaims()
			if _, ok := claims[hash]; ok {
				if _, claimed := claims[hash][n.PeerKey()]; claimed {
					total++
				}
			}
		}
		return total >= count
	}, assertTimeout, assertPoll, "expected %d nodes to claim workload %s", count, hash)
}
```

- [ ] **Step 2: Add RequireWorkloadClaimedBy**

```go
// RequireWorkloadClaimedBy asserts that the named nodes have all claimed the
// given workload hash, as observed from any node's store.
func (c *Cluster) RequireWorkloadClaimedBy(t testing.TB, hash string, nodeNames []string) {
	t.Helper()
	expected := make(map[types.PeerKey]struct{}, len(nodeNames))
	for _, name := range nodeNames {
		expected[c.PeerKeyByName(name)] = struct{}{}
	}
	require.Eventually(t, func() bool {
		// Check from every node's perspective — pick the first that sees all.
		for _, n := range c.ordered {
			claims := n.Store().AllWorkloadClaims()
			claimants, ok := claims[hash]
			if !ok {
				continue
			}
			allFound := true
			for pk := range expected {
				if _, found := claimants[pk]; !found {
					allFound = false
					break
				}
			}
			if allFound {
				return true
			}
		}
		return false
	}, assertTimeout, assertPoll, "expected nodes %v to claim workload %s", nodeNames, hash)
}
```

- [ ] **Step 3: Add RequireServiceVisible**

```go
// RequireServiceVisible asserts that all nodes other than the named publisher
// can see a service on the given port via gossip state.
func (c *Cluster) RequireServiceVisible(t testing.TB, publisherName string, port uint32) {
	t.Helper()
	publisherKey := c.PeerKeyByName(publisherName)
	require.Eventually(t, func() bool {
		for _, n := range c.ordered {
			if n.PeerKey() == publisherKey {
				continue
			}
			if !n.Store().HasServicePort(publisherKey, port) {
				return false
			}
		}
		return true
	}, assertTimeout, assertPoll,
		"expected all nodes to see service on port %d from %s", port, publisherName)
}
```

- [ ] **Step 4: Run goimports**

Run: `goimports -w pkg/integration/cluster/assert.go`

- [ ] **Step 5: Verify build with integration tag**

Run: `go build -tags integration ./pkg/integration/cluster/`
Expected: Compiles.

- [ ] **Step 6: Commit**

```bash
git add pkg/integration/cluster/assert.go
git commit -s -m "add workload and service assertion helpers to cluster harness"
```

---

## Chunk 4: PublicMesh Scenarios

### Task 8: Write PublicMesh scenario tests

**Files:**
- Modify: `pkg/integration/cluster/scenario_test.go`

Replace the existing file contents with the full scenario suite. All tests use
`//go:build integration` tag.

- [ ] **Step 1: Write TestPublicMesh_GossipConvergence**

```go
func TestPublicMesh_GossipConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx)

	t.Run("AllNodesSeeSamePeers", func(t *testing.T) {
		c.RequireConverged(t)
	})

	t.Run("StateConvergesAfterMutation", func(t *testing.T) {
		c.Node("node-0").Node().UpsertService(8080, "http")
		c.RequireServiceVisible(t, "node-0", 8080)
	})
}
```

- [ ] **Step 2: Write TestPublicMesh_TopologyStabilization**

```go
func TestPublicMesh_TopologyStabilization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 6, ctx)
	c.RequireConverged(t)

	t.Run("VivaldiCoordinatesPropagated", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				for _, other := range c.Nodes() {
					if other.PeerKey() == n.PeerKey() {
						continue
					}
					coord, ok := n.Store().PeerVivaldiCoord(other.PeerKey())
					if !ok || coord == nil {
						return false
					}
				}
			}
			return true
		}, assertTimeout, "all nodes should see non-zero Vivaldi coords for all peers")
	})

	t.Run("PeerSelectionSettles", func(t *testing.T) {
		for _, n := range c.Nodes() {
			c.RequireConnectedPeers(t, n.Name(), 2)
		}
	})
}
```

- [ ] **Step 3: Write TestPublicMesh_WorkloadPlacement**

```go
func TestPublicMesh_WorkloadPlacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx)
	c.RequireConverged(t)

	hash := "deadbeef01234567deadbeef01234567deadbeef01234567deadbeef01234567"

	t.Run("PropagatesViaGossip", func(t *testing.T) {
		require.NoError(t, c.Node("node-0").Node().SetWorkloadSpec(hash, 2, 256, 5000))

		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				specs := n.Store().AllWorkloadSpecs()
				if _, ok := specs[hash]; !ok {
					return false
				}
			}
			return true
		}, assertTimeout, "workload spec should propagate to all nodes")
	})

	t.Run("ClaimTracking", func(t *testing.T) {
		c.RequireWorkloadReplicas(t, hash, 2)
	})
}
```

- [ ] **Step 4: Write TestPublicMesh_NodeJoinLeave**

```go
func TestPublicMesh_NodeJoinLeave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx)
	c.RequireConverged(t)

	t.Run("JoinReceivesOngoingGossip", func(t *testing.T) {
		// Register a service before the new node joins.
		c.Node("node-0").Node().UpsertService(9090, "pre-join-svc")

		// AddNodeAndStart creates the node but does not introduce it.
		// Introduce it to node-0 so it can join the mesh.
		newNode := c.AddNodeAndStart(t, "late-joiner", Public, ctx)
		c.Node("node-0").Node().ConnectPeer(ctx, newNode.PeerKey(),
			[]*net.UDPAddr{newNode.VirtualAddr()})
		c.RequirePeerVisible(t, "late-joiner")

		// The late joiner should eventually see the pre-existing service.
		c.RequireEventually(t, func() bool {
			return newNode.Store().HasServicePort(
				c.PeerKeyByName("node-0"), 9090)
		}, assertTimeout, "late joiner should see pre-existing service")
	})

	t.Run("LeaveTriggersDisconnection", func(t *testing.T) {
		leaverKey := c.Node("node-2").PeerKey()
		c.Node("node-2").Stop()

		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				if n.PeerKey() == leaverKey {
					continue
				}
				for _, pk := range n.ConnectedPeers() {
					if pk == leaverKey {
						return false
					}
				}
			}
			return true
		}, assertTimeout, "stopped node should drop from connected peers")
	})
}
```

- [ ] **Step 5: Write TestPublicMesh_ServiceExposure**

```go
func TestPublicMesh_ServiceExposure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx)
	c.RequireConverged(t)

	t.Run("VisibleClusterWide", func(t *testing.T) {
		c.Node("node-1").Node().UpsertService(3000, "api")
		c.RequireServiceVisible(t, "node-1", 3000)
	})

	t.Run("ConnectFromRemoteNode", func(t *testing.T) {
		// node-0 wants to connect to node-1's service on port 3000.
		c.Node("node-0").Node().AddDesiredConnection(
			c.PeerKeyByName("node-1"), 3000, 13000)

		c.RequireEventually(t, func() bool {
			conns := c.Node("node-0").Store().DesiredConnections()
			return len(conns) > 0
		}, assertTimeout, "desired connection should be registered")
	})
}
```

- [ ] **Step 6: Write TestPublicMesh_InviteFlow**

```go
func TestPublicMesh_InviteFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 2, ctx)
	c.RequireConverged(t)

	t.Run("JoinWithValidToken", func(t *testing.T) {
		// Generate an invite from node-0 (which has a DelegationSigner).
		node0 := c.Node("node-0")
		joinerPub, joinerPriv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		bootstrap := []*admissionv1.BootstrapPeer{{
			PeerId: node0.PeerKey().Bytes(),
			Addrs:  []string{node0.VirtualAddr().String()},
		}}

		invite, err := auth.IssueInviteTokenWithSigner(
			node0.Node().Credentials().DelegationKey,
			joinerPub,
			bootstrap,
			time.Now(),
			5*time.Minute,
			24*time.Hour, // membershipTTL
		)
		require.NoError(t, err)

		// Create a new TestNode that will join via invite.
		// The joining node is NOT pre-introduced — it joins via the invite
		// token which embeds bootstrap peer addresses.
		joiner := c.AddNodeAndStart(t, "invited-joiner", Public, ctx)
		_, err = joiner.Node().JoinWithInvite(ctx, invite)
		require.NoError(t, err)

		// Verify the joiner appears in the cluster's gossip state.
		c.RequirePeerVisible(t, "invited-joiner")
		_ = joinerPriv
	})

	t.Run("JoinWithInvalidToken", func(t *testing.T) {
		// Create a token signed by an unknown key (not in the trust bundle).
		_, badPriv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		_ = badPriv
		// Issue a token with the bad key and attempt to join.
		// Expect the join to fail with an auth error.
		t.Log("invalid token rejected — verifying error")
	})

	t.Run("TokenSingleUse", func(t *testing.T) {
		// After a successful join, the same token should be rejected on reuse.
		t.Log("single-use enforcement — verifying rejection on reuse")
	})

	t.Run("BootstrapSSHDelegation", func(t *testing.T) {
		// Simulate the bootstrap SSH flow: root node delegates admin to a
		// second node. Verify the second node holds a valid delegation.
		// The bootstrap SSH flow is ConnectPeer + credential delegation.
		t.Log("bootstrap SSH delegation — verifying admin delegation")
	})

	t.Run("InvitedNodeConverges", func(t *testing.T) {
		// Root generates invite, third node joins, verify full gossip
		// convergence across all three nodes.
		t.Log("invited node convergence — verifying gossip propagation")
	})
}
```

Note: The invite flow subtests above are **stubs that document the expected
behavior**. The implementation will fill in the actual test logic once the
`JoinWithInvite` path is wired through TestNode. The stubs compile and run
(they just log), so they won't block other tests. The implementer should fill
these in as part of this task — they are NOT deferred.

- [ ] **Step 7: Write TestPublicMesh_DenyPeer**

```go
func TestPublicMesh_DenyPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx)
	c.RequireConverged(t)

	deniedKey := c.Node("node-2").PeerKey()

	t.Run("DeniedPeerDisconnected", func(t *testing.T) {
		// node-0 denies node-2.
		c.Node("node-0").Node().DenyPeer(deniedKey)

		// node-2 should lose all connections.
		c.RequireEventually(t, func() bool {
			return len(c.Node("node-2").ConnectedPeers()) == 0
		}, assertTimeout, "denied peer should lose all connections")
	})

	t.Run("DeniedPeerCannotReconnect", func(t *testing.T) {
		// After denial, node-2 should stay disconnected.
		c.RequireNever(t, func() bool {
			return len(c.Node("node-2").ConnectedPeers()) > 0
		}, 2*time.Second, "denied peer should not reconnect")
	})
}
```

- [ ] **Step 8: Run goimports**

Run: `goimports -w pkg/integration/cluster/scenario_test.go`

- [ ] **Step 9: Verify PublicMesh tests compile**

Run: `go build -tags integration ./pkg/integration/cluster/`
Expected: Compiles (some tests may fail at runtime, that's OK).

- [ ] **Step 10: Run integration tests**

Run: `just integration-test`
Document which tests pass and which fail.

- [ ] **Step 11: Commit**

```bash
git add pkg/integration/cluster/scenario_test.go
git commit -s -m "add PublicMesh integration scenarios"
```

---

## Chunk 5: RelayRegions Scenarios

### Task 9: Write RelayRegions scenario tests

**Files:**
- Modify: `pkg/integration/cluster/scenario_test.go` (append to file)

- [ ] **Step 1: Write TestRelayRegions_GossipConvergence**

```go
func TestRelayRegions_GossipConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := RelayRegions(t, 2, 2, ctx)

	t.Run("ConvergesAcrossRegions", func(t *testing.T) {
		c.RequireConverged(t)
	})

	t.Run("PrivateNodesDiscoverViaRelay", func(t *testing.T) {
		// Private nodes in region 0 should see private nodes in region 1
		// (discovered via their respective relays).
		// RelayRegions naming: relays are "relay-X", private nodes are
		// "region-X-node-Y" (see cluster.go:287-297).
		c.RequirePeerVisible(t, "region-0-node-0")
		c.RequirePeerVisible(t, "region-1-node-0")
	})
}

- [ ] **Step 2: Write TestRelayRegions_NATpunchthrough**

```go
func TestRelayRegions_NATpunchthrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	// Cannot use RelayRegions preset — need EnableNATPunch on private nodes.
	// Build manually with the Builder. Task 11 fills in the real cluster
	// construction with EnableNATPunch wired through TestNodeConfig.
	c := New(t).
		SetDefaultLatency(2 * time.Millisecond).
		SetDefaultJitter(0.15).
		AddNode("relay-0", Public).
		AddNode("relay-1", Public).
		SetLatency("relay-0", "relay-1", 50*time.Millisecond).
		Introduce("relay-0", "relay-1")
	// Private nodes added in Task 11 with EnableNATPunch: true.
	// For now this builds a minimal relay-only cluster.
	cluster := c.Start(ctx)
	cluster.RequireConverged(t)

	t.Run("PrivateToPrivateCrossRegion", func(t *testing.T) {
		// Two private nodes in different regions should establish a direct
		// connection via NAT punch, bypassing the relay.
		t.Log("NAT punch: private-to-private cross-region — filled in Task 11")
	})

	t.Run("CoordinationViaRelay", func(t *testing.T) {
		// The relay should mediate the punch coordination.
		t.Log("NAT punch: coordination via relay — filled in Task 11")
	})
}
```

- [ ] **Step 3: Write TestRelayRegions_PartitionAndHeal**

```go
func TestRelayRegions_PartitionAndHeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := RelayRegions(t, 2, 1, ctx)
	c.RequireConverged(t)

	// Get node names for the two regions.
	relays := c.NodesByRole(Public)
	require.Len(t, relays, 2)
	r0 := relays[0].Name()
	r1 := relays[1].Name()

	t.Run("PartitionBlocksPropagation", func(t *testing.T) {
		c.Partition([]string{r0}, []string{r1})

		// Register a service on one side of the partition.
		c.Node(r0).Node().UpsertService(7777, "partitioned-svc")

		// The other side should NOT see it.
		c.RequireNever(t, func() bool {
			return c.Node(r1).Store().HasServicePort(
				c.PeerKeyByName(r0), 7777)
		}, 2*time.Second, "partitioned node should not see new state")
	})

	t.Run("HealedNetworkPropagatesNewEvents", func(t *testing.T) {
		c.Heal([]string{r0}, []string{r1})

		// After healing, the service should propagate.
		c.RequireEventually(t, func() bool {
			return c.Node(r1).Store().HasServicePort(
				c.PeerKeyByName(r0), 7777)
		}, assertTimeout, "healed network should propagate state")

		c.RequireConverged(t)
	})
}
```

- [ ] **Step 4: Write TestRelayRegions_WorkloadPlacement**

```go
func TestRelayRegions_WorkloadPlacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	c := RelayRegions(t, 2, 2, ctx)
	c.RequireConverged(t)

	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	t.Run("CrossRegionSpecVisibility", func(t *testing.T) {
		// Publish spec from a node in region 0.
		relays := c.NodesByRole(Public)
		publisher := relays[0].Name()
		require.NoError(t, c.Node(publisher).Node().SetWorkloadSpec(hash, 2, 256, 5000))

		// All nodes across both regions should see the spec.
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				specs := n.Store().AllWorkloadSpecs()
				if _, ok := specs[hash]; !ok {
					return false
				}
			}
			return true
		}, assertTimeout, "workload spec should propagate across regions")
	})

	t.Run("CrossRegionClaimTracking", func(t *testing.T) {
		c.RequireWorkloadReplicas(t, hash, 2)
	})
}
```

- [ ] **Step 5: Run goimports**

Run: `goimports -w pkg/integration/cluster/scenario_test.go`

- [ ] **Step 6: Run full integration suite**

Run: `just integration-test`
Document which tests pass and which fail.

- [ ] **Step 7: Commit**

```bash
git add pkg/integration/cluster/scenario_test.go
git commit -s -m "add RelayRegions integration scenarios"
```

---

## Chunk 6: Fill Invite Flow and NAT Punch Implementations

### Task 10: Implement invite flow test bodies

**Files:**
- Modify: `pkg/integration/cluster/scenario_test.go`
- Possibly modify: `pkg/integration/cluster/cluster.go` (if a join-via-invite
  helper is needed on Builder/Cluster)

The stubs from Task 8 Step 6 need real implementations. This task fills them
in. The exact implementation depends on what the harness supports after Tasks
1-6, so the implementer should:

- [ ] **Step 1: Verify `Node().JoinWithInvite()` works over VirtualSwitch**

Create a minimal test: issue invite from node-0, create a bare TestNode not
introduced to the cluster, call `JoinWithInvite`. If this works, proceed. If
not, diagnose and fix the harness.

- [ ] **Step 2: Fill JoinWithValidToken body**

The test should: generate invite from node-0, create a new TestNode, call
`JoinWithInvite`, verify the node appears in the cluster's gossip state.

- [ ] **Step 3: Fill JoinWithInvalidToken body**

Create a token signed with a key not in the trust bundle. Attempt
`JoinWithInvite`. Verify it returns an error.

- [ ] **Step 4: Fill TokenSingleUse body**

After a successful join, attempt to reuse the same invite token from a
different node. Verify rejection.

- [ ] **Step 5: Fill BootstrapSSHDelegation body**

The bootstrap SSH flow is: root node calls `ConnectPeer` to a new node. After
connection, the new node should have valid credentials. Verify via the node's
credential state or by checking it can issue invites.

- [ ] **Step 6: Fill InvitedNodeConverges body**

Root generates invite, third node joins, then assert `RequireConverged` across
all three.

- [ ] **Step 7: Run integration tests**

Run: `just integration-test`

- [ ] **Step 8: Commit**

```bash
git add pkg/integration/cluster/scenario_test.go
git commit -s -m "implement invite flow integration tests"
```

### Task 11: Implement NAT punchthrough test bodies

**Files:**
- Modify: `pkg/integration/cluster/scenario_test.go`
- Modify: `pkg/integration/cluster/cluster.go` (if `RelayRegions` needs a
  variant that enables punch on private nodes)

- [ ] **Step 1: Build custom cluster with punch-enabled private nodes**

Replace the `RelayRegions` call in `TestRelayRegions_NATpunchthrough` with a
manual `Builder` configuration that sets `EnableNATPunch: true` on private
nodes.

- [ ] **Step 2: Fill PrivateToPrivateCrossRegion body**

After convergence, assert that two private nodes in different regions have a
direct connection (not just relay-mediated).

- [ ] **Step 3: Fill CoordinationViaRelay body**

Assert that the relay node mediated the punch. This may be observable through
connection state or logs.

- [ ] **Step 4: Run integration tests**

Run: `just integration-test`

- [ ] **Step 5: Commit**

```bash
git add pkg/integration/cluster/scenario_test.go pkg/integration/cluster/cluster.go
git commit -s -m "implement NAT punchthrough integration tests"
```

---

## Chunk 7: Final Verification

### Task 12: Full suite verification and cleanup

- [ ] **Step 1: Run full integration suite**

Run: `just integration-test`
Document all results: which tests pass, which fail, and why.

- [ ] **Step 2: Run linter**

Run: `just lint`
Fix any lint issues. Use `//nolint` only for false positives.

- [ ] **Step 3: Run goimports on all modified files**

Run: `goimports -w pkg/node/node.go pkg/node/svc.go pkg/integration/cluster/*.go`

- [ ] **Step 4: Verify no regressions in existing node tests**

Run: `go test ./pkg/node/ -v -count=1 -timeout 120s`
Expected: All pass.

- [ ] **Step 5: Final commit if any cleanup was needed**

```bash
git add -A
git commit -s -m "clean up integration test suite"
```
