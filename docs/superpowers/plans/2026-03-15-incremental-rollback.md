# Incremental Rollback Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Roll back to v0.1.0-alpha.128 and port the integration test harness to 128's API surface, producing a stable, tested baseline for future incremental migrations.

**Architecture:** Restore the 128 tree in a single commit (preserving refactor history). Then adapt the integration test harness — VirtualSwitch, ClusterAuth, TestNode, Builder, Assertions, Scenarios — to target 128's `pkg/node`, `pkg/mesh`, `pkg/store`, `pkg/peer`, and `pkg/auth` APIs. The key change needed in 128's code is a `PacketConn` injection seam in the mesh so VirtualSwitch can replace real UDP.

**Tech Stack:** Go, QUIC (quic-go), ed25519, protobuf, testify

---

## Chunk 1: Rollback and Release

### Task 1: Roll back to v0.1.0-alpha.128 baseline

**Files:**
- Modify: entire tree (restore to 128 state)

- [ ] **Step 1: Restore the 128 tree**

```bash
git checkout v0.1.0-alpha.128 -- .
```

This restores all tracked files to their 128 versions.

- [ ] **Step 2: Remove files added after 128**

Files added by the refactor (e.g. `pkg/orchestrator/`, `pkg/integration/`, `pkg/control/`, `pkg/state/`, `pkg/transport/`, `pkg/overlay/`, `pkg/identity/`, `pkg/metrics/`, `pkg/traces/`, `pkg/coord/`, `pkg/runtime/`, `cmd/pln/up.go`, `cmd/pln/dir.go`, `docs/superpowers/`, `docs/*.md`, etc.) will linger as untracked or staged-for-delete files.

```bash
git diff --name-only --diff-filter=A v0.1.0-alpha.128..HEAD | xargs git rm -rf --
```

Verify no ghost files remain:

```bash
git status
```

Everything should be staged (green). No untracked files from the refactor should remain. The `docs/superpowers/` directory we just created for the spec and plan should be re-added — see step 3.

- [ ] **Step 3: Re-add the rollback spec and this plan**

The rollback wiped our spec and plan files. Re-add them:

```bash
git checkout HEAD -- docs/superpowers/
```

- [ ] **Step 4: Verify the tree matches 128 (plus our docs)**

```bash
# Should show only our docs/ additions relative to 128
git diff --stat v0.1.0-alpha.128 -- . ':!docs/superpowers'
```

Expected: no output (tree matches 128 outside `docs/superpowers/`).

- [ ] **Step 5: Verify it builds and lint passes**

```bash
go build ./...
just lint
```

Expected: clean build, no errors.

- [ ] **Step 6: Verify tests pass**

```bash
go test ./...
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -s -m "roll back to v0.1.0-alpha.128 baseline"
```

### Task 2: Ship alpha release

This ships the 128 baseline — code that was already proven stable at its original release. The purpose is to give the user a working binary while the harness port proceeds.

- [ ] **Step 1: Tag and push**

Use `/shipit alpha` to ship. This will determine the next alpha version from existing tags, tag, push, and trigger the release workflow.

---

## Chunk 2: Integration Test Harness — Foundation

### Task 3: Add PacketConn injection seam to mesh

At 128, `mesh.Start()` creates its own UDP listener via `net.ListenUDP`. The integration test harness needs to inject a `VirtualPacketConn` instead. This is the minimal change needed to make 128 testable with in-process networking.

**Files:**
- Modify: `pkg/mesh/mesh.go`
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Add PacketConn field to mesh impl**

In `pkg/mesh/mesh.go`, add an optional `injectedConn` field to the `impl` struct and a setter on the `Mesh` interface:

```go
// In Mesh interface, add:
SetPacketConn(conn net.PacketConn)

// In impl struct, add:
injectedConn net.PacketConn

// Implement setter:
func (m *impl) SetPacketConn(conn net.PacketConn) {
    m.injectedConn = conn
}
```

- [ ] **Step 2: Use injected conn in Start()**

In `mesh.Start()`, replace the hard-coded `net.ListenUDP` with:

```go
func (m *impl) Start(ctx context.Context) error {
    var conn net.PacketConn
    if m.injectedConn != nil {
        conn = m.injectedConn
    } else {
        udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
        if err != nil {
            return fmt.Errorf("listen udp:%d: %w", m.port, err)
        }
        conn = udpConn
    }

    qt := &quic.Transport{Conn: conn}
    // ... rest unchanged
}
```

Also update the port derivation to use `conn.LocalAddr()` when using an injected conn, so `ListenPort()` returns the correct value.

- [ ] **Step 3: Thread PacketConn through node.Config**

In `pkg/node/node.go`, add `PacketConn net.PacketConn` to the `Config` struct. In `node.New()`, after `mesh.NewMesh()` returns but before any other setup, call:

```go
if conf.PacketConn != nil {
    m.SetPacketConn(conf.PacketConn)
}
```

The ordering matters: `SetPacketConn` must be called between `mesh.NewMesh()` (which creates the impl) and `mesh.Start()` (which creates the UDP listener). At 128, `NewMesh()` is called in `node.New()` and `Start()` is called in `node.Start()`, so injecting in `node.New()` is correct.

- [ ] **Step 4: Handle the sock/NAT-punch path**

At 128, `mesh.dialPunch()` creates separate UDP sockets via the `sock.SockStore` for NAT traversal. These bypass the injected `PacketConn`. The sock store is initialized inside `mesh.NewMesh()`, not in `node.New()`, so the disable flag must live at the mesh layer.

Add a setter on the `Mesh` interface and impl:

```go
// In Mesh interface, add:
SetDisableNATPunch(disable bool)

// In impl struct, add:
disableNATPunch bool

// Implement setter:
func (m *impl) SetDisableNATPunch(disable bool) {
    m.disableNATPunch = disable
}
```

Then in `mesh.Start()` (or wherever `sock.NewSockStore()` is called), guard it:

```go
if !m.disableNATPunch {
    m.sockStore = sock.NewSockStore()
    // ... existing sock store wiring
}
```

Thread this through `node.Config` the same way as `PacketConn`:

```go
// In node.Config:
DisableNATPunch bool

// In node.New(), after mesh.NewMesh() returns:
if conf.DisableNATPunch {
    m.SetDisableNATPunch(true)
}
```

- [ ] **Step 5: Verify it builds, lint passes, and existing tests still pass**

```bash
go build ./...
just lint
go test ./...
```

Expected: no regressions. The injection seam is unused in production — behavior is identical when `PacketConn` is nil.

- [ ] **Step 6: Commit**

```bash
git add pkg/mesh/mesh.go pkg/node/node.go
git commit -s -m "add PacketConn injection seam to mesh for testing"
```

### Task 4: Port VirtualSwitch

The VirtualSwitch is transport-layer agnostic — it implements `net.PacketConn` and simulates a network with configurable latency, jitter, NAT gating, and partitions. It has no dependencies on any pollen package and can be ported nearly verbatim.

**Files:**
- Create: `pkg/integration/cluster/switch.go`
- Create: `pkg/integration/cluster/switch_test.go`

- [ ] **Step 1: Copy VirtualSwitch from refactored code**

The VirtualSwitch code exists in git history. Extract it:

```bash
mkdir -p pkg/integration/cluster
git show v0.1.0-alpha.139:pkg/integration/cluster/switch.go > pkg/integration/cluster/switch.go
git show v0.1.0-alpha.139:pkg/integration/cluster/switch_test.go > pkg/integration/cluster/switch_test.go
```

- [ ] **Step 2: Fix import paths if needed**

The only pollen import in `switch.go` should be the `NodeRole` type (Public/Private), which is defined in the same package. Verify no imports reference refactored packages. If `identity.PeerKey` is referenced, replace with `types.PeerKey`:

```go
// Replace any occurrence of:
"github.com/sambigeara/pollen/pkg/identity"
// With:
"github.com/sambigeara/pollen/pkg/types"
```

If `types.PeerKey` doesn't exist at 128 (it may be defined elsewhere), check `pkg/types/types.go` for the `PeerKey` type definition. The VirtualSwitch may not use PeerKey at all — it operates on `net.Addr` values. Verify by reading the extracted file.

- [ ] **Step 3: Add build tag**

Ensure both files have `//go:build integration` at the top.

- [ ] **Step 4: Verify switch tests pass and lint is clean**

```bash
go test -tags integration ./pkg/integration/cluster/ -run TestVirtualSwitch -v
go test -tags integration ./pkg/integration/cluster/ -run TestVirtualPacketConn -v
just lint
```

Expected: all VirtualSwitch and VirtualPacketConn tests pass, lint clean.

- [ ] **Step 5: Commit**

```bash
git add pkg/integration/cluster/
git commit -s -m "port VirtualSwitch from refactored integration harness"
```

### Task 5: Port ClusterAuth

ClusterAuth generates root credentials and mints per-node TLS + delegation certs. It needs to target `pkg/auth` instead of `pkg/crypto`.

**Files:**
- Create: `pkg/integration/cluster/auth.go`
- Create: `pkg/integration/cluster/auth_test.go`

- [ ] **Step 1: Extract ClusterAuth from refactored code**

```bash
git show v0.1.0-alpha.139:pkg/integration/cluster/auth.go > pkg/integration/cluster/auth.go
git show v0.1.0-alpha.139:pkg/integration/cluster/auth_test.go > pkg/integration/cluster/auth_test.go
```

- [ ] **Step 2: Update imports**

Replace `polcrypto "github.com/sambigeara/pollen/pkg/crypto"` with `"github.com/sambigeara/pollen/pkg/auth"`. Replace `identity.PeerKey` with `types.PeerKey`.

- [ ] **Step 3: Resolve TLS cert generation**

At 128, there is no `auth.MakeTLSCert()`. TLS certificate generation lives in `pkg/mesh` (likely `mesh.GenerateIdentityCert` or similar). Check:

```bash
grep -r 'func.*TLS\|func.*tls\|func.*Cert.*tls\|GenerateIdentity' pkg/mesh/ pkg/auth/
```

The ClusterAuth `NodeCredentials()` method needs to return a TLS cert. Either:
- (a) Import and call the mesh function that generates TLS certs, or
- (b) Inline the TLS cert generation logic (it's typically ~10 lines of x509 self-signed cert creation)

Option (a) is preferred if the function exists and is exported.

- [ ] **Step 4: Resolve DelegationSigner construction**

At 128, `auth.NodeCredentials` has a `DelegationKey *DelegationSigner` field. The refactored harness didn't need this because the refactored `NodeCredentials` struct was simpler.

Approach: Use `auth.EnsureLocalRootCredentials(tmpDir, pub, now, membershipTTL, delegationTTL)` for the root node — this creates a full `NodeCredentials` including the `DelegationSigner`. For member nodes, use `auth.EnsureNodeCredentialsFromToken(tmpDir, pub, joinToken, now)`.

This means `ClusterAuth` should work as follows:
1. `NewClusterAuth()` creates root ed25519 key, calls `auth.EnsureLocalRootCredentials()` into a temp dir to get root `NodeCredentials` (including DelegationSigner)
2. `NodeCredentials(pub)` for member nodes: issues a join token from root creds, then calls `auth.EnsureNodeCredentialsFromToken()` into a temp dir

This mirrors how the real system works and avoids needing to construct `DelegationSigner` directly.

- [ ] **Step 5: Add build tag and verify**

```bash
go test -tags integration ./pkg/integration/cluster/ -run TestClusterAuth -v
```

Expected: credential generation and verification tests pass.

- [ ] **Step 6: Run lint**

```bash
just lint
```

- [ ] **Step 7: Commit**

```bash
git add pkg/integration/cluster/auth.go pkg/integration/cluster/auth_test.go
git commit -s -m "port ClusterAuth for integration test harness"
```

---

## Chunk 3: Integration Test Harness — Nodes and Scenarios

### Task 6: Port TestNode

TestNode wraps a full node stack. This is the main adaptation point — wiring VirtualSwitch + ClusterAuth into `node.New` at 128.

**Files:**
- Create: `pkg/integration/cluster/node.go`
- Create: `pkg/integration/cluster/node_test.go`

- [ ] **Step 1: Extract TestNode from refactored code**

```bash
git show v0.1.0-alpha.139:pkg/integration/cluster/node.go > pkg/integration/cluster/node.go
git show v0.1.0-alpha.139:pkg/integration/cluster/node_test.go > pkg/integration/cluster/node_test.go
```

- [ ] **Step 2: Define the TestNode struct with 128 types**

Replace the struct fields to use 128's types:

```go
type TestNode struct {
    node    *node.Node
    store   *store.Store
    peerKey types.PeerKey
    role    NodeRole
    addr    *net.UDPAddr
    name    string
    cancel  context.CancelFunc
}
```

Accessors return 128 types: `Node() *node.Node`, `Store() *store.Store`, `PeerKey() types.PeerKey`.

- [ ] **Step 3: Generate keypair and get credentials**

```go
pub, priv, err := ed25519.GenerateKey(nil)
require.NoError(t, err)
creds := cfg.Auth.NodeCredentials(pub)
```

`NodeCredentials` was resolved in Task 5 to return `*auth.NodeCredentials` with the correct `DelegationSigner`.

- [ ] **Step 4: Create state store**

```go
tmpDir := t.TempDir()
stateStore, err := store.Load(tmpDir, pub)
require.NoError(t, err)
t.Cleanup(func() { stateStore.Close() })
```

Note: `store.Load` takes `[]byte` for `identityPub` — `ed25519.PublicKey` is `[]byte` so this compiles. `store.Load` creates on-disk state via `openDisk`; this works with `t.TempDir()` because `openDisk` creates the file if absent.

- [ ] **Step 5: Get VirtualPacketConn and create node**

```go
vconn := cfg.Switch.Conn(cfg.Addr)

nodeConf := &node.Config{
    Port:            cfg.Addr.Port,
    PacketConn:      vconn,
    DisableNATPunch: true,
    PeerTickInterval: 1 * time.Second,
    GossipInterval:   1 * time.Second,
}
n, err := node.New(nodeConf, priv, creds, stateStore, peer.NewStore(), tmpDir)
require.NoError(t, err)
```

- [ ] **Step 6: Start node and wait for ready**

```go
ctx, cancel := context.WithCancel(cfg.Ctx)
t.Cleanup(cancel)
go func() { _ = n.Start(ctx) }()

select {
case <-n.Ready():
case <-time.After(5 * time.Second):
    t.Fatal("node did not become ready")
}
```

- [ ] **Step 7: Verify node test passes and lint is clean**

```bash
go test -tags integration ./pkg/integration/cluster/ -run TestTestNode -v
just lint
```

- [ ] **Step 8: Commit**

```bash
git add pkg/integration/cluster/node.go pkg/integration/cluster/node_test.go
git commit -s -m "port TestNode for integration test harness"
```

### Task 7: Port Builder, Cluster, and Assertions

**Files:**
- Create: `pkg/integration/cluster/cluster.go`
- Create: `pkg/integration/cluster/assert.go`
- Create: `pkg/integration/cluster/cluster_test.go`

- [ ] **Step 1: Extract and adapt**

```bash
git show v0.1.0-alpha.139:pkg/integration/cluster/cluster.go > pkg/integration/cluster/cluster.go
git show v0.1.0-alpha.139:pkg/integration/cluster/assert.go > pkg/integration/cluster/assert.go
git show v0.1.0-alpha.139:pkg/integration/cluster/cluster_test.go > pkg/integration/cluster/cluster_test.go
```

- [ ] **Step 2: Update Builder to use 128 types**

Replace `identity.PeerKey` → `types.PeerKey`. Replace `orchestrator.*` / `transport.*` references with `node.*` equivalents.

The `Introduce` method needs adaptation. At 128, bootstrap peers are configured via `node.Config.BootstrapPeers` (a `[]BootstrapPeer` slice set before `node.New`), not via runtime `ConnectPeer()` calls. The Builder should collect introductions and pass them into `TestNodeConfig` so `NewTestNode` can include them in `node.Config.BootstrapPeers`.

- [ ] **Step 3: Update Assertions to use 128 state queries**

Map each assertion to 128's API:

| Assertion | 128 API |
|---|---|
| `RequireConverged` (all nodes see all peers) | Query `store.KnownPeers()` or equivalent — check `pkg/store/gossip.go` for the method that lists known peer keys |
| `RequireHealthy` (≥1 connected peer) | `node.GetConnectedPeers()` |
| `RequireConnectedPeers` (≥N connections) | `node.GetConnectedPeers()` with len check |
| `RequireWorkloadReplicas` | Store workload query — check `pkg/store/gossip.go` for workload methods |
| `RequireWorkloadRunningOn` | Same |

- [ ] **Step 4: Verify builder and assertion tests pass**

```bash
go test -tags integration ./pkg/integration/cluster/ -run TestCluster -v
```

- [ ] **Step 5: Run lint**

```bash
just lint
```

- [ ] **Step 6: Commit**

```bash
git add pkg/integration/cluster/cluster.go pkg/integration/cluster/assert.go pkg/integration/cluster/cluster_test.go
git commit -s -m "port Builder, Cluster, and Assertions for integration harness"
```

### Task 8: Port Scenario Tests

**Files:**
- Create: `pkg/integration/cluster/scenario_test.go`

- [ ] **Step 1: Extract scenarios**

```bash
git show v0.1.0-alpha.139:pkg/integration/cluster/scenario_test.go > pkg/integration/cluster/scenario_test.go
```

- [ ] **Step 2: Adapt API calls**

Each scenario creates a cluster via `PublicMesh()` or `RelayRegions()`, then runs assertions. The presets and assertions are already ported. The main adaptations:

- Service registration/removal: use 128's `store.UpsertLocalService()` / `store.RemoveLocalServices()`
- Workload operations: use 128's workload APIs from `store`
- Peer connection queries: use `node.GetConnectedPeers()`
- State mutations for testing: use 128's store mutation methods

- [ ] **Step 3: Start with the simplest scenario**

Get `TestPublicMesh_GossipConvergence` compiling and passing first. This exercises the core path: nodes start, discover each other, gossip converges. If this works, the infrastructure is sound.

- [ ] **Step 4: Progressively enable remaining scenarios**

Work through scenarios in order of complexity:
1. `TestPublicMesh_GossipConvergence` (core gossip)
2. `TestPublicMesh_TopologyStabilization` (vivaldi + peer selection)
3. `TestPublicMesh_NodeJoinLeave` (dynamic membership)
4. `TestPublicMesh_ServiceExposure` (service discovery)
5. `TestPublicMesh_WorkloadSeeding` (workload placement)
6. `TestRelayRegions_GossipConvergence` (multi-hop)
7. `TestRelayRegions_WorkloadPlacement` (cross-region workloads)

- [ ] **Step 5: Skip NAT-punch scenarios**

The following scenarios require the sock/NAT-punch path which bypasses VirtualSwitch (see Task 3, Step 4). Skip them for now:

```go
t.Skip("requires sock store injection for VirtualSwitch NAT — future migration task")
```

- `TestRelayRegions_NATpunchthrough`
- `TestRelayRegions_PartitionAndHeal` (if it exercises NAT punch; otherwise adapt it)

These become natural migration targets when the sock seam is added.

- [ ] **Step 6: Run full integration suite**

```bash
go test -tags integration ./pkg/integration/cluster/ -v -timeout 120s
```

- [ ] **Step 7: Commit**

```bash
git add pkg/integration/cluster/scenario_test.go
git commit -s -m "port scenario tests for integration harness"
```

### Task 9: Final verification and release

- [ ] **Step 1: Run full test suite**

```bash
go build ./...
just lint
go test ./...
go test -tags integration ./pkg/integration/cluster/ -v -timeout 120s
```

All must pass (skipped NAT-punch scenarios are acceptable).

- [ ] **Step 2: Commit any remaining fixes**

- [ ] **Step 3: Ship release**

Use `/shipit alpha` to tag and release.
