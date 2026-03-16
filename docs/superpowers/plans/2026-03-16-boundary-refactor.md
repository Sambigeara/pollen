# Boundary Refactor Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor component boundaries (store, mesh, node orchestrator, control plane) to enforce one-owner-per-goroutine, value-type interfaces, and blocking backpressure — without any behavioral regression.

**Architecture:** Four phases applied incrementally (store → mesh → node → control plane). Each phase gates on integration tests, mutation tests, lint, and a full Hetzner cluster verification before proceeding.

**Tech Stack:** Go 1.23, QUIC (quic-go), Protobuf (vtprotobuf), Zap logging, Prometheus metrics, testify, Extism WASM runtime

**Spec:** `docs/superpowers/specs/2026-03-16-boundary-refactor-design.md`

---

## Critical Implementation Notes

**Read these before implementing any task.** The plan provides structural guidance and interface definitions. The implementer MUST read actual source files to get correct field names, method signatures, and import paths. Key issues:

1. **Field names in `nodeRecord`** (`pkg/store/gossip.go`): The plan's `NodeView` struct uses placeholder names. Read the actual `nodeRecord` struct and map fields exactly. Key mismatches to watch for: `LocalPort` (not `Port`), `Reachable` (not `Connected`, and is `map[PeerKey]struct{}` not `map[PeerKey]bool`), `NatType` (is `nat.Type` not `uint32`). Also include `ObservedExternalIP string`.

2. **`WorkloadSpecView`**: The existing type is `{Spec *statev1.WorkloadSpecChange, Publisher types.PeerKey}`. Use it as-is in the snapshot — don't redefine it.

3. **`KnownPeer` fields**: The field is `PeerID types.PeerKey` (not `PeerKey`). Include all fields: `PeerID`, `IPs`, `Port`, `ExternalPort`, `ObservedExternalIP`, `PubliclyAccessible`, `NatType`, `LastAddr`, `IdentityPub`, `VivaldiCoord`.

4. **Key generation in tests**: There is no `types.GenerateKeyPair()`. Use `crypto/ed25519.GenerateKey(nil)` to get `(pub, priv, err)`, then `types.PeerKeyFromBytes(pub)`.

5. **Snapshot filtering**: `Snapshot.PeerKeys` must replicate the `AllPeerKeys()` logic (valid nodes + live component BFS). `Snapshot.Claims` must replicate `AllWorkloadClaims()` filtering (only live-component nodes). `Snapshot.Specs` must replicate `AllWorkloadSpecs()` conflict resolution (lowest PeerKey wins). Read these methods and mirror their logic in `snapshotLocked()`.

6. **Import paths for proto types**: The proto message types are in `api/genpb/pollen/*/v1/` packages (e.g., `meshv1`, `statev1`, `admissionv1`). The `*connect` suffixed packages are for Connect-Go service stubs. Use the non-connect packages for message types.

7. **`broadcastEvents` requires `ctx`**: The actual signature is `func (n *Node) broadcastEvents(ctx context.Context, events []*statev1.GossipEvent)`. Pass `ctx` from the select loop.

8. **`localPeerEvents` cannot be fully eliminated**: This channel carries internal feedback events (`ConnectFailed`, `RetryPeer`) that originate inside the node, not from the mesh or store. Keep it as a node-internal feedback channel from worker pool tasks. The simplified select loop should retain a case for it.

9. **DenyApplied handler must replicate full callback behavior**: The current `OnDenyPeer` callback (lines 291-303) does: log, `tun.DisconnectPeer`, `mesh.ClosePeerSession`, `store.RemoveDesiredConnection`, and sends `PeerDisconnected` + `ForgetPeer` to `localPeerEvents`. The event handler must do all of these.

10. **Task ordering for event migration**: Start consuming store events in the select loop (Task 7 Step 1) BEFORE removing `queueGossipEvents` (Task 7 Step 4). Make `emitEvent` blocking (Task 8) BEFORE removing the old gossip event channel. This prevents an event-drop window.

11. **Worker pool must drain on shutdown**: After `ctx.Done()` in `run()`, drain the `work` channel before returning to prevent silently abandoned tasks.

12. **`StreamType` doesn't exist as a type**: Stream type constants are raw `byte` values in `pkg/mesh/mesh.go` (lines 41-45). Either define `type StreamType byte` in mesh or use `byte` consistently in the Transport interface.

---

## File Map

### New files

| File | Purpose |
|------|---------|
| `docs/behavioral-inventory.md` | Captured baseline of all observable behavior |
| `pkg/store/snapshot.go` | Immutable snapshot type + StoreEvent sum type |
| `pkg/store/snapshot_test.go` | Snapshot correctness and event emission tests |
| `pkg/node/transport.go` | Consumer-defined Transport interface |
| `pkg/node/workers.go` | Bounded worker pool for ephemeral goroutines |
| `pkg/node/workers_test.go` | Worker pool tests |
| `infra/scripts/verify-hetzner.sh` | Automated Hetzner cluster verification |

### Modified files

| File | Changes |
|------|---------|
| `pkg/store/gossip.go` | Add internal goroutine loop, Snapshot(), Events(); remove RWMutex from public API; remove callback registration |
| `pkg/store/disk.go` | Move persistence into store's goroutine loop |
| `pkg/scheduler/reconciler.go` | Evolve SchedulerStore interface to snapshot-based reads |
| `pkg/scheduler/reconciler_test.go` | Adapt mock to new interface |
| `pkg/node/node.go` | Use Transport/StateView interfaces; simplified event loop; worker pool; eliminate drop channels |
| `pkg/node/svc.go` | Use narrow interfaces instead of `*Node` |
| `pkg/node/reachability.go` | Use interface instead of `*store.Store` |
| `pkg/node/topology_profile.go` | Use snapshot types |
| `pkg/mesh/mesh.go` | Implement Transport interface; blocking channels; eliminate `streamCh`/`clockStreamCh`/`inCh` drops |
| `pkg/mesh/session_registry.go` | Adapt for blocking peer events |
| `pkg/integration/cluster/node.go` | Adapt TestNode to new interfaces |
| `pkg/integration/cluster/assert.go` | Adapt assertions to snapshot reads |
| `pkg/integration/cluster/cluster.go` | Adapt builder for new node construction |

---

## Chunk 1: Pre-Phase — Behavioral Inventory

### Task 1: Capture CLI command surface

**Files:**
- Create: `docs/behavioral-inventory.md`

- [ ] **Step 1: Generate CLI help inventory**

Run each command with `--help` and capture output:

```bash
commands="init up down join bootstrap invite serve unserve connect disconnect status seed unseed call deny logs admin id purge version"
echo "# Behavioral Inventory" > docs/behavioral-inventory.md
echo "" >> docs/behavioral-inventory.md
echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> docs/behavioral-inventory.md
echo "Baseline: $(git describe --tags --always)" >> docs/behavioral-inventory.md
echo "" >> docs/behavioral-inventory.md
echo "## CLI Command Surface" >> docs/behavioral-inventory.md
echo "" >> docs/behavioral-inventory.md
for cmd in $commands; do
  echo "### pln $cmd" >> docs/behavioral-inventory.md
  echo '```' >> docs/behavioral-inventory.md
  go run ./cmd/pln $cmd --help 2>&1 >> docs/behavioral-inventory.md || true
  echo '```' >> docs/behavioral-inventory.md
  echo "" >> docs/behavioral-inventory.md
done
```

- [ ] **Step 2: Add status output reference**

Append to `docs/behavioral-inventory.md`:

```markdown
## Status Output Format

### `pln status`
Columns: NODE (8-char prefix or full with --wide), STATUS, ADDR, CPUs, CPU, MEM, TUNNELS, LATENCY, TRAFFIC IN, TRAFFIC OUT
Self row shows "(self)" suffix. Status values: "direct", "direct (public)", "relay", "-" (self).

### `pln status --wide`
Same columns, full 64-char node IDs. Additional DIAGNOSTICS section with vivaldi error, vivaldi samples, eager syncs.

### `pln status --all`
Includes all known peers, not just connected ones.

### Color library
Uses `github.com/fatih/color` for terminal colors.
```

- [ ] **Step 3: Add logging inventory**

Identify every log statement with its level and structured fields. Run:

```bash
grep -rn 'n\.log\.\|s\.log\.\|log\.\(Info\|Warn\|Error\|Debug\)' pkg/node/node.go pkg/node/svc.go pkg/mesh/mesh.go pkg/store/gossip.go pkg/scheduler/reconciler.go | head -200
```

Capture the output and add to inventory under `## Logging Inventory` with sections for startup, peer lifecycle, gossip, services, workloads, cert renewal, shutdown.

- [ ] **Step 4: Add metrics inventory**

```bash
grep -rn 'metrics\.\|prometheus\.\|counter\|gauge\|histogram' pkg/observability/metrics/ | head -100
```

Capture under `## Metrics Inventory` — every metric name, type, labels.

- [ ] **Step 5: Add wire format inventory**

Append to inventory:

```markdown
## Wire Format Constants

### Stream type bytes (pkg/mesh/mesh.go)
- 1: clock (gossip digest)
- 2: tunnel (service streams)
- 3: routed (multi-hop forwarded)
- 4: artifact (WASM binary transfer)
- 5: workload (function invocation)

### Proto files
- api/public/pollen/mesh/v1/mesh.proto
- api/public/pollen/state/v1/state.proto
- api/public/pollen/control/v1/control.proto
- api/public/pollen/admission/v1/admission.proto

### On-disk format
- ~/.pln/state.pb (protobuf RuntimeState)
- ~/.pln/config.yaml
- ~/.pln/keys/ed25519.key, ed25519.pub
```

- [ ] **Step 6: Commit inventory**

```bash
git add docs/behavioral-inventory.md
git commit -s -m "add behavioral inventory for boundary refactor baseline"
```

---

## Chunk 2: Phase 1a — Store Snapshot & Event Types

### Task 2: Define snapshot and event types

**Files:**
- Create: `pkg/store/snapshot.go`

- [ ] **Step 1: Create snapshot and event type definitions**

```go
package store

import (
	"github.com/sambigeara/pollen/api/genpb/pollen/state/v1/statev1"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

// Snapshot is an immutable, point-in-time view of the entire cluster state.
// Safe to hold indefinitely without blocking store mutations.
type Snapshot struct {
	LocalID     types.PeerKey
	Nodes       map[types.PeerKey]NodeView
	PeerKeys    []types.PeerKey // live, reachable peers only
	Specs       map[string]WorkloadSpecView
	Claims      map[string]map[types.PeerKey]struct{}
	Placements  map[types.PeerKey]NodePlacementState
	Heatmaps    map[types.PeerKey]map[types.PeerKey]TrafficSnapshot
	Connections []Connection
}

// NodeView is an immutable view of a single node's gossiped state.
type NodeView struct {
	IPs                []string
	Port               uint32
	ExternalPort       uint32
	IdentityPub        []byte
	CertExpiry         int64
	Connected          map[types.PeerKey]bool
	Services           map[string]*statev1.Service
	PubliclyAccessible bool
	NatType            uint32
	Coord              *topology.Coord
	LastAddr           string
	CPUPercent         uint32
	MemPercent         uint32
	MemTotalBytes      uint64
	NumCPU             uint32
}

// StoreEvent is the sum type for all store-emitted events.
// The store sends these on its Events() channel after processing mutations.
type StoreEvent interface {
	storeEvent()
}

// GossipApplied is emitted after the store processes incoming gossip events.
// Rebroadcast contains events that should be forwarded to other peers.
type GossipApplied struct {
	Rebroadcast []*statev1.GossipEvent
}

func (GossipApplied) storeEvent() {}

// LocalMutationApplied is emitted after a local state mutation (SetLocalNetwork, etc.).
// Events contains the gossip events to broadcast.
type LocalMutationApplied struct {
	Events []*statev1.GossipEvent
}

func (LocalMutationApplied) storeEvent() {}

// DenyApplied is emitted when a deny event is processed.
type DenyApplied struct {
	PeerKey types.PeerKey
}

func (DenyApplied) storeEvent() {}

// RouteInvalidated is emitted when reachability or coordinate changes
// require route table recomputation.
type RouteInvalidated struct{}

func (RouteInvalidated) storeEvent() {}

// WorkloadChanged is emitted when workload specs or claims change.
type WorkloadChanged struct{}

func (WorkloadChanged) storeEvent() {}

// TrafficChanged is emitted when traffic heatmap data changes.
type TrafficChanged struct{}

func (TrafficChanged) storeEvent() {}
```

- [ ] **Step 2: Run goimports**

```bash
goimports -w pkg/store/snapshot.go
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./pkg/store/
```

- [ ] **Step 4: Commit**

```bash
git add pkg/store/snapshot.go
git commit -s -m "add store snapshot and event types"
```

---

### Task 3: Add Snapshot() method to store

**Files:**
- Modify: `pkg/store/gossip.go`
- Create: `pkg/store/snapshot_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/store/snapshot_test.go`:

```go
package store

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/types"
)

func TestSnapshot_ReflectsState(t *testing.T) {
	dir := t.TempDir()
	pub, _ := types.GenerateKeyPair()
	s, err := Load(dir, pub[:])
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	events := s.SetLocalNetwork([]string{"10.0.0.1"}, 60611)
	require.NotEmpty(t, events)

	snap := s.Snapshot()
	require.Equal(t, s.LocalID, snap.LocalID)

	localNode, ok := snap.Nodes[snap.LocalID]
	require.True(t, ok)
	require.Equal(t, []string{"10.0.0.1"}, localNode.IPs)
	require.Equal(t, uint32(60611), localNode.Port)
}

func TestSnapshot_IsImmutable(t *testing.T) {
	dir := t.TempDir()
	pub, _ := types.GenerateKeyPair()
	s, err := Load(dir, pub[:])
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	s.SetLocalNetwork([]string{"10.0.0.1"}, 60611)
	snap1 := s.Snapshot()

	s.SetLocalNetwork([]string{"10.0.0.2"}, 60611)
	snap2 := s.Snapshot()

	// snap1 must not be affected by subsequent mutations
	require.Equal(t, []string{"10.0.0.1"}, snap1.Nodes[snap1.LocalID].IPs)
	require.Equal(t, []string{"10.0.0.2"}, snap2.Nodes[snap2.LocalID].IPs)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./pkg/store/ -run TestSnapshot -v
```

Expected: FAIL — `Snapshot()` method not defined.

- [ ] **Step 3: Implement Snapshot() method**

Add to `pkg/store/gossip.go`:

```go
// Snapshot returns an immutable, point-in-time copy of the cluster state.
// The returned Snapshot is safe to hold indefinitely without blocking writes.
func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *Store) snapshotLocked() Snapshot {
	nodes := make(map[types.PeerKey]NodeView, len(s.nodes))
	for pk, nr := range s.nodes {
		connected := make(map[types.PeerKey]bool, len(nr.connected))
		for k, v := range nr.connected {
			connected[k] = v
		}
		services := make(map[string]*statev1.Service, len(nr.services))
		for k, v := range nr.services {
			services[k] = v
		}
		ips := make([]string, len(nr.ips))
		copy(ips, nr.ips)

		var coord *topology.Coord
		if nr.coord != nil {
			c := *nr.coord
			coord = &c
		}

		nodes[pk] = NodeView{
			IPs:                ips,
			Port:               nr.port,
			ExternalPort:       nr.externalPort,
			IdentityPub:        nr.identityPub,
			CertExpiry:         nr.certExpiry,
			Connected:          connected,
			Services:           services,
			PubliclyAccessible: nr.publiclyAccessible,
			NatType:            nr.natType,
			Coord:              coord,
			LastAddr:           nr.lastAddr,
			CPUPercent:         nr.cpuPercent,
			MemPercent:         nr.memPercent,
			MemTotalBytes:      nr.memTotalBytes,
			NumCPU:             nr.numCPU,
		}
	}

	peerKeys := s.allPeerKeysLocked()

	specs := make(map[string]WorkloadSpecView, len(s.localWorkloadSpecs))
	for hash, spec := range s.localWorkloadSpecs {
		specs[hash] = spec
	}
	// Include remote specs from nodes
	for pk, nr := range s.nodes {
		for hash, ws := range nr.workloadSpecs {
			if _, exists := specs[hash]; !exists {
				specs[hash] = WorkloadSpecView{
					Hash:        hash,
					Replicas:    ws.GetReplicas(),
					MemoryPages: ws.GetMemoryPages(),
					TimeoutMs:   ws.GetTimeoutMs(),
					Publisher:   pk,
				}
			}
		}
	}

	claims := make(map[string]map[types.PeerKey]struct{})
	for pk, nr := range s.nodes {
		for hash := range nr.workloadClaims {
			if claims[hash] == nil {
				claims[hash] = make(map[types.PeerKey]struct{})
			}
			claims[hash][pk] = struct{}{}
		}
	}

	placements := s.allNodePlacementStatesLocked()

	heatmaps := make(map[types.PeerKey]map[types.PeerKey]TrafficSnapshot)
	for pk, nr := range s.nodes {
		if len(nr.trafficHeatmap) > 0 {
			pm := make(map[types.PeerKey]TrafficSnapshot, len(nr.trafficHeatmap))
			for target, ts := range nr.trafficHeatmap {
				pm[target] = ts
			}
			heatmaps[pk] = pm
		}
	}

	conns := make([]Connection, len(s.desiredConnections))
	copy(conns, s.desiredConnections)

	return Snapshot{
		LocalID:     s.localID,
		Nodes:       nodes,
		PeerKeys:    peerKeys,
		Specs:       specs,
		Claims:      claims,
		Placements:  placements,
		Heatmaps:    heatmaps,
		Connections: conns,
	}
}
```

Note: The exact field names inside `nodeRecord` (e.g., `nr.ips`, `nr.port`, `nr.connected`) must match the private struct in `gossip.go`. Read the actual `nodeRecord` struct definition and adjust field names accordingly. Run `goimports -w pkg/store/gossip.go` after editing.

- [ ] **Step 4: Run tests**

```bash
go test ./pkg/store/ -run TestSnapshot -v -race
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/store/gossip.go pkg/store/snapshot.go pkg/store/snapshot_test.go
git commit -s -m "add Snapshot() method to store"
```

---

### Task 4: Add Events() channel to store

**Files:**
- Modify: `pkg/store/gossip.go`
- Modify: `pkg/store/snapshot_test.go`

- [ ] **Step 1: Write the failing test**

Add to `pkg/store/snapshot_test.go`:

```go
func TestEvents_EmitsOnApply(t *testing.T) {
	dir := t.TempDir()
	pub1, _ := types.GenerateKeyPair()
	s, err := Load(dir, pub1[:])
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	// Start consuming events
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Generate a local mutation
	s.SetLocalNetwork([]string{"10.0.0.1"}, 60611)

	// Should receive a LocalMutationApplied event
	select {
	case ev := <-s.Events():
		_, ok := ev.(LocalMutationApplied)
		require.True(t, ok, "expected LocalMutationApplied, got %T", ev)
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}
}

func TestEvents_EmitsDenyApplied(t *testing.T) {
	dir := t.TempDir()
	pub1, _ := types.GenerateKeyPair()
	s, err := Load(dir, pub1[:])
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	s.OnDenyPeer(func(pk types.PeerKey) {}) // keep old callback working during migration

	pub2, _ := types.GenerateKeyPair()
	s.DenyPeer(pub2[:])

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Drain events until we find DenyApplied
	for {
		select {
		case ev := <-s.Events():
			if da, ok := ev.(DenyApplied); ok {
				require.Equal(t, types.PeerKey(pub2), da.PeerKey)
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for DenyApplied event")
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./pkg/store/ -run "TestEvents_" -v
```

Expected: FAIL — `Events()` method not defined.

- [ ] **Step 3: Implement Events() channel**

Add an `events` channel to the Store struct and `Events()` accessor. During the migration period, the store **dual-emits** — both old callbacks and new events fire. This lets consumers migrate one at a time.

In `pkg/store/gossip.go`, add to the `Store` struct:

```go
events chan StoreEvent
```

In `Load()`, initialize:

```go
events: make(chan StoreEvent, 64),
```

Add accessor:

```go
// Events returns the channel on which the store emits state change events.
// Sends block with the store's context — consumers must drain this channel.
func (s *Store) Events() <-chan StoreEvent {
	return s.events
}
```

Add internal emit helper:

```go
func (s *Store) emitEvent(ev StoreEvent) {
	select {
	case s.events <- ev:
	default:
		// During migration, don't block if no consumer is listening yet.
		// This default branch will be removed once all consumers migrate.
	}
}
```

Then add `emitEvent` calls after each callback invocation site:
- After deny callback: `s.emitEvent(DenyApplied{PeerKey: pk})`
- After route invalidate callback: `s.emitEvent(RouteInvalidated{})`
- After workload change callback: `s.emitEvent(WorkloadChanged{})`
- After traffic change callback: `s.emitEvent(TrafficChanged{})`
- After each local mutation method (SetLocalNetwork, etc.): `s.emitEvent(LocalMutationApplied{Events: events})`
- After ApplyEvents with rebroadcast: `s.emitEvent(GossipApplied{Rebroadcast: result.Rebroadcast})`

Run `goimports -w pkg/store/gossip.go` after editing.

- [ ] **Step 4: Run tests**

```bash
go test ./pkg/store/ -run "TestEvents_" -v -race
```

Expected: PASS

- [ ] **Step 5: Run full integration tests**

```bash
just test-integration
```

Expected: PASS — dual-emit doesn't break existing behavior.

- [ ] **Step 6: Commit**

```bash
git add pkg/store/gossip.go pkg/store/snapshot_test.go
git commit -s -m "add Events() channel to store with dual-emit during migration"
```

---

### Task 5: Evolve scheduler interface to snapshot-based reads

**Files:**
- Modify: `pkg/scheduler/reconciler.go`
- Modify: `pkg/scheduler/reconciler_test.go`

The scheduler already has a `SchedulerStore` interface (line 25). We evolve it to use snapshots for reads while keeping the mutation method.

- [ ] **Step 1: Update SchedulerStore interface**

In `pkg/scheduler/reconciler.go`, change the interface from:

```go
type SchedulerStore interface {
	AllWorkloadSpecs() map[string]store.WorkloadSpecView
	AllWorkloadClaims() map[string]map[types.PeerKey]struct{}
	AllPeerKeys() []types.PeerKey
	SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent
	AllNodePlacementStates() map[types.PeerKey]store.NodePlacementState
}
```

to:

```go
type SchedulerStore interface {
	Snapshot() store.Snapshot
	SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent
}
```

- [ ] **Step 2: Update reconciler to use snapshot**

In the `reconcile()` method and `startClaim()`, replace individual store calls with snapshot reads:

```go
// Before:
specViews := r.store.AllWorkloadSpecs()
claimMap := r.store.AllWorkloadClaims()
allPeers := r.store.AllPeerKeys()
nodePlacement := r.store.AllNodePlacementStates()

// After:
snap := r.store.Snapshot()
specViews := snap.Specs
claimMap := snap.Claims
allPeers := snap.PeerKeys
nodePlacement := snap.Placements
```

Apply this pattern at every call site in reconciler.go (lines 186, 208-210, 267, 307, 313-314, 359).

Run `goimports -w pkg/scheduler/reconciler.go`.

- [ ] **Step 3: Update test mock**

In `pkg/scheduler/reconciler_test.go`, update the mock store to implement the new interface. Replace individual method mocks with a `Snapshot()` method that returns a `store.Snapshot` populated with the test data.

- [ ] **Step 4: Run scheduler tests**

```bash
go test ./pkg/scheduler/ -v -race
```

Expected: PASS

- [ ] **Step 5: Run integration tests**

```bash
just test-integration
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/scheduler/reconciler.go pkg/scheduler/reconciler_test.go
git commit -s -m "evolve scheduler to snapshot-based reads"
```

---

## Chunk 3: Phase 1b — Consumer Migration & Internal Refactor

### Task 6: Migrate node reads to snapshots

**Files:**
- Modify: `pkg/node/node.go`
- Modify: `pkg/node/svc.go`
- Modify: `pkg/node/reachability.go`
- Modify: `pkg/node/topology_profile.go`

This is the largest migration task. The node has ~60 store read call sites. The approach: replace individual `store.Get()`, `store.AllNodes()`, etc. calls with `store.Snapshot()` reads. Take a snapshot once at the top of each function that needs store data, then read from the snapshot throughout.

- [ ] **Step 1: Identify read-only call sites in node.go**

Read-only store calls in node.go (calls that don't mutate state):
- `store.LocalID` — accessed everywhere. This is a write-once field; keep direct access.
- `store.Get(peerID)` — lines 895, 1094. Replace with `snap.Nodes[peerID]`.
- `store.AllNodes()` — line 1571. Replace with `snap.Nodes`.
- `store.AllPeerKeys()` — not called directly (scheduler uses it).
- `store.KnownPeers()` — line 848. Replace with helper that builds from `snap.Nodes`.
- `store.NodeIPs()` — lines 887, 1098, 1102, 1103. Replace with `snap.Nodes[pk].IPs`.
- `store.IdentityPub()` — line 1303. Replace with `snap.Nodes[pk].IdentityPub`.
- `store.IsConnected()` — not called in node.go.
- `store.HasServicePort()` — lines 1041, 1066. Replace with check on `snap.Nodes[pk].Services`.
- `store.NatType()` — called indirectly via reachability.go.
- `store.IsPubliclyAccessible()` — called via topology.
- `store.PeerVivaldiCoord()` — line 824. Replace with `snap.Nodes[pk].Coord`.
- `store.AllWorkloadClaims()` — line 1263. Replace with `snap.Claims`.
- `store.DesiredConnections()` — lines 1051, 1319. Replace with `snap.Connections`.
- `store.IsDenied()` — lines 484, 1454. Keep as method (it checks the deny list, not node state).
- `store.AllTrafficHeatmaps()` — called in svc.go. Replace with `snap.Heatmaps`.
- `store.GossipMetrics()` — called in svc.go. Keep as method (metrics, not state).
- `store.Clock()`, `store.EagerSyncClock()`, `store.MissingFor()` — gossip protocol methods, keep as methods (they're gossip operations, not state reads).
- `store.LocalEvents()` — gossip method, keep.
- `store.LocalServices()` — keep (or use snapshot).

Pattern for migration: at the start of `tick()`, `syncPeersFromState()`, `reconcileConnections()`, `reconcileDesiredConnections()`, take a snapshot. Pass it through to sub-functions.

- [ ] **Step 2: Migrate tick() and its callees**

In `tick()` (line 792), take a snapshot at the top and pass it down:

```go
func (n *Node) tick(now time.Time) {
	snap := n.store.Snapshot()
	n.updateVivaldiCoords(snap)
	n.syncPeersFromState(snap)
	n.disconnectExpiredPeers(snap)
	n.reconcileConnections(snap)
	n.reconcileDesiredConnections(snap)
	n.peers.Step(now, peer.Tick{})
}
```

Update each callee to accept `snap store.Snapshot` instead of reading from `n.store` directly. For example, in `syncPeersFromState`:

```go
// Before:
knownPeers := n.store.KnownPeers()
localIPs := n.store.NodeIPs(n.store.LocalID)

// After:
knownPeers := knownPeersFromSnapshot(snap)
localIPs := snap.Nodes[snap.LocalID].IPs
```

Write a helper:

```go
func knownPeersFromSnapshot(snap store.Snapshot) []store.KnownPeer {
	var peers []store.KnownPeer
	for pk, nv := range snap.Nodes {
		if len(nv.IPs) == 0 || nv.Port == 0 {
			continue
		}
		peers = append(peers, store.KnownPeer{
			PeerKey:            pk,
			IPs:                nv.IPs,
			Port:               nv.Port,
			ExternalPort:       nv.ExternalPort,
			PubliclyAccessible: nv.PubliclyAccessible,
			NatType:            nv.NatType,
			LastAddr:           nv.LastAddr,
		})
	}
	return peers
}
```

Run `goimports -w pkg/node/node.go`.

- [ ] **Step 3: Migrate svc.go read sites**

In `svc.go`, the `GetStatus()` method (line 99) is the heaviest reader. Take a snapshot at the top:

```go
func (s *NodeService) GetStatus(ctx context.Context, req *connect.Request[...]) (...) {
	snap := s.node.store.Snapshot()
	// Replace all s.node.store.Get(), .AllNodes(), etc. with snap reads
}
```

Similarly for `GetMetrics()` (line 440) and `GetBootstrapInfo()` (line 44).

Run `goimports -w pkg/node/svc.go`.

- [ ] **Step 4: Migrate reachability.go**

Change `rankCoordinators` signature from `state *store.Store` to `snap store.Snapshot`. Update internal reads accordingly.

Run `goimports -w pkg/node/reachability.go`.

- [ ] **Step 5: Run all tests**

```bash
go test ./pkg/node/ -v -race
just test-integration
```

Expected: PASS

- [ ] **Step 6: Run lint**

```bash
just lint
```

Fix any issues. Run `goimports -w` on any edited files.

- [ ] **Step 7: Commit**

```bash
git add pkg/node/node.go pkg/node/svc.go pkg/node/reachability.go pkg/node/topology_profile.go
git commit -s -m "migrate node and svc to snapshot-based store reads"
```

---

### Task 7: Migrate node from callbacks to event channel

**Files:**
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Add store event consumption to the select loop**

In `node.Start()`, add a new select case for store events. During migration, both callbacks AND event channel are active (dual-emit from Task 4). We consume the event channel but don't act on it yet — we just verify events arrive:

```go
case ev := <-n.store.Events():
	switch e := ev.(type) {
	case store.DenyApplied:
		_ = e // will replace callback
	case store.RouteInvalidated:
		_ = e // will replace callback
	case store.WorkloadChanged:
		_ = e // will replace callback
	case store.TrafficChanged:
		_ = e // will replace callback
	case store.GossipApplied:
		n.broadcastEvents(e.Rebroadcast)
	case store.LocalMutationApplied:
		n.broadcastEvents(e.Events)
	}
```

- [ ] **Step 2: Run integration tests to verify dual-emit works**

```bash
just test-integration
```

Expected: PASS

- [ ] **Step 3: Wire event handlers and remove callbacks**

Now activate the event handlers and remove the old callback registrations:

For `DenyApplied` — replace the `OnDenyPeer` callback (line 291-303) with handling in the event case:

```go
case store.DenyApplied:
	n.mesh.ClosePeerSession(e.PeerKey, mesh.CloseReasonDenied)
	n.handlePeerInput(peer.Input{PeerKey: e.PeerKey, Event: peer.Disconnected{}})
```

For `RouteInvalidated` — replace the `OnRouteInvalidate` callback (line 313) with handling in the event case:

```go
case store.RouteInvalidated:
	n.signalRouteInvalidate()
```

For `WorkloadChanged` — replace the `OnWorkloadChange` callback (line 337):

```go
case store.WorkloadChanged:
	n.sched.Signal()
```

For `TrafficChanged` — replace the `OnTrafficChange` callback (line 338):

```go
case store.TrafficChanged:
	n.sched.SignalTraffic()
```

Remove the four `n.store.On*()` registration calls from the constructor/Start.

- [ ] **Step 4: Remove gossipEvents channel**

With `GossipApplied` and `LocalMutationApplied` events now handled via the store event channel, the `gossipEvents` channel and `queueGossipEvents()` function are no longer needed. Remove:
- The `gossipEvents` field from Node struct
- The `queueGossipEvents()` method
- The `gossipEvents` select case from the main loop
- All `n.queueGossipEvents(events)` calls — these become `// events emitted by store via Events() channel`

Instead, local mutation methods on the store now emit events that the node picks up via the event channel.

**Important:** The local mutation methods (`SetLocalNetwork`, `SetLocalConnected`, etc.) return `[]*statev1.GossipEvent`. Currently the node calls `queueGossipEvents` with these. After this change, the store's `emitEvent(LocalMutationApplied{Events: events})` handles this. So the node can ignore the return values:

```go
// Before:
n.queueGossipEvents(n.store.SetLocalNetwork(ips, uint32(n.conf.Port)))

// After:
n.store.SetLocalNetwork(ips, uint32(n.conf.Port))
```

- [ ] **Step 5: Run all tests**

```bash
go test ./pkg/node/ -v -race
just test-integration
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/node/node.go
git commit -s -m "migrate node from store callbacks to event channel"
```

---

### Task 8: Remove callback infrastructure from store

**Files:**
- Modify: `pkg/store/gossip.go`

Now that no consumers register callbacks, remove the infrastructure.

- [ ] **Step 1: Remove callback fields and registration methods**

Remove from Store struct:
- `onDenyPeer func(types.PeerKey)`
- `onRouteInvalidate func()`
- `onWorkloadChange func()`
- `onTrafficChange func()`

Remove methods:
- `OnDenyPeer(fn func(types.PeerKey))`
- `OnRouteInvalidate(fn func())`
- `OnWorkloadChange(fn func())`
- `OnTrafficChange(fn func())`

Remove callback invocations from `ApplyEvents()` — keep only the `emitEvent()` calls.

- [ ] **Step 2: Remove the migration `select default` from emitEvent**

Now that the event channel is consumed, change `emitEvent` to blocking:

```go
func (s *Store) emitEvent(ctx context.Context, ev StoreEvent) {
	select {
	case s.events <- ev:
	case <-ctx.Done():
	}
}
```

This requires threading a context through the store. Add `ctx context.Context` to the Store struct, set it in a new `Start(ctx context.Context)` method (or pass via Load). Update all `emitEvent` call sites.

- [ ] **Step 3: Run all tests**

```bash
go test ./pkg/store/ -v -race
go test ./pkg/node/ -v -race
just test-integration
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add pkg/store/gossip.go
git commit -s -m "remove callback infrastructure from store, emit events with backpressure"
```

---

### Task 9: Move store to single-goroutine internal loop

**Files:**
- Modify: `pkg/store/gossip.go`
- Modify: `pkg/store/disk.go`

This is the final Phase 1 step: replace the RWMutex with a single-goroutine event loop. All mutations are serialized through the goroutine. Snapshots use an atomic pointer for lock-free reads.

- [ ] **Step 1: Add atomic snapshot pointer**

Replace snapshot generation with an atomic pointer that's updated after every mutation:

```go
import "sync/atomic"

type Store struct {
	// ...
	snap atomic.Pointer[Snapshot]
	// ...
}
```

After every mutation in the internal loop, rebuild and store the snapshot:

```go
func (s *Store) updateSnapshot() {
	snap := s.snapshotLocked()
	s.snap.Store(&snap)
}
```

Change `Snapshot()` to a lock-free read:

```go
func (s *Store) Snapshot() Snapshot {
	return *s.snap.Load()
}
```

- [ ] **Step 2: Add internal goroutine loop**

Add a `Run(ctx context.Context)` method that owns all mutable state:

```go
func (s *Store) Run(ctx context.Context) error {
	s.ctx = ctx
	s.updateSnapshot() // initial snapshot

	saveTicker := time.NewTicker(30 * time.Second)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return s.Save()
		case fn := <-s.work:
			fn()
			s.updateSnapshot()
		case <-saveTicker.C:
			if err := s.Save(); err != nil {
				s.log.Warnw("state save failed", "error", err)
			}
		}
	}
}
```

Add a `work` channel to serialize mutations:

```go
work chan func()
```

Initialized in Load:

```go
work: make(chan func()),
```

- [ ] **Step 3: Convert mutation methods to goroutine dispatch**

Each mutation method sends a closure to the work channel:

```go
func (s *Store) SetLocalNetwork(ips []string, port uint32) {
	s.do(func() {
		events := s.setLocalNetworkInternal(ips, port)
		if len(events) > 0 {
			s.emitEvent(s.ctx, LocalMutationApplied{Events: events})
		}
	})
}

func (s *Store) do(fn func()) {
	done := make(chan struct{})
	s.work <- func() {
		fn()
		close(done)
	}
	<-done
}
```

Rename the current `SetLocalNetwork` implementation to `setLocalNetworkInternal` (remove locking since the goroutine owns the state).

Apply this pattern to ALL mutation methods:
- `SetLocalNetwork`, `SetExternalPort`, `SetObservedExternalIP`, `SetLastAddr`
- `SetLocalConnected`, `SetLocalPubliclyAccessible`, `SetLocalNatType`
- `SetLocalVivaldiCoord`, `SetLocalResourceTelemetry`, `SetLocalTrafficHeatmap`
- `SetLocalCertExpiry`, `UpsertLocalService`, `RemoveLocalServices`
- `DenyPeer`, `SetLocalWorkloadSpec`, `RemoveLocalWorkloadSpec`, `SetLocalWorkloadClaim`
- `ApplyEvents`
- `AddDesiredConnection`, `RemoveDesiredConnection`

Read-only methods (`Get`, `AllNodes`, `Clock`, etc.) now read from the atomic snapshot pointer — no locking needed.

Gossip protocol methods (`Clock`, `EagerSyncClock`, `MissingFor`, `LocalEvents`) need special handling: they read internal log state that may not be fully captured in the snapshot. These should use `do()` to run on the store's goroutine and return results via a channel.

- [ ] **Step 4: Move disk persistence into the goroutine**

Remove `stateSaveTicker` from `node.Start()` — the store's `Run()` loop now handles periodic saves. Remove the `n.store.Save()` call from the node's main loop.

- [ ] **Step 5: Wire store.Run() into node startup**

In `cmd/pln/main.go` or `pkg/node/node.go`, start the store's goroutine alongside the node:

```go
// In the startup pool:
g.Go(func() error { return stateStore.Run(ctx) })
```

- [ ] **Step 6: Remove RWMutex from all store methods**

With all mutations going through the goroutine and all reads using the atomic snapshot, the `mu sync.RWMutex` is no longer needed. Remove it.

- [ ] **Step 7: Run all tests with race detector**

```bash
go test ./pkg/store/ -v -race
go test ./pkg/node/ -v -race
go test ./pkg/scheduler/ -v -race
just test-integration
```

Expected: PASS with no race conditions.

- [ ] **Step 8: Run lint**

```bash
just lint
```

- [ ] **Step 9: Commit**

```bash
git add pkg/store/gossip.go pkg/store/disk.go pkg/node/node.go cmd/pln/main.go
git commit -s -m "move store to single-goroutine loop with atomic snapshots"
```

---

### Task 10: Implement eager gossip propagation

**Files:**
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Handle GossipApplied for eager broadcast**

In the store event handler (already added in Task 7), the `GossipApplied` and `LocalMutationApplied` cases call `broadcastEvents()`. This is already eager — events are broadcast as soon as the store emits them, not waiting for the gossip tick.

Verify this works by checking that the gossip tick (`gossipTicker.C` case) now only handles the periodic consistency fallback (clock digest exchange), not event broadcasting.

- [ ] **Step 2: Write integration test for eager propagation**

Add a test that verifies a local mutation propagates to peers within one peer tick interval (not waiting for gossip tick):

```go
func TestPublicMesh_EagerGossipPropagation(t *testing.T) {
	c := cluster.NewBuilder(t).PublicMesh(3).Build()
	defer c.Stop()

	// Set a service on node 0
	c.Node(0).Store().UpsertLocalService(8080, "eager-test")

	// Should propagate to node 1 within eagerTimeout (much less than gossip interval)
	eagerTimeout := 2 * time.Second
	require.Eventually(t, func() bool {
		snap := c.Node(1).Store().Snapshot()
		for _, nv := range snap.Nodes {
			if _, ok := nv.Services["eager-test"]; ok {
				return true
			}
		}
		return false
	}, eagerTimeout, 100*time.Millisecond)
}
```

- [ ] **Step 3: Run test**

```bash
go test -tags integration ./pkg/integration/cluster/ -run TestPublicMesh_EagerGossipPropagation -v -race -timeout 30s
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add pkg/node/node.go pkg/integration/cluster/scenario_test.go
git commit -s -m "verify eager gossip propagation via store events"
```

---

### Task 11: Phase 1 verification gate

- [ ] **Step 1: Run full integration test suite**

```bash
just test-integration
```

Expected: ALL PASS

- [ ] **Step 2: Run lint**

```bash
just lint
```

Expected: PASS

- [ ] **Step 3: Mutation test critical paths**

For each new test added in this phase, temporarily break the code it covers and verify the test fails:

1. Break `Snapshot()` — return empty snapshot. Verify `TestSnapshot_ReflectsState` fails.
2. Break event emission — comment out `emitEvent` calls. Verify `TestEvents_EmitsOnApply` fails.
3. Break eager propagation — add 10s sleep before `emitEvent`. Verify `TestPublicMesh_EagerGossipPropagation` fails.

Reinstate each break after verification.

- [ ] **Step 4: Verify behavioral inventory**

Run status commands against a local test and verify format matches inventory:
- `pln status` output unchanged
- `pln status --wide` output unchanged
- `pln status --all` output unchanged
- Logging unchanged (check startup, peer connect, gossip)

- [ ] **Step 5: Hetzner cluster verification**

Follow the full Tier 2 verification protocol from the spec:
1. Purge all Hetzner node state
2. Deploy fresh binary
3. Run through cluster formation (steps 1-4)
4. Verify convergence (step 5)
5. Service propagation and connectivity (steps 6-7)
6. Workload seed/unseed/reseed/call/load-test (steps 8-10)
7. Workload migration on node down (step 11)

All steps must pass before proceeding to Phase 2.

- [ ] **Step 6: Commit verification results**

```bash
git add -A
git commit -s -m "phase 1 complete: store boundary with snapshots, events, and eager propagation"
```

---

## Chunk 4: Phase 2 — Mesh/Transport Boundary

### Task 12: Define Transport interface

**Files:**
- Create: `pkg/node/transport.go`

- [ ] **Step 1: Create Transport interface**

```go
package node

import (
	"context"
	"io"
	"net"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1/meshv1connect"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1/admissionv1connect"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

// StreamType identifies the purpose of an incoming stream.
type StreamType = mesh.StreamType // re-export from mesh for now

// PeerEvent represents a peer lifecycle change from the transport layer.
type PeerEvent = peer.Input // re-export during migration

// Transport is the consumer-defined interface for the network transport layer.
// The node depends on this interface, not on *mesh.impl directly.
type Transport interface {
	Start(ctx context.Context) error
	Close() error

	// Peer lifecycle
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error
	PeerEvents() <-chan PeerEvent
	ClosePeerSession(peer types.PeerKey, reason mesh.CloseReason)
	ConnectedPeers() []types.PeerKey
	IsOutbound(types.PeerKey) bool
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)

	// Messaging
	Send(ctx context.Context, peer types.PeerKey, env *meshv1.Envelope) error
	Recv(ctx context.Context) (mesh.Packet, error)

	// Streams — single accept point, consumer dispatches by type
	AcceptStream(ctx context.Context) (io.ReadWriteCloser, StreamType, types.PeerKey, error)
	OpenStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	OpenClockStream(ctx context.Context, peer types.PeerKey) (mesh.Stream, error)
	OpenArtifactStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	OpenWorkloadStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)

	// TLS and auth
	UpdateMeshCert(cert tls.Certificate)
	RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
	PeerCertExpiresAt(peer types.PeerKey) (time.Time, bool)
	PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)
	BroadcastDisconnect() error

	// Join flow
	JoinWithToken(ctx context.Context, token *admissionv1.JoinToken) error
	JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)

	// Observability
	ListenPort() int
	GetConn(peer types.PeerKey) (*quic.Conn, bool)

	// Injection seams
	SetPacketConn(conn net.PacketConn)
	SetDisableNATPunch(disable bool)
	SetRouter(r mesh.Router)
	SetTrafficTracker(t traffic.Recorder)
	SetTracer(t *traces.Tracer)
}
```

Note: This is intentionally wide during migration. It will narrow as we move handler callbacks into the node's stream dispatch (Task 14).

- [ ] **Step 2: Run goimports and compile**

```bash
goimports -w pkg/node/transport.go
go build ./pkg/node/
```

- [ ] **Step 3: Commit**

```bash
git add pkg/node/transport.go
git commit -s -m "define Transport interface for mesh abstraction"
```

---

### Task 13: Implement unified AcceptStream in mesh

**Files:**
- Modify: `pkg/mesh/mesh.go`

Currently `acceptBidiStreams` dispatches streams to `streamCh` (tunnel), `clockStreamCh` (clock), or directly to handlers (artifact, workload, routed). We unify this into a single blocking `AcceptStream` that returns all non-routed stream types.

- [ ] **Step 1: Add a unified accept channel**

Add to impl struct:

```go
acceptCh chan acceptedStream
```

Where:

```go
type acceptedStream struct {
	stream   io.ReadWriteCloser
	stype    byte
	peerKey  types.PeerKey
}
```

Initialize in NewMesh with buffer 0 (blocking):

```go
acceptCh: make(chan acceptedStream),
```

- [ ] **Step 2: Modify acceptBidiStreams to use unified channel**

Replace the per-type channel dispatch with a single blocking send to `acceptCh` for all non-routed types:

```go
switch streamType {
case streamTypeRouted:
	go m.handleRoutedStream(ctx, qs, peerKey)
default:
	select {
	case m.acceptCh <- acceptedStream{stream: s, stype: streamType, peerKey: peerKey}:
	case <-ctx.Done():
		s.Close()
	}
}
```

This eliminates the `select { default: }` drop pattern. If the consumer is slow, the send blocks — providing backpressure to the QUIC session.

- [ ] **Step 3: Implement unified AcceptStream method**

```go
func (m *impl) AcceptStream(ctx context.Context) (io.ReadWriteCloser, byte, types.PeerKey, error) {
	select {
	case as := <-m.acceptCh:
		return as.stream, as.stype, as.peerKey, nil
	case <-ctx.Done():
		return nil, 0, types.PeerKey{}, ctx.Err()
	}
}
```

- [ ] **Step 4: Remove old streamCh and clockStreamCh**

Remove:
- `streamCh` field and initialization
- `clockStreamCh` field and initialization
- Old `AcceptStream()` method (that read from `streamCh`)
- Old `AcceptClockStream()` method (that read from `clockStreamCh`)

The old methods' functionality is now covered by the unified `AcceptStream`.

- [ ] **Step 5: Fix inCh to use blocking sends**

Replace the 5s timeout drop pattern for `inCh`:

```go
// Before (timeout drop):
select {
case m.inCh <- peer.Input{...}:
case <-time.After(eventSendTimeout):
	m.log.Warnw("dropped peer event", ...)
}

// After (blocking with context):
select {
case m.inCh <- peer.Input{...}:
case <-ctx.Done():
}
```

Apply at all `inCh` send sites.

- [ ] **Step 6: Fix renewalCh to use blocking sends**

```go
// Before:
select {
case m.renewalCh <- resp:
default:
}

// After:
select {
case m.renewalCh <- resp:
case <-ctx.Done():
}
```

- [ ] **Step 7: Remove artifact/workload handler callbacks**

Remove `SetArtifactHandler()` and `SetWorkloadHandler()` — stream dispatch now happens in the node, not the mesh. Remove the handler fields from the impl struct.

- [ ] **Step 8: Run tests**

```bash
go test ./pkg/mesh/ -v -race
go test ./pkg/node/ -v -race
just test-integration
```

Expected: FAIL initially — node still calls old AcceptStream/AcceptClockStream. We fix this in the next task.

- [ ] **Step 9: Commit (may be WIP if tests fail)**

```bash
git add pkg/mesh/mesh.go
git commit -s -m "unify mesh stream acceptance, eliminate drop channels"
```

---

### Task 14: Wire node to unified stream dispatch

**Files:**
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Replace acceptClockStreamsLoop with unified dispatch**

Remove the `acceptClockStreamsLoop` goroutine. Replace with a single stream dispatch goroutine:

```go
func (n *Node) streamDispatchLoop(ctx context.Context) {
	for {
		stream, stype, peerKey, err := n.mesh.AcceptStream(ctx)
		if err != nil {
			return
		}
		switch stype {
		case mesh.StreamTypeClock:
			go n.handleClockStream(ctx, peerKey, stream)
		case mesh.StreamTypeTunnel:
			n.tun.HandleIncoming(stream, peerKey)
		case mesh.StreamTypeArtifact:
			go n.sched.HandleArtifactStream(stream, peerKey)
		case mesh.StreamTypeWorkload:
			go n.handleWorkloadStream(ctx, stream, peerKey)
		default:
			n.log.Warnw("unknown stream type", "type", stype)
			stream.Close()
		}
	}
}
```

Start this in `Start()` where `acceptClockStreamsLoop` was:

```go
go n.streamDispatchLoop(ctx)
```

- [ ] **Step 2: Remove SetArtifactHandler/SetWorkloadHandler calls**

Remove from node constructor:
```go
// Remove these lines:
n.mesh.SetArtifactHandler(...)
n.mesh.SetWorkloadHandler(...)
```

The handlers are now inline in `streamDispatchLoop`.

- [ ] **Step 3: Change node.mesh field to Transport interface**

```go
// Before:
mesh mesh.Mesh

// After:
mesh Transport
```

Update the constructor to accept `Transport` instead of creating mesh directly. The mesh is still created in `cmd/pln/main.go` and passed in.

- [ ] **Step 4: Update integration test harness**

In `pkg/integration/cluster/node.go`, update `TestNode` to work with the new stream dispatch. The mesh is still created the same way; the node just receives it as a `Transport` interface.

- [ ] **Step 5: Run all tests**

```bash
go test ./pkg/node/ -v -race
just test-integration
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/node/node.go pkg/integration/cluster/node.go
git commit -s -m "wire node to unified stream dispatch, accept Transport interface"
```

---

### Task 15: Extract Router interface from mesh

**Files:**
- Modify: `pkg/mesh/mesh.go`
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Verify Router interface already exists**

The mesh already defines `Router` interface (line 75-77):
```go
type Router interface {
	Lookup(dest types.PeerKey) (nextHop types.PeerKey, ok bool)
}
```

And `SetRouter(r Router)` is already an interface-based injection. Verify that `pkg/route` is not imported directly by `pkg/mesh` — only through this interface.

- [ ] **Step 2: If mesh imports pkg/route directly, remove the import**

Check: `grep 'pkg/route' pkg/mesh/mesh.go`

If found, replace direct route usage with the Router interface. The node wires the route table into the mesh via `SetRouter()`.

- [ ] **Step 3: Run tests**

```bash
go test ./pkg/mesh/ -v -race
just test-integration
```

- [ ] **Step 4: Commit (if changes made)**

```bash
git add pkg/mesh/mesh.go
git commit -s -m "ensure mesh depends on Router interface, not pkg/route"
```

---

### Task 16: Phase 2 verification gate

- [ ] **Step 1: Run full test suite**

```bash
just test-integration
just lint
```

- [ ] **Step 2: Verify no silent drops remain in mesh**

```bash
grep -n 'select {' pkg/mesh/mesh.go | head -30
```

Manually verify: no `default:` branch on any channel send (except the routed stream dispatcher which is acceptable). Every send blocks with context.

- [ ] **Step 3: Mutation test stream dispatch**

1. Remove the `StreamTypeClock` case from `streamDispatchLoop`. Run clock-related integration tests. Verify they fail (gossip sync breaks).
2. Reinstate.

- [ ] **Step 4: Hetzner cluster verification**

Full Tier 2 verification protocol from the spec. All steps must pass.

- [ ] **Step 5: Commit**

```bash
git commit -s --allow-empty -m "phase 2 complete: mesh/transport boundary with blocking backpressure"
```

---

## Chunk 5: Phase 3 — Node Orchestrator & Phase 4 — Control Plane

### Task 17: Create bounded worker pool

**Files:**
- Create: `pkg/node/workers.go`
- Create: `pkg/node/workers_test.go`

- [ ] **Step 1: Write failing test**

```go
package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool_BoundsConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newWorkerPool(2)
	go pool.run(ctx)

	var running atomic.Int32
	var maxSeen atomic.Int32

	for i := 0; i < 10; i++ {
		pool.submit(ctx, func() {
			cur := running.Add(1)
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			running.Add(-1)
		})
	}

	pool.wait()
	require.LessOrEqual(t, int(maxSeen.Load()), 2)
}

func TestWorkerPool_ShutdownDrainsWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pool := newWorkerPool(4)
	go pool.run(ctx)

	var completed atomic.Int32
	for i := 0; i < 5; i++ {
		pool.submit(ctx, func() {
			time.Sleep(10 * time.Millisecond)
			completed.Add(1)
		})
	}

	cancel()
	pool.wait()
	require.Equal(t, int32(5), completed.Load())
}
```

- [ ] **Step 2: Implement worker pool**

```go
package node

import (
	"context"
	"sync"
)

type workerPool struct {
	sem  chan struct{}
	wg   sync.WaitGroup
	work chan func()
}

func newWorkerPool(maxConcurrency int) *workerPool {
	return &workerPool{
		sem:  make(chan struct{}, maxConcurrency),
		work: make(chan func(), 64),
	}
}

func (p *workerPool) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Drain remaining work before returning
			for {
				select {
				case fn := <-p.work:
					p.exec(fn)
				default:
					return
				}
			}
		case fn := <-p.work:
			p.exec(fn)
		}
	}
}

func (p *workerPool) exec(fn func()) {
	p.sem <- struct{}{}
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			p.wg.Done()
		}()
		fn()
	}()
}

func (p *workerPool) submit(ctx context.Context, fn func()) {
	select {
	case p.work <- fn:
	case <-ctx.Done():
	}
}

func (p *workerPool) wait() {
	p.wg.Wait()
}
```

- [ ] **Step 3: Run tests**

```bash
go test ./pkg/node/ -run TestWorkerPool -v -race
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add pkg/node/workers.go pkg/node/workers_test.go
git commit -s -m "add bounded worker pool for ephemeral goroutines"
```

---

### Task 18: Simplify node event loop

**Files:**
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Replace fire-and-forget goroutines with worker pool submissions**

Add worker pool to Node struct:

```go
workers *workerPool
```

Initialize in constructor:

```go
workers: newWorkerPool(8), // bounded concurrency for connect/punch/sync
```

Start in `Start()`:

```go
go n.workers.run(ctx)
```

Replace all fire-and-forget `go` calls:

```go
// Before (doConnect):
go func() { ... n.doConnect(ctx, ...) ... }()

// After:
n.workers.submit(ctx, func() { n.doConnect(ctx, ...) })
```

Apply to: `doConnect` calls (lines 1030, 1032), `requestPunchCoordination` (line 1034), eager sync (line 1014), punch coord relay sends (lines 1146, 1155).

- [ ] **Step 2: Replace punchLoop goroutines with worker pool**

Remove the 3 persistent `punchLoop` goroutines. Remove `punchCh` channel. In `handlePunchCoordTrigger`, submit punch work to the worker pool instead of `punchCh`:

```go
// Before:
select {
case n.punchCh <- punchRequest{...}:
default:
}

// After:
n.workers.submit(ctx, func() {
	punchCtx, cancel := context.WithTimeout(ctx, punchTimeout)
	defer cancel()
	if err := n.mesh.Punch(punchCtx, req.PeerKey, req.Addr, req.NATType); err != nil {
		// handle failure
	}
})
```

- [ ] **Step 3: Remove localPeerEvents channel**

Already done in Task 7 if callbacks are fully migrated. If any remaining writers exist, route them through `transport.PeerEvents()` or `store.Events()`.

- [ ] **Step 4: Verify the simplified select loop**

The main loop should now have these cases:
1. `<-ctx.Done()`
2. `ev := <-n.mesh.PeerEvents()`
3. `ev := <-n.store.Events()`
4. `in := <-n.localPeerEvents` (retained for internal feedback: ConnectFailed, RetryPeer from worker pool tasks)
5. `<-routeDebounceT.C`
6. `<-peerTicker.C`
7. `<-gossipTicker.C`
8. `<-certTicker.C`
9. `<-ipRefreshTicker.C`

- [ ] **Step 5: Add worker pool drain to shutdown**

In `shutdown()`, after stopping new work:

```go
n.workers.wait() // drain all in-flight work before closing mesh
```

This goes before `n.mesh.BroadcastDisconnect()`.

- [ ] **Step 6: Run all tests**

```bash
go test ./pkg/node/ -v -race
just test-integration
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add pkg/node/node.go
git commit -s -m "simplify node event loop: worker pool, eliminate drop channels"
```

---

### Task 19: Migrate control service to interfaces

**Files:**
- Modify: `pkg/node/svc.go`
- Modify: `pkg/node/node.go`

- [ ] **Step 1: Define service interfaces in svc.go**

```go
type statusProvider interface {
	Snapshot() store.Snapshot
	GossipMetrics() *metrics.GossipMetrics
	ConnectedPeers() []types.PeerKey
	GetActivePeerAddress(types.PeerKey) (*net.UDPAddr, bool)
	GetConn(types.PeerKey) (*quic.Conn, bool)
	ListConnections() []tunnel.ConnectionInfo
	ListWorkloads() map[string]workload.Status
	VivaldiError() float64
	VivaldiSamples() int64
	EagerSyncs() int64
	EagerSyncFailures() int64
	PeerStates() map[types.PeerKey]peer.PeerState
}

type serviceManager interface {
	Register(ctx context.Context, port uint16, name string) error
	Unregister(ctx context.Context, name string) error
}

type workloadManager interface {
	Seed(ctx context.Context, binary []byte, replicas, memPages, timeoutMs uint32) (string, error)
	Unseed(ctx context.Context, hash string) error
	Call(ctx context.Context, hash string, fn string, input []byte) ([]byte, error)
}

type peerManager interface {
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Deny(ctx context.Context, pub []byte) error
}
```

- [ ] **Step 2: Update NodeService to hold interfaces**

```go
type NodeService struct {
	status    statusProvider
	services  serviceManager
	workloads workloadManager
	peers     peerManager
	shutdown  func()
}
```

- [ ] **Step 3: Have Node satisfy the interfaces**

Add methods to Node that satisfy each interface. Many already exist; some need thin wrappers. For example:

```go
func (n *Node) VivaldiError() float64     { return n.smoothedErr.Value() }
func (n *Node) VivaldiSamples() int64     { return n.vivaldiSamples.Load() }
func (n *Node) EagerSyncs() int64         { return n.eagerSyncs.Load() }
func (n *Node) EagerSyncFailures() int64  { return n.eagerSyncFailures.Load() }
func (n *Node) PeerStates() map[types.PeerKey]peer.PeerState { return n.peers.States() }
```

- [ ] **Step 4: Update svc.go to use interfaces**

Replace all `s.node.store.X` with `s.status.Snapshot().X` or `s.status.X()`.
Replace all `s.node.mesh.X` with `s.status.X()` or `s.peers.X()`.

- [ ] **Step 5: Run tests**

```bash
go test ./pkg/node/ -v -race
just test-integration
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/node/svc.go pkg/node/node.go
git commit -s -m "migrate control service to narrow interfaces"
```

---

### Task 20: Phase 3 + 4 verification gate

- [ ] **Step 1: Run full test suite**

```bash
just test-integration
just lint
```

- [ ] **Step 2: Verify goroutine reduction**

Add a temporary test or log line that prints `runtime.NumGoroutine()` at steady state with a 3-node cluster. Compare before/after. The number should be lower (no 3x punchLoop, no streamCh/clockStreamCh per session, bounded worker pool).

- [ ] **Step 3: Verify no silent drops**

```bash
grep -rn 'select {' pkg/node/node.go pkg/mesh/mesh.go pkg/store/gossip.go | grep -A1 'default:'
```

Expected: zero hits on channel sends (only acceptable in test code or intentional coalescing like routeInvalidate — which is now handled via store events).

- [ ] **Step 4: Mutation test worker pool**

1. Set worker pool concurrency to 0. Verify integration tests hang/fail (no work gets done).
2. Reinstate.

- [ ] **Step 5: Hetzner cluster verification**

Full Tier 2 protocol. All steps must pass. Pay special attention to:
- Eager convergence timing (should be near-instant)
- Status output format (must match inventory exactly)
- Logging verbosity (must match inventory)
- Workload migration on node-down (step 11)

- [ ] **Step 6: Final commit**

```bash
git commit -s --allow-empty -m "phase 3+4 complete: simplified orchestrator, interface-based control plane"
```

---

## Chunk 6: Verification Automation

### Task 21: Create Hetzner verification script

**Files:**
- Create: `infra/scripts/verify-hetzner.sh`

- [ ] **Step 1: Write the verification script**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Hetzner cluster verification script for boundary refactor.
# Runs the full Tier 2 verification protocol.
#
# Prerequisites:
#   - Hetzner cluster provisioned (just deploy-hetzner <version>)
#   - Local pln binary built (just bin)
#   - SSH access to Hetzner nodes configured

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/.."

# Get node IPs from terraform
NODE_IPS=$(cd "$INFRA_DIR/hetzner" && terraform output -json node_ips)
ROOT_IP=$(cd "$INFRA_DIR/hetzner" && terraform output -raw root_node_ip)
NODE2_IP=$(echo "$NODE_IPS" | jq -r '."nbg1-2"')
NODE3_IP=$(echo "$NODE_IPS" | jq -r '.hel1')

SSH="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

log() { echo "=== $1 ===" >&2; }
fail() { echo "FAIL: $1" >&2; exit 1; }
pass() { echo "PASS: $1" >&2; }

# Step 0: Purge state on all nodes
log "Purging state on all nodes"
for IP in $ROOT_IP $NODE2_IP $NODE3_IP; do
    $SSH root@"$IP" "systemctl stop pln 2>/dev/null || true; rm -rf /var/lib/pln; rm -rf /root/.pln" || true
done
# Purge local state (WARNING: destructive — only run on test machines)
pln down 2>/dev/null || true
pln purge 2>/dev/null || true

# Step 1: Initialize local root
log "Step 1: Initialize local root"
pln init
pln up -d
sleep 2
pln status | grep -q "HEALTHY" || fail "root not healthy"
pass "Root initialized"

# Step 2: Bootstrap first Hetzner node
log "Step 2: Bootstrap first node via SSH"
pln bootstrap ssh root@"$ROOT_IP"
sleep 3
pln status | grep -q "$ROOT_IP" || fail "first node not visible"
pass "First node bootstrapped"

# Step 3: Invite second node
log "Step 3: Invite/join second node"
TOKEN=$(pln invite)
$SSH root@"$NODE2_IP" "pln join '$TOKEN'"
sleep 3
pln status | grep -c "direct" | grep -q "[2-3]" || fail "second node not connected"
pass "Second node joined"

# Step 4: Targeted invite for third node
log "Step 4: Targeted invite for third node"
THIRD_ID=$($SSH root@"$NODE3_IP" "pln id")
TOKEN2=$(pln invite --subject "$THIRD_ID")
$SSH root@"$NODE3_IP" "pln join '$TOKEN2'"
sleep 3
STATUS=$(pln status)
echo "$STATUS"
PEER_COUNT=$(echo "$STATUS" | grep -c "direct")
[ "$PEER_COUNT" -ge 3 ] || fail "not all nodes connected (got $PEER_COUNT)"
pass "Third node joined, all nodes meshed"

# Step 5: Vivaldi convergence
log "Step 5: Checking Vivaldi convergence"
for i in $(seq 1 12); do
    DIAG=$(pln status --wide 2>&1 | grep "vivaldi error" || true)
    if [ -n "$DIAG" ]; then
        ERROR=$(echo "$DIAG" | awk '{print $NF}')
        echo "  vivaldi error: $ERROR (attempt $i/12)"
        # Check if error is below 0.5
        if awk "BEGIN{exit !($ERROR < 0.5)}"; then
            pass "Vivaldi converged (error=$ERROR)"
            break
        fi
    fi
    [ "$i" -eq 12 ] && fail "Vivaldi did not converge within 60s"
    sleep 5
done

# Step 6: Service propagation
log "Step 6: Service propagation"
$SSH root@"$NODE2_IP" "pln serve 8080 test-svc"
sleep 3
pln status | grep -q "test-svc" || fail "service not visible on root"
$SSH root@"$NODE3_IP" "pln status" | grep -q "test-svc" || fail "service not visible on node3"
pass "Service propagated"

# Step 7: Service connectivity
log "Step 7: Service connectivity"
# Start a simple HTTP server on node2
$SSH root@"$NODE2_IP" 'python3 -c "
import http.server, threading
s = http.server.HTTPServer((\"0.0.0.0\", 8080), http.server.SimpleHTTPRequestHandler)
threading.Thread(target=s.serve_forever, daemon=True).start()
import time; time.sleep(30)
" &'
sleep 2
pln connect test-svc
sleep 1
curl -s http://localhost:8080 > /dev/null && pass "Service tunnel works" || fail "Service tunnel broken"
pln disconnect test-svc
$SSH root@"$NODE2_IP" "pln unserve test-svc"

# Step 8: Workload seed/unseed/reseed
log "Step 8: Workload lifecycle"
HASH=$(pln seed examples/echo/echo.wasm --replicas 1 | grep -oE '[a-f0-9]{16,}')
sleep 5
pln status | grep -q "running" || fail "workload not running"
pass "Workload seeded"

pln unseed "$HASH"
sleep 3
pln status | grep -q "$HASH" && fail "workload not unseeded"
pass "Workload unseeded"

HASH=$(pln seed examples/echo/echo.wasm --replicas 2 | grep -oE '[a-f0-9]{16,}')
sleep 5
pln status | grep -q "2/2" || fail "workload not at 2 replicas"
pass "Workload reseeded with 2 replicas"

# Step 9: Call from all nodes
log "Step 9: Workload call from all nodes"
RESULT=$(pln call "$HASH" handle "hello")
echo "$RESULT" | grep -q "hello" || fail "call failed from root"
for IP in $ROOT_IP $NODE2_IP $NODE3_IP; do
    R=$($SSH root@"$IP" "pln call '$HASH' handle 'hello'" 2>&1)
    echo "$R" | grep -q "hello" || fail "call failed from $IP"
done
pass "All nodes can call workload"

# Step 10: Load test
log "Step 10: Load test (1000 calls from 2 nodes)"
$SSH root@"$NODE2_IP" "for i in \$(seq 1 1000); do pln call '$HASH' handle 'test' > /dev/null 2>&1; done; echo DONE" &
PID2=$!
$SSH root@"$NODE3_IP" "for i in \$(seq 1 1000); do pln call '$HASH' handle 'test' > /dev/null 2>&1; done; echo DONE" &
PID3=$!
wait $PID2 || fail "load test failed on node2"
wait $PID3 || fail "load test failed on node3"
pass "2000 calls completed"

# Step 11: Workload migration on node down
log "Step 11: Workload migration"
# Find which nodes have the claims
STATUS_WIDE=$(pln status --wide)
# Stop one node that has a claim
$SSH root@"$NODE2_IP" "pln down"
sleep 10
# Verify replica count restored
pln status | grep -q "2/2" || fail "workload did not migrate after node down"
pass "Workload migrated to replacement node"

log "ALL VERIFICATION STEPS PASSED"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x infra/scripts/verify-hetzner.sh
```

- [ ] **Step 3: Commit**

```bash
git add infra/scripts/verify-hetzner.sh
git commit -s -m "add hetzner cluster verification script"
```

---

### Task 22: Add just recipe for verification

**Files:**
- Modify: `infra/justfile`

- [ ] **Step 1: Add verify recipe**

```just
# Run full verification against hetzner cluster.
verify-hetzner:
    bash {{ justfile_directory() }}/scripts/verify-hetzner.sh
```

- [ ] **Step 2: Commit**

```bash
git add infra/justfile
git commit -s -m "add verify-hetzner just recipe"
```
