# Clean Room Architecture Migration Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Incrementally migrate the Pollen codebase from its current package structure to the clean room architecture defined in `docs/superpowers/specs/2026-03-16-clean-room-architecture-design.md`.

**Architecture:** Bottom-up migration — foundation first, then core (state/transport), then domain services, then composition layer. Each phase produces a fully working system. Code is shovelled (copy-paste + adapt signatures), never rewritten.

**Tech Stack:** Go 1.23+, QUIC (quic-go), Protocol Buffers (vtprotobuf), testify, just

**Spec:** `docs/superpowers/specs/2026-03-16-clean-room-architecture-design.md`

**Behavioral baseline:** `docs/behavioral-inventory.md`

---

## Verification Protocol

Every phase follows this cycle. No exceptions. No shortcuts.

```
1. Port code into target package (copy-paste, adapt signatures)
2. Port/adapt unit tests (signatures change, assertions don't)
3. Adapt integration tests to new APIs (NEVER remove, only adapt or add)
4. Add new integration tests for new boundaries
5. Mutation test: deliberately break production code, verify each test catches it
6. Full test suite: go test -race -count=1 ./...
7. Lint: just lint
8. Hetzner cluster verification: just hetzner-verify
9. Commit only after all gates pass
```

**Mutation test protocol:** For each integration test, introduce a targeted mutation (swap condition, drop message, return early) in the production code it guards. Verify the test fails. Revert. Verify it passes. A test that passes despite its mutation is ineffective — fix it.

**API surface audit:** After porting a component to its target package, audit its exported API against the spec. Run `go doc pkg/<package>` and verify that only the spec'd types, functions, and methods are exported. Unexport everything else — internal helpers, intermediate types, implementation details. The spec's API is the contract; everything else is private.

---

## Phase 0: Strengthen Verification Gates

**Goal:** Ensure the safety net is solid before touching any production code. Every integration test must earn its place via mutation testing.

### Task 0.1: Baseline All Tests

**Files:**
- Read: `pkg/integration/cluster/*_test.go`
- Read: `pkg/store/*_test.go`
- Read: `pkg/mesh/*_test.go`

- [ ] **Step 1: Run full test suite, record baseline**

Run: `go test -race -count=1 ./... 2>&1 | tee /tmp/test-baseline.txt`
Expected: All tests pass. Record exact pass count.

- [ ] **Step 2: Run lint, record baseline**

Run: `just lint 2>&1 | tee /tmp/lint-baseline.txt`
Expected: Clean or known-acceptable nolint directives only.

- [ ] **Step 3: Run Hetzner verification, record baseline**

Run: `just hetzner-verify 2>&1 | tee /tmp/hetzner-baseline.txt`
Expected: All checks pass. This is the behavioral baseline for the entire migration.

- [ ] **Step 4: Commit baseline records**

No code changes — just confirm the gates pass.

### Task 0.2: Mutation Test Integration Tests

**Files:**
- Modify (temporarily): production code files guarded by each integration test
- Read: `pkg/integration/cluster/*_test.go`

For each integration test scenario, perform the mutation cycle:

- [ ] **Step 1: Identify what each integration test guards**

Read every test function in `pkg/integration/cluster/*_test.go`. For each test, document:
- What production behavior it validates
- Which production code path it exercises
- What mutation would cause it to fail

- [ ] **Step 2: Mutate and verify each test**

For each test, introduce a targeted mutation in the production code. Examples:
- GossipConvergence: drop gossip messages (in `pkg/store/gossip.go`, skip `applyRemoteEvent` or equivalent merge function)
- TopologyStabilization: corrupt Vivaldi updates (in `pkg/topology/vivaldi.go`, return zero coord from `Update`)
- WorkloadPlacement: skip claim propagation (in `pkg/scheduler/reconciler.go`, return early from `Evaluate` or equivalent)
- NodeJoinLeave: break peer event emission (in `pkg/mesh/mesh.go`, skip peer event channel send)

Verify: each test FAILS with the mutation. Revert. Verify each test PASSES.

- [ ] **Step 3: Fix any ineffective tests**

If a test passes despite its mutation, it doesn't guard the behavior it claims to. Fix the test to actually detect the regression. Re-run mutation verification.

- [ ] **Step 4: Document test-to-code mapping**

Create `docs/test-mutation-map.md` documenting which production code each test guards and what mutation was used to verify it. This map will be maintained across the migration.

- [ ] **Step 5: Commit documentation**

```
git add docs/test-mutation-map.md
git commit -s -m "add test mutation map for migration verification"
```

---

## Phase 1: Foundation — Extract `coords` Package

**Goal:** Extract Vivaldi coordinate math from `pkg/topology/vivaldi.go` into `pkg/coords/`. This is the smallest, safest possible change — pure math with zero I/O.

### Task 1.1: Create `pkg/coords/` with Vivaldi Math

**Files:**
- Create: `pkg/coords/coords.go` (copy from `pkg/topology/vivaldi.go`)
- Create: `pkg/coords/coords_test.go` (copy from `pkg/topology/vivaldi_test.go` if it exists)
- Delete: `pkg/topology/vivaldi.go` (contents moved)
- Modify: `pkg/topology/topology.go` (update imports to use `coords.Coord`)
- Modify: `pkg/topology/*_test.go` (update any `Coord` references to `coords.Coord`)
- Modify: `pkg/store/snapshot.go` (update `NodeView.VivaldiCoord` type)
- Modify: `pkg/store/gossip.go` (update Vivaldi references)
- Modify: `pkg/route/table.go` (update imports)
- Modify: `pkg/node/node.go` (update imports)
- Modify: `pkg/node/topology_profile.go` (update imports)
- Modify: `pkg/integration/cluster/assert.go` (update imports if needed)

- [ ] **Step 1: Create `pkg/coords/coords.go`**

Copy `pkg/topology/vivaldi.go` verbatim to `pkg/coords/coords.go`. Change only the package declaration from `package topology` to `package coords`. All types (`Coord`, `Sample`), constants (`CcDefault`, `CeDefault`, etc.), and functions (`RandomCoord`, `Distance`, `MovementDistance`, `Update`, `clamp`) move unchanged.

- [ ] **Step 2: Verify `pkg/coords` compiles**

Run: `go build ./pkg/coords/...`
Expected: Clean build.

- [ ] **Step 3: Update `pkg/topology/topology.go` to import `coords`**

Replace all references to `Coord` with `coords.Coord`. The `topology` package now imports `coords` instead of defining `Coord` itself. Add a type alias `type Coord = coords.Coord` temporarily if needed for backward compat, or update all call sites.

- [ ] **Step 4: Copy Vivaldi tests**

If `pkg/topology/vivaldi_test.go` exists, copy it to `pkg/coords/coords_test.go` and change the package declaration. Run `go test -race ./pkg/coords/...` to verify tests pass in the new location.

- [ ] **Step 5: Delete `pkg/topology/vivaldi.go`**

The contents now live in `pkg/coords/coords.go`.

- [ ] **Step 6: Update all consumers**

Run `grep -rn 'topology\.Coord\|topology\.Sample\|topology\.Distance\|topology\.Update\|topology\.RandomCoord\|topology\.MovementDistance' pkg/ --include='*.go'` to find ALL references.

Update imports in each file found — including test files like `pkg/topology/*_test.go`. For each file: run `goimports -w <file>` after editing.

- [ ] **Step 7: Verify full build**

Run: `go build ./...`
Expected: Clean build. Zero import errors.

- [ ] **Step 8: Run full test suite**

Run: `go test -race -count=1 ./...`
Expected: All tests pass. Same count as baseline.

- [ ] **Step 9: Run lint**

Run: `just lint`
Expected: Clean.

- [ ] **Step 10: Mutation test — break `coords.Update`, verify topology tests fail**

In `pkg/coords/coords.go`, change `Update` to return the input coordinate unchanged:
```go
func Update(local Coord, localErr float64, s Sample) (Coord, float64) {
    return local, localErr  // MUTATION: no coordinate update
}
```
Run: `go test -race ./pkg/integration/cluster/...`
Expected: TopologyStabilization test FAILS (coordinates don't converge).
Revert the mutation. Verify tests pass.

- [ ] **Step 11: Run Hetzner verification**

Run: `just hetzner-verify`
Expected: All checks pass.

- [ ] **Step 12: Commit**

```
git add pkg/coords/ pkg/topology/ pkg/store/ pkg/route/ pkg/node/ pkg/integration/
git commit -s -m "extract coords package from topology"
```

---

## Phase 2: State Package — Rename and Refine `store`

**Goal:** Rename `pkg/store` to `pkg/state`, refine the API to match the spec (typed mutations return `[]Event`, typed projections on `Snapshot`). This is the largest Phase 1-2 change.

**Strategy:** Create `pkg/state` alongside `pkg/store`. Move code file by file. Update consumers one at a time. Delete `pkg/store` only when empty.

### Task 2.1: Create `pkg/state/` Package Shell

**Files:**
- Create: `pkg/state/doc.go`

- [ ] **Step 1: Create package shell**

Create `pkg/state/doc.go` with just the package declaration:
```go
// Package state provides the CRDT-based distributed state store with
// immutable snapshots, typed projections, and synchronous event emission.
package state
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./pkg/state/...`

- [ ] **Step 3: Commit**

```
git add pkg/state/
git commit -s -m "add state package shell"
```

### Task 2.2: Move Snapshot Types to `pkg/state`

**Files:**
- Create: `pkg/state/snapshot.go` (copy from `pkg/store/snapshot.go`)
- Create: `pkg/state/event.go` (extract event types from `pkg/store/snapshot.go`)
- Modify: `pkg/store/snapshot.go` (replace with type aliases to `pkg/state`)

- [ ] **Step 1: Copy `store/snapshot.go` to `state/snapshot.go`**

Copy verbatim. Change package to `state`. Keep all types: `Snapshot`, `NodeView`, `WorkloadSpecView`, `NodePlacementState`, `TrafficSnapshot`, `Connection`, `StoreEvent` (and all event variants).

- [ ] **Step 2: Update imports in `state/snapshot.go`**

Ensure imports reference the correct packages (`coords.Coord` instead of `topology.Coord` if Phase 1 is complete, otherwise `topology.Coord`).

Run: `goimports -w pkg/state/snapshot.go`

- [ ] **Step 3: Make `store/snapshot.go` re-export from `state`**

Replace `pkg/store/snapshot.go` contents with type aliases. For every exported type in the original `store/snapshot.go`, create a corresponding type alias. Use `go doc pkg/store` or grep for exported types to ensure completeness. Run `go build ./...` to catch any missing aliases — build failures will indicate types that consumers reference but are not aliased.

This ensures all existing consumers of `store.Snapshot` continue to work without import changes.

- [ ] **Step 4: Verify full build**

Run: `go build ./...`
Expected: Clean. All consumers still import `store` and see the same types.

- [ ] **Step 5: Run full test suite**

Run: `go test -race -count=1 ./...`
Expected: All pass.

- [ ] **Step 6: Commit**

```
git add pkg/state/ pkg/store/
git commit -s -m "move snapshot and event types to state package"
```

### Task 2.3: Move Store Core to `pkg/state`

**Files:**
- Create: `pkg/state/store.go` (copy from `pkg/store/gossip.go`)
- Create: `pkg/state/disk.go` (copy from `pkg/store/disk.go`)
- Modify: `pkg/store/gossip.go` (replace with thin wrapper delegating to `state.Store`)

- [ ] **Step 1: Identify all exported symbols in `store/gossip.go` and `store/disk.go`**

Run: `grep -n '^func \|^type \|^var \|^const ' pkg/store/gossip.go pkg/store/disk.go | grep -v '^[[:lower:]]'`

Document every exported type, function, method, and variable. These will all need delegation wrappers in Step 5.

- [ ] **Step 1b: Copy `store/gossip.go` to `state/store.go`**

Copy the entire 2456-line file. Change package to `state`. This file contains `Store`, all attribute keys, all mutation methods, the CRDT merge logic, gossip encoding/decoding, and the event channel.

The file is large — this is intentional. We're shovelling, not restructuring. Internal restructuring (splitting into smaller files, removing the event channel in favor of synchronous returns) happens in a later task.

- [ ] **Step 1c: Copy `store/disk.go` to `state/disk.go`**

Copy verbatim. Change package to `state`.

- [ ] **Step 2: Update internal imports in `state/store.go`**

Replace any `store.` self-references. Update imports to reference `coords` instead of `topology` for Vivaldi types.

Run: `goimports -w pkg/state/store.go pkg/state/disk.go`

- [ ] **Step 3: Verify `pkg/state` compiles**

Run: `go build ./pkg/state/...`
Expected: Clean build.

- [ ] **Step 4: Make `store/gossip.go` delegate to `state.Store`**

Replace `pkg/store/gossip.go` with a thin wrapper. For every exported symbol identified in Step 1, create either a type alias (`type X = state.X`) or a var delegation (`var F = state.F`). Use `go build ./...` to catch any missing symbols.

- [ ] **Step 4b: Make `store/disk.go` delegate to `state` disk functions**

Replace `pkg/store/disk.go` with delegation to `state` package. Same approach: type aliases for exported types, var declarations for exported functions. Run `go build ./...` to verify all consumers still compile.

- [ ] **Step 5: Verify full build**

Run: `go build ./...`
Expected: Clean. All consumers still import `store` and see the same types/functions.

- [ ] **Step 6: Run full test suite**

Run: `go test -race -count=1 ./...`
Expected: All pass. Same count as baseline.

- [ ] **Step 7: Run lint**

Run: `just lint`

- [ ] **Step 8: Mutation test — break `state.Store` CRDT merge, verify gossip tests fail**

In `pkg/state/store.go`, find the `applyRemoteEvent` (or equivalent) function and add `return nil` at the top to skip all merges.
Run integration tests. Expected: GossipConvergence FAILS. Revert. Verify pass.

- [ ] **Step 9: Run Hetzner verification**

Run: `just hetzner-verify`

- [ ] **Step 10: Commit**

```
git add pkg/state/ pkg/store/
git commit -s -m "move store core logic to state package"
```

### Task 2.4: Move Store Tests to `pkg/state`

**Files:**
- Create: `pkg/state/*_test.go` (copy from `pkg/store/*_test.go`)
- Keep: `pkg/store/*_test.go` (kept as redundant safety net until Task 2.6 removes `pkg/store/` entirely)

- [ ] **Step 1: Copy all test files from `store/` to `state/`**

For each `pkg/store/*_test.go`, copy to `pkg/state/` with package changed to `state` (or `state_test` for external test packages).

- [ ] **Step 2: Adapt test imports**

Update any internal references from `store.` to direct references (since tests are now in the `state` package).

Run: `goimports -w pkg/state/*_test.go`

- [ ] **Step 3: Run state tests**

Run: `go test -race -count=1 ./pkg/state/...`
Expected: All pass.

- [ ] **Step 4: Verify store tests still pass (via delegation)**

Run: `go test -race -count=1 ./pkg/store/...`
Expected: All pass (store delegates to state, tests exercise the same code).

- [ ] **Step 5: Commit**

```
git add pkg/state/
git commit -s -m "port store tests to state package"
```

### Task 2.5: Migrate Consumers from `store` to `state`

**Files:**
- Discover via: `grep -r '"github.com/sambigeara/pollen/pkg/store"' pkg/ cmd/ --include='*.go' -l`

- [ ] **Step 1: Discover all consumers**

Run: `grep -r '"github.com/sambigeara/pollen/pkg/store"' pkg/ cmd/ --include='*.go' -l`

This gives the complete list of files to update. Do NOT rely on a hardcoded list — the grep output is authoritative.

- [ ] **Step 2: Update imports one package at a time**

For each consumer file, replace `"github.com/sambigeara/pollen/pkg/store"` with `"github.com/sambigeara/pollen/pkg/state"` and update all type references (`store.Snapshot` → `state.Snapshot`, `store.Store` → `state.Store`, etc.).

Do this one package at a time. After each package:
- Run `goimports -w <files>`
- Run `go build ./pkg/<package>/...`
- Run `go test -race ./pkg/<package>/...`

- [ ] **Step 3: Verify full build**

Run: `go build ./...`

- [ ] **Step 4: Run full test suite**

Run: `go test -race -count=1 ./...`

- [ ] **Step 5: Run lint**

Run: `just lint`

- [ ] **Step 6: Mutation test — verify delegation chain works end-to-end**

In `pkg/state/store.go`, break a CRDT merge function (add `return nil` to skip merges). Run integration tests. Expected: GossipConvergence FAILS. Revert. Verify pass. This confirms the new import paths exercise the same production code.

- [ ] **Step 7: Run Hetzner verification**

Run: `just hetzner-verify`

- [ ] **Step 8: Commit**

```
git add pkg/ cmd/
git commit -s -m "migrate all consumers from store to state package"
```

### Task 2.6: Remove `pkg/store` (After All Consumers Migrated)

**Files:**
- Delete: `pkg/store/` (entire directory)

- [ ] **Step 1: Verify no imports remain**

Run: `grep -r '"github.com/sambigeara/pollen/pkg/store"' pkg/ cmd/ --include='*.go'`
Expected: Zero results (all consumers now import `state`).

- [ ] **Step 2: Verify test coverage equivalence**

Count test functions in `pkg/state/` and compare to `pkg/store/`:
```
grep -c '^func Test' pkg/state/*_test.go
grep -c '^func Test' pkg/store/*_test.go
```
The state test count must be ≥ the store test count. Run `go test -race -count=1 ./pkg/state/...` to confirm all pass.

- [ ] **Step 3: Delete `pkg/store/`**

Remove the entire directory. This is safe because:
- All consumers now import `state` (verified in Step 1)
- All tests have been ported to `pkg/state/` (verified in Step 2)

- [ ] **Step 4: Verify full build**

Run: `go build ./...`

- [ ] **Step 5: Run full test suite**

Run: `go test -race -count=1 ./...`
Expected: Test count ≥ baseline (no tests lost).

- [ ] **Step 6: Run lint**

Run: `just lint`

- [ ] **Step 7: Mutation test — verify tests catch regressions through new import path**

In `pkg/state/store.go`, break a CRDT merge function. Run integration tests. Expected: FAILS. Revert. Verify pass.

- [ ] **Step 8: Run Hetzner verification**

Run: `just hetzner-verify`

- [ ] **Step 9: Commit**

```
git rm -r pkg/store/
git commit -s -m "remove pkg/store after migration to pkg/state"
```

### Task 2.7: Add Typed Projections to Snapshot

**Goal:** Add convenience methods on `Snapshot` for typed domain views, per the spec.

**Files:**
- Modify: `pkg/state/snapshot.go`
- Create: `pkg/state/snapshot_test.go` (add projection tests)

- [ ] **Step 1: Write failing tests for projection methods**

```go
func TestSnapshot_Peers(t *testing.T) {
    // Build a snapshot with known peers, verify Peers() returns them
}

func TestSnapshot_Services(t *testing.T) {
    // Build a snapshot with known services, verify Services() returns them
}

func TestSnapshot_Workloads(t *testing.T) {
    // Build a snapshot with known workload specs/claims, verify Workloads() returns them
}

func TestSnapshot_DeniedPeers(t *testing.T) {
    // Build a snapshot with denied peers, verify DeniedPeers() returns them
}

func TestSnapshot_Self(t *testing.T) {
    // Build a snapshot with a known local ID, verify Self() returns it
}

func TestSnapshot_Clock(t *testing.T) {
    // Build a snapshot after mutations, verify Clock() returns non-zero
}
```

Run: `go test ./pkg/state/... -run TestSnapshot_`
Expected: FAIL (methods don't exist yet).

- [ ] **Step 2: Implement projection methods on Snapshot**

Add methods to `pkg/state/snapshot.go`:
```go
func (s Snapshot) Peers() []PeerInfo { ... }
func (s Snapshot) Peer(key types.PeerKey) (PeerInfo, bool) { ... }
func (s Snapshot) Services() []ServiceInfo { ... }
func (s Snapshot) Workloads() []WorkloadInfo { ... }
func (s Snapshot) DeniedPeers() []types.PeerKey { ... }
func (s Snapshot) Self() types.PeerKey { ... }
func (s Snapshot) Clock() Clock { ... }
```

These extract and reshape data from the existing `Snapshot.Nodes`, `Snapshot.Specs`, `Snapshot.Claims`, `Snapshot.LocalID` fields. The underlying data structure doesn't change — these are read-only convenience views.

- [ ] **Step 3: Run tests**

Run: `go test -race ./pkg/state/... -run TestSnapshot_`
Expected: PASS.

- [ ] **Step 4: Run full test suite + lint**

Run: `go test -race -count=1 ./...` and `just lint`

- [ ] **Step 5: Run Hetzner verification**

Run: `just hetzner-verify`

- [ ] **Step 6: Commit**

```
git add pkg/state/
git commit -s -m "add typed projection methods to state.Snapshot"
```

### Task 2.8: API Surface Audit

**Goal:** Ensure `pkg/state` exports only the spec'd API. Unexport everything else.

- [ ] **Step 1: Audit exported symbols**

Run: `go doc pkg/state` and compare against the spec (Section "State" in `docs/superpowers/specs/2026-03-16-clean-room-architecture-design.md`).

The spec's public API is:
- `Store`, `New`
- `Snapshot()`, `ApplyDelta()`, `EncodeDelta()`, `FullState()`
- All typed mutation methods (`DenyPeer`, `SetLocalAddresses`, etc.)
- `Snapshot` type with projection methods (`Peers`, `Peer`, `Services`, `Workloads`, `DeniedPeers`, `Self`, `Clock`)
- Event types (`PeerJoined`, `PeerLeft`, `PeerDenied`, `ServiceChanged`, `WorkloadChanged`, `TopologyChanged`, `GossipApplied`)
- View types (`PeerInfo`, `ServiceInfo`, `WorkloadInfo`, `Clock`)

Everything else (attribute keys, internal helpers, CRDT merge internals) should be unexported.

- [ ] **Step 2: Unexport internal symbols**

For each exported symbol not in the spec's API, lowercase it to make it unexported. Run `go build ./...` after each change to catch any external consumers. If an external consumer breaks, that's a signal the spec may be missing something — evaluate whether to add it to the spec or refactor the consumer.

- [ ] **Step 3: Run full test suite + lint + Hetzner**

Run: `go test -race -count=1 ./...`, `just lint`, `just hetzner-verify`

- [ ] **Step 4: Commit**

```
git add pkg/state/
git commit -s -m "unexport state package internals, enforce spec API surface"
```

---

## Phase 3: Transport Package — Rename and Refine `mesh` (Outlined)

**Goal:** Rename `pkg/mesh` to `pkg/transport`, fold in `pkg/peer` and `pkg/sock` as internal implementation details. Narrow the API per the spec.

**Strategy:** Same as Phase 2 — create `pkg/transport` alongside `pkg/mesh`, move code, update consumers, delete old package. The key API change: remove routing logic from transport (routed streams become a higher-level concern).

**Tasks (to be detailed when Phase 2 is complete):**

- **Task 3.1:** Create `pkg/transport/` shell
- **Task 3.2:** Move `mesh.go` core to `transport/transport.go` (copy-paste, adapt package)
- **Task 3.3:** Move `peer/peer.go` into `transport/peer.go` (internal, unexport)
- **Task 3.4:** Move `sock/` into `transport/sock.go` (internal, unexport)
- **Task 3.5:** Move supplementary mesh files (`discovery.go`, `identity.go`, `invite.go`, `session_registry.go`)
- **Task 3.6:** Move tests, adapt integration tests
- **Task 3.7:** Migrate consumers (`node`, `integration/cluster`, `cmd/pln`)
- **Task 3.8:** Remove `pkg/mesh`, `pkg/peer`, `pkg/sock`
- **Task 3.9:** API surface audit — unexport everything not in the spec
- **Task 3.10:** Verification cycle (mutation tests, full suite, Hetzner)

**Key API changes during this phase:**
- Remove `SetRouter` from transport — routing moves to a higher layer
- Remove `OpenStream/OpenArtifactStream/OpenWorkloadStream` routed fallback — transport is direct-peer only
- Remove gossip-specific types from transport API (`Envelope` → opaque `[]byte`)
- Keep `AcceptAllStreams`, `StreamType` constants

---

## Phase 4: Routing Package — Rename `route` (Outlined)

**Goal:** Rename `pkg/route` to `pkg/routing`, simplify API to pure `Build()` + `Table.NextHop()`.

**Tasks:**
- **Task 4.1:** Create `pkg/routing/`, copy `route/table.go`
- **Task 4.2:** Simplify API: `Build(self, topology, connected) Table` + `NextHop(dest) (next, ok)`
- **Task 4.3:** Update consumers, remove `pkg/route`
- **Task 4.4:** Verification cycle

---

## Phase 5: Domain Services — Split `node` (Outlined)

**Goal:** Split the 1870-line `pkg/node/node.go` into three independent domain services + a supervisor. This is the most complex phase.

**Strategy:** Extract one service at a time. After each extraction, the remaining `node` package still works — it just delegates to the extracted service. When all three are extracted, what remains becomes `supervisor`.

**Order:** Membership first (owns gossip — the most critical path), then placement (scheduling — mostly self-contained), then tunneling (service bridging — smallest scope).

### Phase 5a: Extract `pkg/membership`

**Tasks:**
- **Task 5a.1:** Create `pkg/membership/` shell with consumer interfaces (`ClusterState`, `Network`)
- **Task 5a.2:** Move gossip loop from `node.go` to `membership/service.go`
- **Task 5a.3:** Move peer selection from `node.go` + `topology/topology.go` to `membership/`
- **Task 5a.4:** Move cert renewal logic from `node.go` to `membership/`
- **Task 5a.5:** Move Vivaldi exchange (clock stream handler) to `membership/`
- **Task 5a.6:** Add `Events() <-chan state.Event` output channel
- **Task 5a.7:** Wire `node.go` to delegate to `membership.Service`
- **Task 5a.8:** Adapt tests, mutation test, Hetzner

### Phase 5b: Extract `pkg/placement`

**Tasks:**
- **Task 5b.1:** Create `pkg/placement/` with consumer interfaces (`WorkloadState`)
- **Task 5b.2:** Move scheduling logic from `scheduler/` into `placement/`
- **Task 5b.3:** Move workload lifecycle from `workload/` into `placement/`
- **Task 5b.4:** Move CAS interactions and artifact fetch into `placement/`
- **Task 5b.5:** Wire `node.go` to delegate to `placement.Service`
- **Task 5b.6:** Adapt tests, mutation test, Hetzner

### Phase 5c: Extract `pkg/tunneling`

**Tasks:**
- **Task 5c.1:** Create `pkg/tunneling/` with consumer interfaces (`ServiceState`, `StreamTransport`)
- **Task 5c.2:** Move tunnel management from `tunnel/` into `tunneling/`
- **Task 5c.3:** Move traffic tracking from `traffic/` into `tunneling/`
- **Task 5c.4:** Move relay/routed stream handling from mesh into `tunneling/`
- **Task 5c.5:** Wire `node.go` to delegate to `tunneling.Service`
- **Task 5c.6:** Adapt tests, mutation test, Hetzner

---

## Phase 6: Supervisor — Transform `node` into Composition Root (Outlined)

**Goal:** After all domain services are extracted, the remaining `node` code is lifecycle management, wiring, and stream dispatch. Rename it to `supervisor`.

**Tasks:**
- **Task 6.1:** Create `pkg/supervisor/` shell
- **Task 6.2:** Move remaining `node.go` orchestration to `supervisor/supervisor.go`
- **Task 6.3:** Move stream dispatch loop
- **Task 6.4:** Move startup/shutdown ordering
- **Task 6.5:** Move `node/svc.go` → `pkg/control/` (gRPC translation layer)
- **Task 6.6:** Update `cmd/pln/` to use `supervisor.New` instead of `node.New`
- **Task 6.7:** Remove `pkg/node/`
- **Task 6.8:** Full verification cycle

---

## Phase 7: Cleanup (Outlined)

**Goal:** Remove old packages, verify no stale references, final behavioral verification.

**Tasks:**
- **Task 7.1:** Remove any remaining delegation wrappers
- **Task 7.2:** Grep for old package imports across entire codebase
- **Task 7.3:** Verify `go vet ./...` clean
- **Task 7.4:** Full mutation test cycle on all integration tests
- **Task 7.5:** Final Hetzner verification
- **Task 7.6:** Update `ARCHITECTURE.md` to reflect new package structure
- **Task 7.7:** Tag release

---

## Phase Dependencies

```
Phase 0 (gates) ─────────────────────────────┐
                                              │
Phase 1 (coords) ────────────────────────────┐│
                                             ││
Phase 2 (state) ─────────────────────────────┼┤
                                             ││
Phase 3 (transport) ─────────────────────────┼┤
                                             ││
Phase 4 (routing) ───────────────────────────┤│
                                             ││
Phase 5a (membership) ───────────────────────┤│
Phase 5b (placement) ────────────────────────┤│
Phase 5c (tunneling) ────────────────────────┘│
                                              │
Phase 6 (supervisor + control) ───────────────┤
                                              │
Phase 7 (cleanup) ────────────────────────────┘
```

Phases 2 and 3 can proceed in parallel (state and transport are siblings).
Phases 5a, 5b, 5c must be sequential (each extraction changes `node.go`).
All other phases are sequential.

---

## Migration Invariants

These must hold true at every commit across the entire migration:

1. `go build ./...` succeeds
2. `go test -race -count=1 ./...` passes with test count ≥ baseline
3. `just lint` passes
4. `just hetzner-verify` passes
5. No integration test removed — only adapted or added
6. Status output, logging verbosity, metrics, wire format, CLI surface unchanged
7. Behavioral inventory (`docs/behavioral-inventory.md`) fully preserved
