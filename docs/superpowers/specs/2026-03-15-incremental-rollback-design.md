# Incremental Rollback to v0.1.0-alpha.128

## Problem

The refactor that began in v0.1.0-alpha.129 cumulatively changed 204 files (+38k/-26k lines) across the 128..139 range. While the new package layout and integration test harness are valuable, the refactor dropped operational glue across permissions, daemon lifecycle, gossip propagation, logging, bootstrap dialing, and admin commands. Eleven alpha releases of bug-chasing have not fully stabilized it, and the big-bang nature makes it impossible to isolate which internal changes caused which regressions.

## Decision

Roll back to v0.1.0-alpha.128 as the working baseline. Port the integration test harness forward. Use the refactored code (preserved in git history) as a design reference for incremental, test-validated migrations.

## Rollback

A single commit restores the v0.1.0-alpha.128 tree on main:

```
git checkout v0.1.0-alpha.128 -- .
git rm -r --cached $(git diff --name-only --diff-filter=A v0.1.0-alpha.128..HEAD)
git commit -m "roll back to v0.1.0-alpha.128 baseline"
```

The `git rm` step removes files that were added after 128 — `git checkout` only restores tracked files, so post-128 additions would otherwise linger as ghost files. This preserves all refactor commits in history for reference. No cherry-picks needed — 128 already has working permissions, `/var/lib/pln` detection, daemon lifecycle, and gossip propagation.

## Integration Test Harness

The test harness (~2,700 lines) is the primary artifact worth porting from the refactor. It consists of:

- **VirtualSwitch**: In-process UDP simulation with configurable latency, jitter, NAT gating, and partitions
- **ClusterAuth**: Root key generation, TLS cert and delegation cert minting per node
- **TestNode**: Wraps the full node stack (transport + state + orchestrator)
- **Builder**: Fluent API for cluster construction with presets (`PublicMesh`, `RelayRegions`)
- **Assertions**: Polling-based convergence checks (`RequireConverged`, `RequireHealthy`, etc.)
- **Scenarios**: 9 test suites covering gossip convergence, topology stabilization, workload placement, NAT punchthrough, partition/heal

The harness is ported by rewriting against 128's APIs (`node.New`, `store.New`, etc.) while preserving the concepts, builder pattern, and scenario structure. This is the hardest single task — the semantic mapping between 128's and 139's APIs is non-trivial and will take iteration. The goal is a green test suite from day one so every subsequent migration step is validated.

## Migration Strategy

Incremental, from leaves inward. Each step is its own commit/PR, validated by integration tests before proceeding.

### Phase 1: Package renames (no behavior change)

Mechanical moves to adopt the cleaner layout. Examples:

- `pkg/auth` to `pkg/crypto`
- `pkg/store` to `pkg/state`

Pure renames with import path updates. No internal changes.

### Phase 2: Isolated internal improvements

Subsystems with clear boundaries that can be swapped independently:

- Peer FSM (`pkg/peer` to `pkg/overlay/peerfsm` or similar)
- Vivaldi coordinates
- Routing table
- Traffic tracking

Each swap: adapt integration tests first, make the change, prove tests pass.

### Phase 3: Core engine

The orchestrator event loop, transport abstraction, and gossip protocol. These are the riskiest changes and require the strongest test coverage before attempting. By this point, phases 1-2 have built up that coverage.

### Phase 4: Control plane

The unjoined-to-joined lifecycle, gRPC control service. Sits on top of everything else, so it comes last.

## Selective Adoption

The refactored code is a menu, not a checklist. Each piece earns its way in by demonstrating value without introducing regressions. Things to evaluate critically before porting:

- **Umbrella packages** (`overlay/`, `runtime/`): is the extra directory level justified, or is it just indirection?
- **Efficiency changes**: only migrate when there's a concrete, measurable reason
- **Any change whose purpose is unclear**: skip until needed

## Reference Material

Available in git history (not kept in working tree):

- All refactor commits (`v0.1.0-alpha.128..v0.1.0-alpha.139`)
- `docs/IDEAL_ARCHITECTURE.md`
- `docs/PACKAGE_AUDIT.md`
- `docs/architectural-analysis.md`
- `docs/concurrency.md`

Access via `git show v0.1.0-alpha.139:<path>` or `git diff v0.1.0-alpha.128..v0.1.0-alpha.139 -- <path>`.
