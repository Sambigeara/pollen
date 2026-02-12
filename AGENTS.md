# Pollen Agent Guide

This repository is Go-based and uses `just` as the primary task runner.
Follow the conventions below when making changes or writing tests.

## Commands

### Build
- `just build` (runs `just generate` + `just lint`)
- `just compile` (fast sanity build + no-op test run)
- `just bin` (builds `pollen` binary)
- `just --list` (show available tasks)

### Lint / Format
- `just lint` (golangci-lint + buf lint + buf format check)
- `just lint-modernize` (go modernize fixer)
- Go formatting: `gofmt -w <files>`

### Test
- `just test` (default: all packages)
- Single package: `just test PKG=./pkg/node`
- Single test: `just test PKG=./pkg/node TEST=Graceful`
- Direct go test: `go test ./pkg/node -run Graceful -count=1 -v`

### Protobufs
- `just generate` after editing any proto definitions
- Generated Go code lives in `api/genpb`

### Tooling Notes
- `just` will install required tools into `${HOME}/.cache/pollen/bin`
- `buf`, `golangci-lint`, and `modernize` are pinned via the `tools/` module

## Repository Layout

- `cmd/pollen`: CLI entry point
- `pkg/`: core libraries (node/link/peer/state/tunnel/transport)
- `internal/testutil/`: in-repo test harnesses (e.g. memtransport)
- `api/public/`: protobuf definitions
- `api/genpb/`: generated protobuf Go code
- `tools/`: tooling module for codegen/lint helpers

## Generated Code & Files

- Do not hand-edit files under `api/genpb/`
- After changing protos in `api/public/`, run `just generate`
- Keep generated code in sync before running lint (buf format/lint will fail)

## Workspace Notes

- This repo uses Go workspaces (`go.work`)
- Tooling installs may set `GOWORK=off` during codegen
- Prefer `just` targets for repo-wide operations

## Test Harness Notes

- Prefer the in-memory transport for deterministic tests:
  - `internal/testutil/memtransport` implements `transport.Transport`
  - Use `memtransport.NewNetwork()` + `network.Bind("ip:port")`
- Use distinct bind addresses per node/link (avoid accidental self-routing)
- Create a fresh memtransport `Network` per test for isolation
- For node/link tests, use explicit timing knobs to reduce waits:
  - `peer.Config` via `peer.NewStoreWithConfig`
  - `link` options (`WithEnsurePeerInterval`, `WithEnsurePeerTimeout`, etc.)
  - `node.Config` fields: `PeerConfig`, `LinkOptions`, `PunchAttemptTimeout`,
    `DisableGossipJitter`
- Stop goroutines cleanly by canceling contexts and waiting on done channels

## Code Style

### Imports
- Standard library first, then third-party, then local imports
- Use gofmt formatting (no manual alignment)
- Avoid adding new dependency groups unless necessary

### Naming
- Use idiomatic Go names: short receiver names (`n`, `i`, `m`) consistent
  with surrounding code
- Prefer clear, intention-revealing names for exported types and functions
- For tests, use descriptive `TestX_Y` names for scenario-based behavior

### Types and APIs
- `context.Context` is the first parameter when used
- Prefer explicit config structs/options over hidden globals
- Keep interfaces small and behavior-focused (see `link.Link`, `transport.Transport`)
- Avoid exporting fields unless needed by tests or other packages

### Error Handling
- Wrap errors with context using `fmt.Errorf("...: %w", err)`
- Avoid swallowing errors; log only when useful and still return errors
- Use sentinel errors only where they are already used
- In tests, use `require.NoError` for setup steps that must succeed

### Logging
- Use `zap.S()` or `zap.S().Named()` as in existing code
- Avoid noisy logs inside tight loops unless debug-only
- Prefer structured fields (`logger.Infow`) over formatted strings

### Concurrency
- Avoid data races; use locks/channels consistently
- Prefer channels for non-blocking event notifications
- When using `select`, include `default` only when truly non-blocking is required
- Ensure goroutines have a clear shutdown path (`context.Context` or close channels)

### Protobuf / VTProto
- Use `MarshalVT` / `UnmarshalVT` for vtproto-generated messages
- Keep payloads consistent with `types.MsgType` enums
- Regenerate after proto edits: `just generate`

### Comments
- Avoid adding comments. Code should be self-documenting through clear naming and structure.
- Only add comments for truly complex logic that cannot be made clear through refactoring.
- Do not add explanatory comments like "// 1 round = try all addresses" - the variable name should convey intent.

## Testing Conventions

- Use `testify/require` for hard failures, `testify/assert` for expectations
- For async behavior, use `require.EventuallyWithT` with bounded timeouts
- Prefer deterministic time control via config rather than long sleeps
- When restarting nodes in tests, reuse the same `PollenDir` to preserve keys
- When tests fail, prefer asserting on local state (peer store, session store)

## Networking Test Patterns

- Distinguish "advertised" addresses from bind addresses in tests
- For punch-coordination tests, ensure coordinator is a different peer than target
- Avoid large timeouts; shrink intervals via config instead
- Use `MsgTypeTest` handlers for simple reachability checks

## State & Peer Notes

- Peer discovery happens via gossiped `statev1.GossipNode` snapshots
- `peer.Store.Step` is pure and accepts an explicit `now` for deterministic tests
- Use `peer.ConnectFailed` to force stage escalation (direct -> punch)
- `PeerDisconnected` transitions back to discovery with a retry interval

## Project-Specific Notes

- Go version: see `go.mod` (`go 1.25.1`)
- The node startup path uses:
  - Invite-based XXpsk2 handshake
  - Gossip for state propagation
  - IK for reconnection
- The test framework supports graceful restart scenarios; use memtransport
  for stable cross-node tests.
- The project prefers deterministic unit tests over sleep-heavy integration tests

## CLI Status/Service Notes

- `pollen status` uses live link sessions for `online/offline` and gossiped node services for service listings.
- `pollen service rm` only removes services exposed by the local node; do not attempt to delete remote services.
- Service removal now hard-revokes active streams and other nodes auto-remove forwards once gossip reflects the service removal.

## Cursor/Copilot Rules

- None found in `.cursor/rules/`, `.cursorrules`, or `.github/copilot-instructions.md`.
