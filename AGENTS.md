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

## Repository Layout

- `cmd/pollen`: CLI entry point
- `pkg/`: core libraries (node/link/peer/state/tunnel/transport)
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

- For now, keep tests minimal, integration heavy, and just at the main component boundaries.

## Code Style

### Imports
- `just lint` takes care of import ordering

### Naming
- Use idiomatic Go names: short receiver names (`n`, `i`, `m`) consistent
  with surrounding code
- Prefer clear, intention-revealing names for exported types and functions
- For tests, use descriptive `TestX_Y` names for scenario-based behavior

### Types and APIs
- `context.Context` is the first parameter when used
- Avoid exporting fields unless needed by tests or other packages

### Invariants and Checks
- Avoid superfluous defensive checks that cannot occur under established lifecycle invariants.
- Prefer enforcing invariants at startup/construction boundaries and failing fast there.
- Only keep nil/state checks that correspond to a real runtime path; remove misleading "just in case" guards.
- Keep hot paths lean: do not re-check state that was already guaranteed in the same control flow.

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

## CLI Status/Service Notes

- `pollen status` uses live link sessions for `online/offline` and gossiped node services for service listings.
- `pollen service rm` only removes services exposed by the local node; do not attempt to delete remote services.
- Service removal now hard-revokes active streams and other nodes auto-remove forwards once gossip reflects the service removal.

## Cursor/Copilot Rules

- None found in `.cursor/rules/`, `.cursorrules`, or `.github/copilot-instructions.md`.

## Long term goal for auth

```
**Pollen QUIC Identity & Membership — Implementation Summary**

---

**Identity model:** Every node has a long-lived Ed25519 keypair. This *is* the node's identity. The TLS certificate is a self-signed X.509 wrapper around this key, existing only to satisfy QUIC's TLS requirement.

**Transport authentication:** QUIC's `tls.Config` uses a custom `VerifyPeerCertificate` (or `VerifyConnection`) callback. It extracts the Ed25519 public key from the peer's presented certificate and checks it against the membership CRDT. The handshake fails if the peer isn't a member. `InsecureSkipVerify: true` is set to disable X.509 chain validation — your callback is the real verification, and it runs during the handshake, not after. Both sides present certificates (`ClientAuth: RequireAnyClientCert`), giving you mutual authentication. Set ALPN via `NextProtos` or QUIC handshakes fail silently.

**Membership CRDT:** The membership set contains signed facts, not raw entries. Every `Add(pubkey, meta, issuedAt)` and `Revoke(pubkey, reason, issuedAt)` is signed by an authority key rooted in `genesis.pub`. Nodes verify these signatures before merging. This prevents a compromised node from injecting itself into the membership set via gossip. Revocation checks take precedence over additions — a revoked key cannot be resurrected by replaying an old `Add`.

**Join flow:**

1. `pollen invite` produces a single join string containing: bootstrap addresses, a short-lived invite secret (signed by an admin/genesis delegate), and the genesis public key fingerprint.

2. `pollen join <token>` connects to a bootstrap node via QUIC. The TLS verifier for this connection is in **enrol mode** — it pins the bootstrap's presented public key against the genesis fingerprint from the invite token. No blind trust.

3. Over this authenticated channel, the joining node sends the invite secret and its own public key.

4. The bootstrap validates the invite, creates a signed `Add` fact for the new node, and merges it into the membership CRDT. It returns the current membership snapshot.

5. The new node stores `genesis.pub`, its own keypair, and the membership snapshot. All future connections use **normal mode** — verifying peers against the membership CRDT.

**What to defer:** Key rotation, 0-RTT, and proactive connection teardown on revocation. These are real concerns but not v1 blockers. Eventual consistency on revocation is fine initially. 0-RTT should stay disabled until after the join flow is proven solid.
```
