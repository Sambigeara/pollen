# Pollen Agent Guide

This repository is Go-based and uses `just` as the primary task runner.
Follow these conventions when changing code or tests.

## Pre-release Compatibility

- Pollen is pre-release and unreleased.
- Prioritize clear ergonomics and correctness over backward compatibility.
- It is acceptable to remove or change existing flows/protos when improving the model.

## What Pollen Is

- Pollen is an ergonomic peer-to-peer mesh and WASM workload orchestration
  layer and runtime.
- Ergonomics come first:
  - You can bring up a cluster with a minimal set of commands.
  - Commands are predictable and clean, for example:
    - `pollen invite [node-pub]`
    - `pollen up`
    - `pollen join <token>`
    - `pollen stop`
    - `pollen status`
    - `pollen serve 8080 foo`
    - `pollen connect foo`
  - Opinionated defaults should cover most use cases, with configuration
    available when needed.
- Pollen is self-governing:
  - Every node is a first-class citizen; there is no central control plane.
  - Decisions are made locally and deterministically from each node's view of
    the world, including topology, workload orchestration, and routing.

## Commands

- Build: `just build`, `just compile`, `just bin`, `just --list`
- Lint: `just lint`, `just lint-modernize`
- Test: `just test`, `just test PKG=./pkg/node`, `just test PKG=./pkg/node TEST=Graceful`
- Proto generation: `just generate` after editing files under `api/public/`
- Tooling binaries are installed into `${HOME}/.cache/pollen/bin`

## Repository Layout

- `cmd/pollen`: CLI entry point
- `pkg/`: core runtime packages
- `pkg/config`: local admin/runtime metadata persisted under the pollen dir
- `api/public/`: protobuf definitions
- `api/genpb/`: generated protobuf Go code (do not hand-edit)
- `tools/`: tooling module for codegen/lint helpers

## Local State Files

- `config.yaml`: admin-owned local config (for example bootstrap peers)
- `state.yaml`: node-owned runtime gossip snapshot persisted by `pkg/store`
- `consumed_invites.json`: signer-owned invite redemption state

## Protobuf / Generated Code

- Do not manually edit files in `api/genpb/`
- Run `just generate` after protobuf changes
- Keep generated code in sync before linting
- Prefer vtproto methods (`MarshalVT` / `UnmarshalVT`) where available
- Use `MarshalVT` / `UnmarshalVT` instead of `proto.Marshal` /
  `proto.Unmarshal` by default; only use `proto.MarshalOptions` when specific
  options (for example deterministic encoding) are required
- Prefer schema-level validation rules in proto (`buf.validate`) and run
  protovalidate in Go for structural checks (lengths, UUID shape, required
  fields) instead of duplicating those checks by hand in business logic

## Git Commit Conventions

- Sign commits with `git commit -s`
- Keep subject lines concise (~50 chars)
- Wrap commit body lines at 72 chars

## Code Style

- Follow idiomatic Go naming and receiver style used in surrounding code
- Keep imports gofmt/goimports-compatible (enforced by lint)
- Put `context.Context` first when present
- Prefer clear APIs over extra exported fields
- Only add comments when they explain something not obvious from reading the code
- Inline single-callsite helper functions; avoid wrappers that only delegate
- Co-locate state with the type that owns it (methods over free functions
  that thread context like directory paths)

## Invariants and Checks

- Avoid superfluous defensive checks that cannot happen under established lifecycle invariants
- Enforce invariants at construction/startup boundaries and fail fast there
- Keep nil/state checks only for real runtime paths
- Keep hot paths lean; do not re-check already-guaranteed state
- Prefer minimal but complete code: keep checks that enforce real security or
  consistency invariants, and remove duplicate checks once those guarantees are
  already established upstream
- Trust cryptographic guarantees: if a signature covers a field, do not
  separately validate that field against the same source of truth
- Lean on protovalidate: if a constraint is enforced in the proto schema
  (`buf.validate`) and protovalidate runs upstream, do not duplicate the
  check in business logic
- Assume well-formed messages from trusted peers; do not defensively guard
  against malformed data that proto validation or the issuance path already
  prevents
- Do not add speculative code paths (lazy-loading, fallback logic) for
  scenarios that no supported control flow can trigger
- Validate trust bundles at controlled ingest boundaries (disk/token/network)
  and avoid re-validating the same trust bundle repeatedly along internal
  call paths

## Error Handling and Logging

- Wrap errors with context using `fmt.Errorf("...: %w", err)`
- Do not silently swallow errors
- Use `zap.S()` / `zap.S().Named()` and structured logging (`Infow`, `Debugw`)
- Avoid noisy logs in tight loops unless they are debug-level and useful

## Concurrency

- Ensure goroutines have a clear shutdown path (`context.Context`, channel close, or both)
- Use channels for non-blocking notifications when appropriate
- Use `WaitGroup.Go` instead of manual `Add`/`Done`
- This repo targets modern Go (1.22+):
  - close over `range` loop vars directly in goroutines
  - prefer `for i := range n` over C-style counted loops for fixed-count iteration

## Testing

- Prefer integration-style tests at component boundaries
- Use `require` for hard setup/assertion failures and `assert` for softer checks
- For async behavior, use bounded `require.Eventually` / `require.EventuallyWithT`
- Prefer deterministic timing via config over sleeps
- Restart tests should reuse the same data directory when persistence is part of behavior

## Product Behavior Notes

- `pollen status` reports online/offline from live sessions and services from gossip
- Service removal should revoke active streams; remote forwards are removed after gossip convergence

## Auth Model

- Pollen uses two Ed25519 key roles:
  - Node key: each node's transport/TLS identity (`keys/ed25519.key`)
  - Admin key: local signing key used for invite/join issuance when paired with
    valid admin authority (`keys/admin_ed25519.key`)
- Cluster trust is rooted in a root admin public key stored in the trust
  bundle (`keys/cluster.trust.pb`):
  - `cluster_id = sha256(root_pub)`
  - all admin and membership authority chains to `root_pub`
- Admin authority model:
  - Root admin self-issues an `AdminCert` when signing.
  - Delegated admins use a root-signed `AdminCert`
    (`keys/admin.cert.pb`).
  - Delegated admin certs are intentionally longer-lived than join/invite
    tokens.
  - Membership and signing authority are separate: a node can be a valid
    cluster member without being an admin signer.
  - Only root admin may delegate admin authority. Delegated admins issue
    invites/join tokens but do not mint further admin delegation.
- Enrollment uses two token types:
  - `InviteToken`: signed by an admin cert, includes trust + issuer + bootstrap
    peers, with optional subject binding (`pollen invite` open,
    `pollen invite <node-pub>` strict).
  - `JoinToken`: signed by an admin cert, includes trust + issuer +
    subject-bound membership cert + bootstrap peers.
- Invite redemption is one-shot:
  - bootstrap verifies invite signature/TTL/cluster/issuer and optional subject
    binding against peer TLS key
  - token IDs are consumed locally in `consumed_invites.json`
  - bootstrap returns a short-lived `JoinToken`
  - invite ALPN is enabled only on signer-capable nodes; non-signer nodes do
    not advertise/accept invite redemption handshakes
- Steady-state mesh auth:
  - every QUIC session does post-TLS bidirectional `AuthProof` exchange
  - membership cert subject must match TLS peer key
  - peers from other clusters or with invalid/expired certs are
    rejected before session admission
- Gossip scope:
  - gossip carries topology/service/reachability state
  - trust bundles, admin certs, and tokens are not gossiped
- Bootstrap ergonomics:
  - `pollen bootstrap ssh <host>` seeds relay, delegates relay admin cert,
    and joins local admin node
  - admin stores bootstrap peers in `config.yaml` so future invites can omit
    explicit `--bootstrap`
  - `pollen admin keygen` and `pollen admin set-cert` are internal bootstrap
    plumbing (hidden), not primary user-facing onboarding commands
