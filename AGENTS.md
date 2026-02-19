# Pollen Agent Guide

This repository is Go-based and uses `just` as the primary task runner.
Follow these conventions when changing code or tests.

## Commands

- Build: `just build`, `just compile`, `just bin`, `just --list`
- Lint: `just lint`, `just lint-modernize`
- Test: `just test`, `just test PKG=./pkg/node`, `just test PKG=./pkg/node TEST=Graceful`
- Proto generation: `just generate` after editing files under `api/public/`
- Tooling binaries are installed into `${HOME}/.cache/pollen/bin`

## Repository Layout

- `cmd/pollen`: CLI entry point
- `pkg/`: core runtime packages
- `api/public/`: protobuf definitions
- `api/genpb/`: generated protobuf Go code (do not hand-edit)
- `tools/`: tooling module for codegen/lint helpers

## Protobuf / Generated Code

- Do not manually edit files in `api/genpb/`
- Run `just generate` after protobuf changes
- Keep generated code in sync before linting
- Prefer vtproto methods (`MarshalVT` / `UnmarshalVT`) where available

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

## Invariants and Checks

- Avoid superfluous defensive checks that cannot happen under established lifecycle invariants
- Enforce invariants at construction/startup boundaries and fail fast there
- Keep nil/state checks only for real runtime paths
- Keep hot paths lean; do not re-check already-guaranteed state

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
- `pollen service rm` only removes local services
- Service removal should revoke active streams; remote forwards are removed after gossip convergence

## Auth Roadmap (Pointer)

- Long-term direction remains: Ed25519 identity per node, QUIC mutual auth bound to membership, and invite-based enrollment rooted in a trusted genesis fingerprint.
- Keep AGENTS concise; detailed auth design notes should live in dedicated design docs.
