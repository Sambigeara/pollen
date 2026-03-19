# Phase 5 Completion Checklist: Stream Type Alignment

Source of truth: `TARGET_ARCHITECTURE_SPEC.md`
Gap analysis: `GAP_ANALYSIS.md`

---

## Decision

Adopted **Option A: `transport.Stream`** — stronger typing, `CloseWrite()` available without type assertion. Consumer `OpenStream` interfaces keep `io.ReadWriteCloser` to avoid unnecessary coupling (only handler parameters changed).

---

## Affected Handlers (6 total)

### Membership
- [x] `HandleClockStream(stream, peer)` — changed from `(_ context.Context, from types.PeerKey, stream io.ReadWriteCloser)` to `(stream transport.Stream, from types.PeerKey)`.
- [x] `HandleCertRenewalStream(stream, peer)` — changed from `(stream io.ReadWriteCloser, peer types.PeerKey)` to `(stream transport.Stream, peer types.PeerKey)`.

### Placement
- [x] `HandleArtifactStream(stream, peer)` — changed from `(stream io.ReadWriteCloser, _ types.PeerKey)` to `(stream transport.Stream, _ types.PeerKey)`.
- [x] `HandleWorkloadStream(stream, peer)` — changed from `(stream io.ReadWriteCloser, _ types.PeerKey)` to `(stream transport.Stream, _ types.PeerKey)`.

### Tunneling
- [x] `HandleTunnelStream(stream, peer)` — changed from `(stream io.ReadWriteCloser, peer types.PeerKey)` to `(stream transport.Stream, peer types.PeerKey)`.
- [x] `HandleRoutedStream(stream, peer)` — changed from `(stream io.ReadWriteCloser, peer types.PeerKey)` to `(stream transport.Stream, peer types.PeerKey)`.

## Stream Dispatch (supervisor)
- [x] `AcceptStream` return type — changed from `io.ReadWriteCloser` to `transport.Stream` in both `transport.impl` and `supervisor.Transport` interface. `acceptedStream.stream` field updated to `Stream`.

## Consumer Interfaces
- [x] `tunneling.StreamTransport.OpenStream` — kept as `io.ReadWriteCloser`. Used internally by services, not passed to handlers.
- [x] `placement.StreamOpener.OpenStream` — kept as `io.ReadWriteCloser`. Same reasoning.

## HandleClockStream Extra Parameter
- [x] Removed unused `context.Context` parameter from `HandleClockStream`. Supervisor dispatch updated to `HandleClockStream(stream, peerKey)`.

## transport.Stream Type
- [x] Verified: `transport.Stream` is exported, wraps `*quic.Stream`, provides `Read`, `Write`, `Close` (via embedding) + explicit `CloseWrite() error`.

---

## Verification Gates

- [x] `go build ./...` — passes
- [x] `go test -count=1 ./...` — all unit tests pass
- [x] `go test -tags integration -count=1 -timeout 120s ./pkg/integration/cluster/...` — passes
- [x] `just lint` — 0 issues
- [ ] `cd infra && just verify-hetzner` — steps 0-10 pass
