# pkg/mesh

## Exported API

### Interfaces

- `Mesh` — full 24-method interface; used for construction and by `pkg/node`
- `Router` — next-hop lookup for multi-hop mesh routing

### Types

- `Packet` — datagram envelope plus sender key
- `CloseReason` — typed reason string for session closes

### Constructor

```go
func NewMesh(defaultPort int, signPriv ed25519.PrivateKey, creds *auth.NodeCredentials,
    tlsIdentityTTL, membershipTTL, reconnectWindow, maxConnectionAge time.Duration,
    isDenied func(types.PeerKey) bool, mm *metrics.MeshMetrics) (Mesh, error)
```

`trafficTracker` is initialised to `traffic.Noop`; wire a real recorder with `SetTrafficTracker`.

### Narrow consumer interfaces

No narrow interfaces are declared in `pkg/mesh`. Consumers declare them locally:

| Interface | Methods | Package |
|-----------|---------|---------|
| `tunnel.StreamTransport` | `OpenStream`, `AcceptStream` | `pkg/tunnel` |
| `scheduler.MeshStreamOpener` | `OpenArtifactStream` | `pkg/scheduler` |

`JoinWithInvite` was removed from the `Mesh` interface (only called from tests); the impl method on `*impl` still exists in `connect.go`.

### Exported functions

- `RedeemInvite` (`invite.go`) — one-shot invite redemption without an active mesh; used by the CLI join flow (`cmd/pln/join.go`)

### Unexported

- `stream` struct — concrete QUIC stream wrapper with `CloseWrite` for half-close; callers see `io.ReadWriteCloser`
- `TailscaleCGNAT`, `TailscaleULA` — accessed only via `DefaultExclusions`

## Concurrency Contract

- `inCh` sends (`PeerConnected`/`PeerDisconnected` events) are **blocking** with `ctx.Done()` fallback — events are never silently dropped unless the context is cancelled.
- `streamCh`/`clockStreamCh` sends in `acceptBidiStreams` use a `default` branch to drop excess streams when channels are full (bounded by `queueBufSize`).
- `trafficTracker` field is set once before goroutines start; no mutex needed.

## File Layout

| File | Content |
|------|---------|
| `mesh.go` | `Mesh` interface, `Router` interface, `Packet`, `CloseReason`, `impl` struct + embedded types, `NewMesh`, `Start`, `Close`, `BroadcastDisconnect`, `ListenPort`, `Recv`, `Events`, config setters, `runMainProbeLoop`, `quicConfig` |
| `session.go` | `Send`, `GetConn`, `GetActivePeerAddress`, `PeerDelegationCert`, `IsOutbound`, `ConnectedPeers`, `ClosePeerSession`, `addPeer`, `closeSession`, `handleSendFailure`, `recvDatagrams`, `acceptBidiStreams`, `sessionReaper`, `acceptLoop`, `handleInviteConnection`, `classifyQUICError` |
| `stream.go` | `openStreamWaitLoop` (unified retry loop for tunnel/artifact/workload), `OpenStream`, `OpenClockStream`, `OpenArtifactStream`, `OpenWorkloadStream`, `AcceptStream`, `AcceptClockStream`, `openTypedStream`, `acceptFromCh`, `openRoutedStream`, `handleRoutedStream`, `deliverRoutedStream`, `forwardRoutedStream`, `bridgeStreams`, `meshCloseWrite`, `stream` type |
| `connect.go` | `Connect`, `Punch`, `JoinWithToken`, `JoinWithInvite` (impl method only), `raceDirectDial`, `dialDirect`, `dialPunch` |
| `cert.go` | `UpdateMeshCert`, `RequestCertRenewal`, `handleInviteRedeem`, `sendInviteRedeemResponse`, `redeemInviteOnConn`, `recvEnvelope`, `sendEnvelope` |
| `identity.go` | `GenerateIdentityCert`, `peerKeyFromRawCert`, `delegationCertFromConn`, TLS config builders, cert verification |
| `discovery.go` | Gossip/discovery — unchanged |
| `invite.go` | `RedeemInvite` (exported), `redeemInviteWithDial` |
| `session_registry.go` | `sessionRegistry` — unchanged |

## Deleted / Changed from Previous State

| Item | Status |
|------|--------|
| `PeerCertExpiresAt` method | Deleted — zero external callers |
| `JoinWithInvite` (from `Mesh` interface) | Removed from interface — impl kept |
| `Stream` interface (export) | Deleted — `OpenClockStream` now returns `io.ReadWriteCloser`; callers use structural `CloseWrite()` assertion |
| `Stream` struct (export) | Unexported to `stream` |
| `TailscaleCGNAT`, `TailscaleULA` (exports) | Unexported — accessed via `DefaultExclusions` only |
| Duplicate `Open*Stream` retry loops | Unified into `openStreamWaitLoop` |
| `inCh` 5s-timeout drop | Replaced with blocking + `ctx.Done()` fallback |
| Nil guards in `closeSession` | Removed — `s.conn` always non-nil |
| Nil guards in `Close()` | Removed — `m.listener`/`m.mainQT` are always set by `Start()` which runs before `Close()` |
| `peerKeyFromConn` | Inlined at sole call site in `acceptLoop`; `peerKeyFromRawCert` kept (two callers) |
