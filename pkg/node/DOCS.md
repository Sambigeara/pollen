# pkg/node — Current State

## Changes from Current

- [SAFE] Extract identity key I/O (`GenIdentityKey`, `ReadIdentityPub`, `loadIdentityKey`, `decodePubKeyPEM`) into `pkg/auth` (~100 LOC moved)
- [SAFE] Split `node.go` by concern into multiple files (gossip, punch, cert lifecycle)
- [SAFE] Inline `pkg/util` (JitterTicker) as unexported helpers (~66 LOC absorbed)
- [SAFE] Inline `pkg/sysinfo` (Sample function) as unexported helper (~23 LOC absorbed)
- [SAFE] Make `requestPunchCoordination` synchronous (mesh.Send is non-blocking)
- [SAFE] Make `handlePunchCoordRequest` sequential instead of 2 concurrent goroutines
- [SAFE] Unify `findNonTargetPeer` / `findNonTargetPublicPeer` with a predicate parameter in tests
- [SAFE] Use node context instead of `context.Background()` in fire-and-forget sends where safe

## Exported API

### Removed exports

- `GenIdentityKey(pollenDir) (priv, pub, error)` — moved to `pkg/auth`
- `ReadIdentityPub(pollenDir) (pub, error)` — moved to `pkg/auth`

### Unchanged exports

All other exported types, functions, and methods remain. `Config`, `Node`, `NodeService`, `New`, `NewNodeService`, `Start`, `Ready`, `ListenPort`, `GetConnectedPeers`, `ConnectService`, `DisconnectService`, `RouteCall`, `MaxDatagramPayload`, `ErrCertExpired`, `BootstrapPeer`.

## File Layout

| File | Content | Est. LOC |
|------|---------|----------|
| `node.go` | `Node` struct, `New`, `Start` (main event loop), `shutdown`, `handlePeerInput`, `handleOutputs`, `handleDatagram`, `tick`, `recomputeRoutes`, debounce logic | ~900 |
| `gossip.go` | `broadcastGossipBatches`, `queueGossipEvents`, `batchEvents`, `gossip`, `handleClockStream`, `sendClockViaStream`, `acceptClockStreamsLoop` | ~200 |
| `punch.go` | `requestPunchCoordination` (now synchronous), `handlePunchCoordRequest` (now sequential), `handlePunchCoordTrigger`, `punchLoop` | ~80 |
| `cert.go` | `checkCertExpiry`, `attemptCertRenewal`, `applyCertRenewal`, `handleCertRenewalRequest` | ~130 |
| `jitter.go` | Inlined `JitterTicker` (from `pkg/util`), unexported | ~50 |
| `sysinfo.go` | Inlined `sysinfo.Sample` (from `pkg/sysinfo`), unexported | ~20 |
| `svc.go` | Already separate — unchanged | ~652 |
| `reachability.go` | Already separate — unchanged | ~117 |
| `topology_profile.go` | Already separate — unchanged | ~64 |

## Deleted Items

| Item | Reason |
|------|--------|
| `GenIdentityKey` (from this package) | Moved to `pkg/auth` |
| `ReadIdentityPub` (from this package) | Moved to `pkg/auth` |
| `loadIdentityKey` internal function | Moved to `pkg/auth` |
| `decodePubKeyPEM` internal function | Moved to `pkg/auth` |
| PEM file path constants | Moved to `pkg/auth` |
| Import of `pkg/util` | Inlined; `pkg/util` deleted |
| Import of `pkg/sysinfo` | Inlined; `pkg/sysinfo` deleted |
| `envelopeOverhead` package-level var | Inlined as local variable in `batchEvents` |
