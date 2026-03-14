# pkg/mesh

## Responsibilities
- Manages peer-to-peer QUIC connections with TLS-based identity and delegation certificates
- Handles message routing: direct, routed (multi-hop), and specialized streams (tunnel, artifact, workload, clock)
- Provides NAT traversal via connection punching and address discovery
- Manages peer session lifecycle including addition, removal, and certificate rotation

## File Layout

| File | Content |
|------|---------|
| `mesh.go` | `Mesh` interface, `Router` interface, `Packet`, `CloseReason`, `impl` struct + embedded types, `NewMesh`, `Start`, `Close`, `BroadcastDisconnect`, `ListenPort`, `Recv`, `Events`, config setters, `runMainProbeLoop`, `quicConfig` |
| `session.go` | `Send`, `GetConn`, `GetActivePeerAddress`, `PeerDelegationCert`, `IsOutbound`, `ConnectedPeers`, `ClosePeerSession`, `addPeer`, `closeSession`, `handleSendFailure`, `recvDatagrams`, `acceptBidiStreams`, `sessionReaper`, `acceptLoop`, `handleInviteConnection`, `classifyQUICError` |
| `stream.go` | `openStreamWaitLoop` (unified retry loop for tunnel/artifact/workload), `OpenStream`, `OpenClockStream`, `OpenArtifactStream`, `OpenWorkloadStream`, `AcceptStream`, `AcceptClockStream`, `openTypedStream`, `acceptFromCh`, `openRoutedStream`, `handleRoutedStream`, `deliverRoutedStream`, `forwardRoutedStream`, `bridgeStreams`, `meshCloseWrite`, `stream` type |
| `connect.go` | `Connect`, `Punch`, `JoinWithToken`, `JoinWithInvite`, `raceDirectDial`, `dialDirect`, `dialPunch` |
| `cert.go` | `UpdateMeshCert`, `RequestCertRenewal`, `handleInviteRedeem`, `sendInviteRedeemResponse`, `redeemInviteOnConn`, `recvEnvelope`, `sendEnvelope` |
| `identity.go` | `GenerateIdentityCert`, `peerKeyFromRawCert`, `delegationCertFromConn`, TLS config builders, cert verification |
| `discovery.go` | Gossip/discovery |
| `invite.go` | `RedeemInvite` (exported), `redeemInviteWithDial` |
| `session_registry.go` | `sessionRegistry` |

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Mesh` | interface | Core mesh operations (24 methods: sessions, streams, datagrams, connections) |
| `Router` | interface | Multi-hop routing lookup |
| `CloseReason` | type | Named string type for session close reasons |
| `CloseReasonDenied` | const | Peer denied access |
| `CloseReasonTopologyPrune` | const | Topology-driven disconnection |
| `CloseReasonCertExpired` | const | Peer cert expired |
| `CloseReasonCertRotation` | const | Cert rotation trigger |
| `CloseReasonDisconnect` | const | Graceful disconnect |
| `CloseReasonDuplicate` | const | Duplicate connection closed |
| `CloseReasonReplaced` | const | Session replaced by newer |
| `CloseReasonDisconnected` | const | Session terminated |
| `CloseReasonShutdown` | const | Mesh shutdown |
| `Packet` | type | Envelope + sender peer key |
| `NewMesh` | func | Constructor |
| `GenerateIdentityCert` | func | Create identity TLS cert from delegation cert |
| `RedeemInvite` | func | Redeem invite token without active mesh (CLI join flow) |
| `GetAdvertisableAddrs` | func | Discover public/private IPs for advertisement |
| `DefaultExclusions` | var | IP ranges to exclude from advertisement |
| `ErrIdentityMismatch` | var | Peer identity mismatch sentinel |

## Concurrency Contract

- `inCh` sends (`PeerConnected`/`PeerDisconnected` events) are **blocking** with `ctx.Done()` fallback — events are never silently dropped unless the context is cancelled.
- `streamCh`/`clockStreamCh` sends in `acceptBidiStreams` use a `default` branch to drop excess streams when channels are full (bounded by `queueBufSize`).
- `trafficTracker` field is set once before goroutines start; no mutex needed.

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/auth | `DelegationSigner`, `NodeCredentials`, `VerifyInviteToken`, `IssueJoinTokenWithIssuer`, `CertExpiresAt`, `CertAccessDeadline` |
| pkg/nat | `Type` |
| pkg/observability/metrics | `MeshMetrics`, `Gauge` |
| pkg/observability/traces | `Tracer` |
| pkg/peer | `Input`, `ConnectPeer`, `PeerDisconnected`, `ForgetPeer`, `DisconnectReason` |
| pkg/sock | `SockStore`, `Conn` |
| pkg/traffic | `Recorder`, `WrapStream`, `Noop` |
| pkg/types | `PeerKey` |

## Consumed by
- pkg/node (uses: `Mesh`, `NewMesh`, `GenerateIdentityCert`, `CloseReason*`, `ErrIdentityMismatch`, `GetAdvertisableAddrs`, `DefaultExclusions`)

## Proposed Minimal API

### Exports kept

| Export | Consumers |
|--------|-----------|
| `Mesh` (interface, 28 methods) | node |
| `Router` | node |
| `Packet` | node |
| `CloseReason`, `CloseReasonDenied`, `CloseReasonTopologyPrune`, `CloseReasonCertExpired` | node |
| `NewMesh` | node |
| `GenerateIdentityCert` | node |
| `RedeemInvite` | cmd/pln |
| `GetAdvertisableAddrs`, `DefaultExclusions` | node |
| `ErrIdentityMismatch` | node |

### Exports stripped (7)

| Export | Action | Reason |
|--------|--------|--------|
| `JoinWithToken` | removed from `Mesh` interface | Dead code; join flow uses `RedeemInvite` instead |
| `CloseReasonCertRotation` | unexported | Internal session management detail |
| `CloseReasonDisconnect` | unexported | Internal session management detail |
| `CloseReasonDuplicate` | unexported | Internal session management detail |
| `CloseReasonReplaced` | unexported | Internal session management detail |
| `CloseReasonDisconnected` | unexported | Internal session management detail |
| `CloseReasonShutdown` | unexported | Internal session management detail |
