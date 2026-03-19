# Phase 3 Completion Checklist: Transport Internalization

Source of truth: `TARGET_ARCHITECTURE_SPEC.md`
Gap analysis: `GAP_ANALYSIS.md`

---

## Spec Target

The spec says transport exports exactly:
- `StreamType` (byte enum + constants)
- `PeerEventType` (int enum)
- `PeerEvent` (struct)
- `Packet` (struct)
- `Stream` (type)
- `QUICTransport` (struct)
- `New` (constructor)
- 10 methods on QUICTransport: Start, Stop, Connect, Disconnect, PeerEvents, ConnectedPeers, Send, Recv, OpenStream, AcceptStream

Everything else should be unexported or removed.

---

## Peer FSM Types — Must Be Unexported

- [ ] `PeerState` (int enum) → `peerState`
- [ ] `PeerStateCounts` (struct) → `peerStateCounts` — NOTE: used by `control.TransportInfo` interface. Either move the type to control, or keep exported. Check callers.
- [ ] `Store` (peer FSM) → `store` or rename to avoid collision — NOTE: `NewStore()` is called by supervisor. If peer FSM stays in supervisor, the type must stay exported OR supervisor creates it differently.
- [ ] `Input` (interface) → `input`
- [ ] `Output` (interface) → `output`

## FSM Event Types — Must Be Unexported

- [ ] `DiscoverPeer` → `discoverPeer`
- [ ] `Tick` → `tick`
- [ ] `ConnectPeer` → `connectPeer`
- [ ] `ConnectFailed` → `connectFailed`
- [ ] `PeerDisconnected` → `peerDisconnected`
- [ ] `RetryPeer` → `retryPeer`
- [ ] `ForgetPeer` → `forgetPeer`
- [ ] `PeerConnected` → `peerConnected`
- [ ] `AttemptConnect` → `attemptConnect`
- [ ] `AttemptEagerConnect` → `attemptEagerConnect`
- [ ] `RequestPunchCoordination` → `requestPunchCoordination`

## Other Types — Assess Each

- [ ] `CloseReason` (string) — used as parameter to `ClosePeerSession`. If ClosePeerSession stays exported (membership calls it), CloseReason and its constants must stay exported. Justify or internalize.
- [ ] `DisconnectReason` (int) — check callers. If only internal, unexport.
- [ ] `Option` (func) — pragmatic, keep exported (constructor options)
- [ ] `ErrIdentityMismatch` — check callers outside transport
- [ ] `ErrUnreachable` — check callers outside transport

## Extra Methods — Assess Each

Methods to keep (justified by domain service needs):
- [ ] `ListenPort()` — integration tests. Document.
- [ ] `GetActivePeerAddress(pk)` — membership Network interface needs it
- [ ] `PeerDelegationCert(pk)` — membership needs for cert renewal
- [ ] `ClosePeerSession(pk, reason)` — membership needs for deny/expiry
- [ ] `UpdateMeshCert(cert)` — membership needs for cert rotation
- [ ] `RequestCertRenewal(ctx, pk)` — membership needs for cert lifecycle
- [ ] `JoinWithInvite(ctx, token)` — supervisor needs for bootstrap
- [ ] `JoinWithToken(ctx, token)` — supervisor needs for bootstrap
- [ ] `GetConn(pk)` — membership needs for Vivaldi RTT

Methods to assess for removal/unexport:
- [ ] `Events() <-chan Input` — if Input is unexported, this return type breaks. Supervisor calls this. Either keep Input exported or change Events() return type.
- [ ] `Punch(ctx, pk, addr, natType)` — should be internal to Connect. Check if supervisor calls it directly.
- [ ] `IsOutbound(pk)` — supervisor calls for topology pruning. Keep or provide alternative.
- [ ] `BroadcastDisconnect()` — supervisor shutdown. Keep or fold into Stop().
- [ ] `SetRouter(r)` — post-Start injection. Keep.
- [ ] `SetTrafficTracker(t)` — post-Start injection. Keep.

## Extra Interfaces

- [ ] `Router` — tunneling defines its own `RoutingTable`. If transport's `Router` is identical, remove from transport (consumers define their own). Check if supervisor uses `transport.Router`.
- [ ] `TrafficRecorder` — tunneling defines its own `Recorder`. Same analysis.

## Extra Package-Level Functions

- [ ] `GenerateIdentityCert` — check callers. If only internal + membership, consider scope.
- [ ] `RedeemInvite` — check callers. If only internal, unexport.
- [ ] `GetAdvertisableAddrs` — check callers. If membership calls it, keep.
- [ ] `NewStore` → if Store is unexported, this must change too. Supervisor creates the store.
- [ ] `NewSockStore` — check callers.

## Key Constraint

The peer FSM (`Store`, `Input`, `Output`, event types) is currently driven by **supervisor** — `n.peers.Step(time.Now(), input)`. If we unexport these types, supervisor can no longer drive the FSM from outside the transport package. Options:

a) **Move the peer FSM into transport** — transport drives its own FSM internally in Start(). Supervisor passes peer discovery info via methods, not FSM events.
b) **Keep the FSM exported** — accept that the FSM is part of transport's extended API.
c) **Move the FSM into supervisor** — it's supervisor logic, not transport logic.

The spec implies option (a): transport handles connections internally, callers just call Connect/Disconnect. But this is a large structural change. Assess feasibility.

---

## Verification Gates

- [ ] `go build ./...` — passes
- [ ] `go test -count=1 ./...` — all unit tests pass
- [ ] `go test -tags integration -count=1 -timeout 120s ./pkg/integration/cluster/...` — passes
- [ ] `just lint` — 0 issues
- [ ] `cd infra && just verify-hetzner` — steps 0-10 pass (step 11 known flake)
