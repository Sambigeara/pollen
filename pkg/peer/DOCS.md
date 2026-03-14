# pkg/peer

## Responsibilities
- Maintains state machine for peer connection lifecycle (Discovered -> Connecting -> Connected / Unreachable)
- Manages connection staging (direct dial -> eager retry -> NAT punch)
- Emits events for attempted connections and disconnections
- Thread-safe store with per-peer state counts and metrics

## File Layout

| File | Content |
|------|---------|
| `peer.go` | All types, state machine, `Store` |
| `peer_test.go` | Unit tests |
| `boundary_test.go` | Build-tag tests for API boundary verification |

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `State` | type | Peer connection state enum |
| `Discovered` | const | Known but not yet connected |
| `Connecting` | const | Connection attempt in progress |
| `Connected` | const | Successfully connected |
| `Unreachable` | const | Failed attempts exhausted |
| `ConnectStage` | type | Connection attempt stage enum |
| `ConnectStageDirect` | const | Direct dial attempt |
| `ConnectStageEagerRetry` | const | Retry on previously-known address |
| `ConnectStagePunch` | const | NAT punch coordination |
| `Peer` | type | Peer record (ID, state, addresses, stage) |
| `Input` | interface | Marker interface for input events |
| `DiscoverPeer` | type | Event: add/update peer with addresses |
| `Tick` | type | Event: evaluate all peers for pending actions |
| `ConnectPeer` | type | Event: signal successful connection |
| `ConnectFailed` | type | Event: signal failed connection attempt |
| `PeerDisconnected` | type | Event: signal disconnection with reason |
| `RetryPeer` | type | Event: retry connection for a peer |
| `ForgetPeer` | type | Event: remove peer from state machine |
| `DisconnectReason` | type | Reason enum for disconnection |
| `DisconnectUnknown` | const | Unknown |
| `DisconnectIdleTimeout` | const | Idle timeout |
| `DisconnectReset` | const | Stateless reset |
| `DisconnectGraceful` | const | Clean app-level close |
| `DisconnectTopologyPrune` | const | Topology pruned edge |
| `DisconnectDenied` | const | Peer denied session/membership |
| `DisconnectCertRotation` | const | Forced reconnection for cert rotation |
| `DisconnectCertExpired` | const | Membership cert expired |
| `Output` | interface | Marker interface for output effects |
| `PeerConnected` | type | Output: peer transitioned to connected |
| `AttemptConnect` | type | Output: attempt direct connection |
| `AttemptEagerConnect` | type | Output: attempt on known address |
| `RequestPunchCoordination` | type | Output: request NAT punch coordination |
| `Store` | type | Thread-safe peer state machine store |
| `NewStore` | func | Create new Store |
| `(*Store).SetPeerMetrics` | method | Wire metrics instruments |
| `(*Store).Step` | method | Process input event, return output effects |
| `(*Store).Get` | method | Get peer state by key |
| `(*Store).InState` | method | Check if peer is in given state |
| `(*Store).GetAll` | method | Get all peers in given state |
| `StateCounts` | type | Per-state peer counts |
| `(*Store).StateCounts` | method | Return current state counts |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/observability/metrics | `PeerMetrics` |
| pkg/types | `PeerKey` |

## Consumed by
- pkg/mesh (uses: `Input`, `Output`, all event types, all state constants, all disconnect reasons)
- pkg/node (uses: `Store`, `NewStore`, all event types, all output types, all disconnect reasons)
- cmd/pln (uses: `Store`)
