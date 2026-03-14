# pkg/peer

## Exported API

### Types

- `State` (was `PeerState`) — peer connectivity state
- `ConnectStage` — dial escalation stage; `ConnectStageDirect` is the iota zero value
- `DisconnectReason` — reason for a session ending
- `StateCounts` (was `PeerStateCounts`) — per-state counts from `Store.StateCounts()`
- `Store` — peer state machine; driven by `Step(now, Input) []Output`
- `Peer` — peer record returned by `Store.Get`

### State constants

```
Discovered  State = 1
Connecting  State = 2
Connected   State = 3
Unreachable State = 4
```

### ConnectStage constants

```
ConnectStageDirect     ConnectStage = 0   // iota zero
ConnectStageEagerRetry ConnectStage = 1
ConnectStagePunch      ConnectStage = 2
```

`ConnectStageUnspecified` was deleted — `ConnectStageDirect` is now the zero value.

### Input events

`DiscoverPeer`, `Tick`, `ConnectPeer`, `ConnectFailed`, `PeerDisconnected`, `RetryPeer`, `ForgetPeer`

### Output effects

`PeerConnected`, `AttemptConnect`, `AttemptEagerConnect`, `RequestPunchCoordination`

### Unexported `Peer` fields

`stage`, `stageAttempts`, `connectingAt` — previously exported as `Stage`, `StageAttempts`, `ConnectingAt`. Tests use `Store.Get` or query methods instead of direct field access.

## File Layout

| File | Content |
|------|---------|
| `peer.go` | All types, state machine, `Store` |
| `peer_test.go` | Unit tests |
| `boundary_test.go` | Build-tag tests verifying renamed exports and deleted constants (build tag: `consolidation_target`) |

## Changes from Previous State

| Item | Change |
|------|--------|
| `PeerState` | Renamed to `State` |
| `PeerStateDiscovered/Connecting/Connected/Unreachable` | Renamed to `Discovered/Connecting/Connected/Unreachable` |
| `PeerStateCounts` | Renamed to `StateCounts` |
| `ConnectStageUnspecified` | Deleted; `ConnectStageDirect` is now iota zero |
| `Peer.Stage`, `.StageAttempts`, `.ConnectingAt` | Unexported to `stage`, `stageAttempts`, `connectingAt` |
| `DisconnectReason.String()` default branch | Removed — switch is exhaustive; unreachable default replaced with `panic("unreachable")` |
