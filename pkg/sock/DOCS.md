# pkg/sock

## Responsibilities
- Manages UDP socket connections for NAT hole punching
- Implements probe-based NAT traversal (STUN-like protocol)
- Tracks active sockets and deduplicates connections
- Handles main socket probe responses from remote peers

## File Layout

| File | Content |
|------|---------|
| `sock.go` | `SockStore` interface, `sockStore` implementation, `Conn` type, `Punch`, scatter-probe logic |
| `punch.go` | `probeAddr`, `randomEphemeralPort`, probe protocol constants |

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `SockStore` | interface | Socket and punch management |
| `Conn` | type | UDP connection with peer address and ref-counted close |
| `ProbeWriter` | type | Function type: writes probe packets to address |
| `ErrUnreachable` | var | Peer unreachable error sentinel |
| `NewSockStore` | func | Constructor |
| `(*Conn).Peer` | method | Return peer address |
| `(*Conn).Close` | method | Close connection (ref-counted) |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/nat | `Type`, `Easy` |

## Consumed by
- pkg/mesh (uses: `SockStore`, `NewSockStore`, `Conn`, `ErrUnreachable`)
