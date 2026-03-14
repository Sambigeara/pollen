# pkg/sock

## Exported API

- `SockStore` interface — `Punch`, `SetMainProbeWriter`, `HandleMainProbePacket`
- `Conn` — shared UDP connection with ref-counting close; `UDPConn` is nil for easy-side punch winners (they reuse the main transport)
- `ProbeWriter` — callback type for sending probe packets
- `ErrUnreachable` — returned when punch fails
- `NewSockStore() SockStore`

## File Layout

| File | Content |
|------|---------|
| `sock.go` | `SockStore` interface, `sockStore` implementation (connList logic inlined), `Conn` type, `Punch`, scatter-probe logic |
| `punch.go` | `probeAddr`, `randomEphemeralPort`, probe protocol constants |
| `punch_test.go` | Tests + `probe()` helper (thin wrapper over `probeAddr`) |

`list.go` was deleted — `connList` logic inlined into `sockStore`.

## Deleted Items

| Item | Reason |
|------|--------|
| `list.go` (entire file) | `connList` inlined into `sockStore` |
| `sockStore.log` field | Created but never read |
| `probe()` function (from production code) | Moved to `punch_test.go` |
| `Conn.onClose` nil check | `onClose` always set when `UDPConn` is non-nil |
| `Conn.UDPConn` nil check in `Close()` | Always non-nil when `onClose` runs on the hard-side path |
