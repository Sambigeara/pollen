# pkg/types

## Responsibilities
- Defines `PeerKey`, the canonical 32-byte ed25519 public key identity for every mesh node
- Provides constructors from bytes and hex strings, plus comparison/ordering utilities

## Files

| File | Content |
|------|---------|
| `types.go` | `PeerKey` type and all constructors/methods |

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `PeerKey` | type | `[32]byte` ed25519 public key; comparable, usable as map key |
| `PeerKeyFromBytes` | func | Construct PeerKey from byte slice |
| `PeerKeyFromString` | func | Construct PeerKey from hex string |
| `(PeerKey).Bytes` | method | Raw 32-byte slice |
| `(PeerKey).String` | method | 64-char lowercase hex encoding |
| `(PeerKey).Short` | method | First 8 hex chars for logging |
| `(PeerKey).Less` | method | Lexicographic less-than |
| `(PeerKey).Compare` | method | Three-way comparison (-1, 0, +1) |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/mesh (uses: `PeerKey`, `PeerKeyFromBytes`)
- pkg/node (uses: `PeerKey`, `PeerKeyFromBytes`)
- pkg/peer (uses: `PeerKey`)
- pkg/store (uses: `PeerKey`, `PeerKeyFromBytes`, `PeerKeyFromString`)
- pkg/topology (uses: `PeerKey`)
- pkg/traffic (uses: `PeerKey`)
- pkg/route (uses: `PeerKey`)
- pkg/tunnel (uses: `PeerKey`)
- pkg/scheduler (uses: `PeerKey`)
- cmd/pln (uses: `PeerKey`, `PeerKeyFromBytes`)
