# pkg/cas

## Responsibilities
- Content-addressable storage for WASM artifacts indexed by SHA-256 hash
- Atomic store and retrieval backed by local filesystem
- Directory sharding by hash prefix for scalability

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `ErrNotFound` | var | Sentinel error when artifact not found |
| `Store` | type | Content-addressable artifact store |
| `New` | func | Create store rooted at pollenDir/cas |
| `(*Store).Put` | method | Write artifact from reader; return SHA-256 hex hash |
| `(*Store).Get` | method | Open artifact for reading by hash |
| `(*Store).Has` | method | Check if artifact exists |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/node (uses: `Store`, `New`)
- pkg/workload (uses: `Store`, `Put`, `Get`)
