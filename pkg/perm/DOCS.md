# pkg/perm

## Responsibilities
- Manages file permissions for the pln data directory (Linux: pln:pln ownership; other platforms: no-ops)
- Provides atomic file writing with appropriate modes (private, group-readable, group-writable)
- Ensures directory creation with correct ownership

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `SetGroupDir` | func | Set directory mode to 0770 with pln:pln ownership |
| `SetGroupReadable` | func | Set file mode to 0640 with pln:pln ownership |
| `SetGroupSocket` | func | Set socket mode to 0660 with pln:pln ownership |
| `SetPrivate` | func | Set file mode to 0600 |
| `EnsureDir` | func | Create directory with parents; pln:pln ownership on Linux |
| `WritePrivate` | func | Atomically write with 0600 mode |
| `WriteGroupReadable` | func | Atomically write with 0640 mode |
| `WriteGroupWritable` | func | Atomically write with 0660 mode |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/auth (uses: `EnsureDir`, `SetGroupReadable`, `WriteGroupReadable`)
- pkg/config (uses: `EnsureDir`, `WriteGroupWritable`)
- pkg/store (uses: `EnsureDir`, `SetPrivate`, `WriteGroupReadable`)
