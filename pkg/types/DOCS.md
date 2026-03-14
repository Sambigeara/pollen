# pkg/types

## Purpose

Shared primitive types used across all packages. Currently contains only `PeerKey`, the canonical identity type for every node in the mesh.

## Files

| File | Content |
|------|---------|
| `types.go` | `PeerKey` type and all constructors/methods |

## Exported API

### Types

| Type | Description |
|------|-------------|
| `PeerKey` | `[32]byte` — ed25519 public key identifying a node; comparable, usable as map key |

### Constructors

| Function | Description |
|----------|-------------|
| `PeerKeyFromBytes(b []byte) PeerKey` | Copies the first 32 bytes of `b` into a `PeerKey` |
| `PeerKeyFromString(s string) (PeerKey, error)` | Decodes a 64-character hex string; returns error if malformed or wrong length |

### Methods on PeerKey

| Method | Description |
|--------|-------------|
| `Bytes() []byte` | Raw 32-byte slice (alias for `pk[:]`) |
| `String() string` | 64-character lowercase hex encoding |
| `Short() string` | First 8 hex characters (for logging) |
| `Less(other PeerKey) bool` | Lexicographic less-than comparison |
| `Compare(other PeerKey) int` | Returns -1, 0, or +1 (lexicographic) |
