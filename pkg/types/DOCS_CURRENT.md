# pkg/types

## Purpose
Defines the core `PeerKey` identity type (32-byte ed25519 public key) used across the entire codebase, plus a vestigial `Envelope` struct.

## Files
| File | LOC | Role |
|------|-----|------|
| types.go | 53 | PeerKey type, constructors, comparison methods, dead Envelope struct |

## Exported API

### Types
- `PeerKey [32]byte` -- 32-byte ed25519 public key used as the universal node identifier. Consumed by nearly every package in the codebase (mesh, node, store, peer, scheduler, topology, route, traffic, tunnel).
- `Envelope struct { Payload []byte }` -- **DEAD CODE**. Never referenced outside this file; the codebase uses `meshv1.Envelope` from protobuf instead.

### Functions
- `PeerKeyFromBytes(b []byte) PeerKey` -- Constructs a PeerKey by copying from a byte slice. Consumed by: mesh, node, store.
- `PeerKeyFromString(s string) (PeerKey, error)` -- Parses a hex-encoded public key string. Consumed by: store/disk, store/gossip.

### Methods on PeerKey
- `Bytes() []byte` -- Returns the key as a byte slice. Widely consumed for serialization into protobuf messages.
- `String() string` -- Hex-encodes the key. Used for logging and display.
- `Short() string` -- Returns the first 8 hex characters. Used for short log identifiers.
- `Less(other PeerKey) bool` -- Lexicographic comparison returning bool. Used by topology for sorting and mesh for tie-breaking.
- `Compare(other PeerKey) int` -- Lexicographic comparison returning -1/0/+1. Used by store/gossip, node, and reachability for `slices.SortFunc`.

## Internal State
None. Pure value type with no mutable state.

## Concurrency Contract

### In
N/A -- no goroutines, channels, or locks.

### Out
N/A.

### Internal
N/A. `PeerKey` is an immutable value type safe for concurrent use.

## Dependencies

### Imports (what it uses from each internal package)
None. Only standard library: `bytes`, `crypto/ed25519`, `encoding/hex`, `fmt`.

### Imported By (what consumers use from this package)
Imported by 19+ packages:
- `pkg/mesh` -- PeerKey for identity, connections, session registry
- `pkg/node` -- PeerKey for node identity, peer management, routing
- `pkg/store` -- PeerKey for gossip state, disk persistence
- `pkg/peer` -- PeerKey for peer representation
- `pkg/scheduler` -- PeerKey for workload placement and reconciliation
- `pkg/topology` -- PeerKey for topology target selection
- `pkg/route` -- PeerKey for routing table entries
- `pkg/traffic` -- PeerKey for traffic tracking and counting
- `pkg/tunnel` -- PeerKey for tunnel manager

## Cross-Boundary Contracts
`PeerKey` satisfies `fmt.Stringer` via its `String()` method.

## Observations
- `Less` and `Compare` both call `bytes.Compare` on the same data. `Less` could be defined in terms of `Compare` to deduplicate, though the performance difference is negligible for a one-liner.
- The package is a leaf dependency with zero internal imports, making it a good foundation type.

## Deletion Candidates
| Candidate | LOC | Reason |
|-----------|-----|--------|
| `Envelope` struct (lines 51-53) | 3 | Completely dead. Never referenced outside this file. The codebase uses `meshv1.Envelope` from generated protobuf code. |
