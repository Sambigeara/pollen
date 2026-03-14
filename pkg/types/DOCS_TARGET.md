# pkg/types — Target State

## Changes from Current

- [SAFE] Delete `Envelope` struct (dead code — codebase uses `meshv1.Envelope`)

## Target Exported API

No new exports. `PeerKey`, `PeerKeyFromBytes`, `PeerKeyFromString`, `Bytes`, `String`, `Short`, `Less`, `Compare` all remain.

## Deleted Items

| Item | Reason |
|------|--------|
| `Envelope` struct (~3 LOC) | Never referenced outside this file; codebase uses `meshv1.Envelope` |
