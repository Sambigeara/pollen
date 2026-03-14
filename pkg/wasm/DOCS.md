# pkg/wasm — Target State

## Changes from Current

- [SAFE] Delete `IsCompiled` method — only called from tests
- [SAFE] Use `mu.RLock` instead of `mu.Lock` in read-only cache lookups (minor perf)

## Target Exported API

### Removed exports

- `IsCompiled(hash string) bool` — only called from tests; tests can use `Call` to verify compilation

## Deleted Items

| Item | Reason |
|------|--------|
| `IsCompiled` method | Only called from tests; no production callers |
