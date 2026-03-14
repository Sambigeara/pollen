# pkg/cas — Target State

## Changes from Current

- [SAFE] Delete unreachable short-hash branch in `path()` — SHA-256 hashes are always 64 chars

## Target Exported API

No API changes. `Store`, `New`, `Put`, `Get`, `Has`, `ErrNotFound` all remain.

## Deleted Items

| Item | Reason |
|------|--------|
| Short-hash guard in `path()` (`if len(hash) < 2`) | SHA-256 hashes always 64 chars; unreachable |
