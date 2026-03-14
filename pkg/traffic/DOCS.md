# pkg/traffic — Target State

## Changes from Current

- [SAFE] Add noop `Recorder` implementation (`var Noop Recorder = noopRecorder{}`)
- [SAFE] Delete unreachable zero-entry removal loop in `RotateAndSnapshot`

## Target Exported API

### New exports

- `Noop` — `var Noop Recorder = noopRecorder{}`. Default no-op implementation of `Recorder`. Used by `pkg/mesh` and `pkg/tunnel` as initial field value instead of nil.

## Deleted Items

| Item | Reason |
|------|--------|
| Zero-entry removal loop in `RotateAndSnapshot` (~4 LOC) | Unreachable — entries always have nonzero counts from `Record` callers |
