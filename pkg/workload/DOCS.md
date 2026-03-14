# pkg/workload — Target State

## Changes from Current

- [SAFE] Delete `StatusStopped` and `StatusErrored` enum values — never assigned

## Target Exported API

### Removed exports

- `StatusStopped` — never assigned by this package
- `StatusErrored` — never assigned by this package

### Unchanged exports

`IsRunning` remains exported — it is part of the `WorkloadManager` interface consumed by
`pkg/scheduler` (which calls `r.workloads.IsRunning` and passes `IsRunning` as a callback to
`evaluate`). Unexporting it would break the scheduler interface.

## Deleted Items

| Item | Reason |
|------|--------|
| `StatusStopped` enum value + `String()` case | Never assigned; workloads are removed from the map on unseed |
| `StatusErrored` enum value + `String()` case | No error-tracking mechanism exists |
