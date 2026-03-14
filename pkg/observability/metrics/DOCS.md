# pkg/observability/metrics — Target State

## Changes from Current

- [SAFE] Delete `Labels` type and all label plumbing — no metric uses labels
- [SAFE] Remove `Snapshot.Labels` field
- [SAFE] Remove label iteration in `LogSink.Flush`
- [SAFE] Simplify `metricKey` (no longer needs labels component)

## Target Exported API

### Removed exports

- `Labels` type — deleted (no metric uses labels)

### Changed exports

- `Snapshot` struct — `Labels` field removed
- `Collector.Counter(name string) *Counter` — `labels Labels` parameter removed
- `Collector.Gauge(name string) *Gauge` — `labels Labels` parameter removed

## Deleted Items

| Item | Reason |
|------|--------|
| `Labels` type (~5 LOC) | No metric in the codebase uses labels; every registration passes `Labels{}` |
| `Snapshot.Labels` field (~1 LOC) | Never populated with non-zero values |
| `LogSink.Flush` label iteration (~5 LOC) | Dead branch — labels always empty |
| `metricKey.labels` field (~2 LOC) | No longer needed without `Labels` |
| `Labels` parameter on `Counter()`/`Gauge()` | Simplified API |
