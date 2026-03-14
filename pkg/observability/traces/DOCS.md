# pkg/observability/traces — Target State

## Changes from Current

- [SAFE] Delete `Span.parentSpanID` field (never set — no child span API)
- [SAFE] Delete `ReadOnlySpan.ParentSpanID` field and conversion in `readOnly()`
- [SAFE] Delete dead `LogSink.Export` parent_span_id branch
- [SAFE] Delete `SpanID.IsZero()` method (only used for dead parentSpanID check)
- [SAFE] Inline `TraceID.Bytes()` into sole caller `Span.TraceIDBytes()`

## Target Exported API

### Removed exports

- `SpanID.IsZero()` — only used for dead parentSpanID check
- `TraceID.Bytes()` — inlined into `Span.TraceIDBytes()`
- `ReadOnlySpan.ParentSpanID` field — removed

## Deleted Items

| Item | Reason |
|------|--------|
| `Span.parentSpanID` field | Never set; no child span API exists |
| `ReadOnlySpan.ParentSpanID` field | Consequence of above — always empty |
| `LogSink.Export` parent branch | Dead — `ParentSpanID` always empty |
| `SpanID.IsZero()` | Only consumer was dead parentSpanID check |
| `TraceID.Bytes()` | Trivial wrapper; inlined into sole caller |
