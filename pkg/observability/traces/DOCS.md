# pkg/observability/traces

## Responsibilities
- Distributed tracing with TraceID (16-byte) and SpanID (8-byte) generation
- Span creation and attribute tracking
- Pluggable `Sink` interface for exporting completed spans
- Nil-safe tracer and span API

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Tracer` | type | Creates and exports spans (nil-safe) |
| `NewTracer` | func | Create tracer with sink |
| `(*Tracer).Start` | method | Begin root span |
| `(*Tracer).StartFromRemote` | method | Continue trace from remote envelope |
| `Attribute` | type | Key-value pair attached to span |
| `Span` | type | Unit of work within trace (nil-safe) |
| `(*Span).End` | method | Record end time and export |
| `(*Span).SetAttr` | method | Append attribute |
| `(*Span).TraceIDBytes` | method | Extract trace ID for wire embedding |
| `TraceID` | type | 16-byte trace identifier |
| `NewTraceID` | func | Generate random TraceID |
| `TraceIDFromBytes` | func | Construct from byte slice |
| `(TraceID).IsZero` | method | Check if zero value |
| `(TraceID).String` | method | Hex-encoded representation |
| `SpanID` | type | 8-byte span identifier |
| `NewSpanID` | func | Generate random SpanID |
| `(SpanID).String` | method | Hex-encoded representation |
| `ReadOnlySpan` | type | Snapshot of completed span for export |
| `Sink` | interface | Receives completed spans (`Export` method) |
| `LogSink` | type | Zap logger sink implementation |
| `NewLogSink` | func | Create sink that logs spans |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/mesh (uses: `Tracer`)
- pkg/node (uses: `Tracer`, `NewTracer`, `LogSink`, `NewLogSink`, `TraceIDFromBytes`)

## Proposed Minimal API

### Exports kept

| Export | Consumers |
|--------|-----------|
| `Tracer`, `NewTracer` | mesh, node |
| `(*Tracer).Start`, `(*Tracer).StartFromRemote` | mesh, node |
| `Span`, `(*Span).End`, `(*Span).SetAttr` | mesh, node |
| `Sink`, `ReadOnlySpan`, `Attribute` | node |
| `LogSink`, `NewLogSink` | node |

### Exports stripped (7)

| Export | Action | Reason |
|--------|--------|--------|
| `TraceID` | unexported | Internal implementation detail; consumers use `Tracer.Start`/`StartFromRemote` |
| `NewTraceID` | unexported | Only used within package |
| `TraceIDFromBytes` | unexported | Only used within package (called by `StartFromRemote`) |
| `SpanID` | unexported | Internal implementation detail |
| `NewSpanID` | unexported | Only used within package |
| `(TraceID).IsZero` | unexported | Internal implementation detail |
| `(*Span).TraceIDBytes` | deleted | Replaced by internal access; no external callers |
