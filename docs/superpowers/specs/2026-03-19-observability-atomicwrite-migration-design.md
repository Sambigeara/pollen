# Observability & Atomic Write Migration

Replace custom metrics, traces, atomic write, and concurrency limiter implementations with industry-standard libraries. Keep the CRDT/gossip protocol.

## Motivation

The codebase contains ~680 lines of custom observability code (metrics + traces) that shadow OpenTelemetry, and ~40 lines of atomic file write code missing a critical parent directory fsync. These are well-written but duplicate what mature libraries provide with better correctness, interoperability, and future capability (histograms, distributed trace visualization, Prometheus/OTLP export).

## Scope

| Component | Action | Library |
|-----------|--------|---------|
| `pkg/observability/metrics/` | Replace | `go.opentelemetry.io/otel/metric` + `otel/sdk/metric` |
| `pkg/observability/traces/` | Replace | `go.opentelemetry.io/otel/trace` + `otel/sdk/trace` |
| `pkg/config/perm.go` atomicWrite | Replace | `github.com/google/renameio/v2` |
| `pkg/cas/cas.go` atomic write | Replace | `github.com/google/renameio/v2` |
| `pkg/observability/metrics/ewma.go` | Keep | Custom (no OTel equivalent) |
| `pkg/supervisor/workers.go` | Adapt | Rewrite atop `golang.org/x/sync/errgroup` with drain-on-shutdown |
| `pkg/wasm/runtime.go` semaphore | Replace | `golang.org/x/sync/semaphore` |

## Design

### 1. OTel Metrics

#### Dependencies

- `go.opentelemetry.io/otel/metric` — API interfaces, imported by domain packages
- `go.opentelemetry.io/otel/metric/noop` — zero-cost disabled mode
- `go.opentelemetry.io/otel/sdk/metric` — SDK, imported only by supervisor wire-up

#### Instrument Bundles

Domain-scoped structs remain: `MeshMetrics`, `PeerMetrics`, `GossipMetrics`, `TopologyMetrics`, `NodeMetrics`. Constructors change signature:

```go
// Before
func NewMeshMetrics(c *Collector) *MeshMetrics

// After
func NewMeshMetrics(mp metric.MeterProvider) *MeshMetrics
```

Fields become OTel interfaces:

```go
type MeshMetrics struct {
    DatagramsSent     metric.Int64Counter
    DatagramsRecv     metric.Int64Counter
    DatagramBytesSent metric.Int64Counter
    DatagramBytesRecv metric.Int64Counter
    DatagramErrors    metric.Int64Counter
    SessionConnects   metric.Int64Counter
    SessionDisconnects metric.Int64Counter
    SessionsActive    metric.Float64Gauge
}
```

When disabled, `noop.NewMeterProvider()` produces zero-cost instruments. No nil checks needed — every method is an empty body.

#### Wire-Up (Supervisor)

When enabled:

```go
exporter := newZapExporter(log.Named("metrics"))
reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(10*time.Second))
mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))
```

When disabled:

```go
mp := noop.NewMeterProvider()
```

Shutdown: `mp.Shutdown(ctx)` handles final flush.

#### Zap Exporter

Custom `Exporter` implementation (~30 lines) replacing `LogSink`. Iterates `metricdata.ResourceMetrics` and writes each data point to zap at Debug level. Equivalent output to current `LogSink`.

#### Control Service Value Reads

The `GetMetrics` RPC needs to read current cumulative counter and gauge values on demand. OTel provides `ManualReader` for exactly this — on-demand metric collection.

Register two readers on the same `MeterProvider`:

1. `PeriodicReader` — flushes to the zap exporter every 10s (replaces `LogSink`)
2. `ManualReader` — called on demand by the control service

```go
manualReader := sdkmetric.NewManualReader()
periodicReader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(10*time.Second))
mp := sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(manualReader),
    sdkmetric.WithReader(periodicReader),
    sdkmetric.WithResource(res),
)
```

Each reader gets its own independent aggregation pipeline. The `supervisorMetricsSource` adapter holds a reference to the `ManualReader` and calls `Collect(ctx, &rm)` when `GetMetrics` is invoked. It walks the returned `metricdata.ResourceMetrics` to populate `control.Metrics`:

- Counters return cumulative `int64` values (default `CumulativeTemporality`)
- Gauges return last-recorded values
- Goroutine-safe, trivial cost for ~30 instruments

No parallel atomics, no dual-write at call sites. The OTel pipeline is the single source of truth for both periodic export and on-demand reads.

Membership-owned values (SmoothedErr, VivaldiSamples, EagerSyncs, EagerSyncFailures) already flow through `membership.Service.ControlMetrics()` using plain atomics — no OTel instruments involved, unchanged.

#### EWMA

Stays as `metrics.EWMA` (~50 lines). Exposed to OTel export pipeline via `Float64ObservableGauge` callback:

```go
m.Float64ObservableGauge("pollen.gossip.stale_ratio",
    metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
        o.Observe(staleRatio.Value())
        return nil
    }))
```

#### PeerMetrics.Enabled() Check

Current code skips peer state iteration when metrics are disabled. With OTel noop, instruments exist but `Enabled(ctx)` returns `false`. Replace custom `.Enabled()` with checking any instrument's `Enabled(ctx)`.

#### Context Parameter

OTel instrument calls require `context.Context`. All current call sites are within goroutines that already have a context. This is a mechanical change.

### 2. OTel Traces

#### Dependencies

- `go.opentelemetry.io/otel/trace` — API interfaces
- `go.opentelemetry.io/otel/trace/noop` — zero-cost disabled mode
- `go.opentelemetry.io/otel/sdk/trace` — SDK, imported only by supervisor wire-up

#### Tracer Wiring

Constructors take `trace.TracerProvider` instead of `*traces.Tracer`. Each domain gets its own tracer:

```go
tracer := provider.Tracer("pollen/transport")
tracer := provider.Tracer("pollen/membership")
```

When disabled: `noop.NewTracerProvider()`. When enabled: `sdktrace.NewTracerProvider` with a `BatchSpanProcessor` and a custom zap exporter.

#### Span Creation (5 Sites)

Mechanical replacement:

| Current | OTel |
|---------|------|
| `tracer.Start("name")` | `ctx, span := tracer.Start(ctx, "name")` |
| `tracer.StartFromRemote("name", traceIDBytes)` | Extract trace context into ctx, then `ctx, span := tracer.Start(ctx, "name")` |
| `span.SetAttr("k", "v")` | `span.SetAttributes(attribute.String("k", "v"))` |
| `span.End()` | `span.End()` |
| `span.TraceIDBytes()` | `trace.SpanFromContext(ctx).SpanContext().TraceID()[:]` |

#### Trace Context Propagation

Currently half-broken: `StartFromRemote` reads `trace_id` from envelopes, but senders never populate it. The migration fixes this:

- **Receive path**: extract `TraceID` from incoming envelope bytes into a remote `SpanContext`, wrap in context, start child span.
- **Send path**: extract `TraceID` from current span context, set on outgoing envelope's `trace_id` field.

This gives working cross-node distributed traces.

#### Parent-Child Spans

Currently impossible (field exists, no API). OTel handles this via context propagation — `tracer.Start(ctx, ...)` automatically creates a child of whatever span is in `ctx`.

#### Shared Resource

Metrics and traces share an OTel `resource.Resource` identifying the node (peer key, version). Both initialized and shut down together in supervisor.

### 3. Atomic Writes — `google/renameio`

#### Dependency

`github.com/google/renameio/v2`

#### `pkg/config/perm.go`

Replace `atomicWrite()` body:

```go
func atomicWrite(path string, data []byte, mode os.FileMode) error {
    if err := renameio.WriteFile(path, data, mode); err != nil {
        return fmt.Errorf("write %s: %w", path, err)
    }
    // Explicit chmod overrides umask.
    if err := os.Chmod(path, mode); err != nil {
        return fmt.Errorf("chmod %s: %w", path, err)
    }
    return setPlnGroup(path)
}
```

Public API unchanged: `WritePrivate`, `WriteGroupReadable`, `WriteGroupWritable`.

Fixes: parent directory fsync after rename (missing in current implementation, required for crash safety on Linux).

Note: `setPlnGroup` now runs after the file is at its final path (renameio does the rename internally). If `setPlnGroup` fails, the file exists with wrong group rather than being cleaned up. This is acceptable — the file content is correct, and the next write attempt will fix group ownership.

#### `pkg/cas/cas.go`

Replace raw `os.CreateTemp` + `os.Rename` with `renameio.NewPendingFile` (supports streaming `io.Reader` input) + `CloseAtomicallyReplace()`. Adds file fsync + dir fsync.

### 4. Concurrency — `errgroup` and `semaphore`

#### WASM Runtime Semaphore (`pkg/wasm/runtime.go`)

Replace the manual `chan struct{}` semaphore with `golang.org/x/sync/semaphore.Weighted`. The current pattern:

```go
select {
case r.sem <- struct{}{}:
case <-ctx.Done():
    return nil, fmt.Errorf("wasm: acquire slot: %w", ctx.Err())
}
defer func() { <-r.sem }()
```

Becomes:

```go
if err := r.sem.Acquire(ctx, 1); err != nil {
    return nil, fmt.Errorf("wasm: acquire slot: %w", err)
}
defer r.sem.Release(1)
```

Field changes from `sem chan struct{}` to `sem *semaphore.Weighted`. Constructor uses `semaphore.NewWeighted(int64(maxConcurrency))`. Same behavior, standard library.

#### Worker Pool (`pkg/supervisor/workers.go`)

Adapt to use `errgroup` as the foundation while preserving drain-on-shutdown. The current pool has three behaviors that matter:

1. **Bounded concurrency** — max 8 concurrent goroutines
2. **Function queue** — 64-slot buffered channel
3. **Drain on shutdown** — when context cancels, finish all queued work before returning

`errgroup.SetLimit(n)` provides (1). For (2) and (3), the pool keeps its work channel and drain loop but delegates goroutine management to errgroup:

```go
type workerPool struct {
    work chan func()
    eg   *errgroup.Group
}

func (p *workerPool) run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            // Drain remaining queued work.
            for {
                select {
                case fn := <-p.work:
                    p.eg.Go(func() error { fn(); return nil })
                default:
                    return
                }
            }
        case fn := <-p.work:
            p.eg.Go(func() error { fn(); return nil })
        }
    }
}
```

`wait()` calls `p.eg.Wait()`. `submit()` stays the same (sends to work channel with context check). This eliminates the manual semaphore + WaitGroup while keeping drain semantics. Error propagation is now available if needed in the future.

## Files Deleted

- `pkg/observability/metrics/collector.go`
- `pkg/observability/metrics/logsink.go`
- `pkg/observability/metrics/metrics.go`
- `pkg/observability/metrics/collector_test.go`
- `pkg/observability/traces/span.go`
- `pkg/observability/traces/sink.go`
- `pkg/observability/traces/trace.go`
- `pkg/observability/traces/trace_test.go`

## Files Added

- `pkg/observability/metrics/otel.go` — MeterProvider construction, zap exporter
- `pkg/observability/traces/otel.go` — TracerProvider construction, zap span exporter

## Files Modified

- `pkg/observability/metrics/instruments.go` — bundle constructors take `metric.MeterProvider`
- `pkg/observability/metrics/ewma.go` — stays, no changes
- `pkg/transport/transport.go`, `accept.go`, `session.go`, `session_registry.go`, `peer.go` — metric/trace call sites
- `pkg/membership/gossip.go`, `service.go`, `options.go` — tracer wiring, span sites
- `pkg/supervisor/supervisor.go`, `topology.go`, `punch.go`, `adapters.go` — OTel provider creation, metric/trace wiring
- `pkg/control/service.go` — no change (reads from `ControlMetrics()` method, not instruments directly)
- `pkg/config/perm.go` — renameio replacement
- `pkg/cas/cas.go` — renameio replacement
- `pkg/transport/api.go` — `WithTracer` option type change
- `pkg/supervisor/workers.go` — rewrite atop errgroup
- `pkg/wasm/runtime.go` — semaphore.Weighted replacing channel semaphore
- `go.mod`, `go.sum` — new dependencies

## ARCH_SPEC Impact

- `observability` remains a foundation layer leaf package
- Noop pattern preserved (OTel noop providers)
- Invariant "Prometheus metric names" — OTel uses dot-separated names (`pollen.mesh.datagrams.sent`) rather than the current underscore Prometheus convention (`pollen_mesh_datagrams_sent_total`). Adopt OTel naming; the Prometheus exporter auto-converts dots to underscores if added later
- Shutdown ordering unchanged: `MeterProvider.Shutdown` + `TracerProvider.Shutdown` in the flush-observability step
- `config` remains a foundation layer leaf package
- No layer boundary changes

## Testing

- Existing `perm_test.go` and `perm_linux_test.go` pass unchanged (test public API)
- Metrics tests rewritten against OTel `sdkmetrictest.NewMetricReader()` for assertion
- Trace tests rewritten against `tracetest.NewSpanRecorder()` for assertion
- EWMA tests unchanged
