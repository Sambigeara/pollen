# Observability & Atomic Write Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace custom metrics, traces, atomic write, and concurrency primitives with OTel, renameio, errgroup, and x/sync/semaphore.

**Architecture:** Five independent migrations sharing a single dependency addition step. OTel metrics and traces share a `resource.Resource` and are initialized together in supervisor. Atomic writes, worker pool, and WASM semaphore are fully independent.

**Tech Stack:** `go.opentelemetry.io/otel` (metrics + traces), `github.com/google/renameio/v2`, `golang.org/x/sync/{errgroup,semaphore}`

**Spec:** `docs/superpowers/specs/2026-03-19-observability-atomicwrite-migration-design.md`

---

### Task 1: Add Dependencies

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add new dependencies**

```bash
cd /Users/samuellock/code/pollen
go get go.opentelemetry.io/otel@latest \
       go.opentelemetry.io/otel/metric@latest \
       go.opentelemetry.io/otel/sdk/metric@latest \
       go.opentelemetry.io/otel/trace@latest \
       go.opentelemetry.io/otel/sdk/trace@latest \
       go.opentelemetry.io/otel/sdk@latest \
       github.com/google/renameio/v2@latest \
       golang.org/x/sync@latest
```

- [ ] **Step 2: Verify**

```bash
go mod tidy && go build ./...
```

Expected: builds clean, no errors.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -s -m "add otel, renameio, and x/sync dependencies"
```

---

### Task 2: Atomic Writes — renameio

**Files:**
- Modify: `pkg/config/perm.go`
- Modify: `pkg/cas/cas.go`
- Test: `pkg/config/perm_test.go` (existing, unchanged)
- Test: `pkg/cas/cas_test.go` (existing, unchanged)

- [ ] **Step 1: Replace `atomicWrite` in `pkg/config/perm.go`**

Replace the entire `atomicWrite` function body (lines 11-51) with:

```go
func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := renameio.WriteFile(path, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return setPlnGroup(path)
}
```

Add `"github.com/google/renameio/v2"` to imports. Run `goimports -w pkg/config/perm.go`.

- [ ] **Step 2: Replace `Put` in `pkg/cas/cas.go`**

Replace the `Put` method (lines 33-62). Since the destination path depends on the content hash (unknown until after reading), we buffer the content, compute the hash, then use `renameio.WriteFile` with the known destination:

```go
func (s *Store) Put(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("cas: read artifact: %w", err)
	}

	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])
	dir := filepath.Join(s.root, hash[:2])
	if err := config.EnsureDir(dir); err != nil {
		return "", fmt.Errorf("cas: create shard dir: %w", err)
	}

	dest := filepath.Join(dir, hash+".wasm")
	if err := renameio.WriteFile(dest, data, 0o644); err != nil {
		return "", fmt.Errorf("cas: write artifact: %w", err)
	}
	return hash, nil
}
```

Add `"github.com/google/renameio/v2"` to imports. Run `goimports -w pkg/cas/cas.go`.

Note: buffering is acceptable — WASM modules are typically 1-5MB. `renameio.WriteFile` handles tmp write → fsync → rename → dir fsync.

- [ ] **Step 3: Run existing tests**

```bash
go test ./pkg/config/ ./pkg/cas/ -v -count=1
```

Expected: all existing perm and CAS tests pass (they test public API, not internals).

- [ ] **Step 4: Commit**

```bash
git add pkg/config/perm.go pkg/cas/cas.go
git commit -s -m "replace custom atomic writes with google/renameio"
```

---

### Task 3: WASM Semaphore — x/sync/semaphore

**Files:**
- Modify: `pkg/wasm/runtime.go`
- Test: `pkg/wasm/runtime_test.go` (existing, unchanged)

- [ ] **Step 1: Replace channel semaphore in `pkg/wasm/runtime.go`**

Change the `Runtime` struct field (line 51):

```go
// Before
sem chan struct{}

// After
sem *semaphore.Weighted
```

Change constructor `NewRuntime` (line 68):

```go
// Before
sem: make(chan struct{}, maxConcurrency),

// After
sem: semaphore.NewWeighted(int64(maxConcurrency)),
```

Change the acquire/release in `Call` method. Find the current pattern:

```go
select {
case r.sem <- struct{}{}:
case <-ctx.Done():
	return nil, fmt.Errorf("wasm: acquire slot: %w", ctx.Err())
}
defer func() { <-r.sem }()
```

Replace with:

```go
if err := r.sem.Acquire(ctx, 1); err != nil {
	return nil, fmt.Errorf("wasm: acquire slot: %w", err)
}
defer r.sem.Release(1)
```

Add `"golang.org/x/sync/semaphore"` to imports. Run `goimports -w pkg/wasm/runtime.go`.

- [ ] **Step 2: Run existing tests**

```bash
go test ./pkg/wasm/ -v -count=1
```

Expected: all tests pass, including `TestCallConcurrencyLimit`.

- [ ] **Step 3: Commit**

```bash
git add pkg/wasm/runtime.go
git commit -s -m "replace channel semaphore with x/sync/semaphore in wasm runtime"
```

---

### Task 4: Worker Pool — errgroup

**Files:**
- Modify: `pkg/supervisor/workers.go`
- Test: `pkg/supervisor/workers_test.go` (existing tests must still pass)

- [ ] **Step 1: Rewrite `pkg/supervisor/workers.go`**

Replace the entire file:

```go
package supervisor

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type workerPool struct {
	work  chan func()
	eg    *errgroup.Group
	limit int
}

func newWorkerPool(maxConcurrency int) *workerPool {
	return &workerPool{
		work:  make(chan func(), 64), //nolint:mnd
		limit: maxConcurrency,
	}
}

func (p *workerPool) run(ctx context.Context) {
	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(p.limit)
	p.eg = eg

	for {
		select {
		case <-ctx.Done():
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

func (p *workerPool) submit(ctx context.Context, fn func()) {
	select {
	case p.work <- fn:
	case <-ctx.Done():
	}
}

func (p *workerPool) wait() {
	if p.eg != nil {
		_ = p.eg.Wait()
	}
}
```

Run `goimports -w pkg/supervisor/workers.go`.

Key changes from current:
- `sync.WaitGroup` + channel semaphore replaced by `errgroup` with `SetLimit`
- `submit` no longer does `wg.Add(1)` / `wg.Done()` on cancel — just drops the work
- `wait` calls `eg.Wait()` which blocks until all launched goroutines finish
- Drain loop preserved: on context cancel, dispatch remaining queued work to errgroup

- [ ] **Step 2: Run existing tests**

```bash
go test ./pkg/supervisor/ -run TestWorkerPool -v -count=1
```

Expected: all three tests pass:
- `TestWorkerPool_BoundsConcurrency` — peak concurrency <= 2
- `TestWorkerPool_ShutdownDrainsWork` — all 5 tasks complete
- `TestWorkerPool_SubmitRespectsContext` — submit returns immediately on cancelled ctx

- [ ] **Step 3: Commit**

```bash
git add pkg/supervisor/workers.go
git commit -s -m "rewrite worker pool atop x/sync/errgroup"
```

---

### Task 5: OTel Metrics — Foundation

**Files:**
- Delete: `pkg/observability/metrics/collector.go`
- Delete: `pkg/observability/metrics/logsink.go`
- Delete: `pkg/observability/metrics/metrics.go`
- Delete: `pkg/observability/metrics/collector_test.go`
- Create: `pkg/observability/metrics/otel.go`
- Modify: `pkg/observability/metrics/instruments.go`
- Keep: `pkg/observability/metrics/ewma.go` (no changes)

- [ ] **Step 1: Create `pkg/observability/metrics/otel.go`**

This file provides the zap exporter and MeterProvider constructors:

```go
package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// Providers holds the OTel providers and the ManualReader for on-demand collection.
type Providers struct {
	Meter         metric.MeterProvider
	ManualReader  *sdkmetric.ManualReader
	shutdown      func(context.Context) error
}

// NewProviders creates a real MeterProvider with periodic log export and a
// ManualReader for on-demand metric collection (used by the control service).
func NewProviders(log *zap.SugaredLogger) *Providers {
	manual := sdkmetric.NewManualReader()
	periodic := sdkmetric.NewPeriodicReader(
		&zapExporter{log: log},
		sdkmetric.WithInterval(10*time.Second),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(manual),
		sdkmetric.WithReader(periodic),
	)
	return &Providers{
		Meter:        mp,
		ManualReader: manual,
		shutdown:     mp.Shutdown,
	}
}

// NewNoopProviders returns a zero-cost noop MeterProvider.
func NewNoopProviders() *Providers {
	return &Providers{
		Meter:    noop.NewMeterProvider(),
		shutdown: func(context.Context) error { return nil },
	}
}

// Shutdown flushes and shuts down the MeterProvider.
func (p *Providers) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

// zapExporter writes metric snapshots to a zap logger at Debug level.
type zapExporter struct {
	log *zap.SugaredLogger
}

func (e *zapExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

func (e *zapExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

func (e *zapExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					e.log.Debugw(m.Name, "value", dp.Value)
				}
			default:
				e.log.Debugw(m.Name, "data", fmt.Sprintf("%T", m.Data))
			}
		}
	}
	return nil
}

func (e *zapExporter) ForceFlush(context.Context) error { return nil }
func (e *zapExporter) Shutdown(context.Context) error   { return nil }
```

Run `goimports -w pkg/observability/metrics/otel.go`.

- [ ] **Step 2: Rewrite `pkg/observability/metrics/instruments.go`**

Replace the entire file. All bundle structs now hold OTel interfaces. Constructors take `metric.MeterProvider`:

```go
package metrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// MeshMetrics instruments the transport layer.
type MeshMetrics struct {
	DatagramsSent      metric.Int64Counter
	DatagramsRecv      metric.Int64Counter
	DatagramBytesSent  metric.Int64Counter
	DatagramBytesRecv  metric.Int64Counter
	DatagramErrors     metric.Int64Counter
	SessionConnects    metric.Int64Counter
	SessionDisconnects metric.Int64Counter
	SessionsActive     metric.Float64Gauge
}

func NewMeshMetrics(mp metric.MeterProvider) *MeshMetrics {
	m := mp.Meter("pollen/mesh")
	mm := &MeshMetrics{}
	mm.DatagramsSent, _ = m.Int64Counter("pollen.mesh.datagrams.sent")
	mm.DatagramsRecv, _ = m.Int64Counter("pollen.mesh.datagrams.recv")
	mm.DatagramBytesSent, _ = m.Int64Counter("pollen.mesh.datagram.bytes.sent")
	mm.DatagramBytesRecv, _ = m.Int64Counter("pollen.mesh.datagram.bytes.recv")
	mm.DatagramErrors, _ = m.Int64Counter("pollen.mesh.datagram.errors")
	mm.SessionConnects, _ = m.Int64Counter("pollen.mesh.session.connects")
	mm.SessionDisconnects, _ = m.Int64Counter("pollen.mesh.session.disconnects")
	mm.SessionsActive, _ = m.Float64Gauge("pollen.mesh.sessions.active")
	return mm
}

// PeerMetrics instruments the peer state machine.
type PeerMetrics struct {
	Connections      metric.Int64Counter
	Disconnects      metric.Int64Counter
	StageEscalations metric.Int64Counter
	StateTransitions metric.Int64Counter
	PeersDiscovered  metric.Float64Gauge
	PeersConnecting  metric.Float64Gauge
	PeersConnected   metric.Float64Gauge
	PeersUnreachable metric.Float64Gauge
}

func NewPeerMetrics(mp metric.MeterProvider) *PeerMetrics {
	m := mp.Meter("pollen/peer")
	pm := &PeerMetrics{}
	pm.Connections, _ = m.Int64Counter("pollen.peer.connections")
	pm.Disconnects, _ = m.Int64Counter("pollen.peer.disconnects")
	pm.StageEscalations, _ = m.Int64Counter("pollen.peer.stage.escalations")
	pm.StateTransitions, _ = m.Int64Counter("pollen.peer.state.transitions")
	pm.PeersDiscovered, _ = m.Float64Gauge("pollen.peer.discovered")
	pm.PeersConnecting, _ = m.Float64Gauge("pollen.peer.connecting")
	pm.PeersConnected, _ = m.Float64Gauge("pollen.peer.connected")
	pm.PeersUnreachable, _ = m.Float64Gauge("pollen.peer.unreachable")
	return pm
}

// Enabled returns true if metrics are actively being collected (not noop).
func (pm *PeerMetrics) Enabled(ctx context.Context) bool {
	return pm.PeersConnected.Enabled(ctx)
}

// GossipMetrics instruments the gossip/state layer.
type GossipMetrics struct {
	EventsReceived metric.Int64Counter
	EventsApplied  metric.Int64Counter
	EventsStale    metric.Int64Counter
	SelfConflicts  metric.Int64Counter
	Revocations    metric.Int64Counter
	BatchSize      metric.Float64Gauge
	StaleRatio     *EWMA
}

func NewGossipMetrics(mp metric.MeterProvider) *GossipMetrics {
	m := mp.Meter("pollen/gossip")
	gm := &GossipMetrics{
		StaleRatio: NewEWMA(0.01),
	}
	gm.EventsReceived, _ = m.Int64Counter("pollen.gossip.events.received")
	gm.EventsApplied, _ = m.Int64Counter("pollen.gossip.events.applied")
	gm.EventsStale, _ = m.Int64Counter("pollen.gossip.events.stale")
	gm.SelfConflicts, _ = m.Int64Counter("pollen.gossip.self.conflicts")
	gm.Revocations, _ = m.Int64Counter("pollen.gossip.revocations")
	gm.BatchSize, _ = m.Float64Gauge("pollen.gossip.batch.size")

	// Expose EWMA as an observable gauge for the export pipeline.
	_, _ = m.Float64ObservableGauge("pollen.gossip.stale.ratio",
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(gm.StaleRatio.Value())
			return nil
		}))
	return gm
}

// TopologyMetrics instruments topology selection.
type TopologyMetrics struct {
	VivaldiError       metric.Float64Gauge
	HMACNearestEnabled metric.Float64Gauge
	TopologyPrunes     metric.Int64Counter
}

func NewTopologyMetrics(mp metric.MeterProvider) *TopologyMetrics {
	m := mp.Meter("pollen/topology")
	tm := &TopologyMetrics{}
	tm.VivaldiError, _ = m.Float64Gauge("pollen.topology.vivaldi.error")
	tm.HMACNearestEnabled, _ = m.Float64Gauge("pollen.topology.hmac.nearest.enabled")
	tm.TopologyPrunes, _ = m.Int64Counter("pollen.topology.prunes")
	return tm
}

// NodeMetrics instruments node-level operations.
type NodeMetrics struct {
	CertExpirySeconds  metric.Float64Gauge
	CertRenewals       metric.Int64Counter
	CertRenewalsFailed metric.Int64Counter
	PunchAttempts      metric.Int64Counter
	PunchFailures      metric.Int64Counter
}

func NewNodeMetrics(mp metric.MeterProvider) *NodeMetrics {
	m := mp.Meter("pollen/node")
	nm := &NodeMetrics{}
	nm.CertExpirySeconds, _ = m.Float64Gauge("pollen.node.cert.expiry.seconds")
	nm.CertRenewals, _ = m.Int64Counter("pollen.node.cert.renewals")
	nm.CertRenewalsFailed, _ = m.Int64Counter("pollen.node.cert.renewals.failed")
	nm.PunchAttempts, _ = m.Int64Counter("pollen.node.punch.attempts")
	nm.PunchFailures, _ = m.Int64Counter("pollen.node.punch.failures")
	return nm
}
```

Run `goimports -w pkg/observability/metrics/instruments.go`.

- [ ] **Step 3: Delete old files**

```bash
rm pkg/observability/metrics/collector.go \
   pkg/observability/metrics/logsink.go \
   pkg/observability/metrics/metrics.go \
   pkg/observability/metrics/collector_test.go
```

- [ ] **Step 4: Verify metrics package compiles**

```bash
go build ./pkg/observability/metrics/
```

Expected: compiles clean. There will be downstream consumer compilation errors — those are fixed in Task 7.

- [ ] **Step 5: Write new metrics tests**

Create `pkg/observability/metrics/otel_test.go`:

```go
package metrics

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"github.com/stretchr/testify/require"
)

func TestMeshMetrics_CountersIncrement(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	mm := NewMeshMetrics(mp)
	ctx := context.Background()

	mm.DatagramsSent.Add(ctx, 5)
	mm.DatagramErrors.Add(ctx, 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	values := extractInt64Sums(rm)
	require.Equal(t, int64(5), values["pollen.mesh.datagrams.sent"])
	require.Equal(t, int64(1), values["pollen.mesh.datagram.errors"])
}

func TestPeerMetrics_EnabledReflectsProvider(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	pm := NewPeerMetrics(mp)
	require.True(t, pm.Enabled(context.Background()))
}

func TestPeerMetrics_NoopDisabled(t *testing.T) {
	pm := NewPeerMetrics(NewNoopProviders().Meter)
	require.False(t, pm.Enabled(context.Background()))
}

func TestGossipMetrics_EWMAExposedViaObservableGauge(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	gm := NewGossipMetrics(mp)
	gm.StaleRatio.Update(1.0)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	values := extractFloat64Gauges(rm)
	require.InDelta(t, 0.01, values["pollen.gossip.stale.ratio"], 0.001)
}

// extractInt64Sums returns metric name -> cumulative value for all Int64 Sum metrics.
func extractInt64Sums(rm metricdata.ResourceMetrics) map[string]int64 {
	out := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					out[m.Name] += dp.Value
				}
			}
		}
	}
	return out
}

// extractFloat64Gauges returns metric name -> value for all Float64 Gauge metrics.
func extractFloat64Gauges(rm metricdata.ResourceMetrics) map[string]float64 {
	out := make(map[string]float64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok {
				for _, dp := range g.DataPoints {
					out[m.Name] = dp.Value
				}
			}
		}
	}
	return out
}
```

- [ ] **Step 6: Run tests**

```bash
go test ./pkg/observability/metrics/ -v -count=1
```

Expected: all new tests pass.

- [ ] **Step 7: Commit**

```bash
git add pkg/observability/metrics/
git commit -s -m "replace custom metrics with OTel metric SDK"
```

---

### Task 6: OTel Traces — Foundation

**Files:**
- Delete: `pkg/observability/traces/span.go`
- Delete: `pkg/observability/traces/sink.go`
- Delete: `pkg/observability/traces/trace.go`
- Delete: `pkg/observability/traces/trace_test.go`
- Create: `pkg/observability/traces/otel.go`

- [ ] **Step 1: Create `pkg/observability/traces/otel.go`**

Provides TracerProvider constructors and the zap span exporter:

```go
package traces

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// Providers holds the OTel TracerProvider.
type Providers struct {
	Tracer   trace.TracerProvider
	shutdown func(context.Context) error
}

// NewProviders creates a real TracerProvider with batch span export to zap.
func NewProviders(log *zap.SugaredLogger) *Providers {
	exp := &zapSpanExporter{log: log}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	return &Providers{
		Tracer:   tp,
		shutdown: tp.Shutdown,
	}
}

// NewNoopProviders returns a zero-cost noop TracerProvider.
func NewNoopProviders() *Providers {
	return &Providers{
		Tracer:   tracenoop.NewTracerProvider(),
		shutdown: func(context.Context) error { return nil },
	}
}

// Shutdown flushes and shuts down the TracerProvider.
func (p *Providers) Shutdown(ctx context.Context) error {
	return p.shutdown(ctx)
}

// SpanContextFromTraceID builds a remote SpanContext from raw trace ID bytes.
// Returns an empty SpanContext if traceID is not exactly 16 bytes.
func SpanContextFromTraceID(traceID []byte) trace.SpanContext {
	if len(traceID) != 16 {
		return trace.SpanContext{}
	}
	var tid trace.TraceID
	copy(tid[:], traceID)
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

// zapSpanExporter writes completed spans to a zap logger at Debug level.
type zapSpanExporter struct {
	log *zap.SugaredLogger
}

func (e *zapSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, s := range spans {
		fields := []any{
			"trace_id", s.SpanContext().TraceID().String(),
			"span_id", s.SpanContext().SpanID().String(),
			"duration", s.EndTime().Sub(s.StartTime()),
		}
		if s.Parent().IsValid() {
			fields = append(fields, "parent_span_id", s.Parent().SpanID().String())
		}
		for _, attr := range s.Attributes() {
			fields = append(fields, string(attr.Key), attr.Value.Emit())
		}
		e.log.Debugw(s.Name(), fields...)
	}
	return nil
}

func (e *zapSpanExporter) Shutdown(context.Context) error { return nil }
```

Run `goimports -w pkg/observability/traces/otel.go`.

- [ ] **Step 2: Delete old files**

```bash
rm pkg/observability/traces/span.go \
   pkg/observability/traces/sink.go \
   pkg/observability/traces/trace.go \
   pkg/observability/traces/trace_test.go
```

- [ ] **Step 3: Verify traces package compiles**

```bash
go build ./pkg/observability/traces/
```

Expected: compiles clean. Downstream consumer errors fixed in Task 7.

- [ ] **Step 4: Write new traces tests**

Create `pkg/observability/traces/otel_test.go`:

```go
package traces

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSpanContextFromTraceID_Valid(t *testing.T) {
	tid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sc := SpanContextFromTraceID(tid[:])
	require.True(t, sc.HasTraceID())
	require.True(t, sc.IsRemote())
	require.Equal(t, trace.TraceID(tid), sc.TraceID())
	// SpanID is zero so IsValid() is false — that's expected.
	// ContextWithRemoteSpanContext only needs the TraceID.
}

func TestSpanContextFromTraceID_InvalidLength(t *testing.T) {
	sc := SpanContextFromTraceID([]byte{1, 2, 3})
	require.False(t, sc.HasTraceID())
}

func TestSpanContextFromTraceID_Nil(t *testing.T) {
	sc := SpanContextFromTraceID(nil)
	require.False(t, sc.HasTraceID())
}

func TestZapSpanExporter_RecordsSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	tracer := tp.Tracer("test")
	_, span := tracer.Start(context.Background(), "test.op")
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "test.op", spans[0].Name())
}

func TestNoopProvider_NoSpans(t *testing.T) {
	p := NewNoopProviders()
	tracer := p.Tracer.Tracer("test")
	_, span := tracer.Start(context.Background(), "noop.op")
	span.End()
	// No panic, no output — just verifying the noop path works.
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./pkg/observability/traces/ -v -count=1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add pkg/observability/traces/
git commit -s -m "replace custom traces with OTel trace SDK"
```

---

### Task 7: Wire OTel Into Consumers

This is the largest task — updating all import sites and call sites across transport, membership, supervisor, and control.

**Files:**
- Modify: `pkg/supervisor/supervisor.go` — OTel provider creation, shutdown
- Modify: `pkg/supervisor/adapters.go` — ManualReader-based ControlMetrics
- Modify: `pkg/transport/transport.go` — metric/trace types
- Modify: `pkg/transport/api.go` — option types
- Modify: `pkg/transport/session.go` — trace span sites
- Modify: `pkg/transport/session_registry.go` — gauge type
- Modify: `pkg/transport/peer.go` — metric call sites, Enabled check
- Modify: `pkg/transport/accept.go` — metric call sites (if any direct refs)
- Modify: `pkg/membership/service.go` — metric/trace types
- Modify: `pkg/membership/options.go` — option types
- Modify: `pkg/membership/gossip.go` — trace span sites
- Modify: `pkg/supervisor/topology.go` — metric call sites
- Modify: `pkg/supervisor/punch.go` — metric call sites

**Approach:** Work package by package. After each package, verify it compiles. The order is bottom-up: transport (leaf consumer) → membership → supervisor (wire-up).

- [ ] **Step 1: Update transport package**

In `pkg/transport/api.go`:
- Change `WithMetrics` option to accept `*metrics.MeshMetrics` (unchanged type name, but fields are now OTel interfaces)
- Change `WithTracer` option to accept `trace.TracerProvider` instead of `*traces.Tracer`
- The `impl` struct stores a `trace.Tracer` (obtained from the provider)

In `pkg/transport/transport.go`:
- `impl` struct: replace `tracer *traces.Tracer` with `tracer trace.Tracer`
- Metric calls add `ctx` parameter: `m.metrics.DatagramsSent.Add(ctx, 1)` instead of `.Inc()`
- `DatagramBytesSent.Add(ctx, int64(len(data)))` instead of `.Add(int64(len(data)))`

In `pkg/transport/session.go`:
- Replace `m.tracer.Start("mesh.addPeer")` with `ctx, span := m.tracer.Start(ctx, "mesh.addPeer")`
- Replace `span.SetAttr(...)` with `span.SetAttributes(attribute.String(...))`
- Replace `span.End()` with `span.End()`
- `SessionConnects.Add(ctx, 1)` instead of `.Inc()`

In `pkg/transport/session_registry.go`:
- Replace `*metrics.Gauge` with `metric.Float64Gauge`
- `.Set(float64(n))` becomes `.Record(ctx, float64(n))` — check OTel Gauge API: `Float64Gauge` has `.Record(ctx, value, opts...)`

In `pkg/transport/peer.go`:
- Replace `*metrics.PeerMetrics` field — type unchanged but fields are OTel interfaces
- `.Inc()` becomes `.Add(ctx, 1)`
- `.Add(n)` becomes `.Add(ctx, n)`
- `.Set(v)` becomes `.Record(ctx, v)`
- `s.metrics.Enabled()` becomes `s.metrics.Enabled(ctx)`
- `StateTransitions.Add(transitions)` becomes `StateTransitions.Add(ctx, transitions)`

Run `goimports -w` on all modified transport files.

```bash
go build ./pkg/transport/
```

Expected: compiles (or identifies remaining issues).

- [ ] **Step 2: Update membership package**

In `pkg/membership/options.go`:
- `WithTracer` accepts `trace.TracerProvider` instead of `*traces.Tracer`
- `WithNodeMetrics` accepts `*metrics.NodeMetrics` (type unchanged, fields are OTel)
- `WithSmoothedErr` unchanged (EWMA)

In `pkg/membership/service.go`:
- `Service` struct: `tracer trace.Tracer` instead of `*traces.Tracer`
- Initialize tracer from provider: `tracer: tp.Tracer("pollen/membership")`
- Metric calls add `ctx`: `s.nodeMetrics.CertExpirySeconds.Record(ctx, remaining.Seconds())`
- `.Inc()` becomes `.Add(ctx, 1)`

In `pkg/membership/gossip.go`:
- `s.tracer.Start("gossip.handleClock")` becomes `ctx, span := s.tracer.Start(ctx, "gossip.handleClock")`
- `span.SetAttr("peer", from.Short())` becomes `span.SetAttributes(attribute.String("peer", from.Short()))`
- `StartFromRemote("gossip.applyDelta", env.GetTraceId())` becomes:
  ```go
  sc := traces.SpanContextFromTraceID(env.GetTraceId())
  ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
  ctx, span := s.tracer.Start(ctx, "gossip.applyDelta")
  ```

Run `goimports -w` on all modified membership files.

```bash
go build ./pkg/membership/
```

- [ ] **Step 3: Update supervisor package**

In `pkg/supervisor/supervisor.go`:
- Replace metrics/traces creation (lines 116-124):
  ```go
  var mp *metrics.Providers
  var tp *traces.Providers
  if conf.MetricsEnabled {
      mp = metrics.NewProviders(log.Named("metrics"))
      tp = traces.NewProviders(log.Named("traces"))
  } else {
      mp = metrics.NewNoopProviders()
      tp = traces.NewNoopProviders()
  }
  meshMetrics := metrics.NewMeshMetrics(mp.Meter)
  peerMetrics := metrics.NewPeerMetrics(mp.Meter)
  gossipMetrics := metrics.NewGossipMetrics(mp.Meter)
  ```
- Replace `metricsCol *metrics.Collector` field with `metricsProviders *metrics.Providers` and `tracesProviders *traces.Providers`
- `WithTracer(tracer)` becomes `WithTracer(tp.Tracer)`
- Shutdown: replace `col.Close()` with `mp.Shutdown(ctx)` and `tp.Shutdown(ctx)`
- Replace trace span sites for punch coordination: same pattern as membership gossip

In `pkg/supervisor/adapters.go`:
- `supervisorMetricsSource` holds `*sdkmetric.ManualReader` (from `mp.ManualReader`) and `membership.MembershipAPI`
- Rewrite `ControlMetrics()` to collect from ManualReader:

```go
type supervisorMetricsSource struct {
	reader     *sdkmetric.ManualReader
	membership membership.MembershipAPI
}

func (m *supervisorMetricsSource) ControlMetrics() control.Metrics {
	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	_ = m.reader.Collect(ctx, &rm)

	int64s := make(map[string]int64)
	float64s := make(map[string]float64)
	for _, sm := range rm.ScopeMetrics {
		for _, met := range sm.Metrics {
			switch data := met.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					int64s[met.Name] += dp.Value
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					float64s[met.Name] = dp.Value
				}
			}
		}
	}

	cm := m.membership.ControlMetrics()
	return control.Metrics{
		CertExpirySeconds:  float64s["pollen.node.cert.expiry.seconds"],
		CertRenewals:       uint64(int64s["pollen.node.cert.renewals"]),       //nolint:gosec
		CertRenewalsFailed: uint64(int64s["pollen.node.cert.renewals.failed"]),//nolint:gosec
		PunchAttempts:      uint64(int64s["pollen.node.punch.attempts"]),      //nolint:gosec
		PunchFailures:      uint64(int64s["pollen.node.punch.failures"]),      //nolint:gosec
		GossipApplied:      uint64(int64s["pollen.gossip.events.applied"]),    //nolint:gosec
		GossipStale:        uint64(int64s["pollen.gossip.events.stale"]),      //nolint:gosec
		SmoothedVivaldiErr: cm.SmoothedErr,
		VivaldiSamples:     uint64(cm.VivaldiSamples),    //nolint:gosec
		EagerSyncs:         uint64(cm.EagerSyncs),        //nolint:gosec
		EagerSyncFailures:  uint64(cm.EagerSyncFailures), //nolint:gosec
	}
}
```

The metric name strings must match those registered in `instruments.go`.

In `pkg/supervisor/topology.go`:
- Metric calls add `ctx`: `.Record(ctx, v)` for gauges, `.Add(ctx, 1)` for counters

In `pkg/supervisor/punch.go`:
- Same pattern: `.Add(ctx, 1)` for counters

Run `goimports -w` on all modified supervisor files.

```bash
go build ./pkg/supervisor/
```

- [ ] **Step 4: Update test files**

Files that import custom metrics/traces types need updating:
- `pkg/transport/send_failure_test.go`
- `pkg/membership/service_test.go`
- `pkg/membership/gossip_test.go`

Update import paths and constructor calls. Tests that create `metrics.NewMeshMetrics(nil)` for noop now use `metrics.NewMeshMetrics(noop.NewMeterProvider())`. Tests that create `traces.NewTracer(nil)` now use `tracenoop.NewTracerProvider()`.

Run `goimports -w` on all modified test files.

- [ ] **Step 5: Full build and test**

```bash
go build ./...
go test ./pkg/transport/ ./pkg/membership/ ./pkg/supervisor/ ./pkg/control/ -v -count=1
```

Expected: all compile, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -s -m "wire OTel metrics and traces into transport, membership, and supervisor"
```

---

### Task 8: Final Verification

- [ ] **Step 1: Full test suite**

```bash
go test ./... -count=1
```

Expected: all tests pass.

- [ ] **Step 2: Lint**

```bash
just lint
```

Expected: passes. Fix any lint issues (likely `goimports` or `nolint` directives that reference removed types).

- [ ] **Step 3: Tidy**

```bash
go mod tidy
```

Verify no unused deps remain (the old `go.opentelemetry.io/proto/otlp` indirect may shift).

- [ ] **Step 4: Verify deleted files are gone**

```bash
test ! -f pkg/observability/metrics/collector.go && \
test ! -f pkg/observability/metrics/logsink.go && \
test ! -f pkg/observability/metrics/metrics.go && \
test ! -f pkg/observability/traces/span.go && \
test ! -f pkg/observability/traces/sink.go && \
test ! -f pkg/observability/traces/trace.go && \
echo "all old files removed"
```

- [ ] **Step 5: Commit final cleanup**

```bash
git add -A
git commit -s -m "tidy deps and remove stale lint directives"
```
