# pkg/traffic

## Responsibilities
- Records per-peer inbound/outbound byte counts from bidirectional streams
- Accumulates traffic with sliding window (3-slot ring buffer)
- Suppresses publication when changes are below deadband threshold (2%)
- Provides noop implementation for optional traffic tracking

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Recorder` | interface | `Record` method for per-peer byte count tracking |
| `Noop` | var | No-op Recorder implementation |
| `PeerTraffic` | type | Accumulated BytesIn and BytesOut for a peer |
| `Tracker` | type | Thread-safe accumulator with sliding window and deadband |
| `New` | func | Create new Tracker |
| `(*Tracker).Record` | method | Add byte counts for a peer |
| `(*Tracker).RotateAndSnapshot` | method | Rotate window, sum, compare against published |
| `WrapStream` | func | Wrap bidirectional stream with traffic counting |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/types | `PeerKey` |

## Consumed by
- pkg/mesh (uses: `Recorder`, `Noop`, `WrapStream`)
- pkg/node (uses: `New`, `Tracker`, `RotateAndSnapshot`)
- pkg/tunnel (uses: `Recorder`, `Noop`, `WrapStream`)

## Proposed Minimal API

No changes — API surface already minimal. All 8 exports have production callers across 3 consumers.
