# pkg/tunnel — Current State

## Changes from Current

- [SAFE] Initialize `trafficTracker` to `traffic.Noop` instead of nil, removing nil checks at call sites
- [SAFE] `SetTrafficTracker` still works for wiring the real implementation

## Exported API

No API changes. All exported types, methods, and interfaces remain.

## Deleted Items

| Item | Reason |
|------|--------|
| Nil checks on `trafficTracker` at call sites | Replaced by noop default initialization |
