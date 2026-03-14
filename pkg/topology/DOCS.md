# pkg/topology — Target State

## Changes from Current

- [SAFE] Unexport internal-only constants: `NearestHysteresis`, `MinHysteresisDistance`, `CcDefault`, `CeDefault`, `MinHeight`, `MaxCoord`, `MinRTTFloor`
- [SAFE] Replace `clamp(v, lo, hi float64)` with `max(lo, min(hi, v))` (Go 1.21 builtins)

## Target Exported API

### Unexported constants

| Current | Target |
|---------|--------|
| `NearestHysteresis` | `nearestHysteresis` |
| `MinHysteresisDistance` | `minHysteresisDistance` |
| `CcDefault` | `ccDefault` |
| `CeDefault` | `ceDefault` |
| `MinHeight` | `minHeight` |
| `MaxCoord` | `maxCoord` |
| `MinRTTFloor` | `minRTTFloor` |

## Deleted Items

| Item | Reason |
|------|--------|
| `clamp` function (~7 LOC) | Replaced by Go 1.21 `max`/`min` builtins |
