# pkg/nat

## Responsibilities
- Classifies NAT behavior as endpoint-independent ("easy") or endpoint-dependent ("hard")
- Provides NAT detection via observation collection from peer reports
- Supplies wire-protocol conversion (uint32)

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Type` | type | NAT classification enum |
| `Unknown` | const | Unclassified NAT |
| `Easy` | const | Endpoint-independent NAT (stable external port) |
| `Hard` | const | Endpoint-dependent/symmetric NAT (variable port) |
| `(Type).String` | method | String representation |
| `(Type).ToUint32` | method | Wire protocol conversion |
| `TypeFromUint32` | func | Reconstruct Type from uint32 |
| `Detector` | type | State machine for NAT type determination |
| `NewDetector` | func | Create a new NAT detector |
| `(*Detector).AddObservation` | method | Record port observation from peer |
| `(*Detector).Type` | method | Get current NAT classification |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/mesh (uses: `Type`)
- pkg/node (uses: `Detector`, `NewDetector`, `Type`)
- pkg/store (uses: `Type`, `Unknown`)
- pkg/topology (uses: `Type`)
- pkg/sock (uses: `Type`, `Easy`)

## Proposed Minimal API

No changes — API surface already minimal. All 11 exports have production callers across 5 consumers.
