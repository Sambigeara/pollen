# pkg/tunnel

## Responsibilities
- Manages service tunneling over peer QUIC streams
- Handles incoming stream acceptance and port header parsing
- Maintains service registrations and remote connections
- Bridges tunnel connections to local services and clients

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Manager` | type | Service tunnel orchestrator |
| `New` | func | Constructor |
| `StreamTransport` | interface | QUIC stream provider (`OpenStream`, `AcceptStream`) |
| `ConnectionInfo` | type | Active connection metadata |
| `(*Manager).SetTrafficTracker` | method | Inject traffic recorder |
| `(*Manager).Start` | method | Start stream acceptance loop |
| `(*Manager).RegisterService` | method | Register local service port handler |
| `(*Manager).UnregisterService` | method | Unregister service; close streams |
| `(*Manager).ConnectService` | method | Open tunnel to remote service; return local port |
| `(*Manager).ListConnections` | method | Return active connections |
| `(*Manager).DisconnectLocalPort` | method | Close connection by local port |
| `(*Manager).DisconnectPeer` | method | Close all connections to peer |
| `(*Manager).DisconnectRemoteService` | method | Close specific remote service connection |
| `(*Manager).Close` | method | Gracefully close all tunnels and services |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/traffic | `Recorder`, `Noop`, `WrapStream` |
| pkg/types | `PeerKey` |

## Consumed by
- pkg/node (uses: `Manager`, `New`, all methods)

## Proposed Minimal API

No changes — API surface already minimal. All 14 exports have a single production consumer (node).
