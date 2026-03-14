# pkg/config

## Responsibilities
- Loads/saves YAML configuration file for node state
- Manages bootstrap peer list with address normalization
- Tracks local services and outbound connections
- Stores certificate TTL settings

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `DefaultBootstrapPort` | const | Default bootstrap port (60611) |
| `BootstrapPeer` | type | YAML-serialized bootstrap peer (PeerPub hex, Addrs list) |
| `CertTTLs` | type | Membership, delegation, TLS identity, and reconnect TTLs |
| `Service` | type | Service name and port binding |
| `Connection` | type | Service, peer, remote port, and local port mapping |
| `Config` | type | Root configuration struct |
| `Load` | func | Load config from YAML; returns empty if not exists |
| `Save` | func | Save config to YAML with header |
| `RememberBootstrapPeer` | func | Add bootstrap peer and canonicalize |
| `ForgetBootstrapPeer` | func | Remove bootstrap peer by public key |
| `BootstrapProtoPeers` | func | Convert bootstrap list to proto format |
| `NormalizeRelayAddr` | func | Normalize relay address with default port |
| `AddService` | func | Add or update service by name/port |
| `RemoveService` | func | Remove service by name |
| `RemoveServiceByPort` | func | Remove service by port |
| `AddConnection` | func | Add outbound connection tracking |
| `RemoveConnection` | func | Remove connections (wildcard matching) |
| `(CertTTLs).MembershipTTL` | method | Return membership TTL or default |
| `(CertTTLs).DelegationTTL` | method | Return delegation TTL or default |
| `(CertTTLs).TLSIdentityTTL` | method | Return TLS identity TTL or default |
| `(CertTTLs).ReconnectWindowDuration` | method | Return reconnect window or default |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/perm | `EnsureDir`, `WriteGroupWritable` |

## Consumed by
- cmd/pln (uses: `Load`, `Save`, `Config`, `RememberBootstrapPeer`, `ForgetBootstrapPeer`, `BootstrapProtoPeers`, `AddService`, `RemoveService`, `AddConnection`, `RemoveConnection`, TTL methods)

## Proposed Minimal API

### Exports kept

| Export | Consumers |
|--------|-----------|
| `Config`, `CertTTLs` | cmd/pln, mesh (invite.go) |
| `Load`, `Save` | cmd/pln |
| `DefaultBootstrapPort`, `NormalizeRelayAddr` | cmd/pln |
| `RememberBootstrapPeer`, `ForgetBootstrapPeer`, `BootstrapProtoPeers` | cmd/pln |
| `AddService`, `RemoveService`, `RemoveServiceByPort` | cmd/pln |
| `AddConnection`, `RemoveConnection` | cmd/pln |
| `CertTTLs` methods (`MembershipTTL`, `DelegationTTL`, `TLSIdentityTTL`, `ReconnectWindowDuration`) | cmd/pln |

### Exports stripped (3)

| Export | Action | Reason |
|--------|--------|--------|
| `BootstrapPeer` | unexported | Only accessed through `Config` fields and helper functions |
| `Service` | unexported | Internal config structure; consumers use `AddService`/`RemoveService` |
| `Connection` | unexported | Internal config structure; consumers use `AddConnection`/`RemoveConnection` |
