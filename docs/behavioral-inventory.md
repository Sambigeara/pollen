# Behavioral Inventory — Boundary Refactor Baseline

This document captures every observable behavior that must be preserved
throughout the boundary refactor. Each phase gate checks against this
inventory.

---

## 1. CLI Command Surface

Default state directory: `~/.pln` (env `PLN_DIR`).
All commands accept `--dir string` global flag.

### Root

```
pln [command]
```

Commands: `bootstrap`, `call`, `completion`, `connect`, `deny`, `disconnect`,
`down`, `help`, `id`, `init`, `invite`, `join`, `logs`, `purge`, `restart`,
`seed`, `serve`, `status`, `unseed`, `unserve`, `up`, `upgrade`, `version`.

### `pln init`

```
Initialize local root cluster state
Usage: pln init [flags]
Flags: -h, --help
```

### `pln up`

```
Start a Pollen node (foreground by default, -d for background service)
Usage: pln up [flags]
Flags:
  -d, --detach        Run as a background service
      --ips ipSlice   Advertised IPs (default [])
      --metrics       Log metrics and trace output at debug level
      --port int      Listening port (default 60611)
      --public        Mark this node as publicly accessible (relay)
```

### `pln down`

```
Stop the background service
Usage: pln down [flags]
Flags: -h, --help
```

### `pln join`

```
Join a cluster using a join or invite token
Usage: pln join <token> [flags]
Flags:
      --no-up    Enroll credentials without starting the daemon
      --public   Mark this node as publicly accessible (relay)
```

### `pln bootstrap`

```
Bootstrap relays and joiners
Usage: pln bootstrap [command]
Subcommands: ssh
```

#### `pln bootstrap ssh`

```
Bootstrap a relay over SSH
Usage: pln bootstrap ssh <host> [flags]
Flags:
      --expire-after duration   Hard access expiry for the relay peer (e.g. 24h)
      --relay-port int          Relay UDP port to advertise (default 60611)
```

### `pln invite`

```
Generate an invite token (open or subject-bound)
Usage: pln invite [subject-pub] [flags]
Flags:
      --bootstrap stringArray   Bootstrap peer as <peer-pub-hex>@<host:port> (repeatable)
      --expire-after duration   Hard access expiry for the invited peer (e.g. 24h)
      --subject string          Optional hex node public key to bind invite
      --ttl duration            Invite token validity duration (default 5m0s)
```

### `pln serve`

```
Expose a local port to the mesh
Usage: pln serve <port> [name] [flags]
Flags: -h, --help
```

### `pln unserve`

```
Stop exposing a local service
Usage: pln unserve <port|name> [flags]
Flags: -h, --help
```

### `pln connect`

```
Tunnel a local port to a service.
If multiple providers serve the same name, use the suffixed form shown by
"pln status" (e.g. "pln connect http-a").
Usage: pln connect <service> [provider] [local-port] [flags]
Flags: -h, --help
```

### `pln disconnect`

```
Close a tunnel to a service.
When the argument is a number it matches by local port. Otherwise it matches
by service name, and an optional provider argument filters by provider.
If multiple providers serve the same name, use the suffixed form shown by
"pln status" (e.g. "pln disconnect http-a").
Usage: pln disconnect <service|local-port> [provider] [flags]
Flags: -h, --help
```

### `pln status`

```
Show status
Usage: pln status [nodes|services|seeds] [flags]
Flags:
      --all    Include offline nodes and services
      --wide   Show full peer IDs and extra details
```

### `pln seed`

```
Deploy a WASM workload to the cluster
Usage: pln seed <file.wasm> [flags]
Flags:
      --memory-pages uint32   memory limit in 64 KiB pages (0 = default 256 = 16 MiB)
      --replicas uint32       number of replicas to run across the cluster (default 1)
      --timeout-ms uint32     per-invocation timeout in milliseconds (0 = default 30s)
```

### `pln unseed`

```
Stop a running WASM workload
Usage: pln unseed <hash-prefix> [flags]
Flags: -h, --help
```

### `pln call`

```
Invoke an exported function on a WASM workload
Usage: pln call <hash-or-prefix> <function> [flags]
Flags:
      --input string        input string to pass to the function
      --input-file string   path to a file whose contents are passed as input
```

### `pln deny`

```
Deny a peer's membership
Usage: pln deny <peer-id> [flags]
Flags: -h, --help
```

### `pln logs`

```
Show daemon logs
Usage: pln logs [flags]
Flags:
  -f, --follow      Stream logs in real time
  -n, --lines int   Number of lines to show (default 50)
```

### `pln admin`

```
Manage admin keys
Usage: (no documented subcommands in help)
```

### `pln id`

```
Show local node identity public key
Usage: pln id [flags]
Flags: -h, --help
```

### `pln purge`

```
Delete local cluster state
Usage: pln purge [flags]
Flags:
      --all    Also delete local node identity keys
      --yes    Skip interactive confirmation
```

### `pln version`

```
Show Pollen version information
Usage: pln version [flags]
Flags:
      --short   Print version only
```

### `pln restart`

```
Restart the background service
Usage: pln restart [flags]
Flags: -h, --help
```

### `pln upgrade`

```
Upgrade pln to the latest version
Usage: pln upgrade [flags]
Flags:
      --restart   Restart the background service after upgrading
```

---

## 2. Status Output Format

Library: `github.com/charmbracelet/lipgloss` + `lipgloss/table`

### Health line

Rendered first when metrics RPC succeeds. Bold, colored by status:

| Health enum                    | Label       | ANSI color |
|-------------------------------|-------------|------------|
| HEALTH_STATUS_HEALTHY          | HEALTHY     | 2 (green)  |
| HEALTH_STATUS_DEGRADED         | DEGRADED    | 3 (yellow) |
| HEALTH_STATUS_UNHEALTHY        | UNHEALTHY   | 1 (red)    |
| HEALTH_STATUS_UNSPECIFIED      | (omitted)   | -          |

### Mode selectors

`pln status [nodes|services|seeds]` — "all" shows all three when data present.
Aliases: `node` = `nodes`, `service`/`serve` = `services`, `seed` = `seeds`.

### PEERS section

Title: `PEERS` (bold, ANSI color 4/blue).

Headers (ANSI color 245/gray, padding-right 2):

| Column       | Source field                  | Format                                 |
|-------------|-------------------------------|-----------------------------------------|
| NODE        | peer_id                       | 8-char hex prefix (full with `--wide`)  |
| STATUS      | NodeStatus enum               | `direct`, `indirect`, `relay`, `offline`|
| ADDR        | addr                          | literal or `-`                          |
| CPUs        | num_cpu                       | integer or `-`                          |
| CPU         | cpu_percent                   | `N%` or `-`                             |
| MEM         | mem_percent                   | `N%` or `-`                             |
| TUNNELS     | tunnel_count                  | integer or `-`                          |
| LATENCY     | latency_ms                    | `N.Nms` or `-`                          |
| TRAFFIC IN  | traffic_bytes_in              | `N B`/`N.N KB`/`N.N MB`/`N.N GB` or `-`|
| TRAFFIC OUT | traffic_bytes_out             | same format                             |

Self row: appends ` (self)` to NODE. Status is `-` or `- (public)`.
Public peers: status gets ` (public)` suffix.
Offline peers hidden unless `--all`; footer: `offline peers: N (use --all)`.

### SERVICES section

Title: `SERVICES` (bold, ANSI color 4/blue).

Headers:

| Column   | Source field | Format                                  |
|----------|-------------|-----------------------------------------|
| SERVICE  | name        | plain, or `name-<suffix>` for collisions|
| PROVIDER | peer_id     | 8-char hex prefix (full with `--wide`)  |
| PORT     | port        | integer                                 |
| LOCAL    | local_port  | integer or `-`                          |

When multiple providers share a name, minimal unique prefixes of provider IDs
are appended as suffixes (e.g., `http-a`, `http-b`). Footer notes:
`service suffixes match the start of the provider ID`.
Offline services hidden unless `--all`; footer: `offline services: N (use --all)`.

### SEEDS section

Title: `SEEDS` (bold, ANSI color 4/blue).

Headers:

| Column   | Source field      | Format                          |
|----------|-------------------|---------------------------------|
| HASH     | hash              | 16-char prefix (full with `--wide`)|
| STATUS   | WorkloadStatus    | `running`, `stopped`, `errored`, `unknown`|
| REPLICAS | active/desired    | `N/M`                           |
| LOCAL    | local             | `*` if local, empty otherwise   |
| UPTIME   | started_at_unix   | humanDuration or `-`            |

### DIAGNOSTICS section (--wide only)

Title: `DIAGNOSTICS` (bold, ANSI color 4/blue). Printed when `--wide` and
metrics RPC succeeds.

```
  vivaldi error:    0.NNN
  vivaldi samples:  N
  eager syncs:      N ok, N failed
  punch success:    N/N         (only if punch_attempts > 0)
  cert renewals:    N ok, N failed  (only if cert_renewals > 0 or cert_renewals_failed > 0)
```

### Certificate expiry footer

Colored line at bottom. Logic:

- **Expired + has access deadline**: red (1) — `temporary access expired — rejoin the cluster or contact a cluster admin`
- **Expired, no deadline**: red (1) — `membership expired — entering degraded mode; will auto-recover when an admin peer is reachable`
- **EXPIRING_SOON**: yellow (3) — `membership expires in <dur> — auto-renewal failed — rejoin the cluster or contact a cluster admin`
- **RENEWING**: yellow (3) — `membership expires in <dur> — auto-renewal in progress`
- **OK / default**: green (2) — `membership expires in <dur>`
- **Has access deadline**: replaces `membership expires` with `temporary access expires`

### humanDuration format

| Duration range    | Format                  | Example       |
|-------------------|-------------------------|---------------|
| < 1m              | `<1m`                   | `<1m`         |
| < 1h              | `Nm`                    | `42m`         |
| < 24h             | `Nh Nm` or `Nh`        | `3h 22m`      |
| < 7d              | `Nd Nh` or `Nd`        | `2d 5h`       |
| < 365d            | `Nd`                    | `45d`         |
| >= 365d           | `Ny Nd` or `Ny`        | `1y 30d`      |

### Table rendering

- Border: `lipgloss.HiddenBorder()` (no visible borders)
- Header style: color 245, padding-right 2
- Data style: padding-right 2
- Section title: bold, color 4

---

## 3. Logging Inventory

Logger: `go.uber.org/zap` (SugaredLogger). Named `node`, sub-named as
`node.metrics`, `node.traces`.

### Startup phase

| Level | Message | Fields |
|-------|---------|--------|
| Info | `advertised IPs changed` | `ips` |
| Warn | `bootstrap peer: resolve address failed` | `addr`, `err` |
| Warn | `bootstrap peer: connect failed` | `peer`, `err` |

### Peer connect/disconnect

| Level | Message | Fields |
|-------|---------|--------|
| Info | `peer connected` | `peer_id`, `ip`, `observedPort` |
| Info | `deny event received via gossip, disconnecting peer` | `peer` |
| Info | `removing stale forward` | `peer`, `port` |
| Warn | `connect failed: peer identity mismatch` | `peer`, `err` |
| Debug | `connect failed` | `peer`, `err` |
| Debug | `failed restoring desired connection` | `peer`, `remotePort`, `localPort`, `err` |
| Warn | `disconnecting peer with expired delegation cert beyond reconnect window` | `peer`, `expired_at` |

### Gossip / clock sync

| Level | Message | Fields |
|-------|---------|--------|
| Debug | `recv failed` | `err` |
| Debug | `accept clock stream failed` | `err` |
| Debug | `read clock stream failed` | `peer`, `err` |
| Warn | `clock stream exceeded size limit` | `peer`, `size` |
| Debug | `unmarshal clock stream failed` | `peer`, `err` |
| Debug | `marshal clock response failed` | `peer`, `err` |
| Debug | `write clock response failed` | `peer`, `err` |
| Debug | `unknown datagram type` | `peer` |
| Debug | `event gossip send failed` | `peer`, `err` |
| Debug | `clock gossip send failed` | `peer`, `err` |
| Debug | `eager sync failed` | `peer`, `err` |

### NAT / punch

| Level | Message | Fields |
|-------|---------|--------|
| Info | `NAT type detected` | `type` |
| Info | `punch coord trigger received` | `peer`, `peerAddr` |
| Debug | `no coordinators available for punch` | `peer` |
| Debug | `punch coord request send failed` | `coordinator`, `err` |
| Debug | `punch coord: missing address` | `from`, `fromOk`, `target`, `targetOk` |
| Debug | `punch coord trigger send failed` | `to`, `err` |
| Debug | `punch failed` | `peer`, `err` |
| Warn | `punch failed: peer identity mismatch` | `peer`, `err` |
| Debug | `punch queue full, dropping` | `peer` |
| Debug | `punch coord trigger: bad peer addr` | `addr`, `err` |

### Observed address

| Level | Message | Fields |
|-------|---------|--------|
| Debug | `observed address received` | `addr`, `from` |
| Debug | `observed address: parse failed` | `addr`, `err` |
| Debug | `failed sending observed address` | `peer`, `err` |

### IP refresh

| Level | Message | Fields |
|-------|---------|--------|
| Debug | `ip refresh failed` | `err` |
| Info | `advertised IPs changed` | `ips` |

### Certificate renewal

| Level | Message | Fields |
|-------|---------|--------|
| Info | `renewing delegation certificate` | |
| Info | `delegation certificate renewed` | `expires_at` |
| Info | `delegation certificate approaching expiry — auto-renewal attempted but failed, will retry` | `expires_in` |
| Warn | `delegation certificate expired — running in degraded mode, will keep retrying renewal` | `expired_at`, `reconnect_until` |
| Warn | `delegation certificate expiring soon — auto-renewal failed — rejoin the cluster or contact a cluster admin` | `expires_in` |
| Warn | `delegation certificate renewal failed: no connected peers` | |
| Warn | `delegation certificate renewal failed: invalid cert` | `peer`, `err` |
| Warn | `delegation certificate renewal failed: all peers refused or returned errors` | |
| Warn | `failed to persist renewed credentials` | `err` |
| Error | `delegation certificate expired beyond reconnect window, shutting down — rejoin the cluster or contact a cluster admin` | `expired_at` |
| Debug | `delegation certificate renewal failed` | `peer`, `err` |

### Workloads

| Level | Message | Fields |
|-------|---------|--------|
| Debug | `workload stream failed, trying next claimant` | `target`, `hash`, `err` |
| Warn | `workload invocation failed, trying next claimant` | `target`, `hash`, `err` |

### State persistence

| Level | Message | Fields |
|-------|---------|--------|
| Info | `state saved to disk` | |
| Warn | `periodic state save failed` | `err` |
| Warn | `failed to save state after deny` | `err` |
| Error | `failed to save state` | `err` |
| Error | `failed to close state store` | `err` |

### Shutdown

| Level | Message | Fields |
|-------|---------|--------|
| Info | `successfully shut down mesh` | |
| Debug | `successfully shutdown Node` | |
| Error | `failed to broadcast disconnect` | `err` |
| Error | `failed to shut down mesh` | `err` |

### Mesh layer (pkg/mesh/mesh.go)

| Level | Message | Fields |
|-------|---------|--------|
| Warn | `dropped peer disconnect event after send failure` | `peer`, `reason` |
| Warn | `dropped connect event, consumer lagging` | `peer` |
| Warn | `dropped disconnect event, consumer lagging` | `peer` |
| Warn | `dropped cert rotation disconnect event` | `peer` |
| Debug | `reconnecting peer to refresh certificates` | `peer`, `age` |
| Debug | `peer session died` | `peer`, `reason` |
| Debug | `rejected invite` | `peer`, `err` |
| Debug | `routed stream TTL exhausted` | `dest`, `source` |
| Debug | `no route to forward` | `dest` |
| Debug | `routing loop detected` | `dest`, `source` |
| Debug | `no session to next hop` | `nextHop` |
| Debug | `open outbound routed stream failed` | `nextHop`, `err` |

### Service layer (pkg/node/svc.go)

| Level | Message | Fields |
|-------|---------|--------|
| Info | `spec rejected` | `hash`, `err` |
| Warn | `skipping unparseable peer IP` | `peer`, `ip` |
| Warn | `connect peer failed` | `peer`, `err` |
| Warn | `connect service failed` | `err` |
| Warn | `disconnect service failed` | `err` |
| Warn | `seed workload failed` | `err` |
| Error | `seed workload failed` | `err` |
| Warn | `unseed workload failed` | `hash`, `err` |
| Warn | `call workload failed` | `hash`, `function`, `err` |

### Scheduler (pkg/scheduler/reconciler.go)

| Level | Message | Fields |
|-------|---------|--------|
| Info | `claimed workload` | `hash` |
| Info | `released workload` | `hash` |
| Info | `spec removed during fetch, skipping claim` | `hash` |
| Info | `no longer a winner after fetch, skipping claim` | `hash` |
| Info | `cleaned up stale claim` | `hash` |
| Warn | `fetch artifact failed` | `hash`, `err` |
| Warn | `seed from CAS failed` | `hash`, `err` |
| Warn | `unseed failed` | `hash`, `err` |

---

## 4. Metrics Inventory

Metrics library: `pkg/observability/metrics` (custom, opt-in, atomic counters/gauges).
Flush sink: `LogSink` (writes to zap at debug level).
Flush interval: 10s default.
Instrument types: `Counter` (monotonic int64), `Gauge` (float64), `EWMA` (lock-free).

### Mesh metrics

| Name | Type | Labels |
|------|------|--------|
| `pollen_mesh_datagrams_sent_total` | Counter | - |
| `pollen_mesh_datagrams_recv_total` | Counter | - |
| `pollen_mesh_datagram_bytes_sent_total` | Counter | - |
| `pollen_mesh_datagram_bytes_recv_total` | Counter | - |
| `pollen_mesh_datagram_errors_total` | Counter | - |
| `pollen_mesh_session_connects_total` | Counter | - |
| `pollen_mesh_session_disconnects_total` | Counter | - |
| `pollen_mesh_sessions_active` | Gauge | - |

### Peer metrics

| Name | Type | Labels |
|------|------|--------|
| `pollen_peer_connections_total` | Counter | - |
| `pollen_peer_disconnects_total` | Counter | - |
| `pollen_peers_discovered` | Gauge | - |
| `pollen_peers_connecting` | Gauge | - |
| `pollen_peers_connected` | Gauge | - |
| `pollen_peers_unreachable` | Gauge | - |
| `pollen_peer_stage_escalations_total` | Counter | - |
| `pollen_peer_state_transitions_total` | Counter | - |

### Gossip metrics

| Name | Type | Labels |
|------|------|--------|
| `pollen_gossip_events_received_total` | Counter | - |
| `pollen_gossip_events_applied_total` | Counter | - |
| `pollen_gossip_events_stale_total` | Counter | - |
| `pollen_gossip_self_conflicts_total` | Counter | - |
| `pollen_gossip_revocations_total` | Counter | - |
| `pollen_gossip_batch_size` | Gauge | - |

Gossip also tracks `StaleRatio` as an EWMA (alpha = 0.01, ~100-event window),
not a registered metric name.

### Topology metrics

| Name | Type | Labels |
|------|------|--------|
| `pollen_topology_vivaldi_error` | Gauge | - |
| `pollen_topology_hmac_nearest_enabled` | Gauge | - |
| `pollen_topology_prunes_total` | Counter | - |

### Node metrics

| Name | Type | Labels |
|------|------|--------|
| `pollen_node_cert_expiry_seconds` | Gauge | - |
| `pollen_node_cert_renewals_total` | Counter | - |
| `pollen_node_cert_renewals_failed_total` | Counter | - |
| `pollen_node_punch_attempts_total` | Counter | - |
| `pollen_node_punch_failures_total` | Counter | - |

### Always-on diagnostics (not gated by MetricsEnabled)

These atomic counters live on `Node` directly, exposed via the control RPC:

- `vivaldiSamples` (int64)
- `eagerSyncs` (int64)
- `eagerSyncFailures` (int64)

---

## 5. Wire Format Constants

### Stream type bytes (pkg/mesh/mesh.go)

| Byte | Stream type      | Purpose                          |
|------|------------------|----------------------------------|
| 1    | `streamTypeClock`    | Clock/digest exchange            |
| 2    | `streamTypeTunnel`   | Service tunnel forwarding        |
| 3    | `streamTypeRouted`   | Multi-hop routed stream          |
| 4    | `streamTypeArtifact` | CAS artifact transfer            |
| 5    | `streamTypeWorkload` | Workload invocation              |

### Routed stream header

66 bytes: 32 dest + 32 source + 1 TTL + 1 inner stream type.
Default TTL: 16. Accepted inner stream types at delivery: tunnel (2),
artifact (4), workload (5).

### QUIC config

| Parameter | Value |
|-----------|-------|
| Max idle timeout | 30s |
| Keep-alive period | 10s |
| Datagrams enabled | true |
| Max incoming uni streams | -1 (disabled) |
| Max incoming bidi streams | 256 |
| Max datagram payload | 1100 bytes |

### Proto files

| Path | Package |
|------|---------|
| `api/public/pollen/admission/v1/admission.proto` | `pollen.admission.v1` |
| `api/public/pollen/control/v1/control.proto` | `pollen.control.v1` |
| `api/public/pollen/mesh/v1/mesh.proto` | `pollen.mesh.v1` |
| `api/public/pollen/state/v1/state.proto` | `pollen.state.v1` |

### Datagram envelope (meshv1.Envelope)

Oneof body variants:

1. `PunchCoordRequest`
2. `PunchCoordTrigger`
3. `InviteRedeemRequest`
4. `InviteRedeemResponse`
5. `ObservedAddress`
6. `GossipEventBatch` (events)
7. `CertRenewalRequest`
8. `CertRenewalResponse`

Plus `trace_id` (bytes, field 9).

### Gossip event oneof (statev1.GossipEvent)

Field numbers and change types:

| Field # | Name | Type |
|---------|------|------|
| 3 | network | NetworkChange |
| 4 | external_port | ExternalPortChange |
| 5 | identity_pub | IdentityChange |
| 6 | service | ServiceChange |
| 7 | reachability | ReachabilityChange |
| 8 | observed_external_ip | ObservedExternalIPChange |
| 9 | publicly_accessible | PubliclyAccessibleChange |
| 10 | vivaldi | VivaldiCoordinateChange |
| 11 | nat_type | NatTypeChange |
| 12 | resource_telemetry | ResourceTelemetryChange |
| 13 | deny | DenyChange |
| 14 | deleted | bool (shared, on parent) |
| 15 | workload_spec | WorkloadSpecChange |
| 16 | workload_claim | WorkloadClaimChange |
| 17 | traffic_heatmap | TrafficHeatmapChange |

---

## 6. On-Disk Format

Default directory: `~/.pln` (macOS/dev) or `/var/lib/pln` (Linux production).

### Directory layout

```
<pollenDir>/
  config.yaml            # YAML config, group-writable (0660)
  state.pb               # Protobuf RuntimeState, group-readable (0640)
  .state.lock            # flock(2) exclusive lock, private (0600)
  pln.sock               # Unix domain socket for control RPC (0660)
  keys/
    ed25519.key          # Node identity private key (PEM, 0640)
    ed25519.pub          # Node identity public key (PEM, 0640)
    admin_ed25519.key    # Cluster admin private key (PEM, 0640)
    admin_ed25519.pub    # Cluster admin public key (PEM, 0640)
    cluster.trust.pb     # TrustBundle protobuf (0640)
    delegation.cert.pb   # DelegationCert protobuf (0640)
  cas/
    <sha256hex[0:2]>/
      <sha256hex>.wasm   # WASM artifacts (content-addressed)
```

### state.pb schema (statev1.RuntimeState)

```protobuf
message RuntimeState {
  repeated PeerState peers = 1;
  repeated ConsumedInvite consumed_invites = 2;
  repeated bytes denied_peers = 3;
  repeated WorkloadSpecChange workload_specs = 4;
}
```

Serialized with `MarshalVT`/`UnmarshalVT` (vtprotobuf).

### config.yaml schema

```yaml
advertiseIPs: []        # optional
bootstrapPeers:         # list of {peerPub: hex, addrs: [host:port]}
services:               # list of {name: string, port: uint32}
connections:            # list of {service, peer, remotePort, localPort}
certTTLs:              # {membership, delegation, tlsIdentity, reconnectWindow}
port: 0                # optional
public: false          # optional
```

Header: `# Manual edits while the daemon runs will be overwritten.\n# Use 'pln serve', 'pln connect', 'pln disconnect', 'pln seed', and 'pln unseed' to manage services.\n`

### Default cert TTLs

| TTL | Default |
|-----|---------|
| Membership | 4h |
| Delegation | 30d |
| TLS Identity | 4h |
| Reconnect Window | 7d |

### Key file formats

- `ed25519.key`: PEM block, type `ED25519 PRIVATE KEY`
- `ed25519.pub`: PEM block, type `ED25519 PUBLIC KEY`
- `admin_ed25519.key`: PEM block, type `POLLEN ADMIN ED25519 PRIVATE KEY`
- `admin_ed25519.pub`: PEM block, type `POLLEN ADMIN ED25519 PUBLIC KEY`

### Permission matrix (Linux, /var/lib/pln)

| Path | Owner:Group | Mode | Set by |
|------|-------------|------|--------|
| `/var/lib/pln/` | pln:pln | 0770 | postinstall.sh |
| `keys/` | pln:pln | 0770 | EnsureDir |
| `keys/ed25519.key` | pln:pln | 0640 | SetGroupReadable |
| `keys/ed25519.pub` | pln:pln | 0640 | SetGroupReadable |
| `keys/admin_ed25519.key` | pln:pln | 0640 | SetGroupReadable |
| `keys/admin_ed25519.pub` | pln:pln | 0640 | SetGroupReadable |
| `keys/cluster.trust.pb` | pln:pln | 0640 | WriteGroupReadable |
| `keys/delegation.cert.pb` | pln:pln | 0640 | WriteGroupReadable |
| `config.yaml` | pln:pln | 0660 | WriteGroupWritable |
| `state.pb` | pln:pln | 0640 | WriteGroupReadable |
| `.state.lock` | pln:pln | 0600 | SetPrivate |
| `pln.sock` | pln:pln | 0660 | SetGroupSocket |

### Migration paths

- `state.yaml` to `state.pb`: auto-migrated on startup, original renamed to `.bak`
- `consumed_invites.json` into `state.pb`: merged and JSON file deleted

### Timing constants (node)

| Constant | Value |
|----------|-------|
| certCheckInterval | 5m |
| certWarnThreshold | 1h |
| certCriticalThreshold | 15m |
| certRenewalTimeout | 10s |
| expirySweepInterval | 30s |
| directTimeout | 2s |
| punchTimeout | 3s |
| eagerSyncCooldown | 5s |
| eagerSyncTimeout | 5s |
| gossipStreamTimeout | 5s |
| workloadInvocationTimeout | 60s |
| revokeStreakThreshold | 3 |
| revokeStreakThresholdPublic | 30 |
| vivaldiEnterHMACThreshold | 0.6 |
| vivaldiExitHMACThreshold | 0.35 |
| vivaldiWarmupDuration | 5s |
| routeDebounceInterval | 100ms |
| maxRouteDelay | 1s |
| loopIntervalJitter | 0.1 |
| peerEventBufSize | 64 |
| gossipEventBufSize | 64 |
| punchChBufSize | 32 |
| punchWorkers | 3 |
| ipRefreshInterval | 5m |
| stateSaveInterval | 30s |

### Timing constants (mesh)

| Constant | Value |
|----------|-------|
| handshakeTimeout | 3s |
| quicIdleTimeout | 30s |
| quicKeepAlivePeriod | 10s |
| eventSendTimeout | 5s |
| maxBidiStreams | 256 |
| probeBufSize | 2048 |
| inviteRedeemTTL | 5m |
| sessionReapInterval | 5m |
| streamTypeTimeout | 5s |

### Timing constants (scheduler)

| Constant | Value |
|----------|-------|
| debounceInterval | 200ms |
| maxDebounceDelay | 2s |
| evictionCooldown | 30s |
| trafficDebounceInterval | 10s |
| maxTrafficDebounceDelay | 30s |
| minResidencyDuration | 10s |

---

## 7. Control RPC Surface

Service: `pollen.control.v1.ControlService` (ConnectRPC over Unix socket `pln.sock`).

| RPC | Request | Response |
|-----|---------|----------|
| Shutdown | `ShutdownRequest` | `ShutdownResponse` |
| GetBootstrapInfo | `GetBootstrapInfoRequest` | `GetBootstrapInfoResponse` |
| GetStatus | `GetStatusRequest` | `GetStatusResponse` |
| GetMetrics | `GetMetricsRequest` | `GetMetricsResponse` |
| RegisterService | `RegisterServiceRequest` | `RegisterServiceResponse` |
| UnregisterService | `UnregisterServiceRequest` | `UnregisterServiceResponse` |
| ConnectService | `ConnectServiceRequest` | `ConnectServiceResponse` |
| ConnectPeer | `ConnectPeerRequest` | `ConnectPeerResponse` |
| DisconnectService | `DisconnectServiceRequest` | `DisconnectServiceResponse` |
| DenyPeer | `DenyPeerRequest` | `DenyPeerResponse` |
| SeedWorkload | `SeedWorkloadRequest` | `SeedWorkloadResponse` |
| UnseedWorkload | `UnseedWorkloadRequest` | `UnseedWorkloadResponse` |
| CallWorkload | `CallWorkloadRequest` | `CallWorkloadResponse` |
