<p align="center">
  <img src="assets/mascot.svg" alt="Pollen" width="128"/>
</p>

# Pollen

<p align="center">
  <a href="https://github.com/sambigeara/pollen/actions/workflows/ci.yml"><img src="https://github.com/sambigeara/pollen/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/sambigeara/pollen"><img src="https://goreportcard.com/badge/github.com/sambigeara/pollen" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/sambigeara/pollen"><img src="https://pkg.go.dev/badge/github.com/sambigeara/pollen.svg" alt="Go Reference"></a>
  <a href="https://github.com/sambigeara/pollen/releases"><img src="https://img.shields.io/github/v/release/sambigeara/pollen" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/sambigeara/pollen" alt="License"></a>
</p>

Distributed WASM runtime. Workloads place themselves over a zero-trust mesh. One static binary.

![Pollen demo](assets/demo.gif)

*Zero to cluster to loadtest. Errors are nodes applying backpressure. Full video at [pln.sh](https://pln.sh).*

Pollen is a single Go binary. Install it on a few machines and you get a WASM runtime where workloads place themselves: nodes pick up replicas based on what they can host and where the traffic is, with no scheduler running anywhere. You also get a zero-trust mesh: `pln serve 8080 api` on one machine, `pln connect api` on another, mTLS end-to-end.

Pollen runs WASM, not containers. You can't `pln seed` a Postgres image. That constraint is the whole point: WASM is narrow enough that placement can be leaderless. If you need containers, k3s and Nomad are still where to go. If your workloads can compile to WASM (Go, Rust, JS, Python, C#, Zig via [Extism](https://extism.org)), try pln!

> [!NOTE]
Pollen runs end-to-end on real clusters today. That said, it's pre-1.0 and built by one person across a limited set of hardware. Expect breaking changes and rough edges. Please [raise an issue](https://github.com/sambigeara/pollen/issues) if you hit any snags.

## Highlights

- **WASM seeds.** `pln seed ./hello.wasm` here, `pln call hello greet`
  there; artifacts distribute peer-to-peer by hash. One host call
  invokes another seed by name (`pln://seed/<name>/<fn>`), so authz,
  routing, and policy can live inside WASM. Authored in Go, Rust, JS,
  Python, C#, Zig via
  [Extism](https://extism.org/docs/quickstart/plugin-quickstart).
- **Mesh services.** `pln serve 8080 api` here, `pln connect api`
  there (or `pln://service/<name>` from a seed). TCP and UDP,
  end-to-end mTLS.
- **Static sites & blobs.** `pln seed ./public` publishes a site;
  `pln seed ./file` shares a file. Same verb across workloads, sites,
  and blobs; kind is autodetected from what you point at.
  Content-addressed, gossiped, streamed peer-to-peer over QUIC.
- **Self-organising.** No scheduler, no leader, no coordinator.
  Topology, placement, and routing emerge from local state; calls go
  to the nearest, least-loaded replica, and replicas migrate toward
  demand.
- **CRDT-native.** A converging document on every node; changes
  gossip, conflicts resolve.
- **Partition-tolerant.** Both sides of a split keep running; state
  converges on rejoin; survivors rehost workloads from failed nodes.
- **QUIC transport.** One multiplexed, encrypted, UDP-based
  connection per peer carries gossip, services, and seeds.
  Connections punch direct between peers; otherwise they relay
  through any cluster node both peers can reach.
- **Cryptographic admission.** No shared secrets, no firewall rules.
  Every link is mTLS.
- **Edge-ready.** Pure Go, no CGO. Raspberry Pi to cloud host.
- **Ergonomic.** Opinionated defaults, opt-in configuration.

## Quickstart

### Install

```bash
curl -fsSL https://pln.sh/install.sh | bash
```

A thin wrapper around your platform's package manager (Homebrew on
macOS, apt or yum on Linux), so upgrades, uninstalls, and service files
are managed natively. On macOS, see the [FAQ](#faq) for a first-connect
permissions note.

### Two commands to a cluster

```bash
pln init                                # creates a new cluster rooted here
pln bootstrap ssh user@host [--admin]   # requires passwordless SSH + sudo
```

You have a zero-trust mesh, a peer-to-peer artifact store, and a WASM
runtime. Public nodes automatically become relays, so the mesh handles
NAT traversal without configuration. Pass `--admin` to delegate admin
authority to the new node, so your root machine doesn't need to stay
online.

### Add more nodes

**With SSH.** From any admin node:

```bash
pln bootstrap ssh user@host [--admin]

# Or pipe labelled targets from stdin or a file:
echo "media=alice@10.0.0.5" | pln bootstrap ssh -
```

Installs Pollen, enrols in the cluster, and starts. Linux targets only;
needs SSH as root or passwordless sudo. `--admin` delegates admin
authority; prefix a target with `name=` to label the node. Run
`pln bootstrap ssh --help` for the full flag set.

**Out-of-band.** Mint a token on an admin node, ship it to the joiner:

```bash
# Admin node:
pln invite [--subject foo]   # subject key can be retrieved with `pln id` on the subject node

# New node:
pln join <token>
```

The token is self-contained: signed admission credentials, the cluster's
root key, and every public relay address the cluster has organically
learned. Any public node you've bootstrapped is already acting as a
relay, and its address is woven into new invites automatically, so a
joiner behind NAT has a route in without you plumbing anything. Ship the
token over any channel; it's signed and valid until its TTL expires.

### Expose a service

```bash
# Machine A:
pln serve 8080 api

# Machine B:
pln connect api
curl localhost:8080           # served from A, over the mesh
```

TCP and UDP. Connections punch directly if both peers can reach each other,
and relay over the shortest mesh path otherwise. No ingress controller, no
DNS, no port forwarding.

### Run a seed

```bash
pln seed ./hello.wasm
pln call hello greet '{"name":"world"}'
```

`pln seed` publishes a WASM binary into the cluster. Nodes decide
*locally* whether to claim a replica, scoring themselves on available
capacity, cached artifacts, and proximity to traffic. There is no central
scheduler. When a node goes down, survivors pick up the slack.

Example modules live in [`examples/`](examples/). Run `pln --help` for
the full CLI reference. For the architecture, see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

### Grant capabilities

```bash
# Delegate admin authority to an existing peer; handy for keeping
# the mesh operable (admissions, cert re-issues, etc.) with the root
# node offline:
pln grant <peer-id> --admin

# Bake arbitrary key/value metadata into a peer's cert. Seeds see
# the caller's peer key and properties on every invocation, so auth,
# routing, and policy decisions can live inside the workload:
pln grant <peer-id> --prop role=lead --prop team=backend

# Or bake them in at join time:
pln invite --prop role=engineer --prop team=backend

# Pipe a JSON payload from a file:
cat props.json | pln grant <peer-id> --prop -

# Set the root node's own properties at init time (or later by
# editing `properties:` in config.yaml and restarting):
pln init --prop role=primary --prop region=eu
```

### Serve a static site

```bash
# On each node that should serve HTTP. Port is optional;
# defaults to :8080. `restart` to apply.
pln set static-http               # or `pln set static-http 9000`

# From any node:
pln seed ./public my-site

# Fetch via any serving node:
curl -H "Host: my-site" http://<node-addr>:8080/
```

`pln seed` on a directory hashes every file into the local
content-addressed store and publishes the site under `<name>`. Other
nodes replicate the files and serve the site themselves. Each node's
HTTP listener routes requests by `Host` header to the matching site.

### Share a blob

```bash
# From any node:
pln seed ./big-file.bin           # prints sha-256 digest
pln seed ./big-file.bin payload   # …or publish under a name

# From any other node:
pln fetch <digest|name>           # pulls peer-to-peer over QUIC into the local store
```

> Blobs are the primitive behind static sites: content-addressed,
> gossip-advertised, streamed peer-to-peer over QUIC. Receivers verify
> the digest on arrival.

## FAQ

- **macOS: `sendmsg: no route to host` on LAN dials**

  Most likely macOS Local Network Privacy. Grant `pln` access in
  **System Settings → Privacy & Security → Local Network**. The prompt
  appears the first time `pln` tries to reach a LAN peer; if you miss
  it, or the binary's signature changes after an upgrade, LAN dials
  silently fail while WAN traffic keeps working. Re-granting access
  fixes it.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
