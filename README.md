<p align="center">
  <img src="assets/mascot.svg" alt="Pollen" width="128"/>
</p>

# Pollen: a local-first, zero-trust, leaderless, emergent mesh and WASM runtime, in a single static binary

<p align="center">
  <a href="https://github.com/sambigeara/pollen/actions/workflows/ci.yml"><img src="https://github.com/sambigeara/pollen/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/sambigeara/pollen"><img src="https://goreportcard.com/badge/github.com/sambigeara/pollen" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/sambigeara/pollen"><img src="https://pkg.go.dev/badge/github.com/sambigeara/pollen.svg" alt="Go Reference"></a>
  <a href="https://github.com/sambigeara/pollen/releases"><img src="https://img.shields.io/github/v/release/sambigeara/pollen" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/sambigeara/pollen" alt="License"></a>
</p>

_Pollen is in early development — expect breaking changes and sharp edges._

## Highlights

- **Ergonomic.** Opinionated defaults, opt-in configuration.
- **CRDT-native.** A converging document on every node; changes
  gossip, conflicts resolve.
- **Self-organising.** No scheduler, no leader, no coordinator.
  Topology, placement, and routing emerge from local state; calls go
  to the nearest, least-loaded replica, and replicas migrate toward
  demand.
- **Partition-tolerant.** Both sides of a split keep running; state
  converges on rejoin; survivors rehost workloads from failed nodes.
- **Edge-ready.** Pure Go, no CGO. Raspberry Pi to cloud host.
- **Mesh services.** `pln serve 8080 api` here, `pln connect api`
  there. TCP and UDP, end-to-end mTLS.
- **WASM seeds.** Deploy with `pln seed`; artifacts distribute
  peer-to-peer by hash. Modules compose via one Extism host call,
  authored in [a variety of languages](https://extism.org/docs/quickstart/plugin-quickstart).
- **Static sites & blobs.** `pln seed ./public` publishes a site;
  `pln seed ./file` shares a file. Same verb across workloads, sites,
  and blobs — kind is autodetected from what you point at.
  Content-addressed, gossiped, streamed peer-to-peer over QUIC.
- **QUIC transport.** One multiplexed, encrypted, UDP-based
  connection per peer carries gossip, services, and seeds. NAT
  traversal built in.
- **Cryptographic admission.** No shared secrets, no firewall rules —
  every link is mTLS.

## Quickstart

### Install

```bash
curl -fsSL https://pln.sh/install.sh | bash
```

A thin wrapper around your platform's package manager (Homebrew on
macOS, apt or yum on Linux), so upgrades, uninstalls, and service files
are managed natively.

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
authority; prefix a target with `name=` to label the node.

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
# Delegate admin authority to an existing peer — handy for keeping
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

Blobs are the primitive behind static sites — content-addressed,
gossip-advertised, streamed peer-to-peer over QUIC. Receivers verify
the digest on arrival.

## Project status

Pollen runs end-to-end on real clusters, but it is not yet what it's
trying to be. Expect breaking changes until formats and APIs stabilise.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
