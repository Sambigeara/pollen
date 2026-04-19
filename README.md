<p align="center">
  <img src="assets/mascot.svg" alt="Pollen" width="128"/>
</p>

# Pollen: a zero-trust, peer-to-peer, local-first runtime for services and WASM workloads, in a single static binary

<p align="center">
  <a href="https://github.com/sambigeara/pollen/actions/workflows/ci.yml"><img src="https://github.com/sambigeara/pollen/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/sambigeara/pollen"><img src="https://goreportcard.com/badge/github.com/sambigeara/pollen" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/sambigeara/pollen"><img src="https://pkg.go.dev/badge/github.com/sambigeara/pollen.svg" alt="Go Reference"></a>
  <a href="https://github.com/sambigeara/pollen/releases"><img src="https://img.shields.io/github/v/release/sambigeara/pollen" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/sambigeara/pollen" alt="License"></a>
</p>

_Pollen is in early development — expect breaking changes and sharp edges._

## Pollen is…

- **Ergonomic.** Opinionated defaults, opt-in configuration.
- **CRDT-native, local-first.** A converging document on every node.
  Changes gossip; conflicts resolve.
- **Self-organising.** Topology, placement, and routing emerge from
  local state. No scheduler, no leader, no coordinator.
- **Partition-tolerant.** Both sides of a split keep running. State
  converges on rejoin; survivors rehost workloads from failed nodes.
- **Built on QUIC.** Multiplexed streams, UDP-based for NAT traversal.
  One connection per peer carries gossip, services, and seeds.
- **Mesh services.** `pln serve 8080 api` here, `pln connect api` there.
  TCP and UDP.
- **WASM seeds.** Deploy a `.wasm` with `pln seed`; artifacts distribute
  peer-to-peer by hash.
- **Seeds compose.** WASM modules call each other with one host
  function. Plugins in [any Extism-compatible
  language](https://extism.org/docs/quickstart/plugin-quickstart).
- **Organic load balancing.** Calls go to the nearest, least-loaded
  replica; replicas migrate toward demand.
- **Zero-trust.** mTLS on every link. Admission is cryptographic, not
  shared secrets or firewall rules.
- **Edge-ready.** Pure Go, no CGO, one static binary. Raspberry Pi to
  cloud host. Call your Pi like a supercomputer.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash
```

A thin wrapper around your platform's package manager (Homebrew on
macOS, apt or yum on Linux), so upgrades, uninstalls, and service files
are managed natively.

## Two commands to a cluster

```bash
pln init                                # creates a new cluster rooted here
pln bootstrap ssh user@host [--admin]   # requires passwordless SSH + sudo
```

You have a zero-trust mesh, a peer-to-peer artifact store, and a WASM
runtime. Public nodes automatically become relays, so the mesh handles
NAT traversal without configuration. Pass `--admin` to delegate admin
authority to the new node, so your root machine doesn't need to stay
online.

## Add more nodes

**With SSH.** From any admin node:

```bash
pln bootstrap ssh user@host [--admin]
```

Installs Pollen, enrols in the cluster, and starts. Needs SSH as root or
passwordless sudo. `--admin` delegates admin authority.

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

## Expose a service

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

## Run a seed

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

## Project status

Pollen runs end-to-end on real clusters, but it is not yet what it's
trying to be. Expect breaking changes until formats and APIs stabilise.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
