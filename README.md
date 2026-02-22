# Pollen

> **Work in progress.** This project is in early development — expect breaking
> changes, missing features, and rough edges. APIs and on-disk formats are not
> yet stable.

Pollen is an ergonomic peer-to-peer mesh runtime for workloads and service
exposure.

This README is intentionally short and focused on day-to-day commands.

## Core commands

- `pollen init` initialize local root cluster state
- `pollen up [--join <token>]` start/resume a node (`--join` does one-shot enroll+start)
- `pollen down` gracefully stop a running local node
- `pollen purge [--all]` reset local cluster state (`--all` also removes node keys)
- `pollen status` show nodes/services
- `pollen invite [--subject <node-pub>]` create an open or subject-bound invite token
- `pollen version [--short]` show CLI version/build metadata

## Install (linux + macOS)

Install latest release:

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash
```

Install a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash -s -- --version v0.2.0
```

Configure a per-user service runner (optional):

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash -s -- --with-service --start
```

Defaults and behavior:

- Installer supports `linux` and `darwin` on `amd64` and `arm64`
- Installs to `/usr/local/bin` when writable; otherwise `~/.local/bin`
- Checks release checksums before install
- Updates are semver-gated; potentially breaking upgrades require `--allow-breaking`
- Use `--dir <path>` to set the Pollen state directory used by service setup

## Quick start: laptop + public relay

On your laptop (admin):

```bash
pollen init
pollen up
pollen bootstrap ssh ubuntu@<RELAY_PUBLIC_IP>
```

If the bootstrap command prints a local join command, run it:

```bash
pollen up --join "<LOCAL_JOIN_TOKEN>"
```

## Add another node

Create an invite on admin:

```bash
pollen invite
```

Join from the other node:

```bash
pollen up --join "<INVITE_TOKEN>"
```

Subject-bound invite flow (stricter):

```bash
# on joining node
pollen id

# on admin node
pollen invite <NODE_PUB>
```

## Service ergonomics

```bash
pollen serve 8080 api
pollen connect api
pollen unserve api
```

## Notes

- Default state directory: `~/.pollen`
- Use `--dir` to run isolated test clusters
