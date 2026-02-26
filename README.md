# Pollen

> **Work in progress.** This project is in early development — expect breaking
> changes, missing features, and rough edges. APIs and on-disk formats are not
> yet stable.

Pollen is a zero-trust, ergonomic peer-to-peer mesh runtime for workloads and service
exposure.

This README is intentionally short and focused on day-to-day commands.

## Core commands

- `pollen init` — initialize local root cluster state
- `pollen up` — start a node in the foreground
- `pollen down` — gracefully stop a running local node
- `pollen join <token>` — enroll into a cluster and start the daemon (`--no-start` to enroll only)
- `pollen daemon start|stop|restart|status` — manage the background service
- `pollen logs [-f]` — show daemon logs
- `pollen purge [--all]` — reset local cluster state (`--all` also removes node keys)
- `pollen status` — show nodes/services
- `pollen invite [--subject <node-pub>]` — create an invite token
- `pollen serve <port> [name]` / `pollen unserve <port|name>` — expose/unexpose services
- `pollen connect <service> [provider]` — tunnel to a service
- `pollen version [--short]` — show CLI version

## Install (Linux + macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash
```

The install script is a thin wrapper around native package managers — Homebrew on
macOS, apt/yum on Linux — so upgrades, uninstalls, and service files are all
managed by your platform's package tooling.

## Quick start: local (root) + public relay

On your laptop (admin):

```bash
pollen init
pollen up
pollen bootstrap ssh ubuntu@<RELAY_PUBLIC_IP>
```

If the bootstrap command prints a local join command, run it:

```bash
pollen join "<LOCAL_JOIN_TOKEN>"
```

## Add another node

On the admin node, create an invite:

```bash
pollen invite
```

On the joining node, install and join:

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash
sudo pollen join "<INVITE_TOKEN>"  # Linux only; adds your user to the pollen group
```

`pollen join` enrolls credentials and starts the daemon automatically. Use
`--no-start` to enroll without starting.

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

- On macOS, state defaults to `~/.pollen`
- On Linux, the package installs a systemd service and defaults state to `/var/lib/pollen` when that directory exists
- On Linux, `pollen join` automatically adds your user to the `pollen` group so CLI commands (`pollen status`, `pollen peers`, etc.) work without `sudo`. Log out and back in after joining for group membership to take effect.
- Use `--dir` to run isolated test clusters
