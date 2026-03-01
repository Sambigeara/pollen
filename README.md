# Pollen

> **Work in progress.** This project is in early development — expect breaking
> changes, missing features, and rough edges. APIs and on-disk formats are not
> yet stable.

Pollen is a zero-trust, ergonomic peer-to-peer mesh runtime for workloads and service
exposure.

This README is intentionally short and focused on day-to-day commands.

## Core commands

- `pln init` — initialize local root cluster state
- `pln up` — start a node in the foreground
- `pln start|stop|restart` — manage the background service
- `pln join <token>` — enroll into a cluster and start the daemon (`--no-start` to enroll only)
- `pln logs [-f]` — show daemon logs
- `pln purge [--all]` — reset local cluster state (`--all` also removes node keys)
- `pln status` — show nodes/services
- `pln invite [--subject <node-pub>]` — create an invite token
- `pln serve <port> [name]` / `pln unserve <port|name>` — expose/unexpose services
- `pln connect <service> [provider]` — tunnel to a service
- `pln version [--short]` — show CLI version

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
pln init
pln up
pln bootstrap ssh ubuntu@<RELAY_PUBLIC_IP>
```

If the bootstrap command prints a local join command, run it:

```bash
pln join "<LOCAL_JOIN_TOKEN>"
```

## Add another node

On the admin node, create an invite:

```bash
pln invite
```

On the joining node, install and join:

```bash
curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash
sudo pln join "<INVITE_TOKEN>"  # Linux only; adds your user to the pln group
```

`pln join` enrolls credentials and starts the daemon automatically. Use
`--no-start` to enroll without starting.

Subject-bound invite flow (stricter):

```bash
# on joining node
pln id

# on admin node
pln invite <NODE_PUB>
```

## Service ergonomics

```bash
pln serve 8080 api
pln connect api
pln unserve api
```

## Notes

- On macOS, state defaults to `~/.pln`
- On Linux, the package installs a systemd service and defaults state to `/var/lib/pln` when that directory exists
- On Linux, `pln join` automatically adds your user to the `pln` group so CLI commands (`pln status`, `pln peers`, etc.) work without `sudo`. Log out and back in after joining for group membership to take effect.
- Use `--dir` to run isolated test clusters
