# Pollen Demo · Cue Card

Teleprompter for the asciinema-style demo. Keep this file open on the second
screen. Primary screen: left-half terminal + right-half Grafana
**Pollen · Demo** dashboard, full-screen.

All commands are run from the repo root. The demo lives in a dedicated `demo`
pln context bound to `/tmp/pln-demo`, so the narrative never touches your
personal default context. `pln` commands operate on that context via `pln ctx
use demo`; `just` targets are invoked against `internal/dev/justfile`.

---

## Before recording

### One-time
```fish
just internal/dev/build-demo    # exerciser binary + WASM modules + world map
just internal/dev/deploy-demo   # terraform + root SSH + exerciser host (idempotent)
```

### Each time, before hitting record
```fish
# 1. Tear down any previous demo: context gone (also unloads launchd plist
#    on macOS, which stops the demo daemon if running), then wipe state.
pln ctx rm demo 2>/dev/null || true
rm -rf /tmp/pln-demo

# 2. Fresh demo context bound to /tmp/pln-demo, and make it active.
mkdir -p /tmp/pln-demo
pln ctx add demo /tmp/pln-demo
pln ctx use demo

# 3. Bind Prometheus' scrape port on the local root.
pln set http :9090

# 4. Bring up grafana + prometheus and regenerate the file-SD targets
#    from the current terraform state.
just internal/dev/start-observability

# 5. Snapshot the peer list we'll pipe into bootstrap.
cd internal/dev/demo-cluster
terraform output -json node_ips \
  | jq -r 'to_entries | sort_by(.key) | .[] | "\(.key)=root@\(.value)"' \
  > ~/bootstrap_peers.txt
cd -
```

- Grafana: http://localhost:3000 → **Pollen · Demo** → fullscreen, time range **Last 5 minutes**, refresh **2s**.
- Starting view: 10 grey dots on the map, no workloads, replica/rate panels empty.

---

## Beat 1 · Init the root

```fish
# Create cluster credentials on this laptop — the root trust anchor.
pln init
```

> Pollen starts with a single root. This laptop is now a cluster of one. Everything else will derive its authority from here.

**Watch:** nothing yet — the daemon isn't up. Map still shows 10 grey dots.

---

## Beat 2 · Bootstrap the 10 nodes

```fish
# bootstrap_peers.txt has one target per line, format: name=root@host
cat ~/bootstrap_peers.txt | pln bootstrap ssh -
```

> One command. Pollen brings the local daemon up, installs itself on each host over SSH, enrols them, and binds a remote control API on every node by default. All ten in parallel.

**Watch:** "root" appears on the map, then grey dots become labelled as nodes enrol. Still no workloads.

---

## Beat 3 · Deploy two WASM seeds

```fish
pln seed ./examples/demo/ingest.wasm   --latency-slo 500ms
pln seed ./examples/demo/terminal.wasm --latency-slo 50ms
```

> Two WASM workloads. I publish them once. The mesh decides where they run — there is no scheduler.

**Watch:**
- Replicas panel: ingest (blue) and terminal (orange) both at 1.
- Map: one blue + one orange claim dot appear somewhere in the world.

---

## Beat 4 · Cluster is alive

```fish
pln status
```

> Ten nodes, two workloads, both claimed. Ready for traffic.

---

## Beat 5 · Fire from one region

```fish
# 1000 rps at us-west for 5 minutes.
just internal/dev/start-exerciser usw 1000 5m
```

> The exerciser runs on a dedicated host. One thousand requests per second into the mesh at us-west.

**Watch:**
- Request-rate panel: a single green line ramps to 1000 rps, labelled `usw ok`.
- Map: ingest load ripples out of USW; replicas spread to track demand.
- Replicas panel: step-changes as the autoscaler adds replicas.

---

## Beat 6 · Fan out

```fish
# Same command. Four more regions, concurrently.
just internal/dev/start-exerciser use,eun,sao,tok 1000 5m
```

> Same call. Comma-separated targets. Four more regions firing at the same rate — five thousand requests per second globally, from four continents.

**Watch:**
- Request-rate panel: four more lines appear at 1000 rps each.
- Map: load ripples out of five regions at once. Claim dots redistribute.
- Replicas panel: both workloads scale further.

---

## Close

> No scheduler. No central controller. Every decision is local to the node making it.

- Let the 5-minute duration expire, or `just internal/dev/stop-exerciser <node>` to end a single region early.
- Tear down:
```fish
pln ctx rm demo                              # stops the demo daemon, clears the context
just internal/dev/destroy-demo   # docker stack down + terraform destroy
```
