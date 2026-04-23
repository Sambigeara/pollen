# Pollen Demo · Cue Card

Teleprompter for the asciinema-style demo. Keep this file open on the second
screen. Primary screen: left-half terminal + right-half Grafana
**Pollen · Demo** dashboard, full-screen. Demo uses the default `pln`
context — no `PLN_DIR` to set, `pln` vs `just` prefixes distinguish cluster
ops from load.

---

## Before recording

### One-time
```bash
just build-demo    # pln + exerciser + WASM modules
just deploy-demo   # provision 10 Hetzner nodes + 1 exerciser host (idempotent)
```

### Each time, before hitting record
```bash
# 1. Reset the default context: daemon down, cluster state wiped.
pln down 2>/dev/null || true
pln purge --yes 2>/dev/null || true

# 2. Re-apply the HTTP port so Prometheus can scrape the local node.
pln set http :9090

# 3. Bring up the observability stack (grafana + prometheus).
cd internal/dev/demo && docker compose up -d && cd -

# 4. Snapshot the peer list we'll pipe into bootstrap.
(cd internal/dev/demo-cluster \
  && terraform output -json node_ips \
     | jq -r 'to_entries | sort_by(.key) | .[] | "\(.key)=root@\(.value)"') \
  > ~/bootstrap_peers.txt
```

- Grafana: http://localhost:3000 → **Pollen · Demo** → fullscreen, time range **Last 5 minutes**, refresh **2s**.
- Starting view: 10 grey dots on the map, no workloads, replica/rate panels empty.

---

## Beat 1 · Init the root

```bash
# Create cluster credentials on this laptop — the root trust anchor.
pln init
```

> Pollen starts with a single root. This laptop is now a cluster of one. Everything else will derive its authority from here.

**Watch:** nothing yet — the daemon isn't up. Map still shows 10 grey dots.

---

## Beat 2 · Bootstrap the 10 nodes

```bash
# bootstrap_peers.txt has one target per line, format: name=root@host
```

```bash
cat ~/bootstrap_peers.txt | pln bootstrap ssh -
```

> One command. Pollen brings the local daemon up, mints an invite per host, pipes it over SSH, and each node joins the mesh. All ten in parallel.

**Watch:** "root" appears on the map, then grey dots become labelled as nodes enrol. Still no workloads.

---

## Beat 3 · Deploy two WASM seeds

```bash
pln seed ./examples/demo/ingest.wasm   --name ingest   --latency-slo-ms 500
pln seed ./examples/demo/terminal.wasm --name terminal --latency-slo-ms 50
```

> Two WASM workloads. I publish them once. The mesh decides where they run — there is no scheduler.

**Watch:**
- Replicas panel: ingest (blue) and terminal (orange) both at 1.
- Map: one blue + one orange claim dot appear somewhere in the world.

---

## Beat 4 · Cluster is alive

```bash
pln status
```

> Ten nodes, two workloads, both claimed. Ready for traffic.

---

## Beat 5 · Fire from one region

```bash
# 1000 rps at us-west for 5 minutes.
just start-exerciser usw 1000 5m
```

> The exerciser runs on a dedicated host. One thousand requests per second into the mesh at us-west.

**Watch:**
- Request-rate panel: a single green line ramps to 1000 rps, labelled `usw ok`.
- Map: ingest load ripples out of USW; replicas spread to track demand.
- Replicas panel: step-changes as the autoscaler adds replicas.

---

## Beat 6 · Fan out

```bash
# Same command. Four more regions, concurrently.
just start-exerciser use,eun,sao,tok 1000 5m
```

> Same call. Comma-separated targets. Four more regions firing at the same rate — five thousand requests per second globally, from four continents.

**Watch:**
- Request-rate panel: four more lines appear at 1000 rps each.
- Map: load ripples out of five regions at once. Claim dots redistribute.
- Replicas panel: both workloads scale further.

---

## Close

> No scheduler. No central controller. Every decision is local to the node making it.

- Let the 5-minute duration expire, or `just stop-exerciser <node>` to end a single region early.
- Tear down: `just destroy-demo`.
