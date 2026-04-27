# Pollen Demo

A global Pollen mesh with a Grafana (or grafterm TUI) dashboard. 10 AWS
nodes across the world plus a dedicated load-generator host. The demo is
driven from a laptop — see `SCRIPT.md` for the narrative.

## Prerequisites

- AWS credentials configured
- Terraform, Docker, `just` installed
- `pln` on PATH (`brew install pln`)
- SSH key at `~/.ssh/id_ed25519`

## Lifecycle

```bash
# One-time
just internal/dev/build-demo            # exerciser, sink, WASM, world map
just internal/dev/deploy-demo           # terraform + exerciser host

# Per-recording
just internal/dev/start-observability   # grafana + prometheus
just internal/dev/start-terminal-dash   # grafterm TUI (alternative)

# Drive from the laptop — see SCRIPT.md
pln init
cat ~/bootstrap_peers.txt | pln bootstrap ssh -
pln seed ./examples/demo/ingest.wasm   --latency-slo 500ms
pln seed ./examples/demo/terminal.wasm --latency-slo 50ms
./sink &                                # run sink locally
pln serve 8090 sink                     # expose as pln://service/sink

# Load
just internal/dev/start-exerciser usw 1000 5m
just internal/dev/start-exerciser usw,use,eun,sao,tok 1000 5m
just internal/dev/stop-exerciser usw

# Teardown
just internal/dev/destroy-demo
pln ctx rm demo
```

## The chain

```
exerciser → ingest seed → terminal seed → pln://service/sink → ./sink (laptop)
```

The exerciser dials each pollen node's TCP control endpoint (bound at
bootstrap) authenticated with the per-node token at
`/var/lib/pln/control.token`, then invokes `pln://seed/ingest/handle_firehose`.

## Dashboard

| Panel | What |
|-------|------|
| **Mesh Topology** | ECharts world map. Dot colour = workload; size = pressure. |
| **Nodes / Workloads / Replicas** | Cluster-wide counts |
| **Workload Pressure** | Demand-to-capacity ratio over time |
| **Outbound Traffic** | Gossip byte rate from the root |
| **Workload Placement** | Which workloads run on which nodes |

## Node topology

| Name | Location | AWS Region |
|------|----------|------------|
| root | Laptop | — |
| use  | Virginia | us-east-1 |
| usw  | Oregon | us-west-2 |
| euw  | Ireland | eu-west-1 |
| eun  | Stockholm | eu-north-1 |
| euc  | Frankfurt | eu-central-1 |
| tok  | Tokyo | ap-northeast-1 |
| sgp  | Singapore | ap-southeast-1 |
| syd  | Sydney | ap-southeast-2 |
| mum  | Mumbai | ap-south-1 |
| sao  | São Paulo | sa-east-1 |

## Ports

| Port | Service |
|------|---------|
| 3000 | Grafana |
| 8090 | Sink (local) |
| 9090 | Pollen metrics (local root) |
| 9091 | Prometheus |
| 9192–9201 | Per-region exerciser metrics |
| 50051/tcp | Per-node pollen control API |
| 60611/udp | Pollen mesh (remote nodes) |
| 60612/udp | Pollen mesh (laptop root) |
