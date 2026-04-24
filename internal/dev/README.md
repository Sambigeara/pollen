# internal/dev

Development-only infrastructure: verify clusters, topology experiments, demo stack.
Not part of the user-facing product.

```
internal/dev/
  hetzner/             # 3-node Hetzner cluster used by verify-hetzner.sh
  bootstrap/           # single-node bootstrap (eu-west-2 AWS)
  vivaldi/             # 28-node mixed (public+private) AWS topology
  vivaldi-public/      # 28-node all-public AWS topology
  demo-cluster/        # 10-node demo AWS topology
  demo/                # local docker-compose (grafana + prometheus) for the demo
  scripts/             # verify-hetzner.sh end-to-end smoke test
  justfile             # dev recipes (deploy-vivaldi, deploy-demo, verify-hetzner, …)
```

## Hetzner verify cluster

```bash
cd internal/dev
just deploy-hetzner
just verify-hetzner     # runs the full smoke test against the 3-node cluster
just destroy-hetzner
```

## Vivaldi (28-node AWS)

```bash
cd internal/dev
just deploy-vivaldi         # terraform only; cluster formation moved to pln-native
just destroy-vivaldi
```

`init-vivaldi` / `push-vivaldi` were removed with the Ansible retirement — use
`pln bootstrap ssh -` directly against the terraform output.

## Demo cluster

The narrative demo is driven from `demo/SCRIPT.md`; these recipes cover
only the infrastructure plane.

```bash
cd internal/dev
just build-demo              # exerciser + WASM + world map
just deploy-demo             # terraform + exerciser host (pollen nodes untouched)
just start-observability     # grafana + prometheus

# … narrative from demo/SCRIPT.md (pln init, pln bootstrap ssh -, pln seed, …)

just start-exerciser usw 1000 5m
just destroy-demo
```
