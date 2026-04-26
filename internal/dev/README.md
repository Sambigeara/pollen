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

Two flavours: `vivaldi` (mixed public+private) and `vivaldi-public` (all-public).

```bash
cd internal/dev
just deploy-vivaldi          # mixed topology
just deploy-vivaldi-pub      # all-public topology
just destroy-vivaldi         # / destroy-vivaldi-pub
```

Terraform only provisions infra — form the mesh with `pln bootstrap ssh` against
the terraform output. Use a dedicated `pln` context so vivaldi state stays out
of your prod / dev ctxs:

```fish
pln ctx add vivaldi-pub        # one-time; ephemeral local port
set -x PLN_CONTEXT vivaldi-pub # scope the rest of the shell to this ctx
pln init                       # mint a root for this ctx

cd internal/dev/vivaldi-public
terraform output -json all_public_ips \
  | jq -r 'to_entries | .[] | "node\(.key)=ubuntu@\(.value)"' \
  | pln bootstrap ssh --admin -
```

The same shape works for `vivaldi/` (mixed) — swap the terraform dir.

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
