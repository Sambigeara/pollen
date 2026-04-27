# Pollen Demo · Cue Card

## Before recording

just internal/dev/build-demo    # exerciser, sink, WASM modules, world map
just internal/dev/deploy-demo   # terraform + exerciser host (idempotent)

pln ctx rm demo 2>/dev/null || true
rm -rf /tmp/pln-demo
mkdir -p /tmp/pln-demo
pln ctx add demo /tmp/pln-demo
pln ctx use demo
pln set http :9090
just internal/dev/start-terminal-dash   # grafterm TUI

cd internal/dev/demo-cluster
terraform output -json node_ips \
  | jq -r 'to_entries | sort_by(.key) | .[] | "\(.key)=root@\(.value)"' \
  > ~/bootstrap_peers.txt
cd -

pln init
cat ~/bootstrap_peers.txt | pln bootstrap ssh -
pln seed ./examples/demo/ingest.wasm   --latency-slo 500ms
pln seed ./examples/demo/terminal.wasm --latency-slo 50ms

./sink

pln serve 8090 sink
pln status

just internal/dev/start-exerciser usw 1000 5m
just internal/dev/start-exerciser use,eun,sao,tok 1000 5m

## Teardown

pln unserve sink
pln ctx rm demo
just internal/dev/destroy-demo
