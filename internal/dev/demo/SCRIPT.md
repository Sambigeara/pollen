# Pollen Demo · Cue Card

## Before recording

just internal/dev/build-demo
just internal/dev/deploy-demo

pln ctx rm demo 2>/dev/null || true
rm -rf /tmp/pln-demo
mkdir -p /tmp/pln-demo
pln ctx add demo /tmp/pln-demo
pln ctx use demo
pln set http :9090

just internal/dev/start-terminal-dash

cd internal/dev/demo-cluster
terraform output -json node_ips \
  | jq -r 'to_entries | sort_by(.key) | .[] | "\(.key)=root@\(.value)"' \
  > ~/bootstrap_peers.txt
cd -

pln init
cat ~/bootstrap_peers.txt | pln bootstrap ssh -
pln seed ./examples/demo/ingest.wasm
pln seed ./examples/demo/terminal.wasm

./sink

pln serve 8090 sink
pln status

just internal/dev/start-exerciser usw 1000 5m
just internal/dev/start-exerciser use,eun,sao,tok 1000 5m

## Loopback (max-throughput probe, no chain)

The terminal seed exports an `echo` function that returns the input
unchanged — useful for measuring admission + dispatch + wasm-runtime
cost with no downstream service in the critical path.

just internal/dev/start-exerciser eun,sao,tok,use,usw 5000 0 pln://seed/terminal/echo

## Teardown

pln unserve sink
pln ctx rm demo
just internal/dev/destroy-demo
just internal/dev/stop-observability
