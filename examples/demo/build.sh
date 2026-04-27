#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "building ingest.wasm (TinyGo)..."
(cd ingest && tinygo build -o ../ingest.wasm -target wasi -no-debug -gc=leaking .)

echo "building terminal.wasm (Go wasip1)..."
(cd terminal && GOOS=wasip1 GOARCH=wasm go build -o ../terminal.wasm .)

echo "building ingest-zig.wasm (Zig)..."
(cd ingest-zig && zig build && cp zig-out/bin/ingest.wasm ../ingest-zig.wasm)

echo "building terminal-zig.wasm (Zig)..."
(cd terminal-zig && zig build && cp zig-out/bin/terminal.wasm ../terminal-zig.wasm)

echo "building store..."
(cd store && GOWORK=off go build -o store .)

echo "done"
