#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

for name in ingest processor terminal; do
  echo "building ${name}.wasm..."
  (cd "${name}-zig" && zig build)
  cp "${name}-zig/zig-out/bin/${name}.wasm" "${name}.wasm"
done

echo "building store..."
(cd store && GOWORK=off go build -o store .)

echo "done"
