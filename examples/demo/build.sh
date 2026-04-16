#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export GOWORK=off

for dir in ingest processor terminal; do
  echo "building ${dir}.wasm..."
  (cd "$dir" && GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o "../${dir}.wasm" .)
done

echo "building store..."
(cd store && go build -o store .)

echo "done"
