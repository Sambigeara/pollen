#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$(dirname "$0")/.."
export GOWORK=off

for dir in echo upper; do
  echo "building ${dir}.wasm..."
  (cd "$dir" && GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o "${SCRIPT_DIR}/${dir}.wasm" .)
done

echo "done"
