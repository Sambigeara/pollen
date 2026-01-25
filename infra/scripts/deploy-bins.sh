#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

goreleaser build --clean --snapshot

echo "Copying to local..." >&2
cp dist/pollen_darwin_arm64_v8.0/pollen "$ROOT/pollen"
echo "Successfully copied to local!" >&2

if command -v tailscale >/dev/null 2>&1; then
  TS_BIN="$(command -v tailscale)"
elif [[ -x /Applications/Tailscale.app/Contents/MacOS/Tailscale ]]; then
  TS_BIN="/Applications/Tailscale.app/Contents/MacOS/Tailscale"
else
  echo "tailscale CLI not found" >&2
  exit 1
fi

tellybox_ip="$("$TS_BIN" status --json \
  | jq -r '
      [
        (.Peer[]? | select(.HostName=="tellybox" or .HostName=="samflix") | .TailscaleIPs[]?),
        (.Peers[]? | select(.HostName=="tellybox" or .HostName=="samflix") | .TailscaleIPs[]?)
      ]
      | map(select(startswith("100.")))[0] // .[0] // empty
    ')"

if [[ -z "$tellybox_ip" ]]; then
  echo "tellybox/samflix not found on Tailscale" >&2
  exit 1
fi

echo "Copying to tellybox..." >&2
scp dist/pollen_linux_amd64_v1/pollen "sambigeara@${tellybox_ip}:~/pollen"
echo "Successfully copied to tellybox!" >&2

echo "Copying to relay..." >&2
aws_ip="$(cd infra && tofu output -raw ip)"
scp dist/pollen_linux_arm64_v8.0/pollen "ubuntu@${aws_ip}:~/pollen"
echo "Successfully copied to relay!" >&2
