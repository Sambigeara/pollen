#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# 1. Build
goreleaser build --clean --snapshot

# 2. Local Install
echo "Copying to local..." >&2
cp dist/pollen_darwin_arm64_v8.0/pollen "$ROOT/pollen"
echo "Successfully copied to local!" >&2

# 3. Resolve Tellybox IP (Tailscale)
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

pi_ip="$("$TS_BIN" status --json \
  | jq -r '
      [
        (.Peer[]? | select(.HostName=="pi") | .TailscaleIPs[]?),
        (.Peers[]? | select(.HostName=="pi") | .TailscaleIPs[]?)
      ]
      | map(select(startswith("100.")))[0] // .[0] // empty
    ')"

if [[ -z "$pi_ip" ]]; then
  echo "pi not found on Tailscale" >&2
  exit 1
fi

# 4. Resolve AWS IP (Tofu)
aws_ip="$(cd infra && tofu output -raw ip)"

if [[ -z "$aws_ip" ]]; then
  echo "AWS IP could not be resolved from tofu output" >&2
  exit 1
fi

# 5. Concurrent Deploy
# SSH_OPTS:
#   ConnectTimeout=10: Fail fast if host is down (don't wait minutes)
#   BatchMode=yes: Fail immediately if password/key interaction is required
SSH_OPTS="-o ConnectTimeout=10 -o BatchMode=yes"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

echo "Syncing to tellybox (${tellybox_ip})..." >&2
# -a: Archive (preserve permissions/exec bit)
# -z: Compress
# -e: Specific ssh options
rsync -az -e "ssh $SSH_OPTS" \
  dist/pollen_linux_amd64_v1/pollen "sambigeara@${tellybox_ip}:~/pollen" &
pids+=("$!")

echo "Syncing to pi (${pi_ip})..." >&2
rsync -az -e "ssh $SSH_OPTS" \
  dist/pollen_linux_arm64_v8.0/pollen "sambigeara@${pi_ip}:~/pollen" &
pids+=("$!")

echo "Syncing to relay (${aws_ip})..." >&2
rsync -az -e "ssh $SSH_OPTS" \
  dist/pollen_linux_arm64_v8.0/pollen "ubuntu@${aws_ip}:~/pollen" &
pids+=("$!")

fail=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    fail=1
    # We don't break immediately so we can see if the other one finishes or fails too
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo "One or more deployments failed." >&2
  exit 1
fi

echo "Successfully deployed to tellybox!" >&2
echo "Successfully deployed to pi!" >&2
echo "Successfully deployed to relay!" >&2
