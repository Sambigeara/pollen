#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# Terraform state lives in the main worktree, not in git worktrees.
# Resolve the main repo root via git.
MAIN_ROOT="$(git -C "$REPO_ROOT" worktree list --porcelain | head -1 | sed 's/^worktree //')"
TF_DIR="${MAIN_ROOT}/infra/hetzner"
WASM="$REPO_ROOT/examples/echo/echo.wasm"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
PLN_LOCAL="$REPO_ROOT/pln-local"
PLN_LINUX="$REPO_ROOT/pln-linux"
RESULTS=()

GREEN='\033[0;32m'
RED='\033[0;31m'
BOLD='\033[1m'
RESET='\033[0m'

# --- helpers ---

pass() { RESULTS+=("${GREEN}PASS${RESET}  $1"); echo -e "  ${GREEN}PASS${RESET}  $1"; }
fail() { RESULTS+=("${RED}FAIL${RESET}  $1"); echo -e "  ${RED}FAIL${RESET}  $1: $2"; exit 1; }

rssh() { ssh $SSH_OPTS "root@$1" "${@:2}"; }
rpln() { rssh "$1" /usr/bin/pln --dir /var/lib/pln "${@:2}"; }

poll() {
    local desc="$1" timeout="$2"; shift 2
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        if "$@" 2>/dev/null; then return 0; fi
        sleep 1
    done
    fail "$desc" "timed out after ${timeout}s"
}

node_ips() { cd "$TF_DIR" && terraform output -json node_ips 2>/dev/null; }

# --- resolve IPs ---

echo -e "${BOLD}Resolving Hetzner node IPs...${RESET}"
if ! IPS_JSON=$(node_ips) || [ -z "$IPS_JSON" ]; then
    echo -e "${RED}ERROR${RESET}: Could not read Hetzner node IPs from terraform."
    echo "  The Hetzner cluster may not be provisioned."
    echo "  Run:  just deploy-hetzner <version>"
    echo "  Then re-run this script."
    exit 1
fi
ROOT_IP=$(echo "$IPS_JSON" | jq -r '.["nbg1-1"]')
NODE2_IP=$(echo "$IPS_JSON" | jq -r '.["nbg1-2"]')
NODE3_IP=$(echo "$IPS_JSON" | jq -r '.["hel1"]')
ALL_REMOTE=("$ROOT_IP" "$NODE2_IP" "$NODE3_IP")
echo "  root(nbg1-1)=$ROOT_IP  node2(nbg1-2)=$NODE2_IP  node3(hel1)=$NODE3_IP"

# --- step 0: build & deploy ---

echo -e "\n${BOLD}Step 0: Build and deploy${RESET}"

echo "  Purging local state..."
"$PLN_LOCAL" down 2>/dev/null || true
"$PLN_LOCAL" purge --all --yes 2>/dev/null || rm -rf ~/.pln

echo "  Purging remote state..."
for ip in "${ALL_REMOTE[@]}"; do
    rssh "$ip" "systemctl stop pln 2>/dev/null; dpkg --purge pln 2>/dev/null; rm -rf /var/lib/pln /root/.pln" || true
done

echo "  Cross-compiling linux/arm64..."
(cd "$REPO_ROOT" && GOOS=linux GOARCH=arm64 go build -o "$PLN_LINUX" ./cmd/pln)

echo "  Building local binary..."
(cd "$REPO_ROOT" && go build -o "$PLN_LOCAL" ./cmd/pln)

echo "  Replacing homebrew binary with dev build..."
BREW_PLN="$(brew --prefix pln 2>/dev/null)/bin/pln"
if [ -f "$BREW_PLN" ]; then
    chmod u+w "$BREW_PLN"
    cp "$PLN_LOCAL" "$BREW_PLN"
fi

echo "  Deploying to remote nodes..."
for ip in "${ALL_REMOTE[@]}"; do
    scp $SSH_OPTS "$PLN_LINUX" "root@${ip}:/usr/bin/pln"
    # Provision the pln system user/group, state dirs, and systemd unit
    # so that ensureRemotePollen skips the deb install and `pln up -d` works.
    rssh "$ip" "id pln >/dev/null 2>&1 || (groupadd --system pln && useradd --system -g pln -d /var/lib/pln -s /usr/sbin/nologin pln)" || true
    rssh "$ip" "install -d -m 0770 -o pln -g pln /var/lib/pln && install -d -m 0770 -o pln -g pln /var/lib/pln/keys" || true
    rssh "$ip" "cat > /lib/systemd/system/pln.service << 'UNIT'
[Unit]
Description=PLN Mesh Node
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=pln
Group=pln
ExecStart=/usr/bin/pln --dir /var/lib/pln up
Restart=on-failure
RestartSec=5
StateDirectory=pln
StateDirectoryMode=0770
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload" || true
done
pass "Step 0: Build and deploy"

# --- step 1: local root ---

echo -e "\n${BOLD}Step 1: Local root${RESET}"
"$PLN_LOCAL" init
"$PLN_LOCAL" up -d
sleep 2

STATUS=$("$PLN_LOCAL" status 2>&1)
echo "$STATUS" | grep -q "(self)" || fail "Step 1" "self not visible in status"
pass "Step 1: Local root initialized"

# --- step 2: bootstrap relay ---

echo -e "\n${BOLD}Step 2: Bootstrap relay (nbg1-1)${RESET}"
"$PLN_LOCAL" bootstrap ssh "root@${ROOT_IP}"
sleep 3

peer_count() { "$PLN_LOCAL" status 2>&1 | grep -cE 'direct|relay|indirect' || true; }
remote_peer_count() { ssh $SSH_OPTS "root@$1" "/usr/bin/pln --dir /var/lib/pln status" 2>&1 | grep -cE 'direct|relay|indirect' || true; }

poll "root sees relay" 15 bash -c "[[ \$(\"$PLN_LOCAL\" status 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 1 ]]"
poll "relay sees root" 15 bash -c "[[ \$(ssh $SSH_OPTS root@$ROOT_IP '/usr/bin/pln --dir /var/lib/pln status' 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 1 ]]"
pass "Step 2: Bootstrap relay — bidirectional"

# --- step 3: join node 2 ---

echo -e "\n${BOLD}Step 3: Join node 2 (nbg1-2)${RESET}"
TOKEN=$("$PLN_LOCAL" invite)
rpln "$NODE2_IP" join "$TOKEN"
sleep 3

poll "3 nodes visible" 15 bash -c "[[ \$(\"$PLN_LOCAL\" status 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 2 ]]"
pass "Step 3: Node 2 joined — 3 nodes visible"

# --- step 4: targeted invite node 3 ---

echo -e "\n${BOLD}Step 4: Targeted invite node 3 (hel1)${RESET}"
rssh "$NODE3_IP" "pln --dir /var/lib/pln init --no-up 2>/dev/null" || true
NODE3_ID=$(rpln "$NODE3_IP" id)
TOKEN=$("$PLN_LOCAL" invite --subject "$NODE3_ID")
rpln "$NODE3_IP" join "$TOKEN"
sleep 3

poll "4 nodes visible" 20 bash -c "[[ \$(\"$PLN_LOCAL\" status 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 3 ]]"
pass "Step 4: Node 3 joined — 4 nodes visible"

# --- step 5: vivaldi convergence ---

echo -e "\n${BOLD}Step 5: Vivaldi convergence${RESET}"
check_vivaldi() {
    local err
    err=$("$PLN_LOCAL" status --wide 2>&1 | grep 'vivaldi error' | awk '{print $NF}')
    [ -n "$err" ] && awk "BEGIN{exit(!($err < 0.5))}"
}
poll "vivaldi error < 0.5" 60 check_vivaldi
pass "Step 5: Vivaldi error converged below 0.5"

# --- step 6: service propagation ---

echo -e "\n${BOLD}Step 6: Service propagation${RESET}"
rpln "$NODE2_IP" serve 8080 test-svc
poll "service visible on root" 5 bash -c "\"$PLN_LOCAL\" status services 2>&1 | grep -q test-svc"
poll "service visible on nbg1-1" 5 bash -c "ssh $SSH_OPTS root@$ROOT_IP '/usr/bin/pln --dir /var/lib/pln status services' 2>&1 | grep -q test-svc"
poll "service visible on hel1" 5 bash -c "ssh $SSH_OPTS root@$NODE3_IP '/usr/bin/pln --dir /var/lib/pln status services' 2>&1 | grep -q test-svc"
pass "Step 6: Service propagated to all nodes"

# --- step 8: workload lifecycle ---

echo -e "\n${BOLD}Step 8: Workload lifecycle${RESET}"
HASH=$("$PLN_LOCAL" seed "$WASM" --replicas 1 2>&1 | grep -oE '[a-f0-9]{16,}' | head -1)
[ -n "$HASH" ] || fail "Step 8" "seed returned no hash"
SHORT="${HASH:0:16}"

poll "seed running 1/1" 30 bash -c "\"$PLN_LOCAL\" status seeds 2>&1 | grep '$SHORT' | grep -q '1/1'"
echo "  Seed running 1/1"

"$PLN_LOCAL" unseed "$SHORT"
sleep 2
check_seed_gone() { "$PLN_LOCAL" status seeds 2>&1 | grep -q "$SHORT" && return 1 || return 0; }
poll "seed gone" 15 check_seed_gone
echo "  Seed removed"

HASH=$("$PLN_LOCAL" seed "$WASM" --replicas 2 2>&1 | grep -oE '[a-f0-9]{16,}' | head -1)
SHORT="${HASH:0:16}"
poll "seed running 2/2" 30 bash -c "\"$PLN_LOCAL\" status seeds 2>&1 | grep '$SHORT' | grep -q '2/2'"
pass "Step 8: Workload lifecycle — seed/unseed/reseed"

# --- step 9: call from all nodes ---

echo -e "\n${BOLD}Step 9: Call from all nodes${RESET}"
for label_ip in "local:" "nbg1-1:$ROOT_IP" "nbg1-2:$NODE2_IP" "hel1:$NODE3_IP"; do
    label="${label_ip%%:*}"
    ip="${label_ip#*:}"
    if [ "$label" = "local" ]; then
        OUT=$("$PLN_LOCAL" call "$SHORT" handle --input test 2>&1)
    else
        OUT=$(rpln "$ip" call "$SHORT" handle --input test 2>&1)
    fi
    echo "$OUT" | grep -q "test" || fail "Step 9" "call from $label returned unexpected: $OUT"
done
pass "Step 9: Call from all 4 nodes"

# --- step 10: load test ---

echo -e "\n${BOLD}Step 10: Load test (1000 calls)${RESET}"
LOAD_A="/tmp/pln-load-a-$$"
LOAD_B="/tmp/pln-load-b-$$"
# Single SSH session per node with internal loop — no per-call SSH overhead.
rssh "$ROOT_IP" "P=0;F=0;for i in \$(seq 1 500);do if /usr/bin/pln --dir /var/lib/pln call $SHORT handle --input t >/dev/null 2>&1;then P=\$((P+1));else F=\$((F+1));fi;done;echo \$P \$F" > "$LOAD_A" 2>&1 &
PID_A=$!
rssh "$NODE2_IP" "P=0;F=0;for i in \$(seq 1 500);do if /usr/bin/pln --dir /var/lib/pln call $SHORT handle --input t >/dev/null 2>&1;then P=\$((P+1));else F=\$((F+1));fi;done;echo \$P \$F" > "$LOAD_B" 2>&1 &
PID_B=$!
wait $PID_A
wait $PID_B
FAIL_A=$(awk '{print $2}' "$LOAD_A")
FAIL_B=$(awk '{print $2}' "$LOAD_B")
rm -f "$LOAD_A" "$LOAD_B"
TOTAL_FAIL=$(( ${FAIL_A:-0} + ${FAIL_B:-0} ))
[ "$TOTAL_FAIL" -eq 0 ] || fail "Step 10" "$TOTAL_FAIL / 1000 calls failed"
pass "Step 10: 1000 calls — zero failures"

# --- step 11: workload migration ---

echo -e "\n${BOLD}Step 11: Workload migration${RESET}"
# Find a node running the workload, then stop it
SEED_IP=""
for ip in "${ALL_REMOTE[@]}"; do
    if rpln "$ip" status seeds 2>&1 | grep "$SHORT" | grep -q '\*'; then
        SEED_IP="$ip"
        break
    fi
done
if [ -z "$SEED_IP" ]; then
    # Local might be running it — stop local
    if "$PLN_LOCAL" status seeds 2>&1 | grep "$SHORT" | grep -q '\*'; then
        "$PLN_LOCAL" down
        poll "2/2 maintained after local down" 30 bash -c "ssh $SSH_OPTS root@$ROOT_IP '/usr/bin/pln --dir /var/lib/pln status seeds' 2>&1 | grep '$SHORT' | grep -q '2/2'"
        pass "Step 11: Workload migrated after local node down"
    else
        fail "Step 11" "could not find a seed-holding node"
    fi
else
    rssh "$SEED_IP" "systemctl stop pln 2>/dev/null; pln --dir /var/lib/pln down 2>/dev/null" || true
    # Pick a surviving remote node to check
    CHECK_IP="$ROOT_IP"
    [ "$SEED_IP" = "$ROOT_IP" ] && CHECK_IP="$NODE2_IP"
    poll "2/2 maintained after node down" 30 bash -c "ssh $SSH_OPTS root@$CHECK_IP '/usr/bin/pln --dir /var/lib/pln status seeds' 2>&1 | grep '$SHORT' | grep -q '2/2'"
    pass "Step 11: Workload migrated after node down"
fi

# --- summary ---

echo -e "\n${BOLD}=== VERIFICATION SUMMARY ===${RESET}"
for r in "${RESULTS[@]}"; do echo -e "  $r"; done
echo -e "\n${GREEN}${BOLD}All steps passed.${RESET}"
