#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
# Terraform state lives in the main worktree, not in git worktrees.
# Resolve the main repo root via git.
MAIN_ROOT="$(git -C "$REPO_ROOT" worktree list --porcelain | head -1 | sed 's/^worktree //')"
TF_DIR="${MAIN_ROOT}/internal/dev/hetzner"
WASM="$REPO_ROOT/examples/echo/echo.wasm"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
PLN_LOCAL="$REPO_ROOT/pln-local"
PLN_LINUX="$REPO_ROOT/pln-linux"
PLN_TEST_DIR="/tmp/pln-verify-$$"
PLN_TEST_PORT=60612
PLN_PID=""
PLN_SHUTDOWN_GRACE=5
RESULTS=()

GREEN='\033[0;32m'
RED='\033[0;31m'
BOLD='\033[1m'
RESET='\033[0m'

pass() { RESULTS+=("${GREEN}PASS${RESET}  $1"); echo -e "  ${GREEN}PASS${RESET}  $1"; }
fail() { RESULTS+=("${RED}FAIL${RESET}  $1"); echo -e "  ${RED}FAIL${RESET}  $1: $2"; exit 1; }

lpln() { "$PLN_LOCAL" --dir "$PLN_TEST_DIR" "$@"; }
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

stop_pln() {
    local pid="$1"
    [ -z "$pid" ] && return 0
    kill -TERM "$pid" 2>/dev/null || true
    local deadline=$((SECONDS + PLN_SHUTDOWN_GRACE))
    while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 1; done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

reap_stale_pln() {
    local pids=()
    while IFS= read -r pid; do
        [ -n "$pid" ] && pids+=("$pid")
    done < <(
        {
            pgrep -f 'pln-local .*--dir /tmp/pln-verify-' 2>/dev/null || true
            lsof -ti "udp:$PLN_TEST_PORT" 2>/dev/null || true
        } | sort -u
    )
    (( ${#pids[@]} == 0 )) && return 0
    echo "  Reaping stale pln-local processes: ${pids[*]}"
    kill -TERM "${pids[@]}" 2>/dev/null || true
    sleep 1
    local survivors=()
    for p in "${pids[@]}"; do
        kill -0 "$p" 2>/dev/null && survivors+=("$p")
    done
    if (( ${#survivors[@]} > 0 )); then
        echo "  Force-killing: ${survivors[*]}"
        kill -KILL "${survivors[@]}" 2>/dev/null || true
    fi
}

cleanup() {
    local rc=$?
    stop_pln "$PLN_PID"
    rm -rf "$PLN_TEST_DIR"
    rm -f /tmp/pln-verify-*.log
    exit $rc
}
trap cleanup EXIT INT TERM HUP

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

echo -e "\n${BOLD}Step 0: Build and deploy${RESET}"

echo "  Purging local test state..."
reap_stale_pln
rm -rf "$PLN_TEST_DIR"

echo "  Purging remote state..."
for ip in "${ALL_REMOTE[@]}"; do
    rssh "$ip" "systemctl stop pln 2>/dev/null; dpkg --purge pln 2>/dev/null; rm -rf /var/lib/pln /root/.pln" || true
done

echo "  Cross-compiling linux/arm64..."
(cd "$REPO_ROOT" && GOOS=linux GOARCH=arm64 go build -o "$PLN_LINUX" ./cmd/pln)

echo "  Building local binary..."
(cd "$REPO_ROOT" && go build -o "$PLN_LOCAL" ./cmd/pln)

echo "  Deploying to remote nodes..."
for ip in "${ALL_REMOTE[@]}"; do
    (
        rsync -z -e "ssh $SSH_OPTS" "$PLN_LINUX" "root@${ip}:/usr/bin/pln"
        rssh "$ip" "pln daemon install"
    ) &
done
wait
pass "Step 0: Build and deploy"

echo -e "\n${BOLD}Step 1: Local root${RESET}"
lpln init
PLN_LOG="/tmp/pln-verify-$$.log"
lpln up --port "$PLN_TEST_PORT" &>"$PLN_LOG" &
PLN_PID=$!
sleep 2

if ! kill -0 "$PLN_PID" 2>/dev/null; then
    echo "  Local node crashed on startup:"
    cat "$PLN_LOG"
    fail "Step 1" "local node exited unexpectedly"
fi

STATUS=$(lpln status 2>&1)
echo "$STATUS" | grep -q "(self)" || fail "Step 1" "self not visible in status"
pass "Step 1: Local root initialized"

echo -e "\n${BOLD}Step 2: Bootstrap relay (nbg1-1)${RESET}"
lpln bootstrap ssh "root@${ROOT_IP}"
sleep 3

poll "root sees relay" 15 bash -c "[[ \$(\"$PLN_LOCAL\" --dir \"$PLN_TEST_DIR\" status 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 1 ]]"
poll "relay sees root" 15 bash -c "[[ \$(ssh $SSH_OPTS root@$ROOT_IP '/usr/bin/pln --dir /var/lib/pln status' 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 1 ]]"
pass "Step 2: Bootstrap relay — bidirectional"

echo -e "\n${BOLD}Step 3: Join node 2 (nbg1-2)${RESET}"
TOKEN=$(lpln invite)
rpln "$NODE2_IP" join "$TOKEN"
sleep 3

poll "3 nodes visible" 15 bash -c "[[ \$(\"$PLN_LOCAL\" --dir \"$PLN_TEST_DIR\" status 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 2 ]]"
pass "Step 3: Node 2 joined — 3 nodes visible"

echo -e "\n${BOLD}Step 4: Targeted invite node 3 (hel1)${RESET}"
rssh "$NODE3_IP" "pln --dir /var/lib/pln init --no-up 2>/dev/null" || true
NODE3_ID=$(rpln "$NODE3_IP" id)
TOKEN=$(lpln invite --subject "$NODE3_ID")
rpln "$NODE3_IP" join "$TOKEN"
sleep 3

poll "4 nodes visible" 20 bash -c "[[ \$(\"$PLN_LOCAL\" --dir \"$PLN_TEST_DIR\" status 2>&1 | grep -cE 'direct|relay|indirect' || true) -ge 3 ]]"
pass "Step 4: Node 3 joined — 4 nodes visible"

echo -e "\n${BOLD}Step 5: Vivaldi convergence${RESET}"
check_vivaldi() {
    local err
    err=$(lpln status --wide 2>&1 | grep 'vivaldi error' | awk '{print $NF}')
    [ -n "$err" ] && awk "BEGIN{exit(!($err < 0.5))}"
}
poll "vivaldi error < 0.5" 60 check_vivaldi
pass "Step 5: Vivaldi error converged below 0.5"

echo -e "\n${BOLD}Step 6: Service propagation${RESET}"
rpln "$NODE2_IP" serve 8080 test-svc
rssh "$NODE2_IP" "nohup python3 -c '
import socket, threading
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((\"0.0.0.0\", 8080)); s.listen(4)
while True:
    c, _ = s.accept()
    def h(c):
        d = c.recv(4096); c.sendall(d); c.close()
    threading.Thread(target=h, args=(c,), daemon=True).start()
' &>/dev/null &"
poll "service visible on root" 5 bash -c "\"$PLN_LOCAL\" --dir \"$PLN_TEST_DIR\" status services 2>&1 | grep -q test-svc"
poll "service visible on nbg1-1" 5 bash -c "ssh $SSH_OPTS root@$ROOT_IP '/usr/bin/pln --dir /var/lib/pln status services' 2>&1 | grep -q test-svc"
poll "service visible on hel1" 5 bash -c "ssh $SSH_OPTS root@$NODE3_IP '/usr/bin/pln --dir /var/lib/pln status services' 2>&1 | grep -q test-svc"
pass "Step 6: Service propagated to all nodes"

echo -e "\n${BOLD}Step 7: Connection tunnel${RESET}"
lpln connect test-svc
TUNNEL_PORT=$(lpln status services 2>&1 | grep test-svc | awk '{print $NF}')
[ -n "$TUNNEL_PORT" ] || fail "Step 7" "could not determine local tunnel port"
REPLY=$(python3 -c "
import socket; s = socket.socket(); s.settimeout(5)
s.connect(('127.0.0.1', $TUNNEL_PORT))
s.sendall(b'step7-ping\n'); print(s.recv(4096).decode().strip()); s.close()
")
[ "$REPLY" = "step7-ping" ] || fail "Step 7" "tunnel echo failed: got '$REPLY'"
lpln disconnect test-svc
rssh "$NODE2_IP" "pkill -f 'python3 -c'" || true
pass "Step 7: Connection tunnel — data round-tripped"

echo -e "\n${BOLD}Step 8: Workload lifecycle${RESET}"
HASH=$(lpln seed "$WASM" --min-replicas 1 2>&1 | grep -oE '[a-f0-9]{8,}' | head -1)
[ -n "$HASH" ] || fail "Step 8" "seed returned no hash"
SHORT="${HASH:0:16}"

poll "seed running 1/1" 30 bash -c "\"$PLN_LOCAL\" --dir \"$PLN_TEST_DIR\" status seeds 2>&1 | grep '$SHORT' | grep -q '1/1'"
echo "  Seed running 1/1"

lpln unseed "$SHORT"
sleep 2
check_seed_gone() { lpln status seeds 2>&1 | grep -q "$SHORT" && return 1 || return 0; }
poll "seed gone" 15 check_seed_gone
echo "  Seed removed"

HASH=$(lpln seed "$WASM" 2>&1 | grep -oE '[a-f0-9]{8,}' | head -1)
SHORT="${HASH:0:16}"
poll "seed running 2/2" 30 bash -c "\"$PLN_LOCAL\" --dir \"$PLN_TEST_DIR\" status seeds 2>&1 | grep '$SHORT' | grep -q '2/2'"
pass "Step 8: Workload lifecycle — seed/unseed/reseed"

echo -e "\n${BOLD}Step 9: Call from all nodes${RESET}"
for label_ip in "local:" "nbg1-1:$ROOT_IP" "nbg1-2:$NODE2_IP" "hel1:$NODE3_IP"; do
    label="${label_ip%%:*}"
    ip="${label_ip#*:}"
    if [ "$label" = "local" ]; then
        OUT=$(lpln call "$SHORT" echo test 2>&1)
    else
        OUT=$(rpln "$ip" call "$SHORT" echo test 2>&1)
    fi
    echo "$OUT" | grep -q "test" || fail "Step 9" "call from $label returned unexpected: $OUT"
done
pass "Step 9: Call from all 4 nodes"

echo -e "\n${BOLD}Step 10: Load test (1000 calls)${RESET}"
LOAD_A="/tmp/pln-load-a-$$"
LOAD_B="/tmp/pln-load-b-$$"
rssh "$ROOT_IP" "P=0;F=0;for i in \$(seq 1 500);do if /usr/bin/pln --dir /var/lib/pln call $SHORT echo t >/dev/null 2>&1;then P=\$((P+1));else F=\$((F+1));fi;done;echo \$P \$F" > "$LOAD_A" 2>&1 &
PID_A=$!
rssh "$NODE2_IP" "P=0;F=0;for i in \$(seq 1 500);do if /usr/bin/pln --dir /var/lib/pln call $SHORT echo t >/dev/null 2>&1;then P=\$((P+1));else F=\$((F+1));fi;done;echo \$P \$F" > "$LOAD_B" 2>&1 &
PID_B=$!
wait $PID_A
wait $PID_B
FAIL_A=$(awk '{print $2}' "$LOAD_A")
FAIL_B=$(awk '{print $2}' "$LOAD_B")
rm -f "$LOAD_A" "$LOAD_B"
TOTAL_FAIL=$(( ${FAIL_A:-0} + ${FAIL_B:-0} ))
[ "$TOTAL_FAIL" -eq 0 ] || fail "Step 10" "$TOTAL_FAIL / 1000 calls failed"
pass "Step 10: 1000 calls — zero failures"

echo -e "\n${BOLD}Step 11: Workload migration${RESET}"
SEED_IP=""
for ip in "${ALL_REMOTE[@]}"; do
    if rpln "$ip" status seeds 2>&1 | grep "$SHORT" | grep -q '\*'; then
        SEED_IP="$ip"
        break
    fi
done
if [ -z "$SEED_IP" ]; then
    if lpln status seeds 2>&1 | grep "$SHORT" | grep -q '\*'; then
        stop_pln "$PLN_PID"; PLN_PID=""
        poll "2/2 maintained after local down" 30 bash -c "ssh $SSH_OPTS root@$ROOT_IP '/usr/bin/pln --dir /var/lib/pln status seeds' 2>&1 | grep '$SHORT' | grep -q '2/2'"
        pass "Step 11: Workload migrated after local node down"
    else
        fail "Step 11" "could not find a seed-holding node"
    fi
else
    rssh "$SEED_IP" "systemctl stop pln 2>/dev/null; pln --dir /var/lib/pln down 2>/dev/null" || true
    CHECK_IP="$ROOT_IP"
    [ "$SEED_IP" = "$ROOT_IP" ] && CHECK_IP="$NODE2_IP"
    poll "2/2 maintained after node down" 30 bash -c "ssh $SSH_OPTS root@$CHECK_IP '/usr/bin/pln --dir /var/lib/pln status seeds' 2>&1 | grep '$SHORT' | grep -q '2/2'"
    pass "Step 11: Workload migrated after node down"
fi

rssh "$ROOT_IP" "systemctl start pln" 2>/dev/null || true
poll "root daemon alive" 60 ssh $SSH_OPTS "root@$ROOT_IP" "test -S /var/lib/pln/pln.sock"

echo -e "\n${BOLD}Step 12: -H remote targeting${RESET}"
REMOTE_STATUS=$("$PLN_LOCAL" --dir "$PLN_TEST_DIR" -H "root@$ROOT_IP" status 2>&1)
echo "$REMOTE_STATUS" | grep -q "(self)" || fail "Step 12" "pln -H status did not reach remote: $REMOTE_STATUS"
NATIVE_STATUS=$(rssh "$ROOT_IP" "/usr/bin/pln --dir /var/lib/pln status 2>&1" | head -5)
pass "Step 12: pln -H returned remote daemon's view"

echo -e "\n${BOLD}Step 13: pln context flow${RESET}"
(
    CTX_HOME="/tmp/pln-ctx-$$"
    mkdir -p "$CTX_HOME"
    export HOME="$CTX_HOME"
    trap "rm -rf $CTX_HOME" EXIT
    PLN="$PLN_LOCAL --dir $PLN_TEST_DIR"

    $PLN ctx add prod "root@$ROOT_IP" --from default >/dev/null || { echo "ctx add failed"; exit 1; }
    $PLN ctx switch prod >/dev/null || { echo "ctx switch failed"; exit 1; }

    [ "$($PLN ctx current)" = "prod" ] || { echo "ctx current wrong"; exit 1; }

    CTX_STATUS=$($PLN status 2>&1)
    echo "$CTX_STATUS" | grep -q "(self)" || { echo "status via ctx failed: $CTX_STATUS"; exit 1; }

    TOKEN=$($PLN invite 2>&1)
    echo "$TOKEN" | grep -qE '^[A-Za-z0-9+/=]{40,}$' || { echo "invite failed: $TOKEN"; exit 1; }

    $PLN ctx switch default >/dev/null
    PLN_CONTEXT=prod $PLN status 2>&1 | grep -q "(self)" || { echo "PLN_CONTEXT override failed"; exit 1; }
    [ "$(PLN_CONTEXT=prod $PLN ctx current)" = "prod" ] || { echo "PLN_CONTEXT current wrong"; exit 1; }

    $PLN ctx rm prod >/dev/null || { echo "ctx rm failed"; exit 1; }
    [ ! -d "$CTX_HOME/.pln/contexts/prod" ] || { echo "ctx rm left identity dir behind"; exit 1; }
) || fail "Step 13" "pln context flow failed"
pass "Step 13: pln context — transport, identity signing, env override, cleanup"

echo -e "\n${BOLD}=== VERIFICATION SUMMARY ===${RESET}"
for r in "${RESULTS[@]}"; do echo -e "  $r"; done
echo -e "\n${GREEN}${BOLD}All steps passed.${RESET}"
