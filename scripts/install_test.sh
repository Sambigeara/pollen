#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

# shellcheck disable=SC1091
. "$SCRIPT_DIR/install.sh"

RUN_OUTPUT=""
RUN_STATUS=0

fail() {
    printf 'not ok - %s\n' "$1" >&2
    exit 1
}

make_fake_bins() {
    local dir=$1
    local name
    shift
    for name in "$@"; do
        printf '#!/bin/sh\nexit 0\n' >"$dir/$name"
        chmod +x "$dir/$name"
    done
}

run_detect() {
    local os_release=$1
    local tmp bin tr_path
    shift

    tmp=$(mktemp -d)
    bin="$tmp/bin"
    mkdir "$bin"
    printf '%s\n' "$os_release" >"$tmp/os-release"
    tr_path=$(command -v tr)
    ln -s "$tr_path" "$bin/tr"
    make_fake_bins "$bin" "$@"

    set +e
    RUN_OUTPUT=$(OS_RELEASE_FILE="$tmp/os-release" PATH="$bin" detect_method 2>&1)
    RUN_STATUS=$?
    set -e

    rm -rf "$tmp"
}

assert_detect() {
    local name=$1
    local want=$2
    local os_release=$3
    shift 3

    run_detect "$os_release" "$@"
    if [ "$RUN_STATUS" -ne 0 ]; then
        fail "$name: expected $want, got failure: $RUN_OUTPUT"
    fi
    if [ "$RUN_OUTPUT" != "$want" ]; then
        fail "$name: expected $want, got $RUN_OUTPUT"
    fi
}

assert_refuses() {
    local name=$1
    local want=$2
    local os_release=$3
    shift 3

    run_detect "$os_release" "$@"
    if [ "$RUN_STATUS" -eq 0 ]; then
        fail "$name: expected refusal, got $RUN_OUTPUT"
    fi
    case "$RUN_OUTPUT" in
        *"$want"*) ;;
        *) fail "$name: expected refusal containing '$want', got $RUN_OUTPUT" ;;
    esac
}

assert_method() {
    local name=$1
    local method=$2
    local want_status=$3
    local want=$4

    set +e
    RUN_OUTPUT=$(METHOD="$method" resolve_linux_method 2>&1)
    RUN_STATUS=$?
    set -e

    if [ "$RUN_STATUS" -ne "$want_status" ]; then
        fail "$name: expected status $want_status, got $RUN_STATUS: $RUN_OUTPUT"
    fi
    case "$RUN_OUTPUT" in
        *"$want"*) ;;
        *) fail "$name: expected output containing '$want', got $RUN_OUTPUT" ;;
    esac
}

assert_tarball_installs_root_owned_binary() {
    local tmp src sudo_bin log

    tmp=$(mktemp -d)
    src="$tmp/src"
    INSTALL_TMPDIR="$tmp/work"
    FAKE_SUDO_LOG="$tmp/sudo.log"
    export FAKE_SUDO_LOG
    mkdir "$src" "$INSTALL_TMPDIR"
    printf '#!/bin/sh\nexit 0\n' >"$src/pln"
    chmod +x "$src/pln"

    sudo_bin="$tmp/sudo"
    printf '%s\n' '#!/bin/sh' "printf \"%s\\n\" \"\$*\" >> \"\$FAKE_SUDO_LOG\"" >"$sudo_bin"
    chmod +x "$sudo_bin"
    export SUDO="$sudo_bin"

    download_asset() {
        local output=$2
        tar -czf "$output" -C "$src" pln
    }

    set +e
    RUN_OUTPUT=$(install_tarball 2>&1)
    RUN_STATUS=$?
    set -e

    if [ "$RUN_STATUS" -ne 0 ]; then
        fail "tarball install: expected success, got failure: $RUN_OUTPUT"
    fi

    log=$(<"$FAKE_SUDO_LOG")
    case "$log" in
        *"install -o root -g root -m 0755 "*"/tarball/pln /usr/local/bin/pln"*) ;;
        *) fail "tarball install: expected root-owned install command, got $log" ;;
    esac
    case "$log" in
        *"/usr/local/bin/pln daemon install"*) ;;
        *) fail "tarball install: expected daemon provisioning, got $log" ;;
    esac
    unset SUDO
    rm -rf "$tmp"
}

assert_detect "debian uses apt" "apt" $'ID=debian' apt-get
assert_detect "ubuntu-like uses apt" "apt" $'ID=linuxmint\nID_LIKE="ubuntu debian"' apt-get dnf
assert_detect "fedora prefers dnf" "dnf" $'ID=fedora' dnf yum
assert_detect "rhel can use yum" "yum" $'ID=rocky\nID_LIKE="rhel centos fedora"' yum
assert_detect "rhel prefers yum over foreign dnf" "yum" $'ID=centos\nID_LIKE="rhel fedora"' dnf yum
assert_detect "opensuse ignores foreign dnf" "zypper" $'ID=opensuse-tumbleweed\nID_LIKE="opensuse suse"' dnf zypper
assert_refuses "unknown distro refuses" "unsupported Linux distro" $'ID=arch' dnf
assert_refuses "debian without apt refuses" "apt-get is not installed" $'ID=debian' dnf
assert_method "explicit tarball accepted" "tarball" 0 "tarball"
assert_method "package-manager override refused" "dnf" 1 "unknown install method"
assert_tarball_installs_root_owned_binary

printf 'ok - install detection\n'
