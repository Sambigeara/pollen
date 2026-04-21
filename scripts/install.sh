#!/usr/bin/env bash
# Usage:
#   curl -fsSL https://github.com/sambigeara/pollen/releases/latest/download/install.sh | bash
#   curl -fsSL https://github.com/sambigeara/pollen/releases/latest/download/install.sh | bash -s -- --version v1.2.3
#
# The outer brace block ensures the whole script is parsed before any
# command runs, so a truncated download fails to parse rather than
# executing a partial installer.

set -euo pipefail
{

REPO="sambigeara/pollen"
# Goreleaser rewrites this to the release tag; the untouched sentinel
# triggers the API fallback below.
VERSION="__VERSION__"

log()   { printf '==> %s\n' "$*"; }
fatal() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<EOF
Usage: install.sh [--version TAG]

Installs the pln binary. On Linux, also registers a systemd unit when
systemd is present; the service is not started automatically. Run
'sudo pln join <token>' after install to enrol this node.
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version)
            [ "$#" -ge 2 ] || fatal "--version requires a value"
            VERSION="$2"
            shift 2
            ;;
        -h|--help) usage; exit 0 ;;
        *) fatal "unknown argument: $1 (try --help)" ;;
    esac
done

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)        ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)             fatal "unsupported architecture: $ARCH" ;;
esac

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

need_sudo() {
    if [ "$(id -u)" -eq 0 ]; then
        SUDO=""
    elif command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
    else
        fatal "run as root or install sudo"
    fi
}

resolve_version() {
    [ "$VERSION" = "__VERSION__" ] || return 0
    log "Resolving latest release..."
    local resp
    resp=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest") \
        || fatal "could not reach GitHub API"
    VERSION=$(printf '%s' "$resp" | grep -m1 '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    [ -n "$VERSION" ] || fatal "could not determine latest version"
}

asset_url() {
    local ext=$1
    printf 'https://github.com/%s/releases/download/%s/pln_%s_linux_%s.%s' \
        "$REPO" "$VERSION" "${VERSION#v}" "$ARCH" "$ext"
}

install_macos() {
    log "macOS detected; installing via Homebrew..."
    command -v brew >/dev/null 2>&1 || fatal "Homebrew is required. See https://brew.sh"
    brew install sambigeara/homebrew-pln/pln
    log "Done. Start the service with: pln up -d"
}

install_deb() {
    log "Installing .deb via apt..."
    local deb="$TMPDIR/pln.deb"
    curl -fsSL "$(asset_url deb)" -o "$deb"
    $SUDO env NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive \
        apt-get install -y --allow-downgrades "$deb"
}

install_rpm() {
    local mgr=$1
    log "Installing .rpm via $mgr..."
    $SUDO "$mgr" install -y "$(asset_url rpm)"
}

install_tarball() {
    log "No supported package manager; installing from tarball..."
    local tar="$TMPDIR/pln.tar.gz"
    curl -fsSL "$(asset_url tar.gz)" -o "$tar"
    $SUDO tar -xzf "$tar" -C /usr/local/bin pln
    $SUDO /usr/local/bin/pln service install
}

install_linux() {
    log "Linux detected."
    need_sudo
    resolve_version
    log "Installing version $VERSION..."
    if command -v apt-get >/dev/null 2>&1; then
        install_deb
    elif command -v dnf >/dev/null 2>&1; then
        install_rpm dnf
    elif command -v yum >/dev/null 2>&1; then
        install_rpm yum
    else
        install_tarball
    fi
    log "Done. Enrol this node with: sudo pln join <token>"
}

case "$OS" in
    darwin) install_macos ;;
    linux)  install_linux ;;
    *)      fatal "unsupported OS: $OS" ;;
esac

}
