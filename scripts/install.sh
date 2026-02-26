#!/usr/bin/env bash
set -e

REPO="sambigeara/pollen"
VERSION=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version) VERSION="$2"; shift 2 ;;
        *) shift ;;
    esac
done

log() { echo "==> $1"; }
fatal() { echo "ERROR: $1" >&2; exit 1; }

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) fatal "Unsupported architecture: $ARCH" ;;
esac

if [ "$OS" = "darwin" ]; then
    log "macOS detected. Installing via Homebrew..."
    command -v brew >/dev/null 2>&1 || fatal "Homebrew is required but not installed."
    brew install sambigeara/homebrew-pollen/pollen
    log "Done! Run 'brew services start pollen' to run in the background."
    exit 0
fi

if [ "$OS" = "linux" ]; then
    if [ -z "$VERSION" ]; then
        log "Linux detected. Finding latest release..."
        VERSION=$(curl -s "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
        [ -z "$VERSION" ] && fatal "Could not determine latest version."
    else
        log "Linux detected. Installing version ${VERSION}..."
    fi

    PKG_VER="${VERSION#v}"

    if command -v apt-get >/dev/null 2>&1; then
        log "Debian/Ubuntu detected. Installing .deb..."
        DEB_URL="https://github.com/$REPO/releases/download/${VERSION}/pollen_${PKG_VER}_linux_${ARCH}.deb"
        curl -sL "$DEB_URL" -o /tmp/pollen.deb
        sudo apt-get install -y /tmp/pollen.deb
        log "Done! Run 'sudo pollen join <token>' to enroll this node."
        exit 0
    elif command -v yum >/dev/null 2>&1; then
        log "RHEL/Fedora detected. Installing .rpm..."
        RPM_URL="https://github.com/$REPO/releases/download/${VERSION}/pollen_${PKG_VER}_linux_${ARCH}.rpm"
        sudo yum install -y "$RPM_URL"
        log "Done! Run 'sudo pollen join <token>' to enroll this node."
        exit 0
    else
        fatal "No supported package manager found (apt or yum)."
    fi
fi

fatal "Unsupported OS: $OS"
