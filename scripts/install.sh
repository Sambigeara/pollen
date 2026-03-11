#!/usr/bin/env bash
set -e
{

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
    brew install sambigeara/homebrew-pln/pln
    log "Done! Run 'pln up -d' to start the background service."
    exit 0
fi

if [ "$OS" = "linux" ]; then
    if [ -z "$VERSION" ]; then
        log "Linux detected. Finding latest release..."
        API_RESP=$(curl -sS "https://api.github.com/repos/$REPO/releases/latest" 2>&1) || fatal "Could not reach GitHub API."
        VERSION=$(echo "$API_RESP" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
        [ -z "$VERSION" ] && fatal "Could not determine latest version. GitHub response:\n$API_RESP"
    else
        log "Linux detected. Installing version ${VERSION}..."
    fi

    PKG_VER="${VERSION#v}"

    if command -v apt-get >/dev/null 2>&1; then
        log "Installing .deb via apt..."
        DEB_URL="https://github.com/$REPO/releases/download/${VERSION}/pln_${PKG_VER}_linux_${ARCH}.deb"
        curl -fsSL "$DEB_URL" -o /tmp/pln.deb
        sudo NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive apt-get install -y /tmp/pln.deb
    elif command -v dnf >/dev/null 2>&1 || command -v yum >/dev/null 2>&1; then
        RPM_MGR=$(command -v dnf >/dev/null 2>&1 && echo dnf || echo yum)
        log "Installing .rpm via $RPM_MGR..."
        RPM_URL="https://github.com/$REPO/releases/download/${VERSION}/pln_${PKG_VER}_linux_${ARCH}.rpm"
        sudo "$RPM_MGR" install -y "$RPM_URL"
    else
        log "No supported package manager found. Installing from tarball..."
        TAR_URL="https://github.com/$REPO/releases/download/${VERSION}/pln_${PKG_VER}_linux_${ARCH}.tar.gz"
        curl -fsSL "$TAR_URL" -o /tmp/pln.tar.gz
        sudo tar -xzf /tmp/pln.tar.gz -C /usr/local/bin pln
        rm -f /tmp/pln.tar.gz
        log "WARNING: No systemd service installed. You must manage the pln process manually."
    fi

    getent group pln >/dev/null 2>&1 || sudo groupadd --system pln
    id -u pln >/dev/null 2>&1 || sudo useradd -r -d /var/lib/pln -s /usr/sbin/nologin -g pln pln
    sudo install -d -m 0770 -o pln -g pln /var/lib/pln
    sudo install -d -m 0770 -o pln -g pln /var/lib/pln/keys

    sudo usermod -aG pln "$(whoami)" 2>/dev/null || true
    log "Done! Run 'sudo pln join <token>' to enroll this node."
    exit 0
fi

fatal "Unsupported OS: $OS"
}
