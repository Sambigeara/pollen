#!/usr/bin/env bash

set -euo pipefail
{

REPO="sambigeara/pollen"
# Goreleaser rewrites this to the release tag; the untouched sentinel
# triggers the API fallback below.
VERSION="__VERSION__"
# auto uses /etc/os-release to select a native package manager only when the
# distro match is explicit. tarball is intentionally opt-in for unknown Linux.
METHOD="auto"
OS_RELEASE_FILE="${OS_RELEASE_FILE:-/etc/os-release}"
OS=""
ARCH=""
INSTALL_TMPDIR=""

log()   { printf '==> %s\n' "$*" >&2; }
fatal() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<EOF
Usage: install.sh [--version TAG] [--method METHOD]

Installs the pln binary. On Linux, also registers a systemd unit when
systemd is present; the service is not started automatically. Run
'sudo pln join <token>' after install to enrol this node.

Options:
  --version TAG     install a specific release (default: latest)
  --method METHOD   Linux install method (auto|tarball; default: auto)

auto reads /etc/os-release and only uses apt, dnf, yum, or zypper when the
running distro confidently maps to that native package manager. Unknown Linux
distros are refused instead of guessed; pass --method tarball to opt in to a
/usr/local/bin binary install with daemon provisioning.
EOF
}

parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version)
                [ "$#" -ge 2 ] || fatal "--version requires a value"
                VERSION="$2"
                shift 2
                ;;
            --method)
                [ "$#" -ge 2 ] || fatal "--method requires a value"
                METHOD="$2"
                shift 2
                ;;
            -h|--help) usage; exit 0 ;;
            *) fatal "unknown argument: $1 (try --help)" ;;
        esac
    done
}

validate_method() {
    case "$METHOD" in
        auto|tarball) ;;
        *) fatal "unknown install method: $METHOD (expected auto|tarball)" ;;
    esac
}

detect_platform() {
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64)        ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *)             fatal "unsupported architecture: $ARCH" ;;
    esac
}

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

asset_name() {
    local ext=$1
    printf 'pln_%s_linux_%s.%s' "${VERSION#v}" "$ARCH" "$ext"
}

asset_url() {
    local ext=$1
    printf 'https://github.com/%s/releases/download/%s/%s' \
        "$REPO" "$VERSION" "$(asset_name "$ext")"
}

checksums_url() {
    printf 'https://github.com/%s/releases/download/%s/checksums.txt' "$REPO" "$VERSION"
}

checksum_for_asset() {
    local asset=$1
    local checksums="$INSTALL_TMPDIR/checksums.txt"
    local sum name

    if [ ! -s "$checksums" ]; then
        curl -fsSL "$(checksums_url)" -o "$checksums" || fatal "could not download release checksums"
    fi

    while read -r sum name; do
        name=${name#./}
        if [ "$name" = "$asset" ]; then
            printf '%s' "$sum"
            return 0
        fi
    done <"$checksums"

    return 1
}

verify_checksum() {
    local file=$1
    local asset=$2
    local expected actual

    expected=$(checksum_for_asset "$asset") || fatal "checksum for $asset not found in release checksums"
    if command -v sha256sum >/dev/null 2>&1; then
        actual=$(sha256sum "$file")
    elif command -v shasum >/dev/null 2>&1; then
        actual=$(shasum -a 256 "$file")
    else
        fatal "sha256sum or shasum is required to verify downloads"
    fi
    actual=${actual%% *}

    [ "$actual" = "$expected" ] || fatal "checksum mismatch for $asset"
}

download_asset() {
    local ext=$1
    local output=$2
    local asset
    asset=$(asset_name "$ext")
    curl -fsSL "$(asset_url "$ext")" -o "$output"
    verify_checksum "$output" "$asset"
}

install_macos() {
    [ "$METHOD" = "auto" ] || fatal "--method is only supported on Linux"
    log "macOS detected; installing via Homebrew..."
    command -v brew >/dev/null 2>&1 || fatal "Homebrew is required. See https://brew.sh"
    brew install sambigeara/homebrew-pln/pln
    log "Done. Start the service with: pln up -d"
}

install_deb() {
    log "Installing .deb via apt..."
    local deb
    deb="$INSTALL_TMPDIR/$(asset_name deb)"
    download_asset deb "$deb"
    # mktemp -d gives us a 0700 dir; apt's _apt user can't read it and
    # falls back to "unsandboxed as root" with a Permission denied notice
    # that looks like a failure. Open the path so the sandbox works.
    chmod a+rx "$INSTALL_TMPDIR"
    chmod a+r "$deb"
    $SUDO env NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive \
        apt-get install -y --allow-downgrades "$deb"
}

install_rpm() {
    local mgr=$1
    local rpm
    rpm="$INSTALL_TMPDIR/$(asset_name rpm)"
    log "Installing .rpm via $mgr..."
    download_asset rpm "$rpm"
    case "$mgr" in
        zypper)
            # zypper aborts on unsigned packages even with --non-interactive;
            # nfpms doesn't sign rpms today, so verify checksums first.
            $SUDO "$mgr" --non-interactive install --allow-unsigned-rpm "$rpm"
            ;;
        *)
            $SUDO "$mgr" install -y "$rpm"
            ;;
    esac
}

install_tarball() {
    log "Installing from tarball..."
    local tar extract_dir
    tar="$INSTALL_TMPDIR/$(asset_name tar.gz)"
    extract_dir="$INSTALL_TMPDIR/tarball"
    mkdir "$extract_dir"
    download_asset tar.gz "$tar"
    tar -xzf "$tar" -C "$extract_dir" pln
    if [ ! -f "$extract_dir/pln" ] || [ -L "$extract_dir/pln" ]; then
        fatal "tarball did not contain a regular pln binary"
    fi
    $SUDO install -o root -g root -m 0755 "$extract_dir/pln" /usr/local/bin/pln
    $SUDO /usr/local/bin/pln daemon install
}

read_os_release_value() {
    local key=$1
    local line value

    [ -r "$OS_RELEASE_FILE" ] || return 1
    while IFS= read -r line; do
        case "$line" in
            "$key="*)
                value=${line#*=}
                case "$value" in
                    \"*\") value=${value#\"}; value=${value%\"} ;;
                    \'*\') value=${value#\'}; value=${value%\'} ;;
                esac
                printf '%s' "$value"
                return 0
                ;;
        esac
    done <"$OS_RELEASE_FILE"
    return 1
}

missing_native_manager() {
    local distro=$1
    local expected=$2
    fatal "detected $distro Linux but $expected is not installed; refusing to guess. Re-run with --method tarball to opt in to a /usr/local/bin binary install with daemon provisioning"
}

unsupported_linux() {
    local id=$1
    local id_like=$2
    fatal "unsupported Linux distro (ID=${id:-unknown}, ID_LIKE=${id_like:-none}); refusing to guess a package manager. Re-run with --method tarball to opt in to a /usr/local/bin binary install with daemon provisioning"
}

# Map the running distro to its native package manager. We parse
# /etc/os-release rather than source it or probe package-manager binaries:
# foreign managers may be installed for cross-distro tooling without being
# safe for this system (issue #4: dnf installed on openSUSE removed kernels).
detect_method() {
    local id id_like tokens
    id=$(read_os_release_value ID || true)
    id_like=$(read_os_release_value ID_LIKE || true)
    [ -n "$id$id_like" ] || unsupported_linux "$id" "$id_like"

    tokens=$(printf ' %s %s ' "$id" "$id_like" | tr '[:upper:]' '[:lower:]')
    case "$tokens" in
        *" debian "*|*" ubuntu "*|*" raspbian "*)
            command -v apt-get >/dev/null 2>&1 && { printf 'apt'; return; }
            missing_native_manager "Debian-like" "apt-get"
            ;;
        *" rhel "*|*" centos "*|*" rocky "*|*" almalinux "*|*" ol "*|*" amzn "*)
            command -v yum >/dev/null 2>&1 && { printf 'yum'; return; }
            command -v dnf >/dev/null 2>&1 && { printf 'dnf'; return; }
            missing_native_manager "Fedora/RHEL-like" "dnf or yum"
            ;;
        *" fedora "*)
            command -v dnf >/dev/null 2>&1 && { printf 'dnf'; return; }
            command -v yum >/dev/null 2>&1 && { printf 'yum'; return; }
            missing_native_manager "Fedora-like" "dnf or yum"
            ;;
        *" opensuse "*|*" opensuse-leap "*|*" opensuse-tumbleweed "*|*" suse "*|*" sles "*)
            command -v zypper >/dev/null 2>&1 && { printf 'zypper'; return; }
            missing_native_manager "SUSE-like" "zypper"
            ;;
    esac
    unsupported_linux "$id" "$id_like"
}

resolve_linux_method() {
    case "$METHOD" in
        auto)    detect_method ;;
        tarball) printf 'tarball' ;;
        *)       fatal "unknown install method: $METHOD (expected auto|tarball)" ;;
    esac
}

install_linux() {
    log "Linux detected."
    local method
    method=$(resolve_linux_method)
    log "Install method: $method"

    need_sudo
    resolve_version
    log "Installing version $VERSION..."

    local upgrade=false
    if command -v pln >/dev/null 2>&1; then
        upgrade=true
    fi

    case "$method" in
        apt)     install_deb ;;
        dnf)     install_rpm dnf ;;
        yum)     install_rpm yum ;;
        zypper)  install_rpm zypper ;;
        tarball) install_tarball ;;
        *)       fatal "internal error: resolved unknown install method: $method" ;;
    esac

    if [ "$upgrade" = true ]; then
        log "Done."
    else
        log "Done. Enrol this node with: sudo pln join <token>"
    fi
}

main() {
    parse_args "$@"
    validate_method
    detect_platform

    INSTALL_TMPDIR=$(mktemp -d)
    trap 'rm -rf "$INSTALL_TMPDIR"' EXIT

    case "$OS" in
        darwin) install_macos ;;
        linux)  install_linux ;;
        *)      fatal "unsupported OS: $OS" ;;
    esac
}

script_source=${BASH_SOURCE[0]-}
if [ -z "$script_source" ] || [ "$script_source" = "$0" ]; then
    main "$@"
fi

}
