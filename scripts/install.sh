#!/usr/bin/env bash
set -euo pipefail

APP="pollen"
REPO="${POLLEN_REPO:-sambigeara/pollen}"
BASE_URL="https://github.com/${REPO}/releases"

VERSION=""
INSTALL_DIR=""
ALLOW_BREAKING=false
FORCE=false

usage() {
	cat <<'EOF'
Pollen installer (linux + macOS)

Usage:
  install.sh [options]

Options:
  --version <vX.Y.Z>   Install a specific release (default: latest)
  --install-dir <dir>  Install directory (default: /usr/local/bin or ~/.local/bin)
  --allow-breaking     Allow semver-breaking upgrades
  --force              Reinstall even if the same version is already installed
  -h, --help           Show help

Examples:
  curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash
  curl -fsSL https://raw.githubusercontent.com/sambigeara/pollen/main/scripts/install.sh | bash -s -- --version v0.2.0
EOF
}

log() {
	printf '%s\n' "$*"
}

warn() {
	printf 'warning: %s\n' "$*" >&2
}

fatal() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

need_cmd() {
	command -v "$1" >/dev/null 2>&1 || fatal "required command not found: $1"
}

download_to() {
	local url="$1"
	local out="$2"

	if command -v curl >/dev/null 2>&1; then
		curl -fsSL "$url" -o "$out"
		return 0
	fi

	if command -v wget >/dev/null 2>&1; then
		wget -q "$url" -O "$out"
		return 0
	fi

	fatal "installer requires curl or wget"
}

fetch_text() {
	local url="$1"

	if command -v curl >/dev/null 2>&1; then
		curl -fsSL "$url"
		return 0
	fi

	if command -v wget >/dev/null 2>&1; then
		wget -q -O- "$url"
		return 0
	fi

	fatal "installer requires curl or wget"
}

normalize_version() {
	local v="$1"
	v="${v#v}"
	printf 'v%s\n' "$v"
}

strip_v() {
	local v="$1"
	printf '%s\n' "${v#v}"
}

semver_core() {
	local v
	v="$(strip_v "$1")"
	printf '%s\n' "${v%%-*}"
}

semver_parse() {
	local core
	core="$(semver_core "$1")"
	IFS='.' read -r major minor patch <<EOF
$core
EOF

	case "${major:-}" in
	'' | *[!0-9]*) return 1 ;;
	esac
	case "${minor:-}" in
	'' | *[!0-9]*) return 1 ;;
	esac
	case "${patch:-}" in
	'' | *[!0-9]*) return 1 ;;
	esac

	printf '%s %s %s\n' "$major" "$minor" "$patch"
}

is_breaking_upgrade() {
	local current="$1"
	local target="$2"

	local c_major c_minor c_patch
	local t_major t_minor t_patch

	if ! read -r c_major c_minor c_patch < <(semver_parse "$current"); then
		return 1
	fi
	if ! read -r t_major t_minor t_patch < <(semver_parse "$target"); then
		return 1
	fi

	if [ "$t_major" -gt "$c_major" ]; then
		return 0
	fi

	if [ "$c_major" -gt "$t_major" ]; then
		return 1
	fi

	if [ "$t_major" -eq 0 ] && [ "$t_minor" -gt "$c_minor" ]; then
		return 0
	fi

	return 1
}

compare_versions_equal() {
	local a b
	a="$(normalize_version "$1")"
	b="$(normalize_version "$2")"
	[ "$a" = "$b" ]
}

detect_platform() {
	local os arch uname_os uname_arch
	uname_os="$(uname -s)"
	uname_arch="$(uname -m)"

	case "$uname_os" in
	Linux) os="linux" ;;
	Darwin) os="darwin" ;;
	*) fatal "unsupported OS: ${uname_os} (linux and macOS only)" ;;
	esac

	case "$uname_arch" in
	x86_64 | amd64) arch="amd64" ;;
	arm64 | aarch64) arch="arm64" ;;
	*) fatal "unsupported architecture: ${uname_arch}" ;;
	esac

	# Detect Rosetta 2: if running as x86_64 under translation on Apple Silicon,
	# prefer the native arm64 binary
	if [ "$os" = "darwin" ] && [ "$arch" = "amd64" ]; then
		if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = "1" ]; then
			arch="arm64"
		fi
	fi

	printf '%s %s\n' "$os" "$arch"
}

latest_version() {
	local api tag
	api="https://api.github.com/repos/${REPO}/releases/latest"
	tag="$(fetch_text "$api" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
	[ -n "$tag" ] || fatal "failed to resolve latest release tag from ${api}"
	normalize_version "$tag"
}

checksum_line_for_file() {
	local checksums_file="$1"
	local target_file="$2"
	awk -v file="$target_file" '$2==file {print $0}' "$checksums_file"
}

verify_checksum() {
	local checksums_file="$1"
	local target_file="$2"
	local archive_path="$3"
	local line expected actual

	line="$(checksum_line_for_file "$checksums_file" "$target_file")"
	[ -n "$line" ] || fatal "checksum not found for ${target_file}"
	expected="${line%% *}"

	# Validate checksum format (SHA256 = 64 hex characters)
	if [[ ! "$expected" =~ ^[a-f0-9]{64}$ ]]; then
		fatal "invalid checksum format in checksums file for ${target_file}"
	fi

	if command -v sha256sum >/dev/null 2>&1; then
		actual="$(sha256sum "$archive_path" | awk '{print $1}')"
	elif command -v shasum >/dev/null 2>&1; then
		actual="$(shasum -a 256 "$archive_path" | awk '{print $1}')"
	else
		fatal "need sha256sum or shasum to verify checksums"
	fi

	[ "$expected" = "$actual" ] || fatal "checksum mismatch for ${target_file}"
}

default_install_dir() {
	# If pollen is already installed somewhere on PATH, upgrade in-place to
	# avoid ending up with stale copies in two different directories.
	local existing
	if existing="$(command -v "$APP" 2>/dev/null)"; then
		printf '%s\n' "$(dirname "$existing")"
		return 0
	fi

	if [ -w /usr/local/bin ]; then
		printf '%s\n' "/usr/local/bin"
		return 0
	fi

	if [ "$(id -u)" -eq 0 ]; then
		printf '%s\n' "/usr/local/bin"
		return 0
	fi

	# Only default to /usr/local/bin if sudo won't prompt for a password
	# (e.g. NOPASSWD on EC2 Ubuntu). Otherwise fall through to ~/.local/bin.
	if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
		printf '%s\n' "/usr/local/bin"
		return 0
	fi

	printf '%s\n' "${HOME}/.local/bin"
}

run_as_root() {
	if [ "$(id -u)" -eq 0 ]; then
		"$@"
		return 0
	fi

	if command -v sudo >/dev/null 2>&1; then
		sudo "$@"
		return 0
	fi

	fatal "need root privileges to write to ${INSTALL_DIR}; re-run as root or use --install-dir"
}

install_binary() {
	local src_bin="$1"
	local dst_bin="${INSTALL_DIR}/${APP}"
	local tmp_bin
	tmp_bin="${INSTALL_DIR}/.${APP}.tmp.$$"

	chmod 0755 "$src_bin"

	if [ -w "$INSTALL_DIR" ]; then
		install -m 0755 "$src_bin" "$tmp_bin"
		mv -f "$tmp_bin" "$dst_bin"
	else
		run_as_root install -d -m 0755 "$INSTALL_DIR"
		run_as_root install -m 0755 "$src_bin" "$tmp_bin"
		run_as_root mv -f "$tmp_bin" "$dst_bin"
	fi

	log "installed ${APP} to ${dst_bin}"
}

current_installed_version() {
	local path="$1"
	if [ ! -x "$path" ]; then
		return 1
	fi

	"$path" version --short 2>/dev/null || return 1
}

parse_args() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		--version)
			[ "$#" -ge 2 ] || fatal "--version requires a value"
			VERSION="$(normalize_version "$2")"
			shift 2
			;;
		--install-dir)
			[ "$#" -ge 2 ] || fatal "--install-dir requires a value"
			INSTALL_DIR="$2"
			shift 2
			;;
		--allow-breaking)
			ALLOW_BREAKING=true
			shift
			;;
		--force)
			FORCE=true
			shift
			;;
		-h | --help)
			usage
			exit 0
			;;
		*)
			fatal "unknown option: $1"
			;;
		esac
	done

}

main() {
	parse_args "$@"
	need_cmd tar
	need_cmd awk
	need_cmd sed

	if [ -z "$INSTALL_DIR" ]; then
		INSTALL_DIR="$(default_install_dir)"
	fi

	if [ -z "$VERSION" ]; then
		VERSION="$(latest_version)"
	else
		VERSION="$(normalize_version "$VERSION")"
	fi

	local os arch archive_name archive_url checksums_url
	read -r os arch < <(detect_platform)

	archive_name="${APP}_$(strip_v "$VERSION")_${os}_${arch}.tar.gz"
	archive_url="${BASE_URL}/download/${VERSION}/${archive_name}"
	checksums_url="${BASE_URL}/download/${VERSION}/checksums.txt"

	TMP_DIR="$(mktemp -d)"
	trap 'rm -rf "${TMP_DIR}"' EXIT

	log "installing ${APP} ${VERSION} for ${os}/${arch}"

	local target_bin current_ver
	target_bin="${INSTALL_DIR}/${APP}"
	if current_ver="$(current_installed_version "$target_bin")"; then
		if compare_versions_equal "$current_ver" "$VERSION" && [ "$FORCE" != true ]; then
			log "${APP} ${VERSION} is already installed at ${target_bin}"
			exit 0
		fi

		if is_breaking_upgrade "$current_ver" "$VERSION"; then
			if [ "$ALLOW_BREAKING" != true ]; then
				fatal "refusing potentially breaking upgrade ${current_ver} -> ${VERSION}; re-run with --allow-breaking"
			fi
			warn "allowing potentially breaking upgrade ${current_ver} -> ${VERSION}"
		fi
	fi

	local archive_path checksums_path extracted_bin
	archive_path="${TMP_DIR}/${archive_name}"
	checksums_path="${TMP_DIR}/checksums.txt"

	download_to "$archive_url" "$archive_path"
	download_to "$checksums_url" "$checksums_path"
	verify_checksum "$checksums_path" "$archive_name" "$archive_path"

	tar -xzf "$archive_path" -C "$TMP_DIR" "$APP"
	extracted_bin="${TMP_DIR}/${APP}"
	[ -f "$extracted_bin" ] || fatal "release archive did not contain ${APP} binary"

	mkdir -p "$INSTALL_DIR" 2>/dev/null || true
	install_binary "$extracted_bin"

	if [ "$INSTALL_DIR" = "${HOME}/.local/bin" ] && ! printf '%s' ":${PATH}:" | grep -q ":${HOME}/.local/bin:"; then
		warn "${INSTALL_DIR} is not on PATH"
		warn "add this to your shell profile: export PATH=${INSTALL_DIR}:\$PATH"
	fi

	log "done"
	log ""
	log "run 'pollen up -d' to configure autostart"
}

main "$@"
