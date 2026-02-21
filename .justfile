set dotenv-load := true

genpb_dir := join(justfile_directory(), "api", "genpb")
tools_mod_dir := join(justfile_directory(), "tools")

export TOOLS_BIN_DIR := join(env_var_or_default("XDG_CACHE_HOME", join(env_var("HOME"), ".cache")), "pollen/bin")

default:
    @just --list

build: generate lint

bin os='' arch='':
    @ CGO_ENABLED=0 GOOS={{os}} GOARCH={{arch}} \
      go build -o pollen ./cmd/pollen

compile:
    @ CGO_ENABLED=0 go build ./... && CGO_ENABLED=0 go test -run=ignore  ./... > /dev/null

generate: generate-proto-code

generate-proto-code: _buf
    #!/usr/bin/env bash
    set -euo pipefail
    cd {{ justfile_directory() }}
    "${TOOLS_BIN_DIR}/buf" format -w
    rm -rf {{ genpb_dir }}/pollen
    (
        cd {{ tools_mod_dir }}
        "${TOOLS_BIN_DIR}/buf" generate --template=api.gen.yaml --output=..
    )
    GOWORK=off go mod tidy -C {{ genpb_dir }}

lint: lint-modernize _golangcilint _buf
    @ "${TOOLS_BIN_DIR}/golangci-lint" run --config=.golangci.yaml --fix
    @ "${TOOLS_BIN_DIR}/buf" lint
    @ "${TOOLS_BIN_DIR}/buf" format --diff --exit-code

lint-modernize: _modernize
    @ "${TOOLS_BIN_DIR}/modernize" -fix -test ./...

test PKG='./...' TEST='.*':
    @ go test -v -failfast -cover -count=1 -run='{{ TEST }}' '{{ PKG }}'

deploy-bins:
    @ infra/scripts/deploy-bins.sh

# Executables

_buf: (_install "buf" "github.com/bufbuild/buf" "cmd/buf")

_golangcilint: (_install "golangci-lint" "github.com/golangci/golangci-lint/v2" "cmd/golangci-lint")

_modernize: (_install "modernize" "golang.org/x/tools/gopls" "internal/analysis/modernize/cmd/modernize")

_install EXECUTABLE MODULE CMD_PKG="":
    #!/usr/bin/env bash
    set -euo pipefail
    cd {{ tools_mod_dir }}
    TMP_VERSION=$(GOWORK=off go list -m -f "{{{{.Version}}" "{{ MODULE }}")
    VERSION="${TMP_VERSION#v}"
    BINARY="${TOOLS_BIN_DIR}/{{ EXECUTABLE }}"
    SYMLINK="${BINARY}-${VERSION}"
    if [[ ! -e "$SYMLINK" ]]; then
      echo "Installing $SYMLINK" 1>&2
      mkdir -p "$TOOLS_BIN_DIR"
      find "${TOOLS_BIN_DIR}" -lname "$BINARY" -delete
      if [[ "{{ EXECUTABLE }}" == "golangci-lint" ]]; then
        curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$TOOLS_BIN_DIR" "v${VERSION}"
      else
        export CGO_ENABLED={{ if EXECUTABLE =~ "(^sql|^tbls)" { "1" } else { "0" } }}
        GOWORK=off GOBIN="$TOOLS_BIN_DIR" go install {{ if CMD_PKG != "" { MODULE + "/" + CMD_PKG } else { MODULE } }}@v${VERSION}
      fi
      ln -s "$BINARY" "$SYMLINK"
    fi
