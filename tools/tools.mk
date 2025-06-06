XDG_CACHE_HOME ?= $(HOME)/.cache
TOOLS_BIN_DIR := $(abspath $(XDG_CACHE_HOME)/pollen/bin)
TOOLS_MOD := tools/go.mod

BUF := $(TOOLS_BIN_DIR)/buf

$(TOOLS_BIN_DIR):
	@ mkdir -p $(TOOLS_BIN_DIR)

$(BUF): $(TOOLS_BIN_DIR)
	@ GOWORK=off GOBIN=$(TOOLS_BIN_DIR) go install -modfile=$(TOOLS_MOD) github.com/bufbuild/buf/cmd/buf
