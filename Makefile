include tools/tools.mk

FILE     = VERSION
VERSION := $(shell git describe --abbrev=0 --tags 2>/dev/null)
DATE    := $(shell git log -1 --format=%ct $(VERSION) 2>/dev/null)
GEN_DIR := proto/genpb

.PHONY: test
test:
	go test ./... -count=1

.PHONY: generate
generate: generate-proto-code

# `buf mod update` run in `proto/public` after adding protovalidate as dependency to ensure it's available
.PHONY: generate-proto-code
generate-proto-code: $(BUF)
	@-rm -rf $(GEN_DIR)/fzn
	@ $(BUF) format -w
	@ cd tools && $(BUF) generate --template=api.gen.yaml --output=.. ..
	@ GOWORK=off go mod tidy -C $(GEN_DIR)
