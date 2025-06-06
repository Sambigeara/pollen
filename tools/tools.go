package tools

import (
	_ "buf.build/go/protovalidate"
	_ "connectrpc.com/connect/cmd/protoc-gen-connect-go"
	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
	_ "gotest.tools/gotestsum"
)
