// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"

	"github.com/extism/go-pdk"
)

const logLevelInfo = 1

//go:wasmimport extism:host/user pollen_log
func pollenLog(level, msgOffset uint64)

func logStr(level uint64, s string) {
	mem := pdk.AllocateString(s)
	pollenLog(level, mem.Offset())
	mem.Free()
}

//go:wasmexport handle
func handle() int32 {
	input := string(pdk.Input())
	out := strings.ToUpper(input)
	logStr(logLevelInfo, "upper: "+out)
	pdk.Output([]byte(out))
	return 0
}

func main() {}
