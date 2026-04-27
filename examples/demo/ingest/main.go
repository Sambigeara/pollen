// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// ingest is the first seed in the firehose chain. It timestamps the inbound
// payload and forwards it to terminal via pln://seed/terminal/handle.
package main

import (
	"strconv"
	"time"

	"github.com/extism/go-pdk"
)

const logLevelInfo = 1

//go:wasmimport extism:host/user pollen_log
func pollenLog(level, msgOffset uint64)

//go:wasmimport extism:host/user pollen_request
func pollenRequest(uriOffset, inputOffset uint64) uint64

func logStr(s string) {
	mem := pdk.AllocateString(s)
	pollenLog(logLevelInfo, mem.Offset())
	mem.Free()
}

//go:wasmexport handle
func handle() int32 {
	payload := []byte(`{"ts":` + strconv.FormatInt(time.Now().Unix(), 10) + `,"data":"` + string(pdk.Input()) + `"}`)

	uriMem := pdk.AllocateString("pln://seed/terminal/handle")
	inputMem := pdk.AllocateBytes(payload)
	outOffset := pollenRequest(uriMem.Offset(), inputMem.Offset())
	uriMem.Free()
	inputMem.Free()

	if outOffset == 0 {
		pdk.Output([]byte(`{"error":"downstream call failed"}`))
		return 1
	}

	outMem := pdk.FindMemory(outOffset)
	outBuf := make([]byte, outMem.Length())
	outMem.Load(outBuf)
	outMem.Free()

	pdk.Output(outBuf)
	logStr("ingest: forwarded to terminal")
	return 0
}

func main() {}
