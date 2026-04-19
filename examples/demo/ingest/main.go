// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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

// forward wraps a single pollen_request call, returning the exported-function
// status code that ingest should propagate back through Pollen.
func forward(uri string, payload []byte) int32 {
	uriMem := pdk.AllocateString(uri)
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
	return 0
}

func timestampedPayload(input []byte) []byte {
	return []byte(`{"ts":` + strconv.FormatInt(time.Now().Unix(), 10) + `,"data":"` + string(input) + `"}`)
}

// handle is the entry point for the SQLite-constrained chain:
// ingest → processor → pln://service/store.
//
//go:wasmexport handle
func handle() int32 {
	rc := forward("pln://seed/processor/handle", timestampedPayload(pdk.Input()))
	if rc == 0 {
		logStr("ingest: forwarded to processor")
	}
	return rc
}

// handle_firehose is the entry point for the mesh-only chain:
// ingest → terminal. No exit-service call, no SQLite. Used to isolate
// the mesh's own throughput from the sink's constraints when comparing
// the two demo scenarios side by side.
//
//go:wasmexport handle_firehose
func handle_firehose() int32 { //nolint:revive // wasm export name matches guest convention.
	rc := forward("pln://seed/terminal/handle", timestampedPayload(pdk.Input()))
	if rc == 0 {
		logStr("ingest: forwarded to terminal")
	}
	return rc
}

func main() {}
