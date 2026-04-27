// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// terminal is the second seed in the firehose chain. It hashes the input
// payload (matching processor's shape of work) and forwards a tick to
// pln://service/sink — the sink runs on the operator's local node and
// renders a live RPS display.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/extism/go-pdk"
)

const (
	logLevelInfo  = 1
	logLevelError = 3
)

//go:wasmimport extism:host/user pollen_log
func pollenLog(level, msgOffset uint64)

//go:wasmimport extism:host/user pollen_request
func pollenRequest(uriOffset, inputOffset uint64) uint64

func logStr(level uint64, s string) {
	mem := pdk.AllocateString(s)
	pollenLog(level, mem.Offset())
	mem.Free()
}

//go:wasmexport handle
func handle() int32 {
	input := pdk.Input()
	sum := sha256.Sum256(input)
	hash := hex.EncodeToString(sum[:])

	// Empty-body POST keeps the wire payload minimal so the chain measures
	// the mesh + sink hot path rather than per-request body throughput.
	req := "POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"

	uriMem := pdk.AllocateString("pln://service/sink")
	inputMem := pdk.AllocateBytes([]byte(req))
	outOffset := pollenRequest(uriMem.Offset(), inputMem.Offset())
	uriMem.Free()
	inputMem.Free()

	if outOffset == 0 {
		logStr(logLevelError, "terminal: sink request failed")
		pdk.Output([]byte(`{"error":"sink request failed"}`))
		return 1
	}

	outMem := pdk.FindMemory(outOffset)
	outBuf := make([]byte, outMem.Length())
	outMem.Load(outBuf)
	outMem.Free()

	if !strings.Contains(string(outBuf), "200 OK") {
		logStr(logLevelError, "terminal: sink returned non-200")
		pdk.Output([]byte(`{"error":"sink returned error"}`))
		return 1
	}

	logStr(logLevelInfo, "terminal: forwarded to sink")
	pdk.Output([]byte(`{"hash":"` + hash + `","sink":true}`))
	return 0
}

func main() {}
