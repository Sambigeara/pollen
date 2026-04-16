package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
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
	payload := `{"hash":"` + hex.EncodeToString(sum[:]) + `","origin":` + string(input) + `}`

	req := "POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: " +
		strconv.Itoa(len(payload)) + "\r\n\r\n" + payload

	uriMem := pdk.AllocateString("pln://service/store")
	inputMem := pdk.AllocateBytes([]byte(req))
	outOffset := pollenRequest(uriMem.Offset(), inputMem.Offset())
	uriMem.Free()
	inputMem.Free()

	if outOffset == 0 {
		logStr(logLevelError, "processor: store request failed")
		pdk.Output([]byte(`{"error":"store request failed"}`))
		return 1
	}

	outMem := pdk.FindMemory(outOffset)
	outBuf := make([]byte, outMem.Length())
	outMem.Load(outBuf)
	outMem.Free()

	if !strings.Contains(string(outBuf), "200 OK") {
		logStr(logLevelError, "processor: store returned non-200")
		pdk.Output([]byte(`{"error":"store returned error"}`))
		return 1
	}

	logStr(logLevelInfo, "processor: stored")
	pdk.Output([]byte("stored"))
	return 0
}

func main() {}
