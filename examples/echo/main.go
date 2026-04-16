package main

import "github.com/extism/go-pdk"

const (
	logLevelInfo  = 1
	logLevelError = 3
)

//go:wasmimport extism:host/user pollen_log
func pollenLog(level, msgOffset uint64)

//go:wasmimport extism:host/user pollen_caller_info
func pollenCallerInfo() uint64

//go:wasmimport extism:host/user pollen_request
func pollenRequest(uriOffset, inputOffset uint64) uint64

func logStr(level uint64, s string) {
	mem := pdk.AllocateString(s)
	pollenLog(level, mem.Offset())
	mem.Free()
}

//go:wasmexport echo
func echo() int32 {
	input := pdk.Input()

	if offset := pollenCallerInfo(); offset != 0 {
		mem := pdk.FindMemory(offset)
		buf := make([]byte, mem.Length())
		mem.Load(buf)
		mem.Free()
		logStr(logLevelInfo, "caller: "+string(buf))
	}

	logStr(logLevelInfo, "echo: "+string(input))
	pdk.Output(input)
	return 0
}

//go:wasmexport shout
func shout() int32 {
	input := pdk.Input()

	uriMem := pdk.AllocateString("pln://seed/upper/handle")
	inputMem := pdk.AllocateBytes(input)
	outOffset := pollenRequest(uriMem.Offset(), inputMem.Offset())
	uriMem.Free()
	inputMem.Free()

	if outOffset == 0 {
		logStr(logLevelError, "upper call failed")
		pdk.Output([]byte(`{"error":"upper call failed"}`))
		return 1
	}

	outMem := pdk.FindMemory(outOffset)
	outBuf := make([]byte, outMem.Length())
	outMem.Load(outBuf)
	outMem.Free()

	logStr(logLevelInfo, "shout: "+string(outBuf))
	pdk.Output(outBuf)
	return 0
}

func main() {}
