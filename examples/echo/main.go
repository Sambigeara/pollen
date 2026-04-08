package main

import "github.com/extism/go-pdk"

const logLevelInfo = 1

//go:wasmimport extism:host/user pollen_log
func pollenLog(level, msgOffset uint64)

//go:wasmimport extism:host/user pollen_caller_info
func pollenCallerInfo() uint64

//export handle
func handle() int32 {
	input := pdk.Input()

	if offset := pollenCallerInfo(); offset != 0 {
		mem := pdk.FindMemory(offset)
		buf := make([]byte, mem.Length())
		mem.Load(buf)
		mem.Free()
		msg := pdk.AllocateString("caller: " + string(buf))
		pollenLog(logLevelInfo, msg.Offset())
		msg.Free()
	}

	msg := pdk.AllocateString("echo: " + string(input))
	pollenLog(logLevelInfo, msg.Offset())
	msg.Free()

	pdk.Output(input)
	return 0
}

func main() {}
