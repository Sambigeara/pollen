package main

import "github.com/extism/go-pdk"

//go:wasmexport handle
func handle() int32 {
	pdk.Output(pdk.Input())
	return 0
}

func main() {}
