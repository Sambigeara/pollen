// terminal is the end-of-chain seed for the firehose demo variant.
// It does the same shape of work as processor (hash the payload) but
// returns directly without calling any downstream service. Used to
// measure the mesh's seed-to-seed throughput in isolation from the
// SQLite-constrained sink path exercised by processor → service/store.
package main

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/extism/go-pdk"
)

const logLevelInfo = 1

//go:wasmimport extism:host/user pollen_log
func pollenLog(level, msgOffset uint64)

func logStr(s string) {
	mem := pdk.AllocateString(s)
	pollenLog(logLevelInfo, mem.Offset())
	mem.Free()
}

//go:wasmexport handle
func handle() int32 {
	input := pdk.Input()
	sum := sha256.Sum256(input)
	out := []byte(`{"hash":"` + hex.EncodeToString(sum[:]) + `","terminal":true}`)
	logStr("terminal: returned")
	pdk.Output(out)
	return 0
}

func main() {}
