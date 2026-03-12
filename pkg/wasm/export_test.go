package wasm

import (
	"context"

	"github.com/tetratelabs/wazero"
)

// SetCompilerFactory replaces the compiler runtime constructor and returns a
// restore function. Intended for tests only.
func SetCompilerFactory(f func(ctx context.Context, memPages uint32) (wazero.Runtime, error)) func() {
	old := makeCompilerRuntime
	makeCompilerRuntime = f
	return func() { makeCompilerRuntime = old }
}
