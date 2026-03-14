package wasm

import (
	"context"

	extism "github.com/extism/go-sdk"
	"go.uber.org/zap"
)

const (
	logLevelDebug uint64 = 0
	logLevelInfo  uint64 = 1
	logLevelWarn  uint64 = 2
)

// InvocationRouter handles inter-workload RPC through the mesh.
type InvocationRouter interface {
	RouteCall(ctx context.Context, targetHash, function string, input []byte) ([]byte, error)
}

// NewHostFunctions creates the Extism host functions exposed to guest WASM modules.
//
// pollen_log(level i64, msg_offset i64): guest writes a log message to the host logger.
//
//	level: 0=debug, 1=info, 2=warn, 3=error
//
// pollen_call(hash_offset i64, func_offset i64, input_offset i64) -> output_offset i64:
//
//	guest calls another workload through the mesh. Returns offset to output bytes (0 on error).
func NewHostFunctions(logger *zap.SugaredLogger, router InvocationRouter) []extism.HostFunction {
	logFn := extism.NewHostFunctionWithStack(
		"pollen_log",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			level := stack[0]
			msg, err := p.ReadString(stack[1])
			if err != nil {
				logger.Warnw("pollen_log: failed to read message", "err", err)
				return
			}
			switch level {
			case logLevelDebug:
				logger.Debugw(msg)
			case logLevelInfo:
				logger.Infow(msg)
			case logLevelWarn:
				logger.Warnw(msg)
			default:
				logger.Errorw(msg)
			}
		},
		[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64},
		nil,
	)

	callFn := extism.NewHostFunctionWithStack(
		"pollen_call",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			targetHash, err := p.ReadString(stack[0])
			if err != nil {
				logger.Warnw("pollen_call: failed to read hash", "err", err)
				stack[0] = 0
				return
			}
			function, err := p.ReadString(stack[1])
			if err != nil {
				logger.Warnw("pollen_call: failed to read function", "err", err)
				stack[0] = 0
				return
			}
			input, err := p.ReadBytes(stack[2])
			if err != nil {
				logger.Warnw("pollen_call: failed to read input", "err", err)
				stack[0] = 0
				return
			}

			output, err := router.RouteCall(ctx, targetHash, function, input)
			if err != nil {
				logger.Warnw("pollen_call: route failed",
					"target", targetHash, "function", function, "err", err)
				stack[0] = 0
				return
			}

			offset, err := p.WriteBytes(output)
			if err != nil {
				logger.Warnw("pollen_call: failed to write output", "err", err)
				stack[0] = 0
				return
			}
			stack[0] = offset
		},
		[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64, extism.ValueTypeI64},
		[]extism.ValueType{extism.ValueTypeI64},
	)

	return []extism.HostFunction{logFn, callFn}
}
