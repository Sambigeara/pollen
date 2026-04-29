// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"context"
	"errors"

	extism "github.com/extism/go-sdk"
	"go.uber.org/zap"
)

const (
	logLevelDebug uint64 = 0
	logLevelInfo  uint64 = 1
	logLevelWarn  uint64 = 2
	logLevelError uint64 = 3
)

// RequestRouter handles pln:// URI-addressed requests from WASM guests.
type RequestRouter interface {
	RouteRequest(ctx context.Context, uri URI, input []byte) ([]byte, error)
}

// NewHostFunctions creates the Extism host functions exposed to guest WASM modules.
//
// pollen_request(uri_offset i64, input_offset i64) -> output_offset i64:
//
//	guest calls a seed or service via a pln:// URI. Returns offset to output bytes (0 on error).
//	Seed:    pln://seed/<name>/<function>
//	Service: pln://service/<name>
//
// pollen_log(level i64, msg_offset i64): guest writes a log message to the host logger.
//
//	level: 0=debug, 1=info, 2=warn, 3=error
//
// pollen_caller_info() -> output_offset i64:
//
//	returns caller metadata as JSON ({"peerKey":"<hex>","attributes":{...}}).
//	Returns 0 if no caller info is available in the invocation context.
func NewHostFunctions(logger *zap.SugaredLogger, router RequestRouter) []extism.HostFunction {
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
			case logLevelError:
				logger.Errorw(msg)
			default:
				logger.Warnw("pollen_log: unknown log level", "level", level)
				logger.Errorw(msg)
			}
		},
		[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64},
		nil,
	)

	requestFn := extism.NewHostFunctionWithStack(
		"pollen_request",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			uriStr, err := p.ReadString(stack[0])
			if err != nil {
				logger.Warnw("pollen_request: failed to read URI", "err", err)
				stack[0] = 0
				return
			}
			input, err := p.ReadBytes(stack[1])
			if err != nil {
				logger.Warnw("pollen_request: failed to read input", "err", err)
				stack[0] = 0
				return
			}

			uri, err := ParseURI(uriStr)
			if err != nil {
				logger.Warnw("pollen_request: invalid URI", "uri", uriStr, "err", err)
				stack[0] = 0
				return
			}

			output, err := router.RouteRequest(ctx, uri, input)
			if err != nil {
				if errors.Is(err, ErrTargetNotFound) {
					logger.Infow("pollen_request: target not found", "uri", uriStr, "err", err)
				} else {
					logger.Warnw("pollen_request: route failed", "uri", uriStr, "err", err)
				}
				stack[0] = 0
				return
			}

			offset, err := p.WriteBytes(output)
			if err != nil {
				logger.Warnw("pollen_request: failed to write output", "err", err)
				stack[0] = 0
				return
			}
			stack[0] = offset
		},
		[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64},
		[]extism.ValueType{extism.ValueTypeI64},
	)

	callerInfoFn := extism.NewHostFunctionWithStack(
		"pollen_caller_info",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			info, ok := CallerInfoFromContext(ctx)
			if !ok {
				stack[0] = 0
				return
			}
			b := MarshalCallerInfo(info)
			if b == nil {
				stack[0] = 0
				return
			}
			offset, err := p.WriteBytes(b)
			if err != nil {
				logger.Warnw("pollen_caller_info: failed to write", "err", err)
				stack[0] = 0
				return
			}
			stack[0] = offset
		},
		nil,
		[]extism.ValueType{extism.ValueTypeI64},
	)

	return []extism.HostFunction{logFn, requestFn, callerInfoFn}
}
