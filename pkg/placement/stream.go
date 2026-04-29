// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
)

const (
	statusOK         byte = 0
	statusNotFound   byte = 1
	statusError      byte = 2
	statusCycle      byte = 4
	statusOverloaded byte = 5

	callerInfoLenSize = 2
	hashLen           = 64
	maxFuncLen        = 255
	maxInputLen       = 4 << 20 // 4 MiB
)

type workloadInvoker interface {
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
}

// ReadHeader reads the caller-info envelope, seed hash, and function
// name from an inbound workload stream. CallerInfo.PeerKey is overridden
// with the transport-authenticated peer; wire values would be spoofable.
func ReadHeader(r io.Reader, peer types.PeerKey) (wasm.CallerInfo, string, string, error) {
	info := wasm.CallerInfo{PeerKey: peer}

	var callerLenBuf [callerInfoLenSize]byte
	if _, err := io.ReadFull(r, callerLenBuf[:]); err != nil {
		return info, "", "", err
	}
	if callerLen := binary.BigEndian.Uint16(callerLenBuf[:]); callerLen > 0 {
		callerBuf := make([]byte, callerLen)
		if _, err := io.ReadFull(r, callerBuf); err != nil {
			return info, "", "", err
		}
		if wireInfo, ok := wasm.CallerInfoFromJSON(callerBuf); ok {
			info.Attributes = wireInfo.Attributes
			info.DeadlineUnixMs = wireInfo.DeadlineUnixMs
		}
	}

	var hashBuf [hashLen]byte
	if _, err := io.ReadFull(r, hashBuf[:]); err != nil {
		return info, "", "", err
	}

	var funcLenBuf [1]byte
	if _, err := io.ReadFull(r, funcLenBuf[:]); err != nil {
		return info, "", "", err
	}

	funcName := make([]byte, funcLenBuf[0])
	if _, err := io.ReadFull(r, funcName); err != nil && len(funcName) > 0 {
		return info, "", "", err
	}

	return info, string(hashBuf[:]), string(funcName), nil
}

func handleWorkloadStream(ctx context.Context, stream io.ReadWriteCloser, info wasm.CallerInfo, hash, function string, invoker workloadInvoker, timeout time.Duration) {
	defer stream.Close()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ctx = wasm.WithCallerInfo(ctx, info)

	// Honour the caller's deadline if it's tighter than our timeout cap.
	if info.DeadlineUnixMs > 0 {
		dl := time.UnixMilli(info.DeadlineUnixMs)
		if time.Until(dl) > 0 {
			var dlCancel context.CancelFunc
			ctx, dlCancel = context.WithDeadline(ctx, dl)
			defer dlCancel()
		}
	}

	var inputLenBuf [4]byte
	if _, err := io.ReadFull(stream, inputLenBuf[:]); err != nil {
		return
	}
	inputLen := binary.BigEndian.Uint32(inputLenBuf[:])
	if inputLen > maxInputLen {
		writeResponse(stream, statusError, []byte("input too large"))
		return
	}

	input := make([]byte, inputLen)
	if _, err := io.ReadFull(stream, input); err != nil && inputLen > 0 {
		return
	}

	// Recursive pollen_request from this seed back into itself would
	// deadlock on the instance pool; stamp the hash so it fails with
	// ErrCycle instead.
	ctx = withChain(ctx, hash)

	output, err := invoker.Call(ctx, hash, function, input)
	if err != nil {
		switch {
		case errors.Is(err, ErrNotRunning):
			writeResponse(stream, statusNotFound, []byte(err.Error()))
		case errors.Is(err, ErrCycle):
			writeResponse(stream, statusCycle, []byte(err.Error()))
		default:
			writeResponse(stream, statusError, []byte(err.Error()))
		}
		return
	}

	writeResponse(stream, statusOK, output)
}

func invokeOverStream(ctx context.Context, stream io.ReadWriteCloser, hash, function string, input []byte) ([]byte, error) {
	defer stream.Close()

	if len(hash) != hashLen {
		return nil, fmt.Errorf("invoke: hash must be %d hex chars, got %d", hashLen, len(hash))
	}
	if len(function) > maxFuncLen {
		return nil, fmt.Errorf("invoke: function name too long (%d > %d)", len(function), maxFuncLen)
	}

	// Propagate the caller's deadline over the wire so the target peer
	// can honour it instead of falling back on its local safety cap.
	info, _ := wasm.CallerInfoFromContext(ctx)
	if dl, ok := ctx.Deadline(); ok {
		info.DeadlineUnixMs = dl.UnixMilli()
	}
	var callerJSON []byte
	if marshaled := wasm.MarshalCallerInfo(info); len(marshaled) <= math.MaxUint16 {
		callerJSON = marshaled
	}

	buf := make([]byte, callerInfoLenSize+len(callerJSON)+hashLen+1+len(function)+4+len(input))
	binary.BigEndian.PutUint16(buf[0:callerInfoLenSize], uint16(len(callerJSON)))
	copy(buf[callerInfoLenSize:callerInfoLenSize+len(callerJSON)], callerJSON)
	off := callerInfoLenSize + len(callerJSON)
	copy(buf[off:off+hashLen], hash)
	buf[off+hashLen] = byte(len(function))
	copy(buf[off+hashLen+1:off+hashLen+1+len(function)], function)
	off += hashLen + 1 + len(function)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(input)))
	copy(buf[off+4:], input)

	if _, err := stream.Write(buf); err != nil {
		return nil, ctxOrWrap(ctx, err, "invoke: write request")
	}

	var statusBuf [1]byte
	if _, err := io.ReadFull(stream, statusBuf[:]); err != nil {
		return nil, ctxOrWrap(ctx, err, "invoke: read status")
	}

	var bodyLenBuf [4]byte
	if _, err := io.ReadFull(stream, bodyLenBuf[:]); err != nil {
		return nil, ctxOrWrap(ctx, err, "invoke: read body length")
	}

	bodyLen := binary.BigEndian.Uint32(bodyLenBuf[:])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(stream, body); err != nil && bodyLen > 0 {
		return nil, ctxOrWrap(ctx, err, "invoke: read body")
	}

	switch statusBuf[0] {
	case statusOK:
		return body, nil
	case statusNotFound:
		return nil, fmt.Errorf("invoke: workload not found: %s: %w", string(body), ErrNotRunning)
	case statusCycle:
		return nil, fmt.Errorf("invoke: %s: %w", string(body), ErrCycle)
	case statusOverloaded:
		return nil, decodeOverload(body, ErrOverloaded)
	case statusError:
		return nil, fmt.Errorf("invoke: %s: %w", string(body), ErrWorkloadFailed)
	default:
		return nil, fmt.Errorf("invoke: unknown status %d", statusBuf[0])
	}
}

func ctxOrWrap(ctx context.Context, err error, msg string) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%s: %w", msg, err)
}

func writeResponse(w io.Writer, status byte, body []byte) {
	buf := make([]byte, 1+4+len(body))
	buf[0] = status
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(body)))
	copy(buf[5:], body)
	w.Write(buf) //nolint:errcheck
}

// writeOverload encodes an overload rejection. The body is the reason
// text; the recipient wraps it in an OverloadError around the supplied
// sentinel.
func writeOverload(w io.Writer, status byte, reason string) {
	writeResponse(w, status, []byte(reason))
}

func decodeOverload(body []byte, sentinel error) error {
	return &OverloadError{Sentinel: sentinel, Reason: string(body)}
}
