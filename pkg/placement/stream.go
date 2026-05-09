// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"encoding/binary"
	"encoding/json"
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

var errInputTooLarge = errors.New("input too large")

type workloadInvoker interface {
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
}

// TODO(saml): migrate the wire envelope to protobuf. JSON was chosen
// here for symmetry with the wasm-guest CallerInfo format (which needs a
// portable encoding any guest language can decode), but this struct is
// only sent pollen-to-pollen and has already diverged from the guest
// shape with CallChain. Protobuf+vtprotobuf would halve the PeerKey hex
// bloat and remove json.Marshal cost from the hot dispatch path.
type workloadCallerJSON struct {
	Attributes     map[string]any `json:"attributes,omitempty"`
	PeerKey        string         `json:"peerKey,omitempty"`
	CallChain      []string       `json:"callChain,omitempty"`
	DeadlineUnixMs int64          `json:"deadlineUnixMs,omitempty"`
}

// ReadHeader reads the caller-info envelope, call chain, seed hash, and
// function name. CallerInfo.PeerKey is overridden with the
// transport-authenticated peer; wire values would be spoofable.
func ReadHeader(r io.Reader, peer types.PeerKey) (wasm.CallerInfo, []string, string, string, error) {
	info := wasm.CallerInfo{PeerKey: peer}
	var chain []string

	var callerLenBuf [callerInfoLenSize]byte
	if _, err := io.ReadFull(r, callerLenBuf[:]); err != nil {
		return info, nil, "", "", err
	}
	if callerLen := binary.BigEndian.Uint16(callerLenBuf[:]); callerLen > 0 {
		callerBuf := make([]byte, callerLen)
		if _, err := io.ReadFull(r, callerBuf); err != nil {
			return info, nil, "", "", err
		}
		if wireInfo, ok := wasm.CallerInfoFromJSON(callerBuf); ok {
			info.Attributes = wireInfo.Attributes
			info.DeadlineUnixMs = wireInfo.DeadlineUnixMs
		}
		chain = callChainFromJSON(callerBuf)
	}

	var hashBuf [hashLen]byte
	if _, err := io.ReadFull(r, hashBuf[:]); err != nil {
		return info, nil, "", "", err
	}

	var funcLenBuf [1]byte
	if _, err := io.ReadFull(r, funcLenBuf[:]); err != nil {
		return info, nil, "", "", err
	}

	funcName := make([]byte, funcLenBuf[0])
	if _, err := io.ReadFull(r, funcName); err != nil && len(funcName) > 0 {
		return info, nil, "", "", err
	}

	return info, chain, string(hashBuf[:]), string(funcName), nil
}

func handleWorkloadStream(ctx context.Context, stream io.ReadWriteCloser, info wasm.CallerInfo, hash, function string, invoker workloadInvoker, timeout time.Duration) {
	defer stream.Close()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ctx = wasm.WithCallerInfo(ctx, info)
	ctx, deadlineCancel := withCallerDeadline(ctx, info)
	defer deadlineCancel()

	input, err := readWorkloadInput(stream)
	if errors.Is(err, errInputTooLarge) {
		_ = writeResponse(stream, statusError, []byte("input too large"))
		return
	}
	if err != nil {
		return
	}
	if err := ctx.Err(); err != nil {
		_ = writeErrorResponse(stream, err)
		return
	}

	ctx = withChain(ctx, hash)

	output, err := invoker.Call(ctx, hash, function, input)
	if err != nil {
		_ = writeErrorResponse(stream, err)
		return
	}

	_ = writeResponse(stream, statusOK, output)
}

func invokeOverStream(ctx context.Context, stream io.ReadWriteCloser, hash, function string, input []byte) ([]byte, error) {
	defer stream.Close()

	if len(hash) != hashLen {
		return nil, fmt.Errorf("invoke: hash must be %d hex chars, got %d", hashLen, len(hash))
	}
	if len(function) > maxFuncLen {
		return nil, fmt.Errorf("invoke: function name too long (%d > %d)", len(function), maxFuncLen)
	}

	info, _ := wasm.CallerInfoFromContext(ctx)
	if dl, ok := ctx.Deadline(); ok {
		info.DeadlineUnixMs = dl.UnixMilli()
	}
	var callerJSON []byte
	marshaled := marshalWorkloadCallerInfo(info, chainForForward(ctx, hash))
	if len(marshaled) > math.MaxUint16 {
		return nil, fmt.Errorf("invoke: caller metadata too large (%d > %d)", len(marshaled), math.MaxUint16)
	}
	callerJSON = marshaled

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
		if err := contextErrorFromWire(body); err != nil {
			return nil, fmt.Errorf("invoke: %s: %w", string(body), err)
		}
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

func writeResponse(w io.Writer, status byte, body []byte) error {
	buf := make([]byte, 1+4+len(body))
	buf[0] = status
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(body)))
	copy(buf[5:], body)
	_, err := w.Write(buf)
	return err
}

func writeErrorResponse(w io.Writer, err error) error {
	switch {
	case errors.Is(err, ErrNotRunning):
		return writeResponse(w, statusNotFound, []byte(err.Error()))
	case errors.Is(err, ErrCycle):
		return writeResponse(w, statusCycle, []byte(err.Error()))
	case errors.Is(err, ErrOverloaded):
		return writeResponse(w, statusOverloaded, []byte(err.Error()))
	default:
		return writeResponse(w, statusError, []byte(err.Error()))
	}
}

func contextErrorFromWire(body []byte) error {
	switch string(body) {
	case context.DeadlineExceeded.Error():
		return context.DeadlineExceeded
	case context.Canceled.Error():
		return context.Canceled
	default:
		return nil
	}
}

func withCallerDeadline(ctx context.Context, info wasm.CallerInfo) (context.Context, context.CancelFunc) {
	if info.DeadlineUnixMs == 0 {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, time.UnixMilli(info.DeadlineUnixMs))
}

func readWorkloadInput(r io.Reader) ([]byte, error) {
	var inputLenBuf [4]byte
	if _, err := io.ReadFull(r, inputLenBuf[:]); err != nil {
		return nil, err
	}
	inputLen := binary.BigEndian.Uint32(inputLenBuf[:])
	if inputLen > maxInputLen {
		return nil, errInputTooLarge
	}

	input := make([]byte, inputLen)
	if _, err := io.ReadFull(r, input); err != nil && inputLen > 0 {
		return nil, err
	}
	return input, nil
}

func writeOverload(w io.Writer, status byte, reason string) error {
	return writeResponse(w, status, []byte(reason))
}

func decodeOverload(body []byte, sentinel error) error {
	return &OverloadError{Sentinel: sentinel, Reason: string(body)}
}

func marshalWorkloadCallerInfo(info wasm.CallerInfo, chain []string) []byte {
	if info.PeerKey == (types.PeerKey{}) && info.Attributes == nil && info.DeadlineUnixMs == 0 && len(chain) == 0 {
		return nil
	}
	j := workloadCallerJSON{
		Attributes:     info.Attributes,
		CallChain:      chain,
		DeadlineUnixMs: info.DeadlineUnixMs,
	}
	if info.PeerKey != (types.PeerKey{}) {
		j.PeerKey = info.PeerKey.String()
	}
	b, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return b
}

func callChainFromJSON(data []byte) []string {
	var j workloadCallerJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil
	}
	return j.CallChain
}
