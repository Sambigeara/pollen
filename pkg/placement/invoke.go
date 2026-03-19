package placement

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

type workloadInvoker interface {
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
}

// Wire protocol:
//
// Request:
//   [64 bytes]  hex hash of target workload
//   [1 byte]    function name length
//   [N bytes]   function name (UTF-8)
//   [4 bytes]   input length (big-endian uint32)
//   [M bytes]   input bytes
//
// Response:
//   [1 byte]    status: 0=ok, 1=not_found, 2=error
//   [4 bytes]   body length (big-endian uint32)
//   [M bytes]   body (output bytes if ok, error message if error)

const (
	statusOK       byte = 0
	statusNotFound byte = 1
	statusError    byte = 2

	hashLen     = 64
	maxFuncLen  = 255
	maxInputLen = 4 << 20 // 4 MiB
)

// handleWorkloadStream reads a workload invocation request from stream,
// calls the invoker, and writes the response.
func handleWorkloadStream(ctx context.Context, stream io.ReadWriteCloser, invoker workloadInvoker, timeout time.Duration) {
	defer stream.Close()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var hashBuf [64]byte
	if _, err := io.ReadFull(stream, hashBuf[:]); err != nil {
		return
	}
	hash := string(hashBuf[:])

	var funcLenBuf [1]byte
	if _, err := io.ReadFull(stream, funcLenBuf[:]); err != nil {
		return
	}
	funcName := make([]byte, funcLenBuf[0])
	if len(funcName) > 0 {
		if _, err := io.ReadFull(stream, funcName); err != nil {
			return
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
	if inputLen > 0 {
		if _, err := io.ReadFull(stream, input); err != nil {
			return
		}
	}

	output, err := invoker.Call(ctx, hash, string(funcName), input)
	if err != nil {
		if errors.Is(err, ErrNotRunning) {
			writeResponse(stream, statusNotFound, []byte(err.Error()))
		} else {
			writeResponse(stream, statusError, []byte(err.Error()))
		}
		return
	}

	writeResponse(stream, statusOK, output)
}

// invokeOverStream writes a workload invocation request to stream and reads the response.
func invokeOverStream(ctx context.Context, stream io.ReadWriteCloser, hash, function string, input []byte) ([]byte, error) {
	defer stream.Close()

	done := make(chan struct{})
	defer close(done)
	if ctxDone := ctx.Done(); ctxDone != nil {
		go func() {
			select {
			case <-ctxDone:
				stream.Close()
			case <-done:
			}
		}()
	}

	if len(hash) != hashLen {
		return nil, fmt.Errorf("invoke: hash must be %d hex chars, got %d", hashLen, len(hash))
	}
	if len(function) > maxFuncLen {
		return nil, fmt.Errorf("invoke: function name too long (%d > %d)", len(function), maxFuncLen)
	}

	buf := make([]byte, hashLen+1+len(function)+4+len(input))
	copy(buf[0:hashLen], hash)
	buf[hashLen] = byte(len(function))
	copy(buf[hashLen+1:hashLen+1+len(function)], function)
	off := hashLen + 1 + len(function)
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
	if bodyLen > 0 {
		if _, err := io.ReadFull(stream, body); err != nil {
			return nil, ctxOrWrap(ctx, err, "invoke: read body")
		}
	}

	switch statusBuf[0] {
	case statusOK:
		return body, nil
	case statusNotFound:
		return nil, fmt.Errorf("invoke: workload not found: %s: %w", string(body), ErrNotRunning)
	case statusError:
		return nil, fmt.Errorf("invoke: %s", string(body))
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
