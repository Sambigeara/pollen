package placement

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
)

const (
	artifactFetchTimeout = 15 * time.Second

	artifactStatusOK       byte = 0
	artifactStatusNotFound byte = 1
	hashDisplayLen              = 16
	sha256Len                   = 32

	statusOK       byte = 0
	statusNotFound byte = 1
	statusError    byte = 2
	statusCycle    byte = 4

	callerInfoLenSize = 2
	hashLen           = 64
	maxFuncLen        = 255
	maxInputLen       = 4 << 20 // 4 MiB
)

type casWriter interface {
	Put(r io.Reader) (string, error)
}

type casReader interface {
	Get(hash string) (io.ReadCloser, error)
}

type workloadInvoker interface {
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
}

// watchStream cancels a blocking stream if the context expires before the operation finishes.
func watchStream(ctx context.Context, stream io.Closer) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stream.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

type meshFetcher struct {
	mesh    StreamOpener
	cas     casWriter
	timeout time.Duration
}

func newArtifactFetcher(mesh StreamOpener, cas casWriter) artifactFetcher {
	return &meshFetcher{mesh: mesh, cas: cas, timeout: artifactFetchTimeout}
}

func (f *meshFetcher) Fetch(ctx context.Context, hash string, peers []types.PeerKey) error {
	var lastErr error
	for _, pk := range peers {
		if err := f.fetchFrom(ctx, hash, pk); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("fetch artifact %s: %w", hash[:min(hashDisplayLen, len(hash))], lastErr)
	}
	return fmt.Errorf("fetch artifact %s: no peers", hash[:min(hashDisplayLen, len(hash))])
}

func (f *meshFetcher) fetchFrom(ctx context.Context, hash string, peer types.PeerKey) error {
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	stream, err := f.mesh.OpenStream(ctx, peer, transport.StreamTypeArtifact)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", peer.Short(), err)
	}
	defer stream.Close()
	defer watchStream(ctx, stream)()

	if len(hash) != hex.EncodedLen(sha256Len) {
		return fmt.Errorf("invalid hash length: %d", len(hash))
	}
	if _, err := stream.Write([]byte(hash)); err != nil {
		return fmt.Errorf("write hash: %w", err)
	}

	var status [1]byte
	if _, err := io.ReadFull(stream, status[:]); err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	if status[0] != artifactStatusOK {
		return fmt.Errorf("peer %s does not have artifact", peer.Short())
	}

	gotHash, err := f.cas.Put(stream)
	if err != nil {
		return fmt.Errorf("store artifact: %w", err)
	}
	if gotHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}

	return nil
}

func handleArtifactStream(stream io.ReadWriteCloser, cas casReader) {
	defer stream.Close()

	var hashBuf [64]byte
	if _, err := io.ReadFull(stream, hashBuf[:]); err != nil {
		return
	}

	rc, err := cas.Get(string(hashBuf[:]))
	if err != nil {
		stream.Write([]byte{artifactStatusNotFound}) //nolint:errcheck
		return
	}
	defer rc.Close()

	stream.Write([]byte{artifactStatusOK}) //nolint:errcheck
	io.Copy(stream, rc)                    //nolint:errcheck
}

func handleWorkloadStream(ctx context.Context, stream io.ReadWriteCloser, peer types.PeerKey, invoker workloadInvoker, utilisation *utilisationTracker, gates *gateRegistry, timeout time.Duration) {
	defer stream.Close()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Read caller info prefix: [2-byte BE length][JSON].
	// Attributes are taken from the wire but the peer key is always
	// overwritten with the transport-authenticated identity.
	var callerLenBuf [2]byte
	if _, err := io.ReadFull(stream, callerLenBuf[:]); err != nil {
		return
	}
	info := wasm.CallerInfo{PeerKey: peer}
	if callerLen := binary.BigEndian.Uint16(callerLenBuf[:]); callerLen > 0 {
		callerBuf := make([]byte, callerLen)
		if _, err := io.ReadFull(stream, callerBuf); err != nil {
			return
		}
		if wireInfo, ok := wasm.CallerInfoFromJSON(callerBuf); ok {
			info.Attributes = wireInfo.Attributes
			info.DeadlineUnixMs = wireInfo.DeadlineUnixMs
		}
	}
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

	var hashBuf [64]byte
	if _, err := io.ReadFull(stream, hashBuf[:]); err != nil {
		return
	}

	var funcLenBuf [1]byte
	if _, err := io.ReadFull(stream, funcLenBuf[:]); err != nil {
		return
	}

	funcName := make([]byte, funcLenBuf[0])
	if _, err := io.ReadFull(stream, funcName); err != nil && len(funcName) > 0 {
		return
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

	// Gate the call against the target's per-(hash, function) concurrency
	// limit. Forwarded calls must respect the target's capacity, not just
	// the caller's — otherwise P2C would route into hotspots its latency
	// EWMA hadn't caught up with yet.
	hash := string(hashBuf[:])
	function := string(funcName)
	gateRelease, err := gates.acquire(ctx, callKey{Hash: hash, Function: function})
	if err != nil {
		writeResponse(stream, statusError, []byte(err.Error()))
		return
	}
	defer gateRelease()

	// Stamp the hash into the local call chain so a recursive
	// pollen_request from this seed back into itself fails fast with
	// ErrCycle instead of starving its own gate.
	ctx = withChain(ctx, hash)

	workStart := time.Now()
	output, err := invoker.Call(ctx, hash, function, input)
	// Record Served + execution-only Invocation on the serving node for
	// every non-ErrNotRunning outcome. SLO is not recorded here — it is
	// caller-perspective and the caller's forwardCall observes it, so
	// the serving node does not double-count or scale on forwarded pain.
	if !errors.Is(err, ErrNotRunning) {
		utilisation.RecordServed(hash, function)
		utilisation.RecordInvocation(hash, function, time.Since(workStart))
	}
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

	if ctx.Done() != nil {
		defer watchStream(ctx, stream)()
	}

	if len(hash) != hashLen {
		return nil, fmt.Errorf("invoke: hash must be %d hex chars, got %d", hashLen, len(hash))
	}
	if len(function) > maxFuncLen {
		return nil, fmt.Errorf("invoke: function name too long (%d > %d)", len(function), maxFuncLen)
	}

	// Caller info prefix: [2-byte BE length][JSON]. Propagate the caller's
	// deadline over the wire so the target peer can honour it instead of
	// falling back on its local safety cap.
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
