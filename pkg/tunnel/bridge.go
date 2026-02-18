package tunnel

import (
	"errors"
	"io"
	"net"
	"sync"

	"go.uber.org/zap"
)

const bufSize = 64 * 1024

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, bufSize)
		return &b
	},
}

func bridge(c1, c2 io.ReadWriteCloser) {
	var wg sync.WaitGroup
	var closeOnce sync.Once

	transfer := func(dst, src io.ReadWriteCloser, direction string) {
		pooled := bufPool.Get()
		bufPtr, ok := pooled.(*[]byte)
		if !ok || bufPtr == nil {
			b := make([]byte, bufSize)
			bufPtr = &b
			pooled = nil
		}
		if pooled != nil {
			defer bufPool.Put(bufPtr)
		}

		_, err := io.CopyBuffer(dst, src, *bufPtr)

		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			closeOnce.Do(func() {
				zap.S().Debugw("bridge copy failed", "direction", direction, "err", err)
				_ = c1.Close()
				_ = c2.Close()
			})
		}

		closeWrite(dst)
	}

	wg.Go(func() { transfer(c1, c2, "c1->c2") })
	wg.Go(func() { transfer(c2, c1, "c2->c1") })

	wg.Wait()

	_ = c1.Close()
	_ = c2.Close()
}

func closeWrite(conn io.Closer) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}

	_ = conn.Close()
}
