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
	var once sync.Once
	teardown := func() {
		_ = c1.Close()
		_ = c2.Close()
	}

	transfer := func(dst, src io.ReadWriteCloser, direction string) {
		bufPtr := bufPool.Get().(*[]byte) //nolint:forcetypeassert
		defer bufPool.Put(bufPtr)

		_, err := io.CopyBuffer(dst, src, *bufPtr)

		closeWrite(dst)
		once.Do(teardown)

		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			zap.S().Named("tun").Debugw("bridge copy failed", "direction", direction, "err", err)
		}
	}

	wg.Go(func() { transfer(c1, c2, "c1->c2") })
	wg.Go(func() { transfer(c2, c1, "c2->c1") })

	wg.Wait()
}

func closeWrite(conn io.Closer) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}

	_ = conn.Close()
}
