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

func bridge(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	var closeOnce sync.Once

	transfer := func(dst, src net.Conn, direction string) {
		bufPtr := bufPool.Get().(*[]byte)
		defer bufPool.Put(bufPtr)

		_, err := io.CopyBuffer(dst, src, *bufPtr)

		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			closeOnce.Do(func() {
				zap.S().Debugw("bridge copy failed", "direction", direction, "err", err)
				_ = c1.Close()
				_ = c2.Close()
			})
		}

		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}

	wg.Go(func() { transfer(c1, c2, "c1->c2") })
	wg.Go(func() { transfer(c2, c1, "c2->c1") })

	wg.Wait()

	_ = c1.Close()
	_ = c2.Close()
}
