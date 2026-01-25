package tcp

import (
	"io"
	"net"
	"sync"

	"go.uber.org/zap"
)

const (
	bridgeCopyWorkers = 2
	bufSize           = 64 * 1024
)

var bufPool = sync.Pool{New: func() any { b := make([]byte, bufSize); return &b }}

func copyWithPooledBuf(dst, src net.Conn) error {
	p := bufPool.Get().(*[]byte) //nolint:forcetypeassert
	defer bufPool.Put(p)
	_, err := io.CopyBuffer(dst, src, *p)
	return err
}

func Bridge(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(bridgeCopyWorkers)

	go func() {
		defer wg.Done()
		if err := copyWithPooledBuf(c1, c2); err != nil {
			zap.S().Debugw("bridge copy failed", "direction", "c1<-c2", "err", err)
		}
		c1.Close()
	}()

	go func() {
		defer wg.Done()
		if err := copyWithPooledBuf(c2, c1); err != nil {
			zap.S().Debugw("bridge copy failed", "direction", "c2<-c1", "err", err)
		}
		c2.Close()
	}()

	wg.Wait()
}
