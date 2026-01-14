package tcp

import (
	"io"
	"net"
	"sync"

	"go.uber.org/zap"
)

const bridgeCopyWorkers = 2

var bufPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

func copyWithPooledBuf(dst, src net.Conn) (int64, error) {
	p := bufPool.Get().(*[]byte)
	defer bufPool.Put(p)
	return io.CopyBuffer(dst, src, *p)
}

func Bridge(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(bridgeCopyWorkers)

	go func() {
		defer wg.Done()
		if _, err := copyWithPooledBuf(c1, c2); err != nil {
			zap.S().Debugw("bridge copy failed", "direction", "c1<-c2", "err", err)
		}
		c1.Close()
	}()

	go func() {
		defer wg.Done()
		if _, err := copyWithPooledBuf(c2, c1); err != nil {
			zap.S().Debugw("bridge copy failed", "direction", "c2<-c1", "err", err)
		}
		c2.Close()
	}()

	wg.Wait()
}
