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

type closeWriter interface {
	CloseWrite() error
}

type closeReader interface {
	CloseRead() error
}

func halfCloseWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func halfCloseRead(conn net.Conn) {
	if cr, ok := conn.(closeReader); ok {
		_ = cr.CloseRead()
	}
}

func Bridge(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(bridgeCopyWorkers)

	go func() {
		defer wg.Done()
		if err := copyWithPooledBuf(c1, c2); err != nil {
			zap.S().Debugw("bridge copy failed", "direction", "c1<-c2", "err", err)
		}
		halfCloseWrite(c1)
		halfCloseRead(c2)
	}()

	go func() {
		defer wg.Done()
		if err := copyWithPooledBuf(c2, c1); err != nil {
			zap.S().Debugw("bridge copy failed", "direction", "c2<-c1", "err", err)
		}
		halfCloseWrite(c2)
		halfCloseRead(c1)
	}()

	wg.Wait()
	_ = c1.Close()
	_ = c2.Close()
}
