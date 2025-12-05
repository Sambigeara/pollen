package tcp

import (
	"io"
	"net"
	"sync"
)

func Bridge(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(c1, c2)
		c1.Close() // Close write end of c1 implies read end of c2 finished
	}()

	go func() {
		defer wg.Done()
		io.Copy(c2, c1)
		c2.Close()
	}()

	wg.Wait()
}
