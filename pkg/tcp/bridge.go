package tcp

import (
	"io"
	"log"
	"net"
	"sync"
)

const bridgeCopyWorkers = 2

func Bridge(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(bridgeCopyWorkers)

	go func() {
		defer wg.Done()
		if _, err := io.Copy(c1, c2); err != nil {
			log.Printf("bridge copy c1<-c2 failed: %v", err)
		}
		c1.Close() // Close write end of c1 implies read end of c2 finished
	}()

	go func() {
		defer wg.Done()
		if _, err := io.Copy(c2, c1); err != nil {
			log.Printf("bridge copy c2<-c1 failed: %v", err)
		}
		c2.Close()
	}()

	wg.Wait()
}
