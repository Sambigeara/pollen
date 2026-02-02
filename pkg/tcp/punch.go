package tcp

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	punchAttemptTimeout = 250 * time.Millisecond
	punchInterval       = 100 * time.Millisecond
)

// SimultaneousOpen attempts TCP simultaneous open by listening and dialing concurrently.
// The listener is pre-bound (passed in) so we know the local port.
// Returns the first successful connection (either from accept or dial).
func SimultaneousOpen(ctx context.Context, ln net.Listener, peerAddr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Get local port from listener for the dialer
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	laddr := &net.TCPAddr{Port: port}

	resCh := make(chan net.Conn, 1)
	var wg sync.WaitGroup

	// Goroutine 1: Accept incoming connection
	wg.Go(func() {
		if dl, ok := ln.(interface{ SetDeadline(time.Time) error }); ok {
			_ = dl.SetDeadline(time.Now().Add(timeout))
		}
		c, err := ln.Accept()
		if err != nil {
			return
		}
		select {
		case resCh <- c:
			cancel()
		default:
			c.Close()
		}
	})

	// Goroutine 2: Dial repeatedly until success or timeout
	wg.Go(func() {
		dialer := &net.Dialer{
			LocalAddr: laddr,
			Control:   reusePortControl,
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			attemptCtx, attemptCancel := context.WithTimeout(ctx, punchAttemptTimeout)
			c, err := dialer.DialContext(attemptCtx, "tcp", peerAddr)
			attemptCancel()
			if err == nil {
				select {
				case resCh <- c:
					cancel()
				default:
					c.Close()
				}
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(punchInterval):
			}
		}
	})

	// Wait for result or timeout
	go func() {
		wg.Wait()
		close(resCh)
	}()

	if c, ok := <-resCh; ok {
		return c, nil
	}
	return nil, errors.New("tcp simultaneous open failed")
}

// reusePortControl sets SO_REUSEADDR and SO_REUSEPORT on the socket.
// This allows binding the same local port for both listen and dial.
func reusePortControl(network, address string, c syscall.RawConn) error {
	var err error
	_ = c.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
		if err != nil {
			return
		}
		err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	})
	return err
}

// NewReusePortDialer creates a dialer that can share a local port with a listener.
func NewReusePortDialer(localPort int, timeout time.Duration) *net.Dialer {
	return &net.Dialer{
		LocalAddr: &net.TCPAddr{Port: localPort},
		Timeout:   timeout,
		Control:   reusePortControl,
	}
}

// ListenReusePort creates a TCP listener with SO_REUSEADDR and SO_REUSEPORT set.
// This is needed for TCP simultaneous open where we need to bind the same port
// for both listening and dialing.
func ListenReusePort(ctx context.Context, address string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: reusePortControl,
	}
	return lc.Listen(ctx, "tcp", address)
}
