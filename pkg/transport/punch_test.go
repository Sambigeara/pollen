package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func listenLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestSimultaneousProbe(t *testing.T) {
	a := listenLoopback(t)
	b := listenLoopback(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addrA := a.LocalAddr().(*net.UDPAddr) //nolint:forcetypeassert
	addrB := b.LocalAddr().(*net.UDPAddr) //nolint:forcetypeassert

	errCh := make(chan error, 2)
	go func() {
		_, err := probeAddr(ctx, a, addrB)
		errCh <- err
	}()
	go func() {
		_, err := probeAddr(ctx, b, addrA)
		errCh <- err
	}()

	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
}
