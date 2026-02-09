package tcp

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBridge_HalfClosePreservesOppositeDirection(t *testing.T) {
	ctx := context.Background()
	lc := net.ListenConfig{}

	ln1, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln1.Close()

	ln2, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln2.Close()

	server1Ch := make(chan net.Conn, 1)
	go func() {
		conn, _ := ln1.Accept()
		server1Ch <- conn
	}()
	server2Ch := make(chan net.Conn, 1)
	go func() {
		conn, _ := ln2.Accept()
		server2Ch <- conn
	}()

	d := net.Dialer{}

	client1, err := d.DialContext(ctx, "tcp", ln1.Addr().String())
	require.NoError(t, err)
	defer client1.Close()

	client2, err := d.DialContext(ctx, "tcp", ln2.Addr().String())
	require.NoError(t, err)
	defer client2.Close()

	server1 := <-server1Ch
	server2 := <-server2Ch
	defer server1.Close()
	defer server2.Close()

	go Bridge(server1, server2)

	deadline := time.Now().Add(2 * time.Second)
	_ = client1.SetDeadline(deadline)
	_ = client2.SetDeadline(deadline)

	c1, ok := client1.(*net.TCPConn)
	require.True(t, ok)
	c2, ok := client2.(*net.TCPConn)
	require.True(t, ok)

	_, err = c1.Write([]byte("ping"))
	require.NoError(t, err)
	require.NoError(t, c1.CloseWrite())

	buf := make([]byte, 4)
	_, err = io.ReadFull(client2, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))

	_, err = c2.Write([]byte("pong"))
	require.NoError(t, err)
	require.NoError(t, c2.CloseWrite())

	buf = make([]byte, 4)
	_, err = io.ReadFull(client1, buf)
	require.NoError(t, err)
	require.Equal(t, "pong", string(buf))
}
