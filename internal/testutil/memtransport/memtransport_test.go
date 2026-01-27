package memtransport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNetworkSendRecv(t *testing.T) {
	net := NewNetwork()

	a, err := net.Bind("127.0.0.1:10001")
	require.NoError(t, err)
	b, err := net.Bind("127.0.0.1:10002")
	require.NoError(t, err)

	msg := []byte("ping")
	require.NoError(t, a.Send("127.0.0.1:10002", msg))

	src, got, err := b.Recv()
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:10001", src)
	require.Equal(t, msg, got)
}

func TestNetworkUnknownDestination(t *testing.T) {
	net := NewNetwork()

	a, err := net.Bind("127.0.0.1:10001")
	require.NoError(t, err)

	err = a.Send("127.0.0.1:10099", []byte("ping"))
	require.ErrorIs(t, err, ErrUnknownDestination)
}

func TestNetworkCloseUnblocksRecv(t *testing.T) {
	net := NewNetwork()

	a, err := net.Bind("127.0.0.1:10001")
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() {
		_, _, err := a.Recv()
		resultCh <- err
	}()

	require.NoError(t, a.Close())

	select {
	case err := <-resultCh:
		require.ErrorIs(t, err, ErrTransportClosed)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Recv to return")
	}
}
