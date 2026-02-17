package sock

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func listenLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestSimultaneousProbe(t *testing.T) {
	a := listenLoopback(t)
	b := listenLoopback(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addrA := a.LocalAddr().(*net.UDPAddr)
	addrB := b.LocalAddr().(*net.UDPAddr)

	errCh := make(chan error, 2)
	go func() { errCh <- probe(ctx, a, addrB) }()
	go func() { errCh <- probe(ctx, b, addrA) }()

	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
}

func TestScatterProbeMainUsesFixedSourcePort(t *testing.T) {
	store := NewSockStore().(*sockStore)

	mainConn := configureMainProbeIO(t, store)
	mainAddr := mainConn.LocalAddr().(*net.UDPAddr)

	responder := listenLoopback(t)
	responderAddr := responder.LocalAddr().(*net.UDPAddr)

	sourcePortCh := make(chan int, 1)
	go func() {
		buf := make([]byte, 1+probeNonceSize)
		for {
			n, sender, err := responder.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n != 1+probeNonceSize || buf[0] != probeReqByte {
				continue
			}
			select {
			case sourcePortCh <- sender.Port:
			default:
			}
			resp := make([]byte, 1+probeNonceSize)
			resp[0] = probeRespByte
			copy(resp[1:], buf[1:n])
			_, _ = responder.WriteToUDP(resp, sender)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dst, err := store.scatterProbeMain(ctx, responderAddr, punchAttempts)
	require.NoError(t, err)
	require.Equal(t, responderAddr.String(), dst.String())
	require.Equal(t, mainAddr.Port, <-sourcePortCh)
}

func TestPunchUsesMainSocketForEasySide(t *testing.T) {
	store := NewSockStore().(*sockStore)

	mainConn := configureMainProbeIO(t, store)
	mainAddr := mainConn.LocalAddr().(*net.UDPAddr)

	responder := listenLoopback(t)
	responderAddr := responder.LocalAddr().(*net.UDPAddr)

	go func() {
		buf := make([]byte, 1+probeNonceSize)
		for {
			n, sender, err := responder.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n != 1+probeNonceSize || buf[0] != probeReqByte {
				continue
			}
			if sender.Port != mainAddr.Port {
				continue
			}
			resp := make([]byte, 1+probeNonceSize)
			resp[0] = probeRespByte
			copy(resp[1:], buf[1:n])
			_, _ = responder.WriteToUDP(resp, sender)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := store.Punch(ctx, responderAddr)
	require.NoError(t, err)
	require.Nil(t, c.UDPConn)
	require.Equal(t, responderAddr.String(), c.Peer().String())
}

func configureMainProbeIO(t *testing.T, store *sockStore) *net.UDPConn {
	t.Helper()

	mainConn := listenLoopback(t)
	store.SetMainProbeWriter(func(payload []byte, addr *net.UDPAddr) error {
		_, err := mainConn.WriteToUDP(payload, addr)
		return err
	})

	go func() {
		buf := make([]byte, 2048)
		for {
			n, sender, err := mainConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			store.HandleMainProbePacket(data, sender)
		}
	}()

	return mainConn
}
