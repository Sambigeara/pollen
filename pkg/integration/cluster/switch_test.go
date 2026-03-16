//go:build integration

package cluster

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func udpAddr(ip string, port int) *net.UDPAddr {
	return &net.UDPAddr{IP: net.ParseIP(ip), Port: port}
}

func TestVirtualPacketConn_WriteAndRead(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{})
	addrA := udpAddr("10.0.0.1", 60611)
	addrB := udpAddr("10.0.0.2", 60612)

	connA := sw.Bind(addrA, Public)
	connB := sw.Bind(addrB, Public)
	t.Cleanup(func() { connA.Close() })
	t.Cleanup(func() { connB.Close() })

	payload := []byte("hello from A")
	_, err := connA.WriteTo(payload, addrB)
	require.NoError(t, err)

	buf := make([]byte, 1024)
	n, sender, err := connB.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, payload, buf[:n])
	require.Equal(t, addrA.String(), sender.String())
}

func TestVirtualPacketConn_ReadDeadline(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{})
	addr := udpAddr("10.0.0.1", 60611)
	conn := sw.Bind(addr, Public)
	t.Cleanup(func() { conn.Close() })

	// Set deadline in the past.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(-time.Second)))

	buf := make([]byte, 1024)
	_, _, err := conn.ReadFrom(buf)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestVirtualPacketConn_DeadlineWakesBlockedRead(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{})
	addr := udpAddr("10.0.0.1", 60611)
	conn := sw.Bind(addr, Public)
	t.Cleanup(func() { conn.Close() })

	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 1024)
		_, _, err := conn.ReadFrom(buf)
		errCh <- err
	}()

	// Give the goroutine time to block in ReadFrom.
	time.Sleep(20 * time.Millisecond)

	// Set deadline to now — should unblock the ReadFrom.
	require.NoError(t, conn.SetReadDeadline(time.Now()))

	require.Eventually(t, func() bool {
		select {
		case err := <-errCh:
			return err != nil && os.IsTimeout(err)
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}

func TestVirtualPacketConn_CloseUnblocksRead(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{})
	addr := udpAddr("10.0.0.1", 60611)
	conn := sw.Bind(addr, Public)

	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 1024)
		_, _, err := conn.ReadFrom(buf)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	conn.Close()

	require.Eventually(t, func() bool {
		select {
		case err := <-errCh:
			return err != nil
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}

func TestVirtualSwitch_Latency(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{DefaultLatency: 50 * time.Millisecond})
	addrA := udpAddr("10.0.0.1", 60611)
	addrB := udpAddr("10.0.0.2", 60612)

	connA := sw.Bind(addrA, Public)
	connB := sw.Bind(addrB, Public)
	t.Cleanup(func() { connA.Close() })
	t.Cleanup(func() { connB.Close() })

	start := time.Now()
	_, err := connA.WriteTo([]byte("latency test"), addrB)
	require.NoError(t, err)

	buf := make([]byte, 1024)
	n, _, err := connB.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, "latency test", string(buf[:n]))

	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "delivery should take at least ~50ms")
}

func TestVirtualSwitch_Partition(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{})
	addrA := udpAddr("10.0.0.1", 60611)
	addrB := udpAddr("10.0.0.2", 60612)

	connA := sw.Bind(addrA, Public)
	connB := sw.Bind(addrB, Public)
	t.Cleanup(func() { connA.Close() })
	t.Cleanup(func() { connB.Close() })

	// Partition A and B.
	sw.Partition([]string{addrA.String()}, []string{addrB.String()})

	// Send from A to B — should be dropped.
	_, err := connA.WriteTo([]byte("should not arrive"), addrB)
	require.NoError(t, err)

	require.NoError(t, connB.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
	buf := make([]byte, 1024)
	_, _, err = connB.ReadFrom(buf)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)

	// Heal and verify delivery works.
	sw.Heal([]string{addrA.String()}, []string{addrB.String()})
	require.NoError(t, connB.SetReadDeadline(time.Time{}))

	_, err = connA.WriteTo([]byte("after heal"), addrB)
	require.NoError(t, err)

	n, _, err := connB.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, "after heal", string(buf[:n]))
}

func TestVirtualSwitch_NATGating(t *testing.T) {
	sw := NewVirtualSwitch(SwitchConfig{})
	addrPub := udpAddr("10.0.0.1", 60611)
	addrPrivA := udpAddr("10.0.0.2", 60612)
	addrPrivB := udpAddr("10.0.0.3", 60613)

	connPub := sw.Bind(addrPub, Public)
	connPrivA := sw.Bind(addrPrivA, Private)
	connPrivB := sw.Bind(addrPrivB, Private)
	t.Cleanup(func() { connPub.Close() })
	t.Cleanup(func() { connPrivA.Close() })
	t.Cleanup(func() { connPrivB.Close() })

	// Private→Public should work.
	_, err := connPrivA.WriteTo([]byte("priv to pub"), addrPub)
	require.NoError(t, err)

	buf := make([]byte, 1024)
	n, sender, err := connPub.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, "priv to pub", string(buf[:n]))
	require.Equal(t, addrPrivA.String(), sender.String())

	// Private→Private should be blocked (no punch).
	_, err = connPrivA.WriteTo([]byte("blocked"), addrPrivB)
	require.NoError(t, err)

	require.NoError(t, connPrivB.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
	_, _, err = connPrivB.ReadFrom(buf)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	require.NoError(t, connPrivB.SetReadDeadline(time.Time{}))

	// Simultaneous open: A sends to B (pending), then B sends to A — both delivered.
	_, err = connPrivA.WriteTo([]byte("punch from A"), addrPrivB)
	require.NoError(t, err)

	_, err = connPrivB.WriteTo([]byte("punch from B"), addrPrivA)
	require.NoError(t, err)

	// Both sides should receive the other's packet.
	n, sender, err = connPrivB.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, "punch from A", string(buf[:n]))
	require.Equal(t, addrPrivA.String(), sender.String())

	n, sender, err = connPrivA.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, "punch from B", string(buf[:n]))
	require.Equal(t, addrPrivB.String(), sender.String())

	// After punch opened, subsequent Private→Private should work.
	_, err = connPrivA.WriteTo([]byte("after punch"), addrPrivB)
	require.NoError(t, err)

	n, _, err = connPrivB.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, "after punch", string(buf[:n]))
}
