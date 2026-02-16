package sock

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"time"
)

const (
	punchAttempts  = 256
	probeTimeout   = 1 * time.Second
	probeReqByte   = 0x01
	probeRespByte  = 0x02
	probeNonceSize = 16
)

// punch brute-forces NAT connectivity by opening many ephemeral sockets,
// sending probes to addr from each, and returning the first that gets a response.
func punch(ctx context.Context, addr *net.UDPAddr) (*net.UDPConn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan *net.UDPConn, 1)

	for range punchAttempts {
		go func() {
			c, err := punchOne(ctx, addr)
			if err != nil {
				return
			}
			select {
			case ch <- c:
			default:
				c.Close()
			}
		}()
	}

	select {
	case c := <-ch:
		return c, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("punch %s: %w", addr, ErrUnreachable)
	}
}

// punchOne opens a single ephemeral socket, sends a probe, and waits for a matching response.
func punchOne(ctx context.Context, addr *net.UDPAddr) (*net.UDPConn, error) {
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, probeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		udpConn.Close()
		return nil, err
	}

	probe := make([]byte, 1+probeNonceSize)
	probe[0] = probeReqByte
	copy(probe[1:], nonce)

	if _, err := udpConn.WriteToUDP(probe, addr); err != nil {
		udpConn.Close()
		return nil, err
	}

	expect := make([]byte, 1+probeNonceSize)
	expect[0] = probeRespByte
	copy(expect[1:], nonce)

	buf := make([]byte, 1+probeNonceSize)
	deadline := time.Now().Add(probeTimeout)
	udpConn.SetReadDeadline(deadline)

	for {
		if ctx.Err() != nil {
			udpConn.Close()
			return nil, ctx.Err()
		}

		n, _, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			udpConn.Close()
			return nil, err
		}
		if n == len(expect) && bytes.Equal(buf[:n], expect) {
			udpConn.SetReadDeadline(time.Time{}) // clear deadline
			return udpConn, nil
		}
	}
}
