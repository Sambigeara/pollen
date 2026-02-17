package sock

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"net"
	"time"
)

const (
	punchAttempts  = 256
	probeTimeout   = 1 * time.Second
	probeReqByte   = 0x01
	probeRespByte  = 0x02
	probeNonceSize = 16

	minEphemeralPort = 1024
	maxEphemeralPort = 65535
)

// probe sends a probe on a socket and waits for the matching response.
// It also echoes any incoming probe requests so that simultaneous punches
// from both sides can discover each other.
func probe(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr) error {
	_, err := probeAddr(ctx, conn, addr)
	return err
}

func probeAddr(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr) (*net.UDPAddr, error) {
	nonce := make([]byte, probeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	req := make([]byte, 1+probeNonceSize)
	req[0] = probeReqByte
	copy(req[1:], nonce)

	if _, err := conn.WriteToUDP(req, addr); err != nil {
		return nil, err
	}

	expect := make([]byte, 1+probeNonceSize)
	expect[0] = probeRespByte
	copy(expect[1:], nonce)

	buf := make([]byte, 1+probeNonceSize)
	conn.SetReadDeadline(time.Now().Add(probeTimeout))
	defer conn.SetReadDeadline(time.Time{})

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		n, sender, err := conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		if n != 1+probeNonceSize {
			continue
		}
		if buf[0] == probeReqByte {
			resp := make([]byte, 1+probeNonceSize)
			resp[0] = probeRespByte
			copy(resp[1:], buf[1:n])
			_, _ = conn.WriteToUDP(resp, sender)
			continue
		}
		if bytes.Equal(buf[:n], expect) {
			return sender, nil
		}
	}
}

// scatterProbe sends probes from a single socket to random ports on the
// target IP. It returns the address of the first peer that responds.
// This implements the "easy-side" of a NAT punch: fixed source port,
// varying destination port.
func scatterProbe(ctx context.Context, conn *net.UDPConn, target *net.UDPAddr, count int) (*net.UDPAddr, error) {
	nonce := make([]byte, probeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	req := make([]byte, 1+probeNonceSize)
	req[0] = probeReqByte
	copy(req[1:], nonce)

	expect := make([]byte, 1+probeNonceSize)
	expect[0] = probeRespByte
	copy(expect[1:], nonce)

	// Include the target address first, then spray random ports.
	_, _ = conn.WriteToUDP(req, target)
	for i := 1; i < count; i++ {
		port, err := randomEphemeralPort()
		if err != nil {
			continue
		}
		dst := &net.UDPAddr{IP: target.IP, Port: port, Zone: target.Zone}
		_, _ = conn.WriteToUDP(req, dst)
	}

	// Wait for any matching response.
	buf := make([]byte, 1+probeNonceSize)
	conn.SetReadDeadline(time.Now().Add(probeTimeout))
	defer conn.SetReadDeadline(time.Time{})

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		n, sender, err := conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		if n != 1+probeNonceSize {
			continue
		}
		if buf[0] == probeReqByte {
			resp := make([]byte, 1+probeNonceSize)
			resp[0] = probeRespByte
			copy(resp[1:], buf[1:n])
			_, _ = conn.WriteToUDP(resp, sender)
			continue
		}
		if bytes.Equal(buf[:n], expect) {
			return sender, nil
		}
	}
}

func randomEphemeralPort() (int, error) {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return minEphemeralPort + int(binary.BigEndian.Uint16(b[:]))%(maxEphemeralPort-minEphemeralPort+1), nil
}
