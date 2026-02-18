package sock

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
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

func probeAddr(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr) (_ *net.UDPAddr, retErr error) {
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
	if err := conn.SetReadDeadline(time.Now().Add(probeTimeout)); err != nil {
		return nil, err
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			if retErr != nil {
				retErr = errors.Join(retErr, err)
				return
			}
			retErr = err
		}
	}()

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
