package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sambigeara/pollen/pkg/nat"
)

const (
	probeReqByte   = 0x01
	probeRespByte  = 0x02
	probeNonceSize = 16
	probeFrameSize = 1 + probeNonceSize // type byte + nonce
	punchWorkers   = 8
	punchAttempts  = 32
	scatterDefault = 256
	minEphemeral   = 1024
)

type probeWriter func(payload []byte, addr *net.UDPAddr) error

type conn struct {
	*net.UDPConn
	peer      *net.UDPAddr
	onClose   func()
	refs      atomic.Int64
	closeOnce sync.Once
}

func (c *conn) Peer() *net.UDPAddr { return c.peer }
func (c *conn) Close() error {
	if c.refs.Add(-1) > 0 {
		return nil
	}
	var err error
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		if c.UDPConn != nil {
			err = c.UDPConn.Close()
		}
	})
	return err
}

type sockStoreImpl struct {
	mainWrite  probeWriter
	mainProbes map[[16]byte]chan *net.UDPAddr
	probeMu    sync.Mutex
}

func newSockStore() *sockStoreImpl {
	return &sockStoreImpl{
		mainProbes: make(map[[16]byte]chan *net.UDPAddr),
	}
}

func (s *sockStoreImpl) SetMainProbeWriter(write probeWriter) { s.mainWrite = write }

func (s *sockStoreImpl) Punch(ctx context.Context, addr *net.UDPAddr, localNAT nat.Type) (*conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan *conn, 1)
	var wg sync.WaitGroup

	if localNAT != nat.Easy {
		// Cap parallel hole punching to 8 workers instead of 256 fire-and-forget loops.
		for range punchWorkers {
			wg.Go(func() {
				for range punchAttempts {
					if ctx.Err() != nil {
						return
					}
					udp, err := net.ListenUDP("udp", &net.UDPAddr{})
					if err != nil {
						continue
					}

					dst, err := probeAddr(ctx, udp, addr)
					if err != nil {
						_ = udp.Close()
						continue
					}

					c := &conn{UDPConn: udp, peer: dst}
					c.refs.Store(1)
					select {
					case ch <- c:
						return // Success, stop trying.
					default:
						_ = udp.Close()
					}
				}
			})
		}
	}

	wg.Go(func() {
		dst, err := s.scatterProbeMain(ctx, addr, scatterDefault)
		if err != nil {
			return
		}
		c := &conn{peer: dst}
		c.refs.Store(1)
		select {
		case ch <- c:
		case <-ctx.Done():
		}
	})

	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case c, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("punch %s: %w", addr, errUnreachable)
		}
		return c, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("punch %s: %w", addr, errUnreachable)
	}
}

func (s *sockStoreImpl) scatterProbeMain(ctx context.Context, target *net.UDPAddr, count int) (*net.UDPAddr, error) {
	nonce := make([]byte, probeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	req := make([]byte, probeFrameSize)
	req[0] = probeReqByte
	copy(req[1:], nonce)

	var nonceKey [probeNonceSize]byte
	copy(nonceKey[:], nonce)

	ch := make(chan *net.UDPAddr, 1)
	s.probeMu.Lock()
	s.mainProbes[nonceKey] = ch
	s.probeMu.Unlock()

	defer func() {
		s.probeMu.Lock()
		delete(s.mainProbes, nonceKey)
		s.probeMu.Unlock()
	}()

	if err := s.mainWrite(req, target); err != nil {
		return nil, err
	}
	for i := 1; i < count; i++ {
		port, err := randomEphemeralPort()
		if err != nil {
			continue
		}
		_ = s.mainWrite(req, &net.UDPAddr{IP: target.IP, Port: port, Zone: target.Zone})
	}

	select {
	case sender := <-ch:
		return sender, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func probeAddr(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr) (*net.UDPAddr, error) {
	nonce := make([]byte, probeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	req := make([]byte, probeFrameSize)
	req[0] = probeReqByte
	copy(req[1:], nonce)

	if _, err := conn.WriteToUDP(req, addr); err != nil {
		return nil, err
	}

	expect := make([]byte, probeFrameSize)
	expect[0] = probeRespByte
	copy(expect[1:], nonce)

	buf := make([]byte, probeFrameSize)
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		n, sender, err := conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		if n != probeFrameSize {
			continue
		}

		if buf[0] == probeReqByte {
			resp := make([]byte, probeFrameSize)
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
	return minEphemeral + int(binary.BigEndian.Uint16(b[:]))%(65535-minEphemeral+1), nil
}

func (s *sockStoreImpl) HandleMainProbePacket(data []byte, sender *net.UDPAddr) {
	if len(data) != probeFrameSize {
		return
	}

	switch data[0] {
	case probeReqByte:
		resp := make([]byte, probeFrameSize)
		resp[0] = probeRespByte
		copy(resp[1:], data[1:])
		if s.mainWrite != nil {
			_ = s.mainWrite(resp, sender)
		}
	case probeRespByte:
		var nonceKey [probeNonceSize]byte
		copy(nonceKey[:], data[1:])

		s.probeMu.Lock()
		ch, ok := s.mainProbes[nonceKey]
		s.probeMu.Unlock()
		if ok {
			select {
			case ch <- sender:
			default:
			}
		}
	}
}
