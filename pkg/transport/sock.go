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
	"go.uber.org/zap"
)

var _ sockStore = (*sockStoreImpl)(nil)

type connList struct {
	byKey map[string]*conn
	mu    sync.Mutex
}

func newConnList() *connList {
	return &connList{
		byKey: make(map[string]*conn),
	}
}

func (l *connList) Append(addr *net.UDPAddr, c *conn) (*conn, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	k := addr.String()
	if existing, ok := l.byKey[k]; ok {
		return existing, false
	}
	l.byKey[k] = c
	return c, true
}

func (l *connList) Remove(addr *net.UDPAddr) {
	l.mu.Lock()
	delete(l.byKey, addr.String())
	l.mu.Unlock()
}

type probeWriter func(payload []byte, addr *net.UDPAddr) error

type sockStore interface {
	Punch(ctx context.Context, addr *net.UDPAddr, localNAT nat.Type) (*conn, error)
	SetMainProbeWriter(write probeWriter)
	HandleMainProbePacket(data []byte, sender *net.UDPAddr)
}

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
	log        *zap.SugaredLogger
	socks      *connList
	mainWrite  probeWriter
	mainProbes map[[probeNonceSize]byte]chan *net.UDPAddr
	probeMu    sync.Mutex
}

func newSockStore() sockStore {
	return &sockStoreImpl{
		log:        zap.S().Named("sockstore"),
		socks:      newConnList(),
		mainProbes: make(map[[probeNonceSize]byte]chan *net.UDPAddr),
	}
}

func (s *sockStoreImpl) SetMainProbeWriter(write probeWriter) {
	s.mainWrite = write
}

func (s *sockStoreImpl) Punch(ctx context.Context, addr *net.UDPAddr, localNAT nat.Type) (*conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan *conn, 1)

	// Hard-side: vary our source port by opening ephemeral sockets,
	// each probing the peer's known address.
	// Skip when local NAT is Easy — our main socket port is stable.
	if localNAT != nat.Easy {
		for range punchAttempts {
			go func() {
				udp, err := net.ListenUDP("udp", &net.UDPAddr{})
				if err != nil {
					return
				}
				dst, err := probeAddr(ctx, udp, addr)
				if err != nil {
					_ = udp.Close()
					return
				}
				c := &conn{
					UDPConn: udp,
					peer:    dst,
					onClose: func() { s.socks.Remove(dst) },
				}
				c.refs.Store(1)
				select {
				case ch <- c:
				default:
					_ = udp.Close()
				}
			}()
		}
	}

	// Easy-side: from one fixed source port, spray probes at random
	// destination ports on the peer. Always scatter fully — even when
	// the remote has a stable port, outbound packets are needed to
	// punch holes in our own NAT for the remote's probes to traverse.
	go func() {
		dst, err := s.scatterProbeMain(ctx, addr, punchAttempts)
		if err != nil {
			return
		}
		c := &conn{peer: dst}
		c.refs.Store(1)
		select {
		case ch <- c:
		default:
		}
	}()

	select {
	case c := <-ch:
		if c.UDPConn != nil {
			if existing, appended := s.socks.Append(c.peer, c); !appended {
				_ = c.UDPConn.Close()
				c = existing
			}
		}
		return c, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("punch %s: %w", addr, ErrUnreachable)
	}
}

func (s *sockStoreImpl) HandleMainProbePacket(data []byte, sender *net.UDPAddr) {
	if len(data) != 1+probeNonceSize {
		return
	}

	switch data[0] {
	case probeReqByte:
		resp := make([]byte, 1+probeNonceSize)
		resp[0] = probeRespByte
		copy(resp[1:], data[1:])
		_ = s.mainWrite(resp, sender)
	case probeRespByte:
		var nonce [probeNonceSize]byte
		copy(nonce[:], data[1:])

		s.probeMu.Lock()
		ch, ok := s.mainProbes[nonce]
		s.probeMu.Unlock()
		if ok {
			select {
			case ch <- sender:
			default:
			}
		}
	}
}

func (s *sockStoreImpl) scatterProbeMain(ctx context.Context, target *net.UDPAddr, count int) (*net.UDPAddr, error) {
	nonce := make([]byte, probeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	req := make([]byte, 1+probeNonceSize)
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
		dst := &net.UDPAddr{IP: target.IP, Port: port, Zone: target.Zone}
		_ = s.mainWrite(req, dst)
	}

	select {
	case sender := <-ch:
		return sender, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

const (
	punchAttempts  = 256
	probeTimeout   = 1 * time.Second
	probeReqByte   = 0x01
	probeRespByte  = 0x02
	probeNonceSize = 16

	minEphemeralPort = 1024
	maxEphemeralPort = 65535
)

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
	if err := conn.SetReadDeadline(time.Now().Add(probeTimeout)); err != nil {
		return nil, err
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

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
