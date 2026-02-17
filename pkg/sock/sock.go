package sock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

var (
	ErrUnreachable             = errors.New("peer unreachable")
	ErrMainProbeIOUnconfigured = errors.New("main probe I/O not configured")
)

var _ SockStore = (*sockStore)(nil)

type ProbeWriter func(payload []byte, addr *net.UDPAddr) error

type SockStore interface {
	GetOrCreate(ctx context.Context, addr *net.UDPAddr, withPunch bool) (*Conn, error)
	SetMainProbeWriter(write ProbeWriter)
	HandleMainProbePacket(data []byte, sender *net.UDPAddr)
}

// Conn is a thin wrapper around a UDPConn with a reference counter for determining closures etc
type Conn struct {
	*net.UDPConn
	peer      *net.UDPAddr
	refs      atomic.Int64
	closeOnce sync.Once
	shared    bool
	onClose   func() // called once when refs hit zero, before closing the UDPConn
}

func (c *Conn) Peer() *net.UDPAddr { return c.peer }

func (c *Conn) Shared() bool { return c.shared }

func (c *Conn) Close() error {
	if c.refs.Add(-1) > 0 {
		return nil
	}
	var err error
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		if !c.shared {
			err = c.UDPConn.Close()
		}
	})
	return err
}

type sockStore struct {
	log   *zap.SugaredLogger
	socks *ConnList

	mainWrite  ProbeWriter
	probeMu    sync.Mutex
	mainProbes map[[probeNonceSize]byte]chan *net.UDPAddr
}

func NewSockStore() SockStore {
	return &sockStore{
		log:        zap.S().Named("sockstore"),
		socks:      newConnList(),
		mainProbes: make(map[[probeNonceSize]byte]chan *net.UDPAddr),
	}
}

func (s *sockStore) SetMainProbeWriter(write ProbeWriter) {
	s.mainWrite = write
}

func (s *sockStore) mainProbeWriter() ProbeWriter {
	return s.mainWrite
}

func (s *sockStore) GetOrCreate(ctx context.Context, addr *net.UDPAddr, withPunch bool) (*Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan *Conn, 1)
	if withPunch && s.mainProbeWriter() == nil {
		return nil, fmt.Errorf("punch %s: %w", addr, ErrMainProbeIOUnconfigured)
	}

	if !withPunch {
		for _, c := range s.socks.Snapshot() {
			go func(c *Conn) {
				if err := probe(ctx, c.UDPConn, addr); err != nil {
					return
				}
				shared := &Conn{UDPConn: c.UDPConn, peer: addr, shared: true}
				shared.refs.Store(1)
				select {
				case ch <- shared:
				default:
				}
			}(c)
		}

		select {
		case c := <-ch:
			return c, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("probe %s: %w", addr, ErrUnreachable)
		}
	}

	// Hard-side: vary our source port by opening ephemeral sockets,
	// each probing the peer's known address.
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
			c := &Conn{
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

	// Easy-side: from one fixed source port, spray probes at random
	// destination ports on the peer.
	go func() {
		dst, err := s.scatterProbeMain(ctx, addr, punchAttempts)
		if err != nil {
			return
		}
		c := &Conn{peer: dst, shared: true}
		c.refs.Store(1)
		select {
		case ch <- c:
		default:
		}
	}()

	select {
	case c := <-ch:
		if !c.Shared() {
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

func (s *sockStore) HandleMainProbePacket(data []byte, sender *net.UDPAddr) {
	if len(data) != 1+probeNonceSize {
		return
	}

	switch data[0] {
	case probeReqByte:
		resp := make([]byte, 1+probeNonceSize)
		resp[0] = probeRespByte
		copy(resp[1:], data[1:])
		_ = s.writeMainPacket(resp, sender)
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

func (s *sockStore) scatterProbeMain(ctx context.Context, target *net.UDPAddr, count int) (*net.UDPAddr, error) {
	if s.mainProbeWriter() == nil {
		return nil, ErrMainProbeIOUnconfigured
	}

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

	if err := s.writeMainPacket(req, target); err != nil {
		return nil, err
	}
	for i := 1; i < count; i++ {
		port, err := randomEphemeralPort()
		if err != nil {
			continue
		}
		dst := &net.UDPAddr{IP: target.IP, Port: port, Zone: target.Zone}
		_ = s.writeMainPacket(req, dst)
	}

	select {
	case sender := <-ch:
		return sender, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *sockStore) writeMainPacket(payload []byte, addr *net.UDPAddr) error {
	write := s.mainProbeWriter()
	if write == nil {
		return ErrMainProbeIOUnconfigured
	}
	return write(payload, addr)
}
