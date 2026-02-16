package sock

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

var (
	ErrUnreachable = errors.New("peer unreachable")
)

var _ SockStore = (*sockStore)(nil)

type SockStore interface {
	GetOrCreate(ctx context.Context, addr *net.UDPAddr, withPunch bool) (*Conn, error)
}

// Conn is a thin wrapper around a UDPConn with a reference counter for determining closures etc
type Conn struct {
	*net.UDPConn
	peer      *net.UDPAddr
	refs      atomic.Int64
	closeOnce sync.Once
	onClose   func() // called once when refs hit zero, before closing the UDPConn
}

func (c *Conn) Close() error {
	if c.refs.Add(-1) > 0 {
		return nil
	}
	var err error
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		err = c.UDPConn.Close()
	})
	return err
}

type sockStore struct {
	mu       sync.Mutex
	log      *zap.SugaredLogger
	basePort int
	socks    *ConnList
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewSockStore(port int) *sockStore {
	return &sockStore{
		log:      zap.S().Named("socketstore"),
		basePort: port,
		socks:    newConnList(),
	}
}

func (s *sockStore) GetOrCreate(ctx context.Context, addr *net.UDPAddr, withPunch bool) (*Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, ok := s.socks.Get(addr)
	if ok {
		conn.refs.Add(1)
		return conn, nil
	}

	var udpConn *net.UDPConn
	var err error
	if withPunch {
		s.mu.Unlock()
		udpConn, err = punch(ctx, addr)
		s.mu.Lock()
	} else {
		udpConn, err = net.ListenUDP("udp", &net.UDPAddr{})
	}
	if err != nil {
		return nil, err
	}

	c := &Conn{
		UDPConn: udpConn,
		peer:    addr,
		onClose: func() { s.socks.Remove(addr) },
	}
	c.refs.Store(1)
	s.socks.Append(addr, c)

	return c, nil
}
