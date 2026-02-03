package memtransport

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sambigeara/pollen/pkg/transport"
)

const defaultQueueSize = 256

var (
	ErrUnknownDestination = errors.New("destination not bound")
	ErrTransportClosed    = errors.New("transport closed")
	ErrQueueFull          = errors.New("receive queue full")
)

type Network struct {
	endpoints map[string]*endpoint
	mu        sync.RWMutex
}

type packet struct {
	src     string
	payload []byte
}

type endpoint struct {
	recvCh    chan packet
	addr      string
	mu        sync.RWMutex
	closeOnce sync.Once
	closed    atomic.Bool
}

func NewNetwork() *Network {
	return &Network{endpoints: make(map[string]*endpoint)}
}

func (n *Network) bindEndpoint(addr string) (*endpoint, error) {
	if addr == "" {
		return nil, errors.New("address required")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if ep, ok := n.endpoints[addr]; ok && !ep.closed.Load() {
		return nil, fmt.Errorf("address already bound: %s", addr)
	}

	ep := &endpoint{
		addr:   addr,
		recvCh: make(chan packet, defaultQueueSize),
	}
	n.endpoints[addr] = ep

	return ep, nil
}

func (n *Network) lookup(addr string) (*endpoint, bool) {
	n.mu.RLock()
	ep, ok := n.endpoints[addr]
	n.mu.RUnlock()
	if !ok || ep.closed.Load() {
		return nil, false
	}
	return ep, true
}

func (n *Network) unbind(ep *endpoint) {
	if ep == nil {
		return
	}
	addr := ep.addr
	n.mu.Lock()
	if curr, ok := n.endpoints[addr]; ok && curr == ep {
		delete(n.endpoints, addr)
	}
	n.mu.Unlock()
}

func (n *Network) send(src string, dst string, b []byte) error {
	dest, ok := n.lookup(dst)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownDestination, dst)
	}

	dest.mu.RLock()
	defer dest.mu.RUnlock()
	if dest.closed.Load() {
		return ErrTransportClosed
	}

	payload := make([]byte, len(b))
	copy(payload, b)

	select {
	case dest.recvCh <- packet{src: src, payload: payload}:
		return nil
	default:
		return ErrQueueFull
	}
}

func (e *endpoint) close() {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed.Store(true)
		close(e.recvCh)
		e.mu.Unlock()
	})
}

// memTransport implements transport.Transport
type memTransport struct {
	net  *Network
	ep   *endpoint
	addr string
}

var _ transport.Transport = (*memTransport)(nil)

func (n *Network) Bind(addr string) (transport.Transport, error) {
	ep, err := n.bindEndpoint(addr)
	if err != nil {
		return nil, err
	}

	return &memTransport{
		net:  n,
		ep:   ep,
		addr: addr,
	}, nil
}

func (t *memTransport) Recv() (string, []byte, error) {
	pkt, ok := <-t.ep.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (t *memTransport) Send(dst string, b []byte) error {
	if t.ep.closed.Load() {
		return ErrTransportClosed
	}
	return t.net.send(t.ep.addr, dst, b)
}

func (t *memTransport) Close() error {
	if t.ep == nil || t.ep.closed.Load() {
		return nil
	}
	t.ep.close()
	t.net.unbind(t.ep)
	return nil
}
