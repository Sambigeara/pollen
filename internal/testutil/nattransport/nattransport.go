package nattransport

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sambigeara/pollen/pkg/transport"
)

const defaultQueueSize = 256

var (
	ErrAddressInUse    = errors.New("address already bound")
	ErrTransportClosed = errors.New("transport closed")
	ErrQueueFull       = errors.New("receive queue full")
	ErrNoAvailablePort = errors.New("no available NAT ports")
)

type Network struct {
	endpoints map[string]*endpoint
	mu        sync.RWMutex
}

type NATConfig struct {
	PublicIP string
	PortMin  int
	PortMax  int
	Seed     int64
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

type publicTransport struct {
	net *Network
	ep  *endpoint
}

type natTransport struct {
	net          *Network
	internalAddr string
	publicIP     string
	recvCh       chan packet
	mu           sync.Mutex
	mappings     map[string]*natMapping
	portPool     []int
	nextPort     int
	closeOnce    sync.Once
	closed       atomic.Bool
}

type natMapping struct {
	destAddr string
	extAddr  string
	ep       *endpoint
}

func NewNetwork() *Network {
	return &Network{endpoints: make(map[string]*endpoint)}
}

func (n *Network) Bind(addr string) (transport.Transport, error) {
	ep, err := n.bindEndpoint(addr)
	if err != nil {
		return nil, err
	}
	return &publicTransport{net: n, ep: ep}, nil
}

func (n *Network) BindNAT(internalAddr string, cfg NATConfig) (transport.Transport, error) {
	if cfg.PublicIP == "" {
		return nil, errors.New("public IP required")
	}
	if cfg.PortMin <= 0 || cfg.PortMax <= 0 || cfg.PortMin > cfg.PortMax {
		return nil, errors.New("invalid port range")
	}

	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	portPool := make([]int, 0, cfg.PortMax-cfg.PortMin+1)
	for p := cfg.PortMin; p <= cfg.PortMax; p++ {
		portPool = append(portPool, p)
	}
	rng.Shuffle(len(portPool), func(i, j int) {
		portPool[i], portPool[j] = portPool[j], portPool[i]
	})

	return &natTransport{
		net:          n,
		internalAddr: internalAddr,
		publicIP:     cfg.PublicIP,
		recvCh:       make(chan packet, defaultQueueSize),
		mappings:     make(map[string]*natMapping),
		portPool:     portPool,
	}, nil
}

func (n *Network) bindEndpoint(addr string) (*endpoint, error) {
	if addr == "" {
		return nil, errors.New("address required")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if ep, ok := n.endpoints[addr]; ok && !ep.closed.Load() {
		return nil, fmt.Errorf("%w: %s", ErrAddressInUse, addr)
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
		return nil
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

func (t *publicTransport) Recv() (string, []byte, error) {
	pkt, ok := <-t.ep.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (t *publicTransport) Send(dst string, b []byte) error {
	if t.ep.closed.Load() {
		return ErrTransportClosed
	}
	return t.net.send(t.ep.addr, dst, b)
}

func (t *publicTransport) Close() error {
	if t.ep == nil || t.ep.closed.Load() {
		return nil
	}
	t.ep.close()
	t.net.unbind(t.ep)
	return nil
}

func (t *natTransport) Recv() (string, []byte, error) {
	pkt, ok := <-t.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (t *natTransport) Send(dst string, b []byte) error {
	if t.closed.Load() {
		return ErrTransportClosed
	}

	m, err := t.ensureMapping(dst)
	if err != nil {
		return err
	}

	return t.net.send(m.extAddr, dst, b)
}

func (t *natTransport) Close() error {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		t.mu.Lock()
		for _, m := range t.mappings {
			m.ep.close()
			t.net.unbind(m.ep)
		}
		t.mu.Unlock()
		close(t.recvCh)
	})
	return nil
}

func (t *natTransport) ensureMapping(dst string) (*natMapping, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed.Load() {
		return nil, ErrTransportClosed
	}

	if m, ok := t.mappings[dst]; ok {
		return m, nil
	}

	port, err := t.nextPortLocked()
	if err != nil {
		return nil, err
	}

	extAddr := net.JoinHostPort(t.publicIP, strconv.Itoa(port))
	ep, err := t.net.bindEndpoint(extAddr)
	if err != nil {
		return nil, err
	}

	m := &natMapping{destAddr: dst, extAddr: extAddr, ep: ep}
	t.mappings[dst] = m
	go t.recvLoop(m)
	return m, nil
}

func (t *natTransport) nextPortLocked() (int, error) {
	for t.nextPort < len(t.portPool) {
		port := t.portPool[t.nextPort]
		t.nextPort++
		extAddr := net.JoinHostPort(t.publicIP, strconv.Itoa(port))
		if _, ok := t.net.lookup(extAddr); ok {
			continue
		}
		return port, nil
	}
	return 0, ErrNoAvailablePort
}

func (t *natTransport) recvLoop(m *natMapping) {
	for {
		pkt, ok := <-m.ep.recvCh
		if !ok {
			return
		}
		if pkt.src != m.destAddr {
			continue
		}
		select {
		case t.recvCh <- pkt:
		default:
		}
	}
}
