package nattransport

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sambigeara/pollen/pkg/transport"
)

var debug = os.Getenv("NAT_DEBUG") == "1"

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
		return nil // silently drop packets to unknown destinations (simulates real network)
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

// publicTransport implements transport.Transport for public endpoints
type publicTransport struct {
	net  *Network
	ep   *endpoint
	addr string
}

var _ transport.Transport = (*publicTransport)(nil)

func (n *Network) Bind(addr string) (transport.Transport, error) {
	ep, err := n.bindEndpoint(addr)
	if err != nil {
		return nil, err
	}

	return &publicTransport{
		net:  n,
		ep:   ep,
		addr: addr,
	}, nil
}

func (t *publicTransport) Recv() (string, []byte, error) {
	pkt, ok := <-t.ep.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (t *publicTransport) Send(dst string, b []byte, _ bool) error {
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

// natTransport implements transport.Transport for endpoints behind a symmetric NAT
type natTransport struct {
	net          *Network
	internalAddr string
	publicIP     string
	portPool     []int
	nextPort     int
	events       chan packet
	mappings     []*natMapping // per-destination mappings
	mu           sync.Mutex
	closeOnce    sync.Once
	closed       atomic.Bool
	wg           sync.WaitGroup
	// Track all destinations we've sent to, for accepting responses
	sentTo map[string]bool
	// Track IPs we've sent to (for accepting responses from any port at that IP)
	sentToIP map[string]bool
}

type natMapping struct {
	extAddr  string // external address (public IP:port)
	destAddr string // destination this mapping is for (symmetric NAT)
	ep       *endpoint
}

var _ transport.Transport = (*natTransport)(nil)

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
		events:       make(chan packet, defaultQueueSize),
		mappings:     make([]*natMapping, 0),
		portPool:     portPool,
		sentTo:       make(map[string]bool),
		sentToIP:     make(map[string]bool),
	}, nil
}

func (t *natTransport) Recv() (string, []byte, error) {
	pkt, ok := <-t.events
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (t *natTransport) Send(dst string, b []byte, _ bool) error {
	if t.closed.Load() {
		return ErrTransportClosed
	}

	mapping, err := t.ensureMappingForDest(dst)
	if err != nil {
		return err
	}

	if debug {
		log.Printf("[NAT %s] send from %s to %s", t.publicIP, mapping.extAddr, dst)
	}

	// Track that we've sent to this destination
	t.mu.Lock()
	t.sentTo[dst] = true
	if host, _, err := net.SplitHostPort(dst); err == nil {
		t.sentToIP[host] = true
	}
	t.mu.Unlock()

	return t.net.send(mapping.extAddr, dst, b)
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

		t.wg.Wait()
		close(t.events)
	})
	return nil
}

func (t *natTransport) ensureMappingForDest(dst string) (*natMapping, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed.Load() {
		return nil, ErrTransportClosed
	}

	// Check if we already have a mapping for this destination
	for _, m := range t.mappings {
		if m.destAddr == dst {
			return m, nil
		}
	}

	// Create a new mapping
	port, err := t.nextPortLocked()
	if err != nil {
		return nil, err
	}

	extAddr := net.JoinHostPort(t.publicIP, strconv.Itoa(port))
	ep, err := t.net.bindEndpoint(extAddr)
	if err != nil {
		return nil, err
	}

	mapping := &natMapping{
		extAddr:  extAddr,
		destAddr: dst,
		ep:       ep,
	}

	t.mappings = append(t.mappings, mapping)
	t.startRecvLoop(mapping)

	return mapping, nil
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

func (t *natTransport) hasSentTo(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Check exact address match first
	if t.sentTo[addr] {
		return true
	}
	// Check if we've sent to any port at this IP
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return t.sentToIP[host]
	}
	return false
}

func (t *natTransport) startRecvLoop(m *natMapping) {
	t.wg.Go(func() {
		for {
			pkt, ok := <-m.ep.recvCh
			if !ok {
				return
			}

			// NAT filtering: accept if we've sent to this source
			accepted := false
			if pkt.src == m.destAddr {
				// Exact match for this mapping's destination
				accepted = true
			} else if t.hasSentTo(pkt.src) {
				// Have a hole for this source
				accepted = true
			}

			if debug {
				log.Printf("[NAT %s] recv on %s from %s, destAddr=%s, hasSentTo=%v, accepted=%v",
					t.publicIP, m.extAddr, pkt.src, m.destAddr, t.hasSentTo(pkt.src), accepted)
			}

			if !accepted {
				continue
			}

			select {
			case t.events <- pkt:
			default:
			}
		}
	})
}
