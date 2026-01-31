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

	"github.com/sambigeara/pollen/pkg/socket"
	"github.com/sambigeara/pollen/pkg/types"
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

// PublicSocket implements socket.Socket for endpoints with a public address
type PublicSocket struct {
	net  *Network
	ep   *endpoint
	addr string
}

var _ socket.Socket = (*PublicSocket)(nil)

func (s *PublicSocket) LocalAddr() net.Addr {
	addr, _ := net.ResolveUDPAddr("udp", s.addr)
	return addr
}

func (s *PublicSocket) Send(dst string, b []byte) error {
	if s.ep.closed.Load() {
		return ErrTransportClosed
	}
	return s.net.send(s.ep.addr, dst, b)
}

func (s *PublicSocket) Recv() (string, []byte, error) {
	pkt, ok := <-s.ep.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (s *PublicSocket) Close() error {
	if s.ep == nil || s.ep.closed.Load() {
		return nil
	}
	s.ep.close()
	s.net.unbind(s.ep)
	return nil
}

func (s *PublicSocket) IsClosed() bool {
	return s.ep.closed.Load()
}

// NATSocket implements socket.Socket for endpoints behind a NAT
type NATSocket struct {
	store       *NATSocketStore
	extAddr     string // external address (public IP:port)
	destAddr    string // destination this mapping is for (symmetric NAT)
	ep          *endpoint
	isEphemeral bool // true if this is an ephemeral socket for birthday punch
}

var _ socket.Socket = (*NATSocket)(nil)

func (s *NATSocket) LocalAddr() net.Addr {
	addr, _ := net.ResolveUDPAddr("udp", s.extAddr)
	return addr
}

func (s *NATSocket) Send(dst string, b []byte) error {
	if s.ep.closed.Load() {
		return ErrTransportClosed
	}
	if debug {
		log.Printf("[NAT %s] send from %s to %s (ephemeral=%v)", s.store.publicIP, s.extAddr, dst, s.isEphemeral)
	}
	// Track that we've sent to this destination (both exact and IP-only)
	s.store.mu.Lock()
	s.store.sentTo[dst] = true
	if host, _, err := net.SplitHostPort(dst); err == nil {
		s.store.sentToIP[host] = true
	}
	s.store.mu.Unlock()
	return s.store.net.send(s.extAddr, dst, b)
}

func (s *NATSocket) Recv() (string, []byte, error) {
	pkt, ok := <-s.ep.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (s *NATSocket) Close() error {
	if s.ep == nil || s.ep.closed.Load() {
		return nil
	}
	s.ep.close()
	s.store.net.unbind(s.ep)
	return nil
}

func (s *NATSocket) IsClosed() bool {
	return s.ep.closed.Load()
}

// PublicSocketStore implements socket.SocketStore for public endpoints
type PublicSocketStore struct {
	net        *Network
	base       *PublicSocket
	ephemeral  map[*PublicSocket]struct{}
	peerSocket map[types.PeerKey]socket.Socket
	events     chan socket.SocketEvent
	mu         sync.RWMutex
	wg         sync.WaitGroup
	closed     atomic.Bool
	addr       string
	nextPort   int
}

var _ socket.SocketStore = (*PublicSocketStore)(nil)

func (n *Network) Bind(addr string) (socket.SocketStore, error) {
	ep, err := n.bindEndpoint(addr)
	if err != nil {
		return nil, err
	}

	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)

	store := &PublicSocketStore{
		net:        n,
		base:       &PublicSocket{net: n, ep: ep, addr: addr},
		ephemeral:  make(map[*PublicSocket]struct{}),
		peerSocket: make(map[types.PeerKey]socket.Socket),
		events:     make(chan socket.SocketEvent, defaultQueueSize),
		addr:       host,
		nextPort:   port + 1,
	}

	store.startRecvLoop(store.base)

	return store, nil
}

func (s *PublicSocketStore) Base() socket.Socket {
	return s.base
}

func (s *PublicSocketStore) CreateEphemeral() (socket.Socket, error) {
	if s.closed.Load() {
		return nil, net.ErrClosed
	}

	s.mu.Lock()
	port := s.nextPort
	s.nextPort++
	s.mu.Unlock()

	addr := net.JoinHostPort(s.addr, strconv.Itoa(port))
	ep, err := s.net.bindEndpoint(addr)
	if err != nil {
		return nil, err
	}

	sock := &PublicSocket{net: s.net, ep: ep, addr: addr}

	s.mu.Lock()
	s.ephemeral[sock] = struct{}{}
	s.mu.Unlock()

	s.startRecvLoop(sock)

	return sock, nil
}

func (s *PublicSocketStore) CreateBatch(count int) ([]socket.Socket, error) {
	sockets := make([]socket.Socket, 0, count)
	for i := 0; i < count; i++ {
		sock, err := s.CreateEphemeral()
		if err != nil {
			for _, created := range sockets {
				created.Close()
			}
			return nil, err
		}
		sockets = append(sockets, sock)
	}
	return sockets, nil
}

func (s *PublicSocketStore) CloseEphemeral() {
	s.mu.Lock()
	ephemeral := s.ephemeral
	s.ephemeral = make(map[*PublicSocket]struct{})
	s.mu.Unlock()

	for sock := range ephemeral {
		sock.Close()
	}
}

func (s *PublicSocketStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	s.CloseEphemeral()

	// Close peer sockets so their recv loops can exit
	s.mu.Lock()
	for _, sock := range s.peerSocket {
		sock.Close()
	}
	s.peerSocket = make(map[types.PeerKey]socket.Socket)
	s.mu.Unlock()

	err := s.base.Close()

	s.wg.Wait()
	close(s.events)

	return err
}

func (s *PublicSocketStore) Events() <-chan socket.SocketEvent {
	return s.events
}

func (s *PublicSocketStore) AssociatePeerSocket(peerKey types.PeerKey, sock socket.Socket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerSocket[peerKey] = sock

	// O(1) removal from ephemeral set
	if impl, ok := sock.(*PublicSocket); ok {
		delete(s.ephemeral, impl)
	}
}

func (s *PublicSocketStore) GetPeerSocket(peerKey types.PeerKey) (socket.Socket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sock, ok := s.peerSocket[peerKey]
	return sock, ok
}

func (s *PublicSocketStore) SendToPeer(peerKey types.PeerKey, dst string, b []byte) error {
	sock, ok := s.GetPeerSocket(peerKey)
	if !ok || sock.IsClosed() {
		sock = s.base
	}
	return sock.Send(dst, b)
}

func (s *PublicSocketStore) startRecvLoop(sock *PublicSocket) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			src, payload, err := sock.Recv()
			if err != nil {
				if sock.IsClosed() || s.closed.Load() {
					return
				}
				continue
			}

			select {
			case s.events <- socket.SocketEvent{
				Socket:  sock,
				Src:     src,
				Payload: payload,
			}:
			default:
			}
		}
	}()
}

// NATSocketStore implements socket.SocketStore for endpoints behind a symmetric NAT
type NATSocketStore struct {
	net          *Network
	internalAddr string
	publicIP     string
	events       chan socket.SocketEvent
	peerSocket   map[types.PeerKey]socket.Socket
	mu           sync.Mutex
	baseSockets  []*NATSocket                 // base sockets (one per destination)
	ephemeral    map[*NATSocket]struct{}      // ephemeral sockets for birthday punch
	portPool     []int
	nextPort     int
	closeOnce    sync.Once
	closed       atomic.Bool
	wg           sync.WaitGroup
	// Track all destinations we've sent to, for accepting responses
	sentTo map[string]bool
	// Track IPs we've sent to (for birthday punch - accept from any port at that IP)
	sentToIP map[string]bool
}

var _ socket.SocketStore = (*NATSocketStore)(nil)

func (n *Network) BindNAT(internalAddr string, cfg NATConfig) (socket.SocketStore, error) {
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

	return &NATSocketStore{
		net:          n,
		internalAddr: internalAddr,
		publicIP:     cfg.PublicIP,
		events:       make(chan socket.SocketEvent, defaultQueueSize),
		peerSocket:   make(map[types.PeerKey]socket.Socket),
		baseSockets:  make([]*NATSocket, 0),
		ephemeral:    make(map[*NATSocket]struct{}),
		portPool:     portPool,
		sentTo:       make(map[string]bool),
		sentToIP:     make(map[string]bool),
	}, nil
}

// Base returns a "virtual" base socket for the NAT. Since symmetric NATs create
// different mappings per destination, we return a wrapper that creates mappings on demand.
func (s *NATSocketStore) Base() socket.Socket {
	return &natBaseSocket{store: s}
}

// natBaseSocket is a wrapper that creates per-destination NAT mappings
type natBaseSocket struct {
	store *NATSocketStore
}

var _ socket.Socket = (*natBaseSocket)(nil)

func (b *natBaseSocket) LocalAddr() net.Addr {
	addr, _ := net.ResolveUDPAddr("udp", b.store.internalAddr)
	return addr
}

func (b *natBaseSocket) Send(dst string, data []byte) error {
	if b.store.closed.Load() {
		return ErrTransportClosed
	}

	sock, err := b.store.ensureMappingForDest(dst, false)
	if err != nil {
		return err
	}
	return sock.Send(dst, data)
}

func (b *natBaseSocket) Recv() (string, []byte, error) {
	// This should not be called directly - events come through the store
	return "", nil, errors.New("use SocketStore.Events() instead")
}

func (b *natBaseSocket) Close() error {
	return nil // Don't close the store from the base socket
}

func (b *natBaseSocket) IsClosed() bool {
	return b.store.closed.Load()
}

func (s *NATSocketStore) ensureMappingForDest(dst string, isEphemeral bool) (*NATSocket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return nil, ErrTransportClosed
	}

	// Track that we've sent to this destination (both exact and IP-only)
	s.sentTo[dst] = true
	if host, _, err := net.SplitHostPort(dst); err == nil {
		s.sentToIP[host] = true
	}

	// For base sockets, check if we already have a mapping for this destination
	if !isEphemeral {
		for _, sock := range s.baseSockets {
			if sock.destAddr == dst {
				return sock, nil
			}
		}
	}

	port, err := s.nextPortLocked()
	if err != nil {
		return nil, err
	}

	extAddr := net.JoinHostPort(s.publicIP, strconv.Itoa(port))
	ep, err := s.net.bindEndpoint(extAddr)
	if err != nil {
		return nil, err
	}

	sock := &NATSocket{
		store:       s,
		extAddr:     extAddr,
		destAddr:    dst,
		ep:          ep,
		isEphemeral: isEphemeral,
	}

	if isEphemeral {
		s.ephemeral[sock] = struct{}{}
	} else {
		s.baseSockets = append(s.baseSockets, sock)
	}

	s.startRecvLoop(sock)

	return sock, nil
}

func (s *NATSocketStore) CreateEphemeral() (socket.Socket, error) {
	if s.closed.Load() {
		return nil, net.ErrClosed
	}

	// For ephemeral sockets, we create a socket without a specific destination
	// It will get a unique external port from the NAT
	s.mu.Lock()
	defer s.mu.Unlock()

	port, err := s.nextPortLocked()
	if err != nil {
		return nil, err
	}

	extAddr := net.JoinHostPort(s.publicIP, strconv.Itoa(port))
	ep, err := s.net.bindEndpoint(extAddr)
	if err != nil {
		return nil, err
	}

	sock := &NATSocket{
		store:       s,
		extAddr:     extAddr,
		destAddr:    "", // No specific destination - accept from anywhere
		ep:          ep,
		isEphemeral: true,
	}

	s.ephemeral[sock] = struct{}{}
	s.startRecvLoop(sock)

	return sock, nil
}

func (s *NATSocketStore) CreateBatch(count int) ([]socket.Socket, error) {
	sockets := make([]socket.Socket, 0, count)
	for i := 0; i < count; i++ {
		sock, err := s.CreateEphemeral()
		if err != nil {
			for _, created := range sockets {
				created.Close()
			}
			return nil, err
		}
		sockets = append(sockets, sock)
	}
	return sockets, nil
}

func (s *NATSocketStore) CloseEphemeral() {
	s.mu.Lock()
	ephemeral := s.ephemeral
	s.ephemeral = make(map[*NATSocket]struct{})
	s.mu.Unlock()

	for sock := range ephemeral {
		sock.Close()
	}
}

func (s *NATSocketStore) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)

		s.mu.Lock()
		for _, sock := range s.baseSockets {
			sock.Close()
		}
		for sock := range s.ephemeral {
			sock.Close()
		}
		// Close peer sockets so their recv loops can exit
		for _, sock := range s.peerSocket {
			sock.Close()
		}
		s.mu.Unlock()

		s.wg.Wait()
		close(s.events)
	})
	return nil
}

func (s *NATSocketStore) Events() <-chan socket.SocketEvent {
	return s.events
}

func (s *NATSocketStore) AssociatePeerSocket(peerKey types.PeerKey, sock socket.Socket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerSocket[peerKey] = sock

	// O(1) removal from ephemeral set
	if impl, ok := sock.(*NATSocket); ok {
		delete(s.ephemeral, impl)
	}
}

func (s *NATSocketStore) GetPeerSocket(peerKey types.PeerKey) (socket.Socket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sock, ok := s.peerSocket[peerKey]
	return sock, ok
}

func (s *NATSocketStore) SendToPeer(peerKey types.PeerKey, dst string, b []byte) error {
	sock, ok := s.GetPeerSocket(peerKey)
	if !ok || sock.IsClosed() {
		// Create a new mapping for this destination
		var err error
		sock, err = s.ensureMappingForDest(dst, false)
		if err != nil {
			return err
		}
	}
	return sock.Send(dst, b)
}

func (s *NATSocketStore) nextPortLocked() (int, error) {
	for s.nextPort < len(s.portPool) {
		port := s.portPool[s.nextPort]
		s.nextPort++
		extAddr := net.JoinHostPort(s.publicIP, strconv.Itoa(port))
		if _, ok := s.net.lookup(extAddr); ok {
			continue
		}
		return port, nil
	}
	return 0, ErrNoAvailablePort
}

func (s *NATSocketStore) hasSentTo(addr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check exact address match first
	if s.sentTo[addr] {
		return true
	}
	// Check if we've sent to any port at this IP (for birthday punch)
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return s.sentToIP[host]
	}
	return false
}

func (s *NATSocketStore) startRecvLoop(sock *NATSocket) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			pkt, ok := <-sock.ep.recvCh
			if !ok {
				return
			}

			// For NAT filtering:
			// Accept if either:
			// 1. This is the socket's designated destination (for base sockets)
			// 2. We've sent to this source from ANY socket (hole punching effect)
			// This simulates "endpoint-independent filtering" NAT behavior where
			// outbound packets create holes that accept responses from those destinations
			accepted := false
			if sock.destAddr != "" && pkt.src == sock.destAddr {
				// Exact match for base socket
				accepted = true
			} else if s.hasSentTo(pkt.src) {
				// Have a hole for this source
				accepted = true
			}

			if debug {
				log.Printf("[NAT %s] recv on %s from %s, destAddr=%s, hasSentTo=%v, accepted=%v",
					s.publicIP, sock.extAddr, pkt.src, sock.destAddr, s.hasSentTo(pkt.src), accepted)
			}

			if !accepted {
				continue
			}

			select {
			case s.events <- socket.SocketEvent{
				Socket:  sock,
				Src:     pkt.src,
				Payload: pkt.payload,
			}:
			default:
			}
		}
	}()
}
