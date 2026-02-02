package memtransport

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/sambigeara/pollen/pkg/socket"
	"github.com/sambigeara/pollen/pkg/types"
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

// MemSocket implements socket.Socket
type MemSocket struct {
	net  *Network
	ep   *endpoint
	addr string
}

var _ socket.Socket = (*MemSocket)(nil)

func (s *MemSocket) LocalAddr() net.Addr {
	addr, _ := net.ResolveUDPAddr("udp", s.addr)
	return addr
}

func (s *MemSocket) Send(dst string, b []byte) error {
	if s.ep.closed.Load() {
		return ErrTransportClosed
	}
	return s.net.send(s.ep.addr, dst, b)
}

func (s *MemSocket) Recv() (string, []byte, error) {
	pkt, ok := <-s.ep.recvCh
	if !ok {
		return "", nil, ErrTransportClosed
	}
	return pkt.src, pkt.payload, nil
}

func (s *MemSocket) Close() error {
	if s.ep == nil || s.ep.closed.Load() {
		return nil
	}
	s.ep.close()
	s.net.unbind(s.ep)
	return nil
}

func (s *MemSocket) IsClosed() bool {
	return s.ep.closed.Load()
}

// MemSocketStore implements socket.SocketStore
type MemSocketStore struct {
	net        *Network
	base       *MemSocket
	ephemeral  map[*MemSocket]struct{}
	peerSocket map[types.PeerKey]socket.Socket
	events     chan socket.SocketEvent
	mu         sync.RWMutex
	wg         sync.WaitGroup
	closed     atomic.Bool
	addr       string
	nextPort   int
}

var _ socket.SocketStore = (*MemSocketStore)(nil)

func (n *Network) Bind(addr string) (socket.SocketStore, error) {
	ep, err := n.bindEndpoint(addr)
	if err != nil {
		return nil, err
	}

	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)

	store := &MemSocketStore{
		net:        n,
		base:       &MemSocket{net: n, ep: ep, addr: addr},
		ephemeral:  make(map[*MemSocket]struct{}),
		peerSocket: make(map[types.PeerKey]socket.Socket),
		events:     make(chan socket.SocketEvent, defaultQueueSize),
		addr:       host,
		nextPort:   port + 1,
	}

	store.startRecvLoop(store.base)

	return store, nil
}

func (s *MemSocketStore) Base() socket.Socket {
	return s.base
}

func (s *MemSocketStore) CreateEphemeral() (socket.Socket, error) {
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

	sock := &MemSocket{net: s.net, ep: ep, addr: addr}

	s.mu.Lock()
	s.ephemeral[sock] = struct{}{}
	s.mu.Unlock()

	s.startRecvLoop(sock)

	return sock, nil
}

func (s *MemSocketStore) CreateBatch(count int) ([]socket.Socket, error) {
	sockets := make([]socket.Socket, 0, count)
	for range count {
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

func (s *MemSocketStore) CloseEphemeral() {
	s.mu.Lock()
	ephemeral := s.ephemeral
	s.ephemeral = make(map[*MemSocket]struct{})
	s.mu.Unlock()

	for sock := range ephemeral {
		sock.Close()
	}
}

func (s *MemSocketStore) Close() error {
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

func (s *MemSocketStore) Events() <-chan socket.SocketEvent {
	return s.events
}

func (s *MemSocketStore) AssociatePeerSocket(peerKey types.PeerKey, sock socket.Socket) {
	var prev socket.Socket
	var closePrev bool

	s.mu.Lock()
	prev = s.peerSocket[peerKey]
	s.peerSocket[peerKey] = sock

	// O(1) removal from ephemeral set
	if impl, ok := sock.(*MemSocket); ok {
		delete(s.ephemeral, impl)
	}

	if prev != nil && prev != sock && prev != s.base {
		shared := false
		for k, v := range s.peerSocket {
			if k == peerKey {
				continue
			}
			if v == prev {
				shared = true
				break
			}
		}
		closePrev = !shared
	}
	s.mu.Unlock()

	if closePrev {
		_ = prev.Close()
	}
}

func (s *MemSocketStore) GetPeerSocket(peerKey types.PeerKey) (socket.Socket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sock, ok := s.peerSocket[peerKey]
	return sock, ok
}

func (s *MemSocketStore) SendToPeer(peerKey types.PeerKey, dst string, b []byte) error {
	sock, ok := s.GetPeerSocket(peerKey)
	if !ok || sock.IsClosed() {
		sock = s.base
	}
	return sock.Send(dst, b)
}

func (s *MemSocketStore) startRecvLoop(sock *MemSocket) {
	&{s wg}.Go(func() {
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
	})
}
