package link

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/sambigeara/pollen/pkg/socket"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	udpReadBufferSize  = 64 * 1024
	socketEventBufSize = 1024
)

// Re-export types from socket package for convenience
type Socket = socket.Socket
type SocketEvent = socket.SocketEvent
type SocketStore = socket.SocketStore

var _ socket.Socket = (*socketImpl)(nil)
var _ socket.SocketStore = (*socketStoreImpl)(nil)

type socketImpl struct {
	conn      *net.UDPConn
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func newSocket(port int) (*socketImpl, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: nil, Port: port})
	if err != nil {
		return nil, fmt.Errorf("failed to listen UDP: %w", err)
	}
	return &socketImpl{conn: conn}, nil
}

func (s *socketImpl) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *socketImpl) Send(dst string, b []byte) error {
	if s.closed.Load() {
		return net.ErrClosed
	}
	addr, err := net.ResolveUDPAddr("udp", dst)
	if err != nil {
		return err
	}
	_, err = s.conn.WriteToUDP(b, addr)
	return err
}

func (s *socketImpl) Recv() (string, []byte, error) {
	buf := make([]byte, udpReadBufferSize)
	n, addr, err := s.conn.ReadFromUDP(buf)
	if err != nil {
		return "", nil, err
	}
	return addr.String(), buf[:n], nil
}

func (s *socketImpl) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.closeErr = s.conn.Close()
	})
	return s.closeErr
}

func (s *socketImpl) IsClosed() bool {
	return s.closed.Load()
}

type socketStoreImpl struct {
	log        *zap.SugaredLogger
	base       *socketImpl
	ephemeral  map[*socketImpl]struct{}
	peerSocket map[types.PeerKey]Socket
	events     chan SocketEvent
	mu         sync.RWMutex
	wg         sync.WaitGroup
	closed     atomic.Bool
}

func NewSocketStore(port int) (socket.SocketStore, error) {
	base, err := newSocket(port)
	if err != nil {
		return nil, err
	}

	store := &socketStoreImpl{
		log:        zap.S().Named("socketstore"),
		base:       base,
		ephemeral:  make(map[*socketImpl]struct{}),
		peerSocket: make(map[types.PeerKey]Socket),
		events:     make(chan SocketEvent, socketEventBufSize),
	}

	store.startRecvLoop(base)

	return store, nil
}

func (s *socketStoreImpl) Base() Socket {
	return s.base
}

func (s *socketStoreImpl) CreateEphemeral() (Socket, error) {
	if s.closed.Load() {
		return nil, net.ErrClosed
	}

	sock, err := newSocket(0) // port 0 = OS assigns ephemeral port
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.ephemeral[sock] = struct{}{}
	s.mu.Unlock()

	s.startRecvLoop(sock)

	return sock, nil
}

func (s *socketStoreImpl) CreateBatch(count int) ([]Socket, error) {
	if s.closed.Load() {
		return nil, net.ErrClosed
	}

	sockets := make([]Socket, 0, count)
	for i := 0; i < count; i++ {
		sock, err := s.CreateEphemeral()
		if err != nil {
			// Close any sockets we already created
			for _, created := range sockets {
				created.Close()
			}
			return nil, fmt.Errorf("failed to create socket %d/%d: %w", i+1, count, err)
		}
		sockets = append(sockets, sock)
	}

	return sockets, nil
}

func (s *socketStoreImpl) CloseEphemeral() {
	s.log.Debug("closing ephemeral")
	s.mu.Lock()
	ephemeral := s.ephemeral
	s.ephemeral = make(map[*socketImpl]struct{})
	s.mu.Unlock()

	for sock := range ephemeral {
		sock.Close()
	}
}

func (s *socketStoreImpl) Close() error {
	s.log.Debug("closing SocketStore")

	if s.closed.Swap(true) {
		return nil
	}

	s.CloseEphemeral()

	s.log.Debug("closing peer sockets")
	// Close peer sockets so their recv loops can exit
	s.mu.Lock()
	for _, sock := range s.peerSocket {
		sock.Close()
	}
	s.peerSocket = make(map[types.PeerKey]Socket)
	s.mu.Unlock()

	s.log.Debug("closing base socket")
	err := s.base.Close()

	// Wait for recv loops to exit
	s.log.Debug("waiting for recv loops to exit")
	s.wg.Wait()
	s.log.Debug("closing events channel")
	close(s.events)

	s.log.Debug("successfully closed SocketStore")
	return err
}

func (s *socketStoreImpl) Events() <-chan SocketEvent {
	return s.events
}

func (s *socketStoreImpl) AssociatePeerSocket(peerKey types.PeerKey, sock Socket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerSocket[peerKey] = sock

	// O(1) removal from ephemeral set
	if impl, ok := sock.(*socketImpl); ok {
		delete(s.ephemeral, impl)
	}
}

func (s *socketStoreImpl) GetPeerSocket(peerKey types.PeerKey) (Socket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sock, ok := s.peerSocket[peerKey]
	return sock, ok
}

func (s *socketStoreImpl) SendToPeer(peerKey types.PeerKey, dst string, b []byte) error {
	sock, ok := s.GetPeerSocket(peerKey)
	if !ok {
		// Fall back to base socket
		sock = s.base
	}

	if sock.IsClosed() {
		sock = s.base
	}

	return sock.Send(dst, b)
}

func (s *socketStoreImpl) startRecvLoop(sock *socketImpl) {
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
			case s.events <- SocketEvent{
				Socket:  sock,
				Src:     src,
				Payload: payload,
			}:
			default:
				// Event buffer full, drop packet
			}
		}
	}()
}
