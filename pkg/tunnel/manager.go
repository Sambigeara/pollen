package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/types"
)

var errNoServicePort = errors.New("no service port in stream header")

// StreamTransport abstracts peer stream transport lifecycle.
// mesh owns QUIC connection/session management and provides this seam.
type StreamTransport interface {
	OpenStream(ctx context.Context, peerID types.PeerKey) (io.ReadWriteCloser, error)
	AcceptStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error)
}

// Manager manages service tunneling over peer streams.
type Manager struct {
	log           *zap.SugaredLogger
	transport     StreamTransport
	connections   map[string]connectionHandler
	services      map[uint32]serviceHandler
	activeStreams map[uint32]map[io.ReadWriteCloser]struct{}
	connectionMu  sync.RWMutex
	serviceMu     sync.RWMutex
	streamMu      sync.Mutex
}

type serviceHandler struct {
	fn     func(io.ReadWriteCloser)
	cancel context.CancelFunc
}

type connectionHandler struct {
	ln     net.Listener
	cancel context.CancelFunc
	remote uint32
	local  uint32
	peerID types.PeerKey
}

type trackedStream struct {
	io.ReadWriteCloser
	onClose   func()
	closeOnce sync.Once
}

func (c *trackedStream) Close() error {
	c.closeOnce.Do(c.onClose)
	return c.ReadWriteCloser.Close()
}

type ConnectionInfo struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

func New(transport StreamTransport) *Manager {
	return &Manager{
		log:           zap.S().Named("tun"),
		transport:     transport,
		connections:   make(map[string]connectionHandler),
		services:      make(map[uint32]serviceHandler),
		activeStreams: make(map[uint32]map[io.ReadWriteCloser]struct{}),
	}
}

func (m *Manager) Start(ctx context.Context) {
	go func() {
		if err := m.acceptStreams(ctx); err != nil {
			m.log.Debugw("tunnel stream loop stopped", "err", err)
		}
	}()
}

func (m *Manager) acceptStreams(ctx context.Context) error {
	for {
		peerID, stream, err := m.transport.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		go m.handleIncomingStream(peerID, stream)
	}
}

func (m *Manager) handleIncomingStream(peerID types.PeerKey, stream io.ReadWriteCloser) {
	port, err := readServicePort(stream)
	if err != nil {
		m.log.Warnw("failed reading service port from stream", "peer", peerID.Short(), "err", err)
		_ = stream.Close()
		return
	}

	tracked := &trackedStream{ReadWriteCloser: stream}
	tracked.onClose = func() { m.removeActiveStream(port, tracked) }
	m.addActiveStream(port, tracked)

	m.serviceMu.RLock()
	h, ok := m.services[port]
	m.serviceMu.RUnlock()

	if !ok {
		m.log.Warnw("no handler for incoming stream", "peer", peerID.Short(), "port", port)
		_ = tracked.Close()
		return
	}

	h.fn(tracked)
}

func readServicePort(r io.Reader) (uint32, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, err
	}

	port := binary.BigEndian.Uint16(header[:])
	if port == 0 {
		return 0, errNoServicePort
	}

	return uint32(port), nil
}

func writeServicePort(w io.Writer, port uint32) error {
	if port == 0 || port > 0xffff {
		return errors.New("remote port missing")
	}

	var header [2]byte
	binary.BigEndian.PutUint16(header[:], uint16(port))
	_, err := w.Write(header[:])
	return err
}

func (m *Manager) RegisterService(port uint32) {
	m.serviceMu.Lock()
	defer m.serviceMu.Unlock()

	curr, ok := m.services[port]
	if ok {
		curr.cancel()
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	m.services[port] = serviceHandler{
		fn: func(tunnelConn io.ReadWriteCloser) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fmt.Sprintf("localhost:%d", port))
			if err != nil {
				m.log.Warnw("failed to dial local service", "port", port, "err", err)
				_ = tunnelConn.Close()
				return
			}
			bridge(tunnelConn, conn)
		},
		cancel: cancelFn,
	}

	m.log.Infow("registered service", "port", port)
}

func (m *Manager) ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	if remotePort == 0 || remotePort > 0xffff {
		return 0, errors.New("remote port missing")
	}

	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	// TODO(saml): use a map keyed by peerID:remotePort instead of iterating all connections
	for _, h := range m.connections {
		if h.peerID == peerID && h.remote == remotePort {
			return 0, fmt.Errorf("already connected to port %d on peer %s (local port %d)", remotePort, peerID.Short(), h.local)
		}
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	listenPort := localPort
	if listenPort == 0 {
		listenPort = remotePort
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+strconv.FormatUint(uint64(listenPort), 10))
	if err != nil && localPort == 0 {
		ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	}

	if err != nil {
		cancelFn()
		return 0, err
	}

	_, boundPortStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		cancelFn()
		_ = ln.Close()
		return 0, err
	}
	boundPort, err := strconv.ParseUint(boundPortStr, 10, 16)
	if err != nil {
		cancelFn()
		_ = ln.Close()
		return 0, err
	}

	go func() {
		logger := m.log.Named("tunnel")
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				stream, err := m.transport.OpenStream(ctx, peerID)
				if err != nil {
					logger.Warnw("open stream failed", "peer", peerID.Short(), "port", remotePort, "err", err)
					_ = clientConn.Close()
					return
				}

				if err := writeServicePort(stream, remotePort); err != nil {
					logger.Warnw("write stream header failed", "peer", peerID.Short(), "port", remotePort, "err", err)
					_ = stream.Close()
					_ = clientConn.Close()
					return
				}

				bridge(clientConn, stream)
			}()
		}
	}()

	m.connections[peerID.String()+":"+boundPortStr] = connectionHandler{
		cancel: cancelFn,
		peerID: peerID,
		remote: remotePort,
		local:  uint32(boundPort),
		ln:     ln,
	}

	m.log.Infow("connected to service", "peer", peerID.Short(), "port", remotePort, "local_port", boundPort)
	return uint32(boundPort), nil
}

func (m *Manager) ListConnections() []ConnectionInfo {
	m.connectionMu.RLock()
	defer m.connectionMu.RUnlock()

	out := make([]ConnectionInfo, 0, len(m.connections))
	for _, h := range m.connections {
		out = append(out, ConnectionInfo{
			PeerID:     h.peerID,
			RemotePort: h.remote,
			LocalPort:  h.local,
		})
	}
	return out
}

func (m *Manager) DisconnectLocalPort(port uint32) bool {
	return m.disconnectWhere(func(h connectionHandler) bool { return h.local == port }) > 0
}

func (m *Manager) DisconnectPeer(peerID types.PeerKey) int {
	return m.disconnectWhere(func(h connectionHandler) bool { return h.peerID == peerID })
}

func (m *Manager) DisconnectRemoteService(peerID types.PeerKey, remotePort uint32) int {
	return m.disconnectWhere(func(h connectionHandler) bool {
		return h.peerID == peerID && h.remote == remotePort
	})
}

func (m *Manager) disconnectWhere(match func(connectionHandler) bool) int {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	removed := 0
	for key, h := range m.connections {
		if match(h) {
			_ = h.ln.Close()
			h.cancel()
			delete(m.connections, key)
			removed++
		}
	}
	return removed
}

func (m *Manager) UnregisterService(port uint32) bool {
	m.serviceMu.Lock()
	defer m.serviceMu.Unlock()

	h, ok := m.services[port]
	if ok {
		h.cancel()
		delete(m.services, port)
	}
	m.closeServiceStreams(port)
	return ok
}

func (m *Manager) closeServiceStreams(port uint32) {
	m.streamMu.Lock()
	streams := m.activeStreams[port]
	delete(m.activeStreams, port)
	m.streamMu.Unlock()

	for stream := range streams {
		_ = stream.Close()
	}
}

func (m *Manager) addActiveStream(port uint32, stream io.ReadWriteCloser) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	streams, ok := m.activeStreams[port]
	if !ok {
		streams = make(map[io.ReadWriteCloser]struct{})
		m.activeStreams[port] = streams
	}
	streams[stream] = struct{}{}
}

func (m *Manager) removeActiveStream(port uint32, stream io.ReadWriteCloser) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	streams, ok := m.activeStreams[port]
	if !ok {
		return
	}
	delete(streams, stream)
	if len(streams) == 0 {
		delete(m.activeStreams, port)
	}
}

func (m *Manager) Close() {
	m.serviceMu.Lock()
	for _, h := range m.services {
		h.cancel()
	}
	m.serviceMu.Unlock()

	m.connectionMu.Lock()
	for _, h := range m.connections {
		_ = h.ln.Close()
		h.cancel()
	}
	m.connectionMu.Unlock()

	m.streamMu.Lock()
	portStreams := m.activeStreams
	m.activeStreams = make(map[uint32]map[io.ReadWriteCloser]struct{})
	m.streamMu.Unlock()

	for _, streams := range portStreams {
		for stream := range streams {
			_ = stream.Close()
		}
	}
}
