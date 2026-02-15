package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/quic"
	"github.com/sambigeara/pollen/pkg/types"
)

// Manager manages service tunneling over QUIC streams. Once two peers have a
// QUIC connection (established by the supersock layer), tunneling is simply
// opening streams on that connection. No separate TCP setup, TLS wrapping,
// or yamux multiplexing needed.
type Manager struct {
	dir           quic.PeerDirectory
	sessions      *SessionManager
	connections   map[string]connectionHandler
	services      map[uint32]serviceHandler
	activeStreams map[uint32]map[net.Conn]struct{}
	connectionMu sync.RWMutex
	serviceMu    sync.RWMutex
	streamMu     sync.Mutex
}

type serviceHandler struct {
	fn     func(net.Conn)
	cancel context.CancelFunc
}

type connectionHandler struct {
	ln     net.Listener
	cancel context.CancelFunc
	remote uint32
	local  uint32
	peerID types.PeerKey
}

type trackedConn struct {
	net.Conn
	onClose   func()
	closeOnce sync.Once
}

func (c *trackedConn) Close() error {
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.Conn.Close()
}

type ConnectionInfo struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

func connectionKey(peerID, port string) string {
	return peerID + ":" + port
}

func New(dir quic.PeerDirectory) *Manager {
	m := &Manager{
		dir:          dir,
		connections:  make(map[string]connectionHandler),
		services:     make(map[uint32]serviceHandler),
		activeStreams: make(map[uint32]map[net.Conn]struct{}),
	}
	m.sessions = NewSessionManager(m.handleIncomingStream)
	return m
}

func (m *Manager) handleIncomingStream(stream net.Conn, servicePort uint16) {
	port := uint32(servicePort)

	tracked := &trackedConn{}
	tracked.Conn = stream
	tracked.onClose = func() {
		m.removeActiveStream(port, tracked)
	}
	stream = tracked
	m.addActiveStream(port, tracked)

	m.serviceMu.RLock()
	h, ok := m.services[port]
	m.serviceMu.RUnlock()

	if !ok {
		zap.S().Warnw("no handler for incoming stream", "port", servicePort)
		_ = stream.Close()
		return
	}

	h.fn(stream)
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
		fn: func(tunnelConn net.Conn) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fmt.Sprintf("localhost:%d", port))
			if err != nil {
				zap.S().Warnw("failed to dial local service", "port", port, "err", err)
				_ = tunnelConn.Close()
				return
			}
			bridge(tunnelConn, conn)
		},
		cancel: cancelFn,
	}

	zap.S().Infow("registered service", "port", port)
}

func (m *Manager) ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	if _, ok := m.dir.IdentityPub(peerID); !ok {
		return 0, errors.New("peerID not recognised")
	}
	if remotePort == 0 {
		return 0, errors.New("remote port missing")
	}

	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	ctx, cancelFn := context.WithCancel(context.Background())
	var ln net.Listener
	var err error
	var requestedLocal uint32

	if localPort > 0 {
		requestedLocal = localPort
	} else {
		requestedLocal = remotePort
	}

	ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+strconv.FormatUint(uint64(requestedLocal), 10))
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

	portNum := uint16(remotePort)

	go func() {
		logger := zap.S().Named("tunnel")
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				// Get or create session to peer (backed by QUIC connection).
				session, err := m.sessions.GetOrCreate(ctx, peerID)
				if err != nil {
					logger.Warnw("session failed", "peer", peerID.String()[:8], "err", err)
					_ = clientConn.Close()
					return
				}

				// Open QUIC stream to remote service.
				stream, err := session.OpenStream(portNum)
				if err != nil {
					logger.Warnw("open stream failed", "peer", peerID.String()[:8], "port", remotePort, "err", err)
					m.sessions.Remove(peerID)
					_ = clientConn.Close()
					return
				}

				bridge(clientConn, stream)
			}()
		}
	}()

	m.connections[connectionKey(peerID.String(), boundPortStr)] = connectionHandler{
		cancel: cancelFn,
		peerID: peerID,
		remote: remotePort,
		local:  uint32(boundPort),
		ln:     ln,
	}

	zap.S().Infow("connected to service", "peer", peerID.String()[:8], "port", remotePort, "local_port", boundPort)
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
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	var key string
	var handler connectionHandler
	found := false
	for k, h := range m.connections {
		if h.local == port {
			key = k
			handler = h
			found = true
			break
		}
	}
	if !found {
		return false
	}
	delete(m.connections, key)
	if handler.ln != nil {
		_ = handler.ln.Close()
	}
	handler.cancel()
	return true
}

func (m *Manager) DisconnectRemoteService(peerID types.PeerKey, remotePort uint32) int {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	removed := 0
	keys := make([]string, 0, len(m.connections))
	for key, handler := range m.connections {
		if handler.peerID == peerID && handler.remote == remotePort {
			if handler.ln != nil {
				_ = handler.ln.Close()
			}
			handler.cancel()
			keys = append(keys, key)
			removed++
		}
	}
	for _, key := range keys {
		delete(m.connections, key)
	}
	return removed
}

func (m *Manager) UnregisterService(port uint32) bool {
	m.serviceMu.Lock()
	defer m.serviceMu.Unlock()

	if h, ok := m.services[port]; ok {
		h.cancel()
		delete(m.services, port)
		m.closeServiceStreams(port)
		return true
	}
	m.closeServiceStreams(port)
	return false
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

func (m *Manager) addActiveStream(port uint32, stream net.Conn) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	streams, ok := m.activeStreams[port]
	if !ok {
		streams = make(map[net.Conn]struct{})
		m.activeStreams[port] = streams
	}
	streams[stream] = struct{}{}
}

func (m *Manager) removeActiveStream(port uint32, stream net.Conn) {
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

// Sessions returns the session manager.
func (m *Manager) Sessions() *SessionManager {
	return m.sessions
}

// Close shuts down the manager and all sessions.
func (m *Manager) Close() {
	m.sessions.Close()

	m.serviceMu.Lock()
	for _, h := range m.services {
		h.cancel()
	}
	m.serviceMu.Unlock()

	m.connectionMu.Lock()
	for _, h := range m.connections {
		h.cancel()
	}
	m.connectionMu.Unlock()
}
