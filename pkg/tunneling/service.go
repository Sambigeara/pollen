package tunneling

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	streamOpenTimeout = 5 * time.Second
	bufSize           = 64 * 1024
)

var (
	errNoServicePort = errors.New("no service port in stream header")

	bufPool = sync.Pool{
		New: func() any {
			b := make([]byte, bufSize)
			return &b
		},
	}
)

type TunnelingAPI interface {
	Start(ctx context.Context) error
	Stop() error

	Connect(ctx context.Context, service, peer string) error
	Disconnect(service string) error
	ExposeService(port uint32, name string) error
	UnexposeService(name string) error
	ListConnections() []ConnectionInfo

	HandleTunnelStream(stream transport.Stream, peer types.PeerKey)
	HandleRoutedStream(stream transport.Stream, peer types.PeerKey)

	HandlePeerDenied(peerID types.PeerKey)
	DesiredPeers() []types.PeerKey
	SeedDesiredConnection(pk types.PeerKey, remotePort, localPort uint32)
	ListDesiredConnections() []ConnectionInfo
	TrafficRecorder() transport.TrafficRecorder
}

var (
	_ TunnelingAPI              = (*Service)(nil)
	_ transport.TrafficRecorder = (*tracker)(nil)
)

type ServiceState interface {
	Snapshot() state.Snapshot
	SetService(port uint32, name string) []state.Event
	RemoveService(name string) []state.Event
	SetLocalTraffic(peer types.PeerKey, in, out uint64) []state.Event
}

type StreamTransport interface {
	OpenStream(ctx context.Context, peerID types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

type RoutingTable interface {
	NextHop(dest types.PeerKey) (next types.PeerKey, ok bool)
}

type Option func(*Service)

func WithLogger(log *zap.SugaredLogger) Option {
	return func(s *Service) { s.log = log }
}

func WithTrafficTracking() Option {
	return func(s *Service) { s.trafficTracker = newTracker() }
}

func WithSampleInterval(d time.Duration) Option {
	return func(s *Service) { s.sampleInterval = d }
}

func WithReconcileInterval(d time.Duration) Option {
	return func(s *Service) { s.reconcileInterval = d }
}

type ConnectionInfo struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

type connectionKey struct {
	peerID types.PeerKey
	port   uint32
}

type desiredKey struct {
	peerID     types.PeerKey
	remotePort uint32
	localPort  uint32
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

type Service struct {
	router             RoutingTable
	store              ServiceState
	streams            StreamTransport
	services           map[uint32]serviceHandler
	activeStreams      map[uint32]map[io.ReadWriteCloser]struct{}
	trafficTracker     *tracker
	desiredConnections map[desiredKey]ConnectionInfo
	connections        map[connectionKey]connectionHandler
	log                *zap.SugaredLogger
	loopWg             sync.WaitGroup
	wg                 sync.WaitGroup
	sampleInterval     time.Duration
	reconcileInterval  time.Duration
	connectionMu       sync.RWMutex
	serviceMu          sync.RWMutex
	streamMu           sync.Mutex
	mu                 sync.Mutex
	self               types.PeerKey
}

func New(self types.PeerKey, services ServiceState, streams StreamTransport, router RoutingTable, opts ...Option) *Service {
	s := &Service{
		self:               self,
		store:              services,
		streams:            streams,
		router:             router,
		log:                zap.S().Named("tun"),
		desiredConnections: make(map[desiredKey]ConnectionInfo),
		connections:        make(map[connectionKey]connectionHandler),
		services:           make(map[uint32]serviceHandler),
		activeStreams:      make(map[uint32]map[io.ReadWriteCloser]struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Service) Start(ctx context.Context) error {
	snap := s.store.Snapshot()
	for _, svc := range snap.Services() {
		if svc.Peer == s.self {
			s.registerService(svc.Port)
		}
	}

	s.sampleTraffic()
	s.runTicker(ctx, s.sampleInterval, s.sampleTraffic)
	s.runTicker(ctx, s.reconcileInterval, func() {
		snap := s.store.Snapshot()
		s.reconcileConnections(snap)
		s.reconcileDesiredConnections(snap)
	})
	return nil
}

func (s *Service) runTicker(ctx context.Context, interval time.Duration, fn func()) {
	if interval <= 0 {
		return
	}
	s.loopWg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fn()
			}
		}
	})
}

func (s *Service) Stop() error {
	s.loopWg.Wait()

	s.serviceMu.Lock()
	for _, h := range s.services {
		h.cancel()
	}
	s.serviceMu.Unlock()

	s.connectionMu.Lock()
	for _, h := range s.connections {
		_ = h.ln.Close()
		h.cancel()
	}
	s.connectionMu.Unlock()

	s.streamMu.Lock()
	portStreams := s.activeStreams
	s.activeStreams = make(map[uint32]map[io.ReadWriteCloser]struct{})
	s.streamMu.Unlock()

	for _, streams := range portStreams {
		for stream := range streams {
			_ = stream.Close()
		}
	}

	s.wg.Wait()
	return nil
}

func (s *Service) TrafficRecorder() transport.TrafficRecorder {
	if s.trafficTracker == nil {
		return nil
	}
	return s.trafficTracker
}

func (s *Service) Connect(ctx context.Context, service, peer string) error {
	peerID, err := types.PeerKeyFromString(peer)
	if err != nil {
		return fmt.Errorf("invalid peer key: %w", err)
	}

	snap := s.store.Snapshot()
	p, ok := snap.Peer(peerID)
	if !ok || len(p.IdentityPub) == 0 {
		return errors.New("peerID not recognised")
	}

	remotePort, ok := findServicePort(snap, peerID, service)
	if !ok {
		return fmt.Errorf("service %q not found on peer %s", service, peerID.Short())
	}

	port, err := s.connectService(peerID, remotePort, 0)
	if err != nil {
		return err
	}
	s.addDesiredConnection(peerID, remotePort, port)
	return nil
}

func (s *Service) Disconnect(service string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.store.Snapshot()
	for key, conn := range s.desiredConnections {
		if matchesServiceName(snap, conn.PeerID, conn.RemotePort, service) {
			s.disconnectWhere(func(h connectionHandler) bool { return h.local == conn.LocalPort })
			delete(s.desiredConnections, key)
			return nil
		}
	}
	return fmt.Errorf("no connection for service %q", service)
}

func (s *Service) ExposeService(port uint32, name string) error {
	s.registerService(port)
	s.store.SetService(port, name)
	return nil
}

func (s *Service) UnexposeService(name string) error {
	snap := s.store.Snapshot()
	for _, svc := range snap.Services() {
		if svc.Peer == s.self && svc.Name == name {
			s.store.RemoveService(name)
			s.unregisterService(svc.Port)
			return nil
		}
	}
	return fmt.Errorf("service %q not found", name)
}

func (s *Service) HandleTunnelStream(stream transport.Stream, peer types.PeerKey) {
	s.wg.Go(func() { s.handleIncomingStream(peer, stream) })
}

func (s *Service) HandleRoutedStream(stream transport.Stream, peer types.PeerKey) {
	s.wg.Go(func() { s.handleIncomingStream(peer, stream) })
}

func (s *Service) ListConnections() []ConnectionInfo {
	s.connectionMu.RLock()
	defer s.connectionMu.RUnlock()
	out := make([]ConnectionInfo, 0, len(s.connections))
	for _, h := range s.connections {
		out = append(out, ConnectionInfo{
			PeerID:     h.peerID,
			RemotePort: h.remote,
			LocalPort:  h.local,
		})
	}
	return out
}

func (s *Service) HandlePeerDenied(peerID types.PeerKey) {
	s.disconnectWhere(func(h connectionHandler) bool { return h.peerID == peerID })
	s.removeDesiredWhere(func(c ConnectionInfo) bool { return c.PeerID == peerID })
}

func (s *Service) DesiredPeers() []types.PeerKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[types.PeerKey]struct{}, len(s.desiredConnections))
	for _, c := range s.desiredConnections {
		seen[c.PeerID] = struct{}{}
	}
	peers := make([]types.PeerKey, 0, len(seen))
	for pk := range seen {
		peers = append(peers, pk)
	}
	return peers
}

func (s *Service) SeedDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	s.addDesiredConnection(pk, remotePort, localPort)
}

func (s *Service) ListDesiredConnections() []ConnectionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ConnectionInfo, 0, len(s.desiredConnections))
	for _, c := range s.desiredConnections {
		out = append(out, c)
	}
	return out
}

func (s *Service) handleIncomingStream(peerID types.PeerKey, stream io.ReadWriteCloser) {
	port, err := readServicePort(stream)
	if err != nil {
		s.log.Warnw("failed reading service port from stream", "peer", peerID.Short(), "err", err)
		_ = stream.Close()
		return
	}

	if s.trafficTracker != nil {
		stream = wrapStream(stream, s.trafficTracker, peerID)
	}

	tracked := &trackedStream{ReadWriteCloser: stream}
	tracked.onClose = func() { s.removeActiveStream(port, tracked) }
	s.addActiveStream(port, tracked)

	s.serviceMu.RLock()
	h, ok := s.services[port]
	s.serviceMu.RUnlock()

	if !ok {
		s.log.Warnw("no handler for incoming stream", "peer", peerID.Short(), "port", port)
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

func (s *Service) registerService(port uint32) {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()

	if curr, ok := s.services[port]; ok {
		curr.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.services[port] = serviceHandler{
		fn: func(tunnelConn io.ReadWriteCloser) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fmt.Sprintf("localhost:%d", port))
			if err != nil {
				s.log.Warnw("failed to dial local service", "port", port, "err", err)
				_ = tunnelConn.Close()
				return
			}
			bridge(tunnelConn, conn)
		},
		cancel: cancel,
	}
	s.log.Infow("registered service", "port", port)
}

func (s *Service) connectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	if remotePort == 0 || remotePort > 0xffff {
		return 0, errors.New("remote port missing")
	}

	s.connectionMu.Lock()
	defer s.connectionMu.Unlock()

	for _, h := range s.connections {
		if h.peerID == peerID && h.remote == remotePort {
			return 0, fmt.Errorf("already connected to port %d on peer %s (local port %d)", remotePort, peerID.Short(), h.local)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	listenPort := localPort
	if listenPort == 0 {
		listenPort = remotePort
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", fmt.Sprintf(":%d", listenPort))
	if err != nil && localPort == 0 {
		ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	}
	if err != nil {
		cancel()
		return 0, err
	}

	boundPort := uint32(ln.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert

	s.wg.Go(func() {
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Go(func() {
				streamCtx, streamCancel := context.WithTimeout(ctx, streamOpenTimeout)
				defer streamCancel()
				stream, err := s.streams.OpenStream(streamCtx, peerID, transport.StreamTypeTunnel)
				if err != nil {
					s.log.Warnw("open stream failed", "peer", peerID.Short(), "port", remotePort, "err", err)
					_ = clientConn.Close()
					return
				}
				if err := writeServicePort(stream, remotePort); err != nil {
					s.log.Warnw("write stream header failed", "peer", peerID.Short(), "port", remotePort, "err", err)
					_ = stream.Close()
					_ = clientConn.Close()
					return
				}
				if s.trafficTracker != nil {
					stream = wrapStream(stream, s.trafficTracker, peerID)
				}
				bridge(clientConn, stream)
			})
		}
	})

	s.connections[connectionKey{peerID: peerID, port: boundPort}] = connectionHandler{
		cancel: cancel,
		peerID: peerID,
		remote: remotePort,
		local:  boundPort,
		ln:     ln,
	}
	s.log.Infow("connected to service", "peer", peerID.Short(), "port", remotePort, "local_port", boundPort)
	return boundPort, nil
}

func (s *Service) disconnectWhere(match func(connectionHandler) bool) {
	s.connectionMu.Lock()
	defer s.connectionMu.Unlock()
	for key, h := range s.connections {
		if match(h) {
			_ = h.ln.Close()
			h.cancel()
			delete(s.connections, key)
		}
	}
}

func (s *Service) unregisterService(port uint32) {
	s.serviceMu.Lock()
	defer s.serviceMu.Unlock()
	if h, ok := s.services[port]; ok {
		h.cancel()
		delete(s.services, port)
	}
	s.closeServiceStreams(port)
}

func (s *Service) closeServiceStreams(port uint32) {
	s.streamMu.Lock()
	streams := s.activeStreams[port]
	delete(s.activeStreams, port)
	s.streamMu.Unlock()

	for stream := range streams {
		_ = stream.Close()
	}
}

func (s *Service) addActiveStream(port uint32, stream io.ReadWriteCloser) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	streams, ok := s.activeStreams[port]
	if !ok {
		streams = make(map[io.ReadWriteCloser]struct{})
		s.activeStreams[port] = streams
	}
	streams[stream] = struct{}{}
}

func (s *Service) removeActiveStream(port uint32, stream io.ReadWriteCloser) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	streams, ok := s.activeStreams[port]
	if !ok {
		return
	}
	delete(streams, stream)
	if len(streams) == 0 {
		delete(s.activeStreams, port)
	}
}

func (s *Service) sampleTraffic() {
	if s.trafficTracker == nil {
		return
	}
	snapshot, changed := s.trafficTracker.RotateAndSnapshot()
	if !changed {
		return
	}
	for pk, pt := range snapshot {
		s.store.SetLocalTraffic(pk, pt.BytesIn, pt.BytesOut)
	}
}

func (s *Service) reconcileConnections(snap state.Snapshot) {
	for _, conn := range s.ListConnections() {
		if peerHasServicePort(snap, conn.PeerID, conn.RemotePort) {
			continue
		}
		s.log.Infow("removing stale forward", "peer", conn.PeerID.Short(), "port", conn.RemotePort)
		s.disconnectWhere(func(h connectionHandler) bool {
			return h.peerID == conn.PeerID && h.remote == conn.RemotePort
		})
		s.removeDesiredWhere(func(c ConnectionInfo) bool {
			return c.PeerID == conn.PeerID && c.RemotePort == conn.RemotePort
		})
	}
}

func (s *Service) reconcileDesiredConnections(snap state.Snapshot) {
	s.mu.Lock()
	desired := make(map[desiredKey]ConnectionInfo, len(s.desiredConnections))
	maps.Copy(desired, s.desiredConnections)
	s.mu.Unlock()

	if len(desired) == 0 {
		return
	}

	existing := make(map[desiredKey]struct{})
	for _, conn := range s.ListConnections() {
		existing[desiredKey{peerID: conn.PeerID, remotePort: conn.RemotePort, localPort: conn.LocalPort}] = struct{}{}
	}

	for _, dc := range desired {
		if _, ok := existing[desiredKey{peerID: dc.PeerID, remotePort: dc.RemotePort, localPort: dc.LocalPort}]; ok {
			continue
		}
		if !peerHasServicePort(snap, dc.PeerID, dc.RemotePort) {
			continue
		}
		p, ok := snap.Peer(dc.PeerID)
		if !ok || len(p.IdentityPub) == 0 {
			continue
		}
		port, err := s.connectService(dc.PeerID, dc.RemotePort, dc.LocalPort)
		if err != nil {
			s.log.Debugw("failed restoring desired connection", "peer", dc.PeerID.Short(), "remotePort", dc.RemotePort, "localPort", dc.LocalPort, "err", err)
			continue
		}
		s.addDesiredConnection(dc.PeerID, dc.RemotePort, port)
	}
}

func (s *Service) addDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	key := desiredKey{peerID: pk, remotePort: remotePort, localPort: localPort}
	s.mu.Lock()
	s.desiredConnections[key] = ConnectionInfo{PeerID: pk, RemotePort: remotePort, LocalPort: localPort}
	s.mu.Unlock()
}

func (s *Service) removeDesiredWhere(match func(ConnectionInfo) bool) {
	s.mu.Lock()
	for key, conn := range s.desiredConnections {
		if match(conn) {
			delete(s.desiredConnections, key)
		}
	}
	s.mu.Unlock()
}

func (s *Service) RemoveDesiredConnectionByPorts(pk types.PeerKey, remotePort, localPort uint32) {
	key := desiredKey{peerID: pk, remotePort: remotePort, localPort: localPort}
	s.mu.Lock()
	delete(s.desiredConnections, key)
	s.mu.Unlock()
}

func bridge(c1, c2 io.ReadWriteCloser) {
	var wg sync.WaitGroup
	var once sync.Once
	teardown := func() {
		_ = c1.Close()
		_ = c2.Close()
	}

	transfer := func(dst, src io.ReadWriteCloser, direction string) {
		bufPtr := bufPool.Get().(*[]byte) //nolint:forcetypeassert
		defer bufPool.Put(bufPtr)

		_, err := io.CopyBuffer(dst, src, *bufPtr)

		closeWrite(dst)
		once.Do(teardown)

		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			zap.S().Named("tun").Debugw("bridge copy failed", "direction", direction, "err", err)
		}
	}

	wg.Go(func() { transfer(c1, c2, "c1->c2") })
	wg.Go(func() { transfer(c2, c1, "c2->c1") })

	wg.Wait()
}

func closeWrite(conn io.Closer) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = conn.Close()
}

func findServicePort(snap state.Snapshot, peerID types.PeerKey, name string) (uint32, bool) {
	for _, svc := range snap.Services() {
		if svc.Peer == peerID && svc.Name == name {
			return svc.Port, true
		}
	}
	return 0, false
}

func peerHasServicePort(snap state.Snapshot, peerID types.PeerKey, port uint32) bool {
	for _, svc := range snap.Services() {
		if svc.Peer == peerID && svc.Port == port {
			return true
		}
	}
	return false
}

func matchesServiceName(snap state.Snapshot, peerID types.PeerKey, port uint32, name string) bool {
	for _, svc := range snap.Services() {
		if svc.Peer == peerID && svc.Port == port && svc.Name == name {
			return true
		}
	}
	return false
}
