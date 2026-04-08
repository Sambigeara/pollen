package tunneling

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	streamOpenTimeout   = 5 * time.Second
	bufSize             = 64 * 1024
	udpReadBuf          = 1500
	tunnelFrameOverhead = 3 // 1-byte DatagramType + 2-byte port
	maxTunnelPayload    = transport.MaxDatagramPayload - tunnelFrameOverhead
)

var (
	errNoServicePort = errors.New("no service port in stream header")
	bufPool          = sync.Pool{New: func() any { b := make([]byte, bufSize); return &b }}
)

type TunnelingAPI interface {
	Start(ctx context.Context) error
	Stop() error
	Connect(ctx context.Context, peer types.PeerKey, remotePort, localPort uint32, protocol statev1.ServiceProtocol) (uint32, error)
	Disconnect(service string) error
	ExposeService(port uint32, name string, protocol statev1.ServiceProtocol) error
	UnexposeService(name string) error
	ListConnections() []ConnectionInfo
	HandleTunnelStream(stream transport.Stream, peer types.PeerKey)
	HandleRoutedStream(stream transport.Stream, peer types.PeerKey)
	HandleTunnelDatagram(data []byte, peer types.PeerKey)
	HandlePeerDenied(peerID types.PeerKey)
	DesiredPeers() []types.PeerKey
	ListDesiredConnections() []ConnectionInfo
	RemoveDesiredConnection(pk types.PeerKey, remotePort uint32)
	TrafficRecorder() transport.TrafficRecorder
}

type ServiceState interface {
	Snapshot() state.Snapshot
	SetService(port uint32, name string, protocol statev1.ServiceProtocol) []state.Event
	RemoveService(name string) []state.Event
	SetLocalTraffic(peer types.PeerKey, in, out uint64) []state.Event
}

type StreamTransport interface {
	OpenStream(ctx context.Context, peerID types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

type DatagramTransport interface {
	SendTunnelDatagram(ctx context.Context, peer types.PeerKey, data []byte) error
}

type RoutingTable interface {
	NextHop(dest types.PeerKey) (next types.PeerKey, ok bool)
}

type ConnectionInfo struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
	Protocol   statev1.ServiceProtocol
}

type connKey struct {
	peer     types.PeerKey
	port     uint32
	protocol statev1.ServiceProtocol
}

type tunnelConn struct {
	ln         net.Listener
	pc         net.PacketConn
	cancel     context.CancelFunc
	lastClient atomic.Pointer[net.UDPAddr]
	info       ConnectionInfo
}

type serviceKey struct {
	port     uint32
	protocol statev1.ServiceProtocol
}

type serviceEntry struct {
	cancel context.CancelFunc
	proxy  *udpServiceProxy // nil for TCP
	name   string
}

type Service struct {
	router            RoutingTable
	store             ServiceState
	streams           StreamTransport
	datagrams         DatagramTransport
	connections       map[connKey]*tunnelConn
	log               *zap.SugaredLogger
	services          map[serviceKey]*serviceEntry
	desired           map[connKey]ConnectionInfo
	tracker           *tracker
	wg                sync.WaitGroup
	sampleInterval    time.Duration
	reconcileInterval time.Duration
	mu                sync.RWMutex
	self              types.PeerKey
}

var _ TunnelingAPI = (*Service)(nil)

func New(self types.PeerKey, store ServiceState, streams StreamTransport, datagrams DatagramTransport, router RoutingTable, opts ...Option) *Service {
	s := &Service{
		self:        self,
		store:       store,
		streams:     streams,
		datagrams:   datagrams,
		router:      router,
		log:         zap.S().Named("tun"),
		services:    make(map[serviceKey]*serviceEntry),
		connections: make(map[connKey]*tunnelConn),
		desired:     make(map[connKey]ConnectionInfo),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

type Option func(*Service)

func WithLogger(log *zap.SugaredLogger) Option  { return func(s *Service) { s.log = log } }
func WithTrafficTracking() Option               { return func(s *Service) { s.tracker = newTracker() } }
func WithSampleInterval(d time.Duration) Option { return func(s *Service) { s.sampleInterval = d } }
func WithReconcileInterval(d time.Duration) Option {
	return func(s *Service) { s.reconcileInterval = d }
}

func (s *Service) Start(ctx context.Context) error {
	snap := s.store.Snapshot()
	for _, svc := range snap.Services() {
		if svc.Peer == s.self {
			_ = s.ExposeService(svc.Port, svc.Name, svc.Protocol)
		}
	}

	if s.sampleInterval > 0 {
		s.wg.Go(func() { s.ticker(ctx, s.sampleInterval, s.sampleTraffic) })
	}
	if s.reconcileInterval > 0 {
		s.wg.Go(func() { s.ticker(ctx, s.reconcileInterval, s.reconcile) })
	}
	return nil
}

func (s *Service) Stop() error {
	s.mu.Lock()
	for _, entry := range s.services {
		if entry.proxy != nil {
			entry.proxy.Close()
		}
		entry.cancel()
	}
	for _, conn := range s.connections {
		s.closeConn(conn)
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Service) closeConn(conn *tunnelConn) {
	if conn.ln != nil {
		_ = conn.ln.Close()
	}
	if conn.pc != nil {
		_ = conn.pc.Close()
	}
	conn.cancel()
}

func (s *Service) Connect(_ context.Context, peer types.PeerKey, remotePort, localPort uint32, protocol statev1.ServiceProtocol) (uint32, error) {
	if localPort == 0 {
		localPort = remotePort
	}

	key := connKey{peer: peer, port: remotePort, protocol: protocol}
	s.mu.Lock()
	s.desired[key] = ConnectionInfo{PeerID: peer, RemotePort: remotePort, LocalPort: localPort, Protocol: protocol}
	s.mu.Unlock()

	s.reconcile()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if conn, ok := s.connections[key]; ok {
		return conn.info.LocalPort, nil
	}
	return 0, fmt.Errorf("failed to bind local port %d: port may be in use", localPort)
}

func (s *Service) Disconnect(service string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.store.Snapshot()
	for key, info := range s.desired {
		if matchesService(snap, info.PeerID, info.RemotePort, info.Protocol, service) {
			if conn, ok := s.connections[key]; ok {
				s.closeConn(conn)
				delete(s.connections, key)
			}
			delete(s.desired, key)
			return nil
		}
	}
	return fmt.Errorf("no connection for service %q", service)
}

func (s *Service) ExposeService(port uint32, name string, protocol statev1.ServiceProtocol) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sk := serviceKey{port: port, protocol: protocol}
	if existing, ok := s.services[sk]; ok {
		if existing.proxy != nil {
			existing.proxy.Close()
		}
		existing.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	entry := &serviceEntry{cancel: cancel, name: name}
	if protocol == statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP && s.datagrams != nil {
		entry.proxy = newUDPServiceProxy(ctx, port, s.datagrams)
	}
	s.services[sk] = entry
	s.store.SetService(port, name, protocol)
	return nil
}

func (s *Service) UnexposeService(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.store.Snapshot()
	for _, svc := range snap.Services() {
		if svc.Peer == s.self && svc.Name == name {
			sk := serviceKey{port: svc.Port, protocol: svc.Protocol}
			if entry, ok := s.services[sk]; ok {
				if entry.proxy != nil {
					entry.proxy.Close()
				}
				entry.cancel()
				delete(s.services, sk)
			}
			s.store.RemoveService(name)
			return nil
		}
	}
	return fmt.Errorf("service %q not found", name)
}

func (s *Service) HandleTunnelStream(stream transport.Stream, peer types.PeerKey) {
	s.handleIncomingStream(stream, peer)
}

func (s *Service) HandleRoutedStream(stream transport.Stream, peer types.PeerKey) {
	s.handleIncomingStream(stream, peer)
}

func (s *Service) handleIncomingStream(stream io.ReadWriteCloser, peer types.PeerKey) {
	s.wg.Go(func() {
		defer stream.Close()
		port, err := readPort(stream)
		if err != nil {
			return
		}

		s.mu.RLock()
		_, ok := s.services[serviceKey{port: port, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}]
		s.mu.RUnlock()

		if !ok {
			return
		}

		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port)) //nolint:noctx
		if err != nil {
			return
		}
		s.bridge(stream, conn, peer)
	})
}

func (s *Service) HandleTunnelDatagram(data []byte, peer types.PeerKey) {
	if len(data) < 3 { //nolint:mnd
		return
	}
	servicePort := uint32(binary.BigEndian.Uint16(data[:2]))
	payload := data[2:]

	// Client side: check if this is a response to an outgoing tunnel.
	s.mu.RLock()
	conn, connOK := s.connections[connKey{peer: peer, port: servicePort, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP}]
	s.mu.RUnlock()

	if connOK && conn.pc != nil {
		if clientAddr := conn.lastClient.Load(); clientAddr != nil {
			if s.tracker != nil {
				s.tracker.Record(peer, uint64(len(payload)), 0)
			}
			_, _ = conn.pc.WriteTo(payload, clientAddr)
			return
		}
	}

	// Service side: forward to local service.
	s.mu.RLock()
	entry, svcOK := s.services[serviceKey{port: servicePort, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP}]
	s.mu.RUnlock()

	if svcOK && entry.proxy != nil {
		if s.tracker != nil {
			s.tracker.Record(peer, uint64(len(payload)), 0)
		}
		entry.proxy.Forward(peer, payload)
	}
}

func (s *Service) HandlePeerDenied(peerID types.PeerKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, conn := range s.connections {
		if conn.info.PeerID == peerID {
			s.closeConn(conn)
			delete(s.connections, key)
		}
	}
	for key, info := range s.desired {
		if info.PeerID == peerID {
			delete(s.desired, key)
		}
	}
}

func (s *Service) ListConnections() []ConnectionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConnectionInfo, 0, len(s.connections))
	for _, c := range s.connections {
		out = append(out, c.info)
	}
	return out
}

func (s *Service) ListDesiredConnections() []ConnectionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConnectionInfo, 0, len(s.desired))
	for _, d := range s.desired {
		out = append(out, d)
	}
	return out
}

func (s *Service) RemoveDesiredConnection(pk types.PeerKey, remotePort uint32) {
	s.mu.Lock()
	// Remove both TCP and UDP desired entries for this peer+port.
	delete(s.desired, connKey{peer: pk, port: remotePort, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP})
	delete(s.desired, connKey{peer: pk, port: remotePort, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP})
	s.mu.Unlock()
	s.reconcile()
}

func (s *Service) DesiredPeers() []types.PeerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[types.PeerKey]struct{})
	for _, d := range s.desired {
		seen[d.PeerID] = struct{}{}
	}
	out := make([]types.PeerKey, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

func (s *Service) TrafficRecorder() transport.TrafficRecorder {
	if s.tracker == nil {
		return nil
	}
	return s.tracker
}

func (s *Service) reconcile() {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.store.Snapshot()

	// Teardown stale connections.
	for key, conn := range s.connections {
		if _, ok := s.desired[key]; !ok || !hasPort(snap, conn.info.PeerID, conn.info.RemotePort, conn.info.Protocol) {
			s.closeConn(conn)
			delete(s.connections, key)
		}
	}

	// Setup missing connections.
	for key, info := range s.desired {
		if _, ok := s.connections[key]; ok || !hasPort(snap, info.PeerID, info.RemotePort, info.Protocol) {
			continue
		}

		switch info.Protocol {
		case statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP:
			s.reconcileUDP(key, info)
		default:
			s.reconcileTCP(key, info)
		}
	}
}

func (s *Service) reconcileTCP(key connKey, info ConnectionInfo) {
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", info.LocalPort)) //nolint:noctx
	if err != nil {
		cancel()
		return
	}

	boundPort := uint32(ln.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert
	c := &tunnelConn{
		info:   ConnectionInfo{PeerID: info.PeerID, RemotePort: info.RemotePort, LocalPort: boundPort, Protocol: info.Protocol},
		ln:     ln,
		cancel: cancel,
	}
	s.connections[key] = c

	s.wg.Go(func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Go(func() {
				defer client.Close()
				sCtx, sCancel := context.WithTimeout(ctx, streamOpenTimeout)
				defer sCancel()

				stream, err := s.streams.OpenStream(sCtx, info.PeerID, transport.StreamTypeTunnel)
				if err != nil {
					return
				}
				defer stream.Close()

				if err := writePort(stream, info.RemotePort); err != nil {
					return
				}
				s.bridge(stream, client, info.PeerID)
			})
		}
	})
}

func (s *Service) reconcileUDP(key connKey, info ConnectionInfo) {
	ctx, cancel := context.WithCancel(context.Background())
	pc, err := (&net.ListenConfig{}).ListenPacket(ctx, "udp", fmt.Sprintf(":%d", info.LocalPort))
	if err != nil {
		cancel()
		return
	}

	boundPort := uint32(pc.LocalAddr().(*net.UDPAddr).Port) //nolint:forcetypeassert
	c := &tunnelConn{
		info:   ConnectionInfo{PeerID: info.PeerID, RemotePort: info.RemotePort, LocalPort: boundPort, Protocol: info.Protocol},
		pc:     pc,
		cancel: cancel,
	}
	s.connections[key] = c

	// Read loop: local app → QUIC datagram.
	s.wg.Go(func() {
		buf := make([]byte, udpReadBuf)
		for {
			n, clientAddr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n > maxTunnelPayload {
				s.log.Debugw("dropping oversized UDP packet", "size", n, "max", maxTunnelPayload, "peer", info.PeerID.Short())
				continue
			}
			udpAddr, ok := clientAddr.(*net.UDPAddr)
			if !ok {
				continue
			}
			c.lastClient.Store(udpAddr)

			frame := make([]byte, 2+n) //nolint:mnd
			binary.BigEndian.PutUint16(frame[:2], uint16(info.RemotePort))
			copy(frame[2:], buf[:n])

			if s.tracker != nil {
				s.tracker.Record(info.PeerID, 0, uint64(n))
			}
			_ = s.datagrams.SendTunnelDatagram(ctx, info.PeerID, frame)
		}
	})
}

func (s *Service) bridge(c1, c2 io.ReadWriteCloser, peer types.PeerKey) {
	if s.tracker != nil {
		c1 = &countedStream{inner: c1, tr: s.tracker, peer: peer}
	}
	done := make(chan struct{}, 2) //nolint:mnd
	copyDir := func(dst io.Writer, src io.Reader) {
		buf := bufPool.Get().(*[]byte) //nolint:forcetypeassert
		defer bufPool.Put(buf)
		_, _ = io.CopyBuffer(dst, src, *buf)
		done <- struct{}{}
	}
	go copyDir(c1, c2)
	go copyDir(c2, c1)
	<-done
	_ = c1.Close()
	_ = c2.Close()
	<-done
}

func (s *Service) ticker(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

func (s *Service) sampleTraffic() {
	if s.tracker == nil {
		return
	}
	if snap, ok := s.tracker.rotate(); ok {
		for pk, pt := range snap {
			s.store.SetLocalTraffic(pk, pt.BytesIn, pt.BytesOut)
		}
	}
}

func readPort(r io.Reader) (uint32, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	p := binary.BigEndian.Uint16(b[:])
	if p == 0 {
		return 0, errNoServicePort
	}
	return uint32(p), nil
}

func writePort(w io.Writer, p uint32) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(p))
	_, err := w.Write(b[:])
	return err
}

func hasPort(snap state.Snapshot, peer types.PeerKey, port uint32, protocol statev1.ServiceProtocol) bool {
	nv, ok := snap.Nodes[peer]
	if !ok {
		return false
	}
	for _, s := range nv.Services {
		if s.Port == port && s.Protocol == protocol {
			return true
		}
	}
	return false
}

func matchesService(snap state.Snapshot, peer types.PeerKey, port uint32, protocol statev1.ServiceProtocol, name string) bool {
	nv, ok := snap.Nodes[peer]
	if !ok {
		return false
	}
	s, ok := nv.Services[name]
	return ok && s.Port == port && s.Protocol == protocol
}

type peerTraffic struct{ BytesIn, BytesOut uint64 }

type tracker struct {
	current   map[types.PeerKey]*peerTraffic
	published map[types.PeerKey]peerTraffic
	mu        sync.Mutex
}

func newTracker() *tracker {
	return &tracker{current: make(map[types.PeerKey]*peerTraffic)}
}

func (t *tracker) Record(pk types.PeerKey, in, out uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.current[pk]
	if p == nil {
		p = &peerTraffic{}
		t.current[pk] = p
	}
	p.BytesIn += in
	p.BytesOut += out
}

func (t *tracker) rotate() (map[types.PeerKey]peerTraffic, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	snap := make(map[types.PeerKey]peerTraffic, len(t.current))
	changed := len(t.current) != len(t.published)
	for k, v := range t.current {
		snap[k] = *v
		if !changed {
			old := t.published[k]
			if absDiff(old.BytesIn, v.BytesIn) > (old.BytesIn/50) || absDiff(old.BytesOut, v.BytesOut) > (old.BytesOut/50) { //nolint:mnd
				changed = true
			}
		}
	}
	if changed {
		t.published = snap
	}
	return snap, changed
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

type countedStream struct {
	io.ReadWriteCloser
	inner io.ReadWriteCloser
	tr    *tracker
	peer  types.PeerKey
}

func (s *countedStream) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	if n > 0 {
		s.tr.Record(s.peer, uint64(n), 0)
	}
	return n, err
}

func (s *countedStream) Write(p []byte) (int, error) {
	n, err := s.inner.Write(p)
	if n > 0 {
		s.tr.Record(s.peer, 0, uint64(n))
	}
	return n, err
}

func (s *countedStream) Close() error { return s.inner.Close() }
