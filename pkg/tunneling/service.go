package tunneling

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	bufPool          = sync.Pool{New: func() any { b := make([]byte, bufSize); return &b }}
)

type TunnelingAPI interface {
	Start(ctx context.Context) error
	Stop() error
	Connect(ctx context.Context, peer types.PeerKey, remotePort, localPort uint32) (uint32, error)
	Disconnect(service string) error
	ExposeService(port uint32, name string) error
	UnexposeService(name string) error
	ListConnections() []ConnectionInfo
	HandleTunnelStream(stream transport.Stream, peer types.PeerKey)
	HandleRoutedStream(stream transport.Stream, peer types.PeerKey)
	HandlePeerDenied(peerID types.PeerKey)
	DesiredPeers() []types.PeerKey
	ListDesiredConnections() []ConnectionInfo
	RemoveDesiredConnection(pk types.PeerKey, remotePort uint32)
	TrafficRecorder() transport.TrafficRecorder
}

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

type ConnectionInfo struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

type connection struct {
	ln     net.Listener
	cancel context.CancelFunc
	info   ConnectionInfo
}

type Service struct {
	router            RoutingTable
	store             ServiceState
	streams           StreamTransport
	connections       map[string]*connection
	log               *zap.SugaredLogger
	services          map[uint32]context.CancelFunc
	desired           map[string]ConnectionInfo
	tracker           *tracker
	wg                sync.WaitGroup
	sampleInterval    time.Duration
	reconcileInterval time.Duration
	mu                sync.RWMutex
	self              types.PeerKey
}

var _ TunnelingAPI = (*Service)(nil)

func New(self types.PeerKey, store ServiceState, streams StreamTransport, router RoutingTable, opts ...Option) *Service {
	s := &Service{
		self:        self,
		store:       store,
		streams:     streams,
		router:      router,
		log:         zap.S().Named("tun"),
		services:    make(map[uint32]context.CancelFunc),
		connections: make(map[string]*connection),
		desired:     make(map[string]ConnectionInfo),
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
			_ = s.ExposeService(svc.Port, svc.Name)
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
	for _, cancel := range s.services {
		cancel()
	}
	for _, conn := range s.connections {
		_ = conn.ln.Close()
		conn.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Service) Connect(_ context.Context, peer types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	if localPort == 0 {
		localPort = remotePort
	}

	key := fmt.Sprintf("%s:%d", peer.String(), remotePort)
	s.mu.Lock()
	s.desired[key] = ConnectionInfo{PeerID: peer, RemotePort: remotePort, LocalPort: localPort}
	s.mu.Unlock()

	s.reconcile()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if conn, ok := s.connections[key]; ok {
		return conn.info.LocalPort, nil
	}
	return localPort, nil
}

func (s *Service) Disconnect(service string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.store.Snapshot()
	for key, info := range s.desired {
		if matchesService(snap, info.PeerID, info.RemotePort, service) {
			if conn, ok := s.connections[key]; ok {
				_ = conn.ln.Close()
				conn.cancel()
				delete(s.connections, key)
			}
			delete(s.desired, key)
			return nil
		}
	}
	return fmt.Errorf("no connection for service %q", service)
}

func (s *Service) ExposeService(port uint32, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cancel, ok := s.services[port]; ok {
		cancel()
	}

	_, cancel := context.WithCancel(context.Background())
	s.services[port] = cancel
	s.store.SetService(port, name)
	return nil
}

func (s *Service) UnexposeService(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.store.Snapshot()
	for _, svc := range snap.Services() {
		if svc.Peer == s.self && svc.Name == name {
			if cancel, ok := s.services[svc.Port]; ok {
				cancel()
				delete(s.services, svc.Port)
			}
			s.store.RemoveService(name)
			return nil
		}
	}
	return fmt.Errorf("service %q not found", name)
}

func (s *Service) HandleTunnelStream(stream transport.Stream, peer types.PeerKey) {
	s.handleIncoming(stream, peer)
}

func (s *Service) HandleRoutedStream(stream transport.Stream, peer types.PeerKey) {
	s.handleIncoming(stream, peer)
}

func (s *Service) handleIncoming(stream io.ReadWriteCloser, peer types.PeerKey) {
	s.wg.Go(func() {
		defer stream.Close()
		port, err := readPort(stream)
		if err != nil {
			return
		}

		s.mu.RLock()
		_, ok := s.services[port]
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

func (s *Service) HandlePeerDenied(peerID types.PeerKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, conn := range s.connections {
		if conn.info.PeerID == peerID {
			_ = conn.ln.Close()
			conn.cancel()
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
	key := fmt.Sprintf("%s:%d", pk.String(), remotePort)
	s.mu.Lock()
	delete(s.desired, key)
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

	for key, conn := range s.connections {
		if _, ok := s.desired[key]; !ok || !hasPort(snap, conn.info.PeerID, conn.info.RemotePort) {
			_ = conn.ln.Close()
			conn.cancel()
			delete(s.connections, key)
		}
	}

	for key, info := range s.desired {
		if _, ok := s.connections[key]; ok || !hasPort(snap, info.PeerID, info.RemotePort) {
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", info.LocalPort)) //nolint:noctx
		if err != nil {
			cancel()
			continue
		}

		boundPort := uint32(ln.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert
		c := &connection{
			info:   ConnectionInfo{PeerID: info.PeerID, RemotePort: info.RemotePort, LocalPort: boundPort},
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

func hasPort(snap state.Snapshot, peer types.PeerKey, port uint32) bool {
	nv, ok := snap.Nodes[peer]
	if !ok {
		return false
	}
	for _, s := range nv.Services {
		if s.Port == port {
			return true
		}
	}
	return false
}

func matchesService(snap state.Snapshot, peer types.PeerKey, port uint32, name string) bool {
	nv, ok := snap.Nodes[peer]
	if !ok {
		return false
	}
	s, ok := nv.Services[name]
	return ok && s.Port == port
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
