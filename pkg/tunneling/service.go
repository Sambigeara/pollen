// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
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
	ExposeService(port uint32, name string, protocol statev1.ServiceProtocol, policy *admissionv1.Predicate) error
	UnexposeService(name string) error
	ListConnections() []ConnectionInfo
	Serve(stream io.ReadWriteCloser, peer types.PeerKey, port uint32)
	HandleTunnelDatagram(data []byte, peer types.PeerKey)
	RequestService(ctx context.Context, peer types.PeerKey, port uint32, body []byte) ([]byte, error)
	HandlePeerDenied(peerID types.PeerKey)
	DesiredPeers() []types.PeerKey
	ListDesiredConnections() []ConnectionInfo
	RemoveDesiredConnection(pk types.PeerKey, remotePort uint32)
	TrafficRecorder() transport.TrafficRecorder
}

type ServiceState interface {
	Snapshot() state.Snapshot
	SetService(port uint32, name string, protocol statev1.ServiceProtocol, policy *admissionv1.Predicate) ([]state.Event, error)
	RemoveService(name string) ([]state.Event, error)
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

type activeServe struct {
	stream io.ReadWriteCloser
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
	activeServes      map[string]map[*activeServe]struct{}
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
		self:         self,
		store:        store,
		streams:      streams,
		datagrams:    datagrams,
		router:       router,
		log:          zap.S().Named("tun"),
		services:     make(map[serviceKey]*serviceEntry),
		connections:  make(map[connKey]*tunnelConn),
		desired:      make(map[connKey]ConnectionInfo),
		activeServes: make(map[string]map[*activeServe]struct{}),
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
	s.mu.Lock()
	for _, svc := range snap.Services() {
		if svc.Peer != s.self {
			continue
		}
		sk := serviceKey{port: svc.Port, protocol: svc.Protocol}
		s.services[sk] = s.buildEntryLocked(svc.Port, svc.Name, svc.Protocol)
	}
	s.mu.Unlock()

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
	entries := make([]*serviceEntry, 0, len(s.services))
	for _, entry := range s.services {
		entries = append(entries, entry)
	}
	conns := make([]*tunnelConn, 0, len(s.connections))
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	var streams []io.Closer
	for _, set := range s.activeServes {
		for sess := range set {
			streams = append(streams, sess.stream)
		}
	}
	clear(s.services)
	clear(s.connections)
	clear(s.activeServes)
	s.mu.Unlock()

	for _, e := range entries {
		closeEntry(e)
	}
	for _, c := range conns {
		s.closeConn(c)
	}
	closeAll(streams)
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

func (s *Service) ExposeService(port uint32, name string, protocol statev1.ServiceProtocol, policy *admissionv1.Predicate) error {
	s.mu.Lock()
	sk := serviceKey{port: port, protocol: protocol}

	if existing, ok := s.services[sk]; ok && existing.name != name {
		s.mu.Unlock()
		return fmt.Errorf("port %d/%s already exposed as %q", port, protocol, existing.name)
	}
	var stale *serviceEntry
	for k, e := range s.services {
		if e.name == name && k != sk {
			stale = e
			delete(s.services, k)
			break
		}
	}

	events, err := s.store.SetService(port, name, protocol, policy)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	if len(events) == 0 {
		if _, ok := s.services[sk]; !ok {
			s.services[sk] = s.buildEntryLocked(port, name, protocol)
		}
		s.mu.Unlock()
		return nil
	}

	displaced := s.services[sk]
	s.services[sk] = s.buildEntryLocked(port, name, protocol)
	toClose := s.detachServesLocked(name)
	s.mu.Unlock()

	closeEntry(displaced)
	closeEntry(stale)
	closeAll(toClose)
	return nil
}

func (s *Service) buildEntryLocked(port uint32, name string, protocol statev1.ServiceProtocol) *serviceEntry {
	ctx, cancel := context.WithCancel(context.Background())
	entry := &serviceEntry{cancel: cancel, name: name}
	if protocol == statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP && s.datagrams != nil {
		entry.proxy = newUDPServiceProxy(ctx, port, s.datagrams)
	}
	return entry
}

func closeEntry(e *serviceEntry) {
	if e == nil {
		return
	}
	if e.proxy != nil {
		e.proxy.Close()
	}
	e.cancel()
}

// UnexposeService tears down both the gossip spec and the runtime
// state (serviceEntry + bridged streams) for a locally-exposed service.
// Idempotent: safe to call when the spec is already tombstoned (e.g.
// from the cap-downgrade cascade in pkg/membership), in which case
// RemoveService is a no-op and only the runtime teardown runs.
func (s *Service) UnexposeService(name string) error {
	s.mu.Lock()
	if _, err := s.store.RemoveService(name); err != nil {
		s.mu.Unlock()
		return err
	}
	var entries []*serviceEntry
	for sk, entry := range s.services {
		if entry.name == name {
			entries = append(entries, entry)
			delete(s.services, sk)
		}
	}
	toClose := s.detachServesLocked(name)
	s.mu.Unlock()

	for _, e := range entries {
		closeEntry(e)
	}
	closeAll(toClose)
	return nil
}

func ReadPort(r io.Reader) (uint32, error) {
	return readPort(r)
}

func (s *Service) Serve(stream io.ReadWriteCloser, peer types.PeerKey, port uint32) {
	s.wg.Go(func() {
		defer stream.Close()

		s.mu.Lock()
		entry, ok := s.services[serviceKey{port: port, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}]
		if !ok {
			s.mu.Unlock()
			return
		}
		sess := &activeServe{stream: stream}
		s.registerServeLocked(entry.name, sess)
		name := entry.name
		s.mu.Unlock()

		defer s.unregisterServe(name, sess)

		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port)) //nolint:noctx
		if err != nil {
			return
		}

		s.bridge(stream, conn, peer)
	})
}

func (s *Service) detachServesLocked(name string) []io.Closer {
	set := s.activeServes[name]
	delete(s.activeServes, name)
	if len(set) == 0 {
		return nil
	}
	streams := make([]io.Closer, 0, len(set))
	for sess := range set {
		streams = append(streams, sess.stream)
	}
	return streams
}

func closeAll(cs []io.Closer) {
	for _, c := range cs {
		_ = c.Close()
	}
}

func (s *Service) registerServeLocked(name string, sess *activeServe) {
	set, ok := s.activeServes[name]
	if !ok {
		set = make(map[*activeServe]struct{})
		s.activeServes[name] = set
	}
	set[sess] = struct{}{}
}

func (s *Service) unregisterServe(name string, sess *activeServe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set, ok := s.activeServes[name]
	if !ok {
		return
	}
	delete(set, sess)
	if len(set) == 0 {
		delete(s.activeServes, name)
	}
}

func (s *Service) HandleTunnelDatagram(data []byte, peer types.PeerKey) {
	if len(data) < 3 { //nolint:mnd
		return
	}
	servicePort := uint32(binary.BigEndian.Uint16(data[:2]))
	payload := data[2:]

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

	for key, conn := range s.connections {
		if _, ok := s.desired[key]; !ok || !hasPort(snap, conn.info.PeerID, conn.info.RemotePort, conn.info.Protocol) {
			s.closeConn(conn)
			delete(s.connections, key)
		}
	}

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
		c1 = transport.WrapTrafficStream(c1, s.tracker, peer)
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
	current      map[types.PeerKey]*peerTraffic
	lastSnap     map[types.PeerKey]peerTraffic
	lastRotation time.Time
	published    map[types.PeerKey]peerTraffic
	mu           sync.Mutex
}

func newTracker() *tracker {
	return &tracker{
		current:      make(map[types.PeerKey]*peerTraffic),
		lastRotation: time.Now(),
	}
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

	now := time.Now()
	elapsed := now.Sub(t.lastRotation).Seconds()
	if elapsed <= 0 {
		return nil, false
	}

	rates := make(map[types.PeerKey]peerTraffic, len(t.current))
	changed := len(t.current) != len(t.published)
	for k, v := range t.current {
		prev := t.lastSnap[k]
		r := peerTraffic{
			BytesIn:  uint64(float64(v.BytesIn-prev.BytesIn) / elapsed),
			BytesOut: uint64(float64(v.BytesOut-prev.BytesOut) / elapsed),
		}
		rates[k] = r
		if !changed {
			old := t.published[k]
			zeroTransition := (old.BytesIn != 0) != (r.BytesIn != 0) || (old.BytesOut != 0) != (r.BytesOut != 0)
			if zeroTransition || absDiff(old.BytesIn, r.BytesIn) > 1024 || absDiff(old.BytesOut, r.BytesOut) > 1024 { //nolint:mnd
				changed = true
			}
		}
	}

	snap := make(map[types.PeerKey]peerTraffic, len(t.current))
	for k, v := range t.current {
		snap[k] = *v
	}
	t.lastSnap = snap
	t.lastRotation = now

	if changed {
		t.published = rates
	}
	return rates, changed
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
