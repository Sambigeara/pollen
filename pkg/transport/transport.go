package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type impl struct {
	bareCert         tls.Certificate
	router           Router
	socks            sockStore
	injectedConn     net.PacketConn
	trafficTracker   TrafficRecorder
	log              *zap.SugaredLogger
	meshCert         atomic.Pointer[tls.Certificate]
	recvCh           chan Packet
	renewalCh        chan *meshv1.CertRenewalResponse
	inviteSigner     *auth.DelegationSigner
	acceptCh         chan acceptedStream
	trustBundle      *admissionv1.TrustBundle
	mainQT           *quic.Transport
	metrics          *metrics.MeshMetrics
	tracer           trace.Tracer
	isDenied         func(types.PeerKey) bool
	peers            *peerStore
	peerEventCh      chan PeerEvent
	supervisorCh     chan PeerEvent
	listener         *quic.Listener
	sessions         *sessionRegistry
	acceptWG         sync.WaitGroup
	port             int
	membershipTTL    time.Duration
	reconnectWindow  time.Duration
	maxConnectionAge time.Duration
	peerTickInterval time.Duration
	localKey         types.PeerKey
	disableNATPunch  bool
}

type acceptedStream struct {
	stream  Stream
	stype   StreamType
	peerKey types.PeerKey
}

func (m *impl) Start(ctx context.Context) error {
	var pconn net.PacketConn
	if m.injectedConn != nil {
		pconn = m.injectedConn
	} else {
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
		if err != nil {
			return fmt.Errorf("listen udp:%d: %w", m.port, err)
		}
		pconn = udpConn
	}

	qt := &quic.Transport{Conn: pconn}
	ln, err := qt.Listen(newServerTLSConfig(serverTLSParams{
		meshCertPtr:     &m.meshCert,
		inviteCert:      m.bareCert,
		trustBundle:     m.trustBundle,
		reconnectWindow: m.reconnectWindow,
		inviteEnabled:   m.inviteSigner != nil,
	}), quicConfig())
	if err != nil {
		_ = pconn.Close()
		return fmt.Errorf("quic listen: %w", err)
	}

	m.mainQT = qt
	m.listener = ln

	if !m.disableNATPunch {
		m.socks = newSockStore()
		m.socks.SetMainProbeWriter(func(payload []byte, addr *net.UDPAddr) error {
			_, err := qt.WriteTo(payload, addr)
			return err
		})
		go m.runMainProbeLoop(ctx, qt)
	}
	go m.acceptLoop(ctx)
	go m.runPeerTickLoop(ctx)
	if m.maxConnectionAge > 0 {
		go m.sessionReaper(ctx)
	}
	return nil
}

func (m *impl) Stop() error {
	for _, s := range m.sessions.drainPeers() {
		m.closeSession(s, closeReasonShutdown)
	}

	m.acceptWG.Wait()
	close(m.acceptCh)
	close(m.recvCh)

	if m.listener != nil {
		_ = m.listener.Close()
	}
	if m.mainQT != nil {
		_ = m.mainQT.Close()
		if m.mainQT.Conn != nil {
			_ = m.mainQT.Conn.Close()
		}
	}
	return nil
}

func (m *impl) ListenPort() int {
	return m.mainQT.Conn.LocalAddr().(*net.UDPAddr).Port //nolint:forcetypeassert
}

func (m *impl) PeerEvents() <-chan PeerEvent {
	return m.peerEventCh
}

func (m *impl) emitPeerEvent(ev PeerEvent) {
	select {
	case m.peerEventCh <- ev:
	default:
	}
	select {
	case m.supervisorCh <- ev:
	default:
	}
}

func (m *impl) SupervisorEvents() <-chan PeerEvent {
	return m.supervisorCh
}

func (m *impl) SetRouter(r Router) {
	m.router = r
}

func (m *impl) SetTrafficTracker(t TrafficRecorder) {
	m.trafficTracker = t
}

func (m *impl) DiscoverPeer(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool) {
	m.peers.step(time.Now(), discoverPeer{
		PeerKey:            pk,
		Ips:                ips,
		Port:               port,
		LastAddr:           lastAddr,
		PrivatelyRoutable:  privatelyRoutable,
		PubliclyAccessible: publiclyAccessible,
	})
}

func (m *impl) ForgetPeer(pk types.PeerKey) {
	m.peers.step(time.Now(), forgetPeer{PeerKey: pk})
}

func (m *impl) RetryPeer(pk types.PeerKey) {
	m.peers.step(time.Now(), retryPeer{PeerKey: pk})
}

func (m *impl) ConnectFailed(pk types.PeerKey) {
	m.peers.step(time.Now(), connectFailed{PeerKey: pk})
}

func (m *impl) HasPeer(pk types.PeerKey) bool {
	_, ok := m.peers.get(pk)
	return ok
}

func (m *impl) IsPeerConnected(pk types.PeerKey) bool {
	return m.peers.inState(pk, peerStateConnected)
}

func (m *impl) IsPeerConnecting(pk types.PeerKey) bool {
	return m.peers.inState(pk, peerStateConnecting)
}

func (m *impl) PeerStateCounts() PeerStateCounts {
	return m.peers.stateCounts()
}

func (m *impl) SetPeerMetrics(pm *metrics.PeerMetrics) {
	m.peers.setPeerMetrics(pm)
}

func (m *impl) MarkPeerConnected(pk types.PeerKey, ip net.IP, port int) {
	m.peers.step(time.Now(), connectPeer{PeerKey: pk, IP: ip, ObservedPort: port})
}

func (m *impl) Send(ctx context.Context, peerKey types.PeerKey, data []byte) error {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return fmt.Errorf("no connection to peer %s", peerKey.Short())
	}
	if err := s.conn.SendDatagram(data); err != nil {
		m.handleSendFailure(peerKey, s, err)
		m.metrics.DatagramErrors.Add(ctx, 1)
		return err
	}
	m.metrics.DatagramsSent.Add(ctx, 1)
	m.metrics.DatagramBytesSent.Add(ctx, int64(len(data)))
	return nil
}

func (m *impl) Recv(ctx context.Context) (Packet, error) {
	select {
	case p := <-m.recvCh:
		return p, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (m *impl) OpenStream(ctx context.Context, peerKey types.PeerKey, st StreamType) (Stream, error) {
	if st == StreamTypeClock {
		return m.openTypedStream(ctx, peerKey, StreamTypeClock)
	}

	for {
		sessionCh := m.sessions.onChange()
		routeCh := routeChanged(m.router)

		if _, ok := m.sessions.get(peerKey); ok {
			return m.openTypedStream(ctx, peerKey, st)
		}

		if m.router == nil {
			return m.openTypedStream(ctx, peerKey, st)
		}

		if nextHop, ok := m.router.NextHop(peerKey); ok {
			if _, ok := m.sessions.get(nextHop); ok {
				return m.openRoutedStream(ctx, peerKey, st, nextHop)
			}
		}

		select {
		case <-sessionCh:
		case <-routeCh:
		case <-ctx.Done():
			return Stream{}, ctx.Err()
		}
	}
}

func routeChanged(r Router) <-chan struct{} {
	type notifier interface{ Changed() <-chan struct{} }
	if n, ok := r.(notifier); ok {
		return n.Changed()
	}
	return nil
}

func (m *impl) AcceptStream(ctx context.Context) (Stream, StreamType, types.PeerKey, error) {
	select {
	case as := <-m.acceptCh:
		return as.stream, as.stype, as.peerKey, nil
	case <-ctx.Done():
		return Stream{}, 0, types.PeerKey{}, ctx.Err()
	}
}

func (m *impl) openTypedStream(ctx context.Context, peerKey types.PeerKey, streamType StreamType) (Stream, error) {
	s, err := m.sessions.waitFor(ctx, peerKey)
	if err != nil {
		return Stream{}, err
	}
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return Stream{}, err
	}
	if _, err := stream.Write([]byte{byte(streamType)}); err != nil {
		cancelStream(stream)
		return Stream{}, err
	}
	return Stream{stream}, nil
}
