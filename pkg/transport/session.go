package transport

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
	"go.opentelemetry.io/otel/attribute"
)

type closeReason string

const (
	closeReasonDenied        closeReason = CloseReasonDenied
	closeReasonTopologyPrune closeReason = CloseReasonTopologyPrune
	closeReasonCertExpired   closeReason = CloseReasonCertExpired
	closeReasonCertRotation  closeReason = "cert_rotation"
	closeReasonDisconnect    closeReason = "disconnect"
	closeReasonDuplicate     closeReason = "duplicate"
	closeReasonReplaced      closeReason = "replaced"
	closeReasonDisconnected  closeReason = "disconnected"
	closeReasonShutdown      closeReason = "shutdown"
)

type peerSession struct {
	conn           *quic.Conn
	transport      *quic.Transport
	sockConn       *conn
	delegationCert *admissionv1.DelegationCert
	createdAt      time.Time
	certExpiresAt  time.Time
	outbound       bool
}

func newPeerSession(qconn *quic.Conn, qt *quic.Transport, outbound bool) *peerSession {
	dc := delegationCertFromConn(qconn)
	return &peerSession{
		conn:           qconn,
		transport:      qt,
		delegationCert: dc,
		createdAt:      time.Now(),
		certExpiresAt:  auth.CertExpiresAt(dc),
		outbound:       outbound,
	}
}

func closeReasonToDisconnectReason(r closeReason) disconnectReason {
	switch r {
	case closeReasonDenied:
		return disconnectDenied
	case closeReasonTopologyPrune:
		return disconnectTopologyPrune
	case closeReasonCertExpired:
		return disconnectCertExpired
	case closeReasonCertRotation:
		return disconnectCertRotation
	default:
		return disconnectGraceful
	}
}

func (m *impl) addPeer(ctx context.Context, s *peerSession, peerKey types.PeerKey) {
	ctx, span := m.tracer.Start(ctx, "mesh.addPeer")
	span.SetAttributes(attribute.String("peer", peerKey.Short()))

	replace, ok := m.sessions.add(peerKey, s, func(current *peerSession) bool {
		if current.conn.Context().Err() != nil {
			return true
		}

		// Both connections are live — deterministic tie-break:
		// both nodes agree to keep the connection dialed by the smaller key.
		// This ensures convergence regardless of arrival order.
		weAreSmaller := m.localKey.Less(peerKey)
		currentPreferred := current.outbound == weAreSmaller
		return !currentPreferred
	})
	if !ok {
		span.SetAttributes(attribute.String("result", "duplicate"))
		span.End()
		m.closeSession(s, closeReasonDuplicate)
		return
	}

	if replace != nil {
		m.closeSession(replace, closeReasonReplaced)
	}

	span.End()
	m.metrics.SessionConnects.Add(ctx, 1)

	m.acceptWG.Go(func() { m.recvDatagrams(s, peerKey) })
	m.acceptWG.Go(func() { m.acceptBidiStreams(s, peerKey) })

	addr := s.conn.RemoteAddr().(*net.UDPAddr) //nolint:forcetypeassert
	m.peers.step(time.Now(), connectPeer{PeerKey: peerKey, IP: addr.IP, ObservedPort: addr.Port})
	m.emitPeerEvent(PeerEvent{Key: peerKey, Type: PeerEventConnected, Addrs: []netip.AddrPort{addr.AddrPort()}})
}

func (m *impl) closeSession(s *peerSession, reason closeReason) {
	if s.conn != nil {
		_ = s.conn.CloseWithError(0, string(reason))
	}
	if s.transport != nil && s.transport != m.mainQT {
		_ = s.transport.Close()
	}
	if s.sockConn != nil {
		_ = s.sockConn.Close()
	}
}

func (m *impl) removePeer(ctx context.Context, pk types.PeerKey, s *peerSession, cr closeReason, dr disconnectReason) bool {
	if !m.sessions.removeIfCurrent(pk, s) {
		return false
	}
	m.metrics.SessionDisconnects.Add(ctx, 1)
	m.closeSession(s, cr)
	m.peers.step(time.Now(), peerDisconnected{PeerKey: pk, Reason: dr})
	m.emitPeerEvent(PeerEvent{Key: pk, Type: PeerEventDisconnected})
	return true
}

func (m *impl) ClosePeerSession(peerKey types.PeerKey, reason string) {
	cr := closeReason(reason)
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return
	}
	m.removePeer(context.Background(), peerKey, s, cr, closeReasonToDisconnectReason(cr))
}

func (m *impl) handleSendFailure(peerKey types.PeerKey, s *peerSession, err error) {
	reason := classifyQUICError(err)
	if reason == disconnectUnknown {
		return
	}
	m.removePeer(context.Background(), peerKey, s, closeReasonDisconnect, reason)
}

func (m *impl) sessionReaper(ctx context.Context) {
	ticker := time.NewTicker(sessionReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for _, peerKey := range m.ConnectedPeers() {
				s, ok := m.sessions.get(peerKey)
				if !ok {
					continue
				}
				if now.Sub(s.createdAt) < m.maxConnectionAge {
					continue
				}
				m.log.Debugw("reconnecting peer to refresh certificates", "peer", peerKey.Short(), "age", now.Sub(s.createdAt))
				m.removePeer(ctx, peerKey, s, closeReasonCertRotation, disconnectCertRotation)
			}
		}
	}
}

func (m *impl) ConnectedPeers() []types.PeerKey {
	return m.sessions.connectedPeers()
}

func (m *impl) GetConn(peerKey types.PeerKey) (*quic.Conn, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, false
	}
	return s.conn, true
}

func (m *impl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, false
	}
	return s.conn.RemoteAddr().(*net.UDPAddr), true //nolint:forcetypeassert
}

func (m *impl) PeerDelegationCert(peerKey types.PeerKey) (*admissionv1.DelegationCert, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, false
	}
	return s.delegationCert, s.delegationCert != nil
}

func (m *impl) IsOutbound(peerKey types.PeerKey) bool {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return false
	}
	return s.outbound
}

func (m *impl) Disconnect(peer types.PeerKey) error {
	m.ClosePeerSession(peer, string(closeReasonDisconnect))
	return nil
}

func (m *impl) UpdateMeshCert(cert tls.Certificate) {
	m.meshCert.Store(&cert)
}
