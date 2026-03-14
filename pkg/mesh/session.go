package mesh

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
)

func (m *impl) Send(_ context.Context, peerKey types.PeerKey, env *meshv1.Envelope) error {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return errors.New("no connection to peer " + peerKey.Short())
	}
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}
	if err := s.conn.SendDatagram(b); err != nil {
		m.handleSendFailure(peerKey, s, err)
		m.metrics.DatagramErrors.Inc()
		return err
	}
	m.metrics.DatagramsSent.Inc()
	m.metrics.DatagramBytesSent.Add(int64(len(b)))
	return nil
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

func (m *impl) ConnectedPeers() []types.PeerKey {
	return m.sessions.connectedPeers()
}

func (m *impl) ClosePeerSession(peerKey types.PeerKey, reason CloseReason) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return
	}
	if m.sessions.removeIfCurrent(peerKey, s) {
		m.metrics.SessionDisconnects.Inc()
		m.closeSession(s, reason)
	}
}

func (m *impl) addPeer(s *peerSession, peerKey types.PeerKey) {
	span := m.tracer.Start("mesh.addPeer")
	span.SetAttr("peer", peerKey.Short())

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
		span.SetAttr("result", "duplicate")
		span.End()
		m.closeSession(s, CloseReasonDuplicate)
		return
	}

	if replace != nil {
		m.closeSession(replace, CloseReasonReplaced)
	}

	span.End()
	m.metrics.SessionConnects.Inc()

	// Use the connection's own context so the recv goroutine lives as long as
	// the QUIC connection, not as long as the (potentially short-lived) dial ctx.
	m.acceptWG.Go(func() { m.recvDatagrams(s, peerKey) })
	m.acceptWG.Go(func() { m.acceptBidiStreams(s, peerKey) })

	addr := s.conn.RemoteAddr().(*net.UDPAddr) //nolint:forcetypeassert
	select {
	case m.inCh <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           addr.IP,
		ObservedPort: addr.Port,
	}:
	case <-s.conn.Context().Done():
	}
}

func (m *impl) closeSession(s *peerSession, reason CloseReason) {
	_ = s.conn.CloseWithError(0, string(reason))
	if s.transport != m.mainQT {
		_ = s.transport.Close()
	}
	if s.sockConn != nil {
		_ = s.sockConn.Close()
	}
}

func (m *impl) handleSendFailure(peerKey types.PeerKey, s *peerSession, err error) {
	reason := classifyQUICError(err)
	if reason == peer.DisconnectUnknown {
		return
	}
	if !m.sessions.removeIfCurrent(peerKey, s) {
		return
	}
	m.metrics.SessionDisconnects.Inc()
	m.closeSession(s, CloseReasonDisconnect)
	select {
	case m.inCh <- peer.PeerDisconnected{PeerKey: peerKey, Reason: reason}:
	case <-m.ctx.Done():
	}
}

func (m *impl) recvDatagrams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		payload, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			if m.sessions.removeIfCurrent(peerKey, s) {
				m.metrics.SessionDisconnects.Inc()
				reason := classifyQUICError(err)
				m.log.Debugw("peer session died",
					"peer", peerKey.Short(),
					"reason", reason,
					"err", err,
				)
				select {
				case m.inCh <- peer.PeerDisconnected{
					PeerKey: peerKey,
					Reason:  reason,
				}:
				case <-ctx.Done():
				}
				m.closeSession(s, CloseReasonDisconnected)
			}
			return
		}
		m.metrics.DatagramsRecv.Inc()
		m.metrics.DatagramBytesRecv.Add(int64(len(payload)))
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}

		if resp, ok := env.GetBody().(*meshv1.Envelope_CertRenewalResponse); ok {
			select {
			case m.renewalCh <- resp.CertRenewalResponse:
			default:
			}
			continue
		}

		select {
		case m.recvCh <- Packet{Peer: peerKey, Envelope: env}:
		case <-ctx.Done():
			return
		}
	}
}

func (m *impl) acceptBidiStreams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		qs, err := s.conn.AcceptStream(ctx)
		if err != nil {
			return
		}

		_ = qs.SetReadDeadline(time.Now().Add(streamTypeTimeout))
		var typeBuf [1]byte
		if _, err := io.ReadFull(qs, typeBuf[:]); err != nil {
			qs.CancelRead(0)
			qs.CancelWrite(0)
			continue
		}
		_ = qs.SetReadDeadline(time.Time{})

		var ch chan incomingStream
		switch typeBuf[0] {
		case streamTypeClock:
			ch = m.clockStreamCh
		case streamTypeTunnel:
			ch = m.streamCh
		case streamTypeRouted:
			go m.handleRoutedStream(ctx, qs, peerKey)
			continue
		case streamTypeArtifact:
			if h := m.artifactHandler; h != nil {
				go h(stream{qs}, peerKey)
			} else {
				qs.CancelRead(0)
				qs.CancelWrite(0)
			}
			continue
		case streamTypeWorkload:
			if h := m.workloadHandler; h != nil {
				go h(stream{qs}, peerKey)
			} else {
				qs.CancelRead(0)
				qs.CancelWrite(0)
			}
			continue
		default:
			qs.CancelRead(0)
			qs.CancelWrite(0)
			continue
		}

		select {
		case ch <- incomingStream{peerKey: peerKey, stream: stream{qs}}:
		case <-ctx.Done():
			qs.CancelRead(0)
			qs.CancelWrite(0)
			return
		default:
			qs.CancelRead(0)
			qs.CancelWrite(0)
		}
	}
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
				if m.sessions.removeIfCurrent(peerKey, s) {
					m.metrics.SessionDisconnects.Inc()
					m.closeSession(s, CloseReasonCertRotation)
					select {
					case m.inCh <- peer.PeerDisconnected{PeerKey: peerKey, Reason: peer.DisconnectCertRotation}:
					case <-ctx.Done():
					}
				}
			}
		}
	}
}

func (m *impl) acceptLoop(ctx context.Context) {
	for {
		qc, err := m.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		peerKey, err := peerKeyFromConn(qc)
		if err != nil {
			_ = qc.CloseWithError(0, "identity failed")
			continue
		}

		switch qc.ConnectionState().TLS.NegotiatedProtocol {
		case alpnMesh:
			if m.isDenied != nil && m.isDenied(peerKey) {
				_ = qc.CloseWithError(0, string(CloseReasonDenied))
				continue
			}
			m.addPeer(newPeerSession(qc, m.mainQT, false), peerKey)
		case alpnInvite:
			go m.handleInviteConnection(ctx, qc, peerKey)
		default:
			_ = qc.CloseWithError(0, "unknown protocol")
		}
	}
}

func (m *impl) handleInviteConnection(ctx context.Context, qc *quic.Conn, peerKey types.PeerKey) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	first, err := recvEnvelope(waitCtx, qc)
	if err != nil {
		_ = qc.CloseWithError(0, "recv failed")
		return
	}

	body, ok := first.GetBody().(*meshv1.Envelope_InviteRedeemRequest)
	if !ok {
		_ = qc.CloseWithError(0, "unexpected message on invite connection")
		return
	}

	if err := m.handleInviteRedeem(qc, peerKey, body.InviteRedeemRequest); err != nil {
		m.log.Debugw("rejected invite", "peer", peerKey.Short(), "err", err)
		_ = qc.CloseWithError(0, "invite failed")
	}
}

func classifyQUICError(err error) peer.DisconnectReason {
	var idleErr *quic.IdleTimeoutError
	if errors.As(err, &idleErr) {
		return peer.DisconnectIdleTimeout
	}
	var resetErr *quic.StatelessResetError
	if errors.As(err, &resetErr) {
		return peer.DisconnectReset
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.ErrorMessage {
		case string(CloseReasonTopologyPrune):
			return peer.DisconnectTopologyPrune
		case string(CloseReasonDenied):
			return peer.DisconnectDenied
		case string(CloseReasonCertExpired):
			return peer.DisconnectCertExpired
		case string(CloseReasonCertRotation):
			return peer.DisconnectCertRotation
		default:
			return peer.DisconnectGraceful
		}
	}
	return peer.DisconnectUnknown
}
