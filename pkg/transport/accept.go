package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

func cancelStream(s *quic.Stream) {
	s.CancelRead(0)
	s.CancelWrite(0)
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
				_ = qc.CloseWithError(0, string(closeReasonDenied))
				continue
			}
			m.addPeer(ctx, newPeerSession(qc, m.mainQT, false), peerKey)
		case alpnInvite:
			go m.handleInviteConnection(ctx, qc, peerKey)
		default:
			_ = qc.CloseWithError(0, "unknown protocol")
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
			cancelStream(qs)
			continue
		}
		_ = qs.SetReadDeadline(time.Time{})

		streamType := StreamType(typeBuf[0])
		switch streamType {
		case StreamTypeRouted:
			go m.handleRoutedStream(ctx, qs, peerKey)
		default:
			wrapped := Stream{qs}
			select {
			case m.acceptCh <- acceptedStream{stream: wrapped, stype: streamType, peerKey: peerKey}:
			case <-ctx.Done():
				cancelStream(qs)
			}
		}
	}
}

func (m *impl) recvDatagrams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		payload, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			reason := classifyQUICError(err)
			if m.removePeer(peerKey, s, closeReasonDisconnected, reason) {
				m.log.Debugw("peer session died",
					"peer", peerKey.Short(),
					"reason", reason,
					"err", err,
				)
			}
			return
		}
		m.metrics.DatagramsRecv.Add(ctx, 1)
		m.metrics.DatagramBytesRecv.Add(ctx, int64(len(payload)))
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}

		if resp, ok := env.GetBody().(*meshv1.Envelope_CertRenewalResponse); ok {
			select {
			case m.renewalCh <- resp.CertRenewalResponse:
			case <-ctx.Done():
				return
			}
			continue
		}

		select {
		case m.recvCh <- Packet{From: peerKey, Data: payload}:
		case <-ctx.Done():
			return
		}
	}
}

func classifyQUICError(err error) disconnectReason {
	var idleErr *quic.IdleTimeoutError
	if errors.As(err, &idleErr) {
		return disconnectIdleTimeout
	}
	var resetErr *quic.StatelessResetError
	if errors.As(err, &resetErr) {
		return disconnectReset
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.ErrorMessage {
		case string(closeReasonTopologyPrune):
			return disconnectTopologyPrune
		case string(closeReasonDenied):
			return disconnectDenied
		case string(closeReasonCertExpired):
			return disconnectCertExpired
		case string(closeReasonCertRotation):
			return disconnectCertRotation
		case string(closeReasonDuplicate):
			return disconnectDuplicate
		case string(closeReasonShutdown):
			return disconnectShutdown
		default:
			return disconnectGraceful
		}
	}
	return disconnectUnknown
}

func (m *impl) runMainProbeLoop(ctx context.Context, qt *quic.Transport) {
	buf := make([]byte, probeBufSize)
	for {
		n, sender, err := qt.ReadNonQUICPacket(ctx, buf)
		if err != nil {
			return
		}
		udpSender, ok := sender.(*net.UDPAddr)
		if !ok {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		m.socks.HandleMainProbePacket(data, udpSender)
	}
}

func (m *impl) runPeerTickLoop(ctx context.Context) {
	ticker := time.NewTicker(m.peerTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			outputs := m.peers.step(time.Now(), tick{})
			m.handlePeerOutputs(outputs)
		}
	}
}

func (m *impl) handlePeerOutputs(outputs []output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case attemptConnect:
			go m.internalConnect(e.PeerKey, e.Ips, e.Port)
		case attemptEagerConnect:
			go m.internalEagerConnect(e.PeerKey, e.Addr)
		case requestPunchCoordination:
			m.emitPeerEvent(PeerEvent{Key: e.PeerKey, Type: peerEventNeedsPunch})
		}
	}
}
