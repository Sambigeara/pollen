package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"go.uber.org/zap"

	tcpv1 "github.com/sambigeara/pollen/api/genpb/pollen/tcp/v1"
	"github.com/sambigeara/pollen/pkg/tcp"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	punchTimeout       = 10 * time.Second
	tcpKeepalivePeriod = 30 * time.Second
	punchStaleTimeout  = 30 * time.Second // Pending punches auto-cleanup after this duration
	probeTimeout       = 2 * time.Second
	probeDialTimeout   = 1 * time.Second
)

// ConnectedPeersProvider is used to find connected peers that can act as coordinators.
type ConnectedPeersProvider interface {
	GetConnectedPeers() []types.PeerKey
	GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool)
}

// punchInflight tracks an in-progress punch dial from the initiator's perspective.
type punchInflight struct {
	cert     tls.Certificate
	listener net.Listener
	ch       chan *tcpv1.TcpPunchResponse
	peerKey  types.PeerKey
}

type probeInflight struct {
	offerCh  chan *tcpv1.TcpPunchProbeOffer
	resultCh chan *tcpv1.TcpPunchProbeResult
}

// pendingPunchReq tracks a punch request from the coordinator's perspective.
type pendingPunchReq struct {
	createdAt    time.Time
	initiatorSrc string
	servicePort  string
	certDer      []byte
	sig          []byte
	localPort    uint32
	initiator    types.PeerKey
}

// HandlePunchRequest handles TcpPunchRequest messages (coordinator role).
// Called when an initiator asks this node to coordinate a punch to a target.
func (m *Manager) HandlePunchRequest(ctx context.Context, fromPeer types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel.punch")

	req := &tcpv1.TcpPunchRequest{}
	if err := req.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch request: %w", err)
	}
	if req.GetRequestId() == 0 {
		return errors.New("punch request missing request_id")
	}

	targetPeer := types.PeerKeyFromBytes(req.PeerId)
	logger.Infow("received punch request",
		"initiator", fromPeer.String()[:8],
		"target", targetPeer.String()[:8],
		"servicePort", req.ServicePort)

	// Use the initiator's request ID for correlation through the entire flow
	reqID := req.RequestId

	initiatorPort := req.LocalPort
	if req.ObservedExternalPort != 0 {
		initiatorPort = req.ObservedExternalPort
	}

	// Verify we have a session to the target
	targetAddr, ok := m.peerProvider.GetActivePeerAddress(targetPeer)
	if !ok {
		logger.Warnw("no session to target peer", "target", targetPeer.String()[:8])
		m.sendPunchReject(ctx, fromPeer, reqID, "no session to target")
		return fmt.Errorf("no session to target peer %s", targetPeer.String()[:8])
	}

	// Get initiator's external address
	initiatorAddr, ok := m.peerProvider.GetActivePeerAddress(fromPeer)
	if !ok {
		m.sendPunchReject(ctx, fromPeer, reqID, "no session to initiator")
		return fmt.Errorf("no session to initiator peer %s", fromPeer.String()[:8])
	}

	// Store pending request so we can complete it when target responds
	//
	// TODO(saml): Port discovery - the current implementation assumes port-preserving NAT.
	// We substitute the peer's local TCP port into their public IP, which only works if
	// the NAT maps local_port -> same external_port. Many NATs (especially CGNAT) don't
	// preserve ports, causing punch failures.
	//
	// Fix: Add a "port discovery" step before punching:
	//   1. After each peer binds their listener on ":0", have them briefly connect to a
	//      known endpoint (STUN server or the coordinator itself) FROM that same local port
	//      using SO_REUSEPORT. The endpoint observes the actual external ip:port.
	//   2. Include the observed external port in TcpPunchRequest/TcpPunchReady messages
	//      (add a field like `observed_external_port` to the proto).
	//   3. Use the observed port here instead of req.LocalPort / ready.LocalPort.
	//
	// This removes the port-preservation assumption and dramatically improves reliability
	// across diverse NAT types. See also: the same substitution in HandlePunchReady at
	// line ~300 where we build PeerAddr for the response.
	m.punchMu.Lock()
	if m.pendingPunch == nil {
		m.pendingPunch = make(map[uint64]pendingPunchReq)
	}
	m.pendingPunch[reqID] = pendingPunchReq{
		initiator:    fromPeer,
		initiatorSrc: net.JoinHostPort(initiatorAddr.IP.String(), strconv.FormatUint(uint64(initiatorPort), 10)),
		localPort:    req.LocalPort,
		certDer:      req.CertDer,
		sig:          req.Sig,
		servicePort:  req.ServicePort,
		createdAt:    time.Now(),
	}
	m.punchMu.Unlock()

	// Auto-cleanup if target never responds
	time.AfterFunc(punchStaleTimeout, func() {
		m.punchMu.Lock()
		delete(m.pendingPunch, reqID)
		m.punchMu.Unlock()
	})

	// Send trigger to target
	trigger := &tcpv1.TcpPunchTrigger{
		PeerId:      fromPeer.Bytes(),
		PeerAddr:    net.JoinHostPort(initiatorAddr.IP.String(), strconv.FormatUint(uint64(initiatorPort), 10)),
		ServicePort: req.ServicePort,
		PeerCertDer: req.CertDer,
		PeerSig:     req.Sig,
		RequestId:   reqID,
	}

	triggerBytes, err := trigger.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal punch trigger: %w", err)
	}

	logger.Infow("sending punch trigger to target",
		"target", targetPeer.String()[:8],
		"targetAddr", targetAddr)

	return m.sender(ctx, targetPeer, types.Envelope{
		Type:    types.MsgTypeTCPPunchTrigger,
		Payload: triggerBytes,
	})
}

// HandlePunchTrigger handles TcpPunchTrigger messages (target role).
// Called when a coordinator tells us to participate in a punch with an initiator.
// This establishes a session (not a per-request tunnel).
func (m *Manager) HandlePunchTrigger(ctx context.Context, fromPeer types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel.punch")

	trigger := &tcpv1.TcpPunchTrigger{}
	if err := trigger.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch trigger: %w", err)
	}

	initiatorPeer := types.PeerKeyFromBytes(trigger.PeerId)
	logger.Infow("received punch trigger for session",
		"initiator", initiatorPeer.String()[:8],
		"peerAddr", trigger.PeerAddr)

	// Verify initiator's cert attestation
	initiatorIDPub, ok := m.dir.IdentityPub(initiatorPeer)
	if !ok {
		return fmt.Errorf("unknown initiator peer: %s", initiatorPeer.String()[:8])
	}

	peerCert, err := tcp.VerifyPeerAttestation(initiatorIDPub, trigger.PeerCertDer, trigger.PeerSig)
	if err != nil {
		return fmt.Errorf("initiator attestation failed: %w", err)
	}

	// Generate our own ephemeral cert
	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		return fmt.Errorf("generate ephemeral cert: %w", err)
	}

	// Bind TCP listener with SO_REUSEPORT
	ln, err := tcp.ListenReusePort(ctx, ":0")
	if err != nil {
		return fmt.Errorf("listen reuseport: %w", err)
	}

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		return fmt.Errorf("split listener addr: %w", err)
	}
	localPort, _ := strconv.ParseUint(portStr, 10, 32)

	observedPort := m.discoverExternalPort(ctx, fromPeer, uint32(localPort))

	// Start TCP simultaneous open IMMEDIATELY (speculative punching).
	// This runs in parallel with sending Ready back to coordinator, so by the time
	// the initiator receives the Response and starts its SimultaneousOpen, we're
	// already sending SYNs. This cuts one round-trip from the critical path.
	go func() {
		defer ln.Close()

		logger.Infow("starting TCP simultaneous open for session (speculative)",
			"peerAddr", trigger.PeerAddr,
			"localPort", localPort)

		rawConn, err := tcp.SimultaneousOpen(ctx, ln, trigger.PeerAddr, punchTimeout)
		if err != nil {
			logger.Warnw("tcp simultaneous open failed", "err", err)
			return
		}

		// Tune socket before TLS wrap
		tuneTCPConn(rawConn)

		// Wrap with TLS (we're the server in TLS terms)
		cfg := tcp.NewPinnedTLSConfig(true, ownCert, peerCert)
		tlsConn := tls.Server(rawConn, cfg)

		handshakeCtx, cancel := context.WithTimeout(ctx, punchTimeout)
		defer cancel()
		if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
			logger.Warnw("tls handshake failed", "err", err)
			rawConn.Close()
			return
		}

		logger.Infow("punch session established (target side)",
			"initiator", initiatorPeer.String()[:8])

		// Register as a session (we're the server side of yamux)
		_, err = m.sessions.Accept(tlsConn, initiatorPeer)
		if err != nil {
			logger.Warnw("failed to accept punched session", "err", err)
			tlsConn.Close()
			return
		}
	}()

	// Send ready to coordinator (in parallel with punching above)
	ready := &tcpv1.TcpPunchReady{
		RequestId:            trigger.RequestId,
		LocalPort:            uint32(localPort),
		ObservedExternalPort: observedPort,
		CertDer:              ownCert.Certificate[0],
		Sig:                  ownSig,
	}

	readyBytes, err := ready.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal punch ready: %w", err)
	}

	logger.Infow("sending punch ready to coordinator",
		"coordinator", fromPeer.String()[:8],
		"localPort", localPort)

	if err := m.sender(ctx, fromPeer, types.Envelope{
		Type:    types.MsgTypeTCPPunchReady,
		Payload: readyBytes,
	}); err != nil {
		return fmt.Errorf("send punch ready: %w", err)
	}

	return nil
}

// HandlePunchReady handles TcpPunchReady messages (coordinator role).
// Called when target is ready to receive the punch connection.
func (m *Manager) HandlePunchReady(ctx context.Context, fromPeer types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel.punch")

	ready := &tcpv1.TcpPunchReady{}
	if err := ready.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch ready: %w", err)
	}

	logger.Infow("received punch ready",
		"target", fromPeer.String()[:8],
		"requestId", ready.RequestId,
		"localPort", ready.LocalPort)

	// Look up pending request
	m.punchMu.Lock()
	pending, ok := m.pendingPunch[ready.RequestId]
	if ok {
		delete(m.pendingPunch, ready.RequestId)
	}
	m.punchMu.Unlock()

	if !ok {
		return fmt.Errorf("no pending punch request for id %d", ready.RequestId)
	}

	// Get target's external address
	targetAddr, ok := m.peerProvider.GetActivePeerAddress(fromPeer)
	if !ok {
		return fmt.Errorf("no session to target peer %s", fromPeer.String()[:8])
	}

	targetPort := ready.LocalPort
	if ready.ObservedExternalPort != 0 {
		targetPort = ready.ObservedExternalPort
	}

	// Send response to initiator with target's address and cert
	resp := &tcpv1.TcpPunchResponse{
		PeerAddr:    net.JoinHostPort(targetAddr.IP.String(), strconv.FormatUint(uint64(targetPort), 10)),
		PeerCertDer: ready.CertDer,
		PeerSig:     ready.Sig,
		RequestId:   ready.RequestId,
	}

	respBytes, err := resp.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal punch response: %w", err)
	}

	logger.Infow("sending punch response to initiator",
		"initiator", pending.initiator.String()[:8],
		"peerAddr", resp.PeerAddr)

	return m.sender(ctx, pending.initiator, types.Envelope{
		Type:    types.MsgTypeTCPPunchResponse,
		Payload: respBytes,
	})
}

// HandlePunchResponse handles TcpPunchResponse messages (initiator role).
// Called when coordinator tells us the target is ready.
func (m *Manager) HandlePunchResponse(_ context.Context, _ types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel.punch")

	resp := &tcpv1.TcpPunchResponse{}
	if err := resp.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch response: %w", err)
	}

	logger.Infow("received punch response", "peerAddr", resp.PeerAddr, "requestId", resp.RequestId)

	// Find the waiting inflight by request ID and signal it
	m.punchMu.Lock()
	inf, ok := m.punchInflight[resp.RequestId]
	m.punchMu.Unlock()

	if !ok {
		logger.Warnw("no inflight punch for request", "requestId", resp.RequestId)
		return nil
	}

	select {
	case inf.ch <- resp:
	default:
		logger.Warnw("inflight channel full, dropping response", "requestId", resp.RequestId)
	}

	return nil
}

// HandlePunchProbeRequest handles TcpPunchProbeRequest messages (coordinator role).
// It spins up a short-lived TCP listener to observe the caller's external port.
func (m *Manager) HandlePunchProbeRequest(ctx context.Context, fromPeer types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel.punch")

	req := &tcpv1.TcpPunchProbeRequest{}
	if err := req.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch probe request: %w", err)
	}
	if req.GetRequestId() == 0 {
		return errors.New("punch probe request missing request_id")
	}

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return fmt.Errorf("listen probe: %w", err)
	}
	defer ln.Close()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return fmt.Errorf("split probe listener addr: %w", err)
	}
	portNum, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return fmt.Errorf("parse probe listener port: %w", err)
	}

	offer := &tcpv1.TcpPunchProbeOffer{
		RequestId: req.RequestId,
		ProbePort: uint32(portNum),
	}
	offerBytes, err := offer.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal probe offer: %w", err)
	}

	if err := m.sender(ctx, fromPeer, types.Envelope{
		Type:    types.MsgTypeTCPPunchProbeOffer,
		Payload: offerBytes,
	}); err != nil {
		return fmt.Errorf("send probe offer: %w", err)
	}

	if tl, ok := ln.(*net.TCPListener); ok {
		_ = tl.SetDeadline(time.Now().Add(probeTimeout))
	}

	conn, err := ln.Accept()
	if err != nil {
		logger.Debugw("probe accept failed", "err", err)
		m.sendProbeResult(ctx, fromPeer, req.RequestId, 0)
		return nil
	}

	observedPort := 0
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		observedPort = addr.Port
	}
	_ = conn.Close()

	m.sendProbeResult(ctx, fromPeer, req.RequestId, uint32(observedPort))
	return nil
}

// HandlePunchProbeOffer handles TcpPunchProbeOffer messages (initiator/target role).
func (m *Manager) HandlePunchProbeOffer(_ context.Context, _ types.PeerKey, payload []byte) error {
	offer := &tcpv1.TcpPunchProbeOffer{}
	if err := offer.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch probe offer: %w", err)
	}
	if offer.GetRequestId() == 0 {
		return errors.New("punch probe offer missing request_id")
	}

	m.probeMu.Lock()
	inf, ok := m.probeInflight[offer.RequestId]
	m.probeMu.Unlock()
	if !ok {
		return nil
	}
	select {
	case inf.offerCh <- offer:
	default:
	}
	return nil
}

// HandlePunchProbeResult handles TcpPunchProbeResult messages (initiator/target role).
func (m *Manager) HandlePunchProbeResult(_ context.Context, _ types.PeerKey, payload []byte) error {
	res := &tcpv1.TcpPunchProbeResult{}
	if err := res.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal punch probe result: %w", err)
	}
	if res.GetRequestId() == 0 {
		return errors.New("punch probe result missing request_id")
	}

	m.probeMu.Lock()
	inf, ok := m.probeInflight[res.RequestId]
	m.probeMu.Unlock()
	if !ok {
		return nil
	}
	select {
	case inf.resultCh <- res:
	default:
	}
	return nil
}

func (m *Manager) discoverExternalPort(ctx context.Context, coordinator types.PeerKey, localPort uint32) uint32 {
	logger := zap.S().Named("tunnel.punch")

	if m.peerProvider == nil {
		return 0
	}
	coordAddr, ok := m.peerProvider.GetActivePeerAddress(coordinator)
	if !ok {
		return 0
	}

	reqID, err := randUint64()
	if err != nil {
		return 0
	}

	offerCh := make(chan *tcpv1.TcpPunchProbeOffer, 1)
	resultCh := make(chan *tcpv1.TcpPunchProbeResult, 1)

	m.probeMu.Lock()
	m.probeInflight[reqID] = probeInflight{offerCh: offerCh, resultCh: resultCh}
	m.probeMu.Unlock()
	defer func() {
		m.probeMu.Lock()
		delete(m.probeInflight, reqID)
		m.probeMu.Unlock()
	}()

	probeReq := &tcpv1.TcpPunchProbeRequest{RequestId: reqID}
	probeBytes, err := probeReq.MarshalVT()
	if err != nil {
		return 0
	}

	if err := m.sender(ctx, coordinator, types.Envelope{
		Type:    types.MsgTypeTCPPunchProbeRequest,
		Payload: probeBytes,
	}); err != nil {
		return 0
	}

	offerCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	var offer *tcpv1.TcpPunchProbeOffer
	select {
	case offer = <-offerCh:
	case <-offerCtx.Done():
		return 0
	}
	if offer.GetProbePort() == 0 {
		return 0
	}

	probeAddr := net.JoinHostPort(coordAddr.IP.String(), strconv.FormatUint(uint64(offer.ProbePort), 10))
	dialer := tcp.NewReusePortDialer(int(localPort), probeDialTimeout)
	dialCtx, dialCancel := context.WithTimeout(ctx, probeDialTimeout)
	conn, err := dialer.DialContext(dialCtx, "tcp", probeAddr)
	dialCancel()
	if err != nil {
		logger.Debugw("probe dial failed", "addr", probeAddr, "err", err)
		return 0
	}
	_ = conn.Close()

	resultCtx, resultCancel := context.WithTimeout(ctx, probeTimeout)
	defer resultCancel()
	var result *tcpv1.TcpPunchProbeResult
	select {
	case result = <-resultCh:
	case <-resultCtx.Done():
		return 0
	}
	if result.GetObservedPort() == 0 {
		return 0
	}
	return result.ObservedPort
}

func (m *Manager) sendProbeResult(ctx context.Context, peer types.PeerKey, reqID uint64, port uint32) {
	res := &tcpv1.TcpPunchProbeResult{RequestId: reqID, ObservedPort: port}
	resBytes, err := res.MarshalVT()
	if err != nil {
		return
	}
	_ = m.sender(ctx, peer, types.Envelope{
		Type:    types.MsgTypeTCPPunchProbeResult,
		Payload: resBytes,
	})
}

func (m *Manager) sendPunchReject(ctx context.Context, initiator types.PeerKey, reqID uint64, reason string) {
	logger := zap.S().Named("tunnel.punch")
	resp := &tcpv1.TcpPunchResponse{RequestId: reqID}
	respBytes, err := resp.MarshalVT()
	if err != nil {
		logger.Warnw("marshal punch reject failed", "reason", reason, "err", err)
		return
	}
	if err := m.sender(ctx, initiator, types.Envelope{
		Type:    types.MsgTypeTCPPunchResponse,
		Payload: respBytes,
	}); err != nil {
		logger.Warnw("send punch reject failed", "reason", reason, "err", err)
	}
}
