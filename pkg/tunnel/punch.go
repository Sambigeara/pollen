package tunnel

import (
	"context"
	"crypto/tls"
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
	punchTimeout         = 10 * time.Second
	punchKeepalivePeriod = 30 * time.Second
	punchStaleTimeout    = 30 * time.Second // Pending punches auto-cleanup after this duration
)

// ConnectedPeersProvider is used to find connected peers that can act as coordinators.
type ConnectedPeersProvider interface {
	GetConnectedPeers() []types.PeerKey
	GetActivePeerAddress(peerKey types.PeerKey) (string, bool)
}

// punchInflight tracks an in-progress punch dial from the initiator's perspective.
type punchInflight struct {
	cert     tls.Certificate
	listener net.Listener
	ch       chan *tcpv1.TcpPunchResponse
	peerKey  types.PeerKey
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

	targetPeer := types.PeerKeyFromBytes(req.PeerId)
	logger.Infow("received punch request",
		"initiator", fromPeer.String()[:8],
		"target", targetPeer.String()[:8],
		"servicePort", req.ServicePort)

	// Verify we have a session to the target
	targetAddr, ok := m.peerProvider.GetActivePeerAddress(targetPeer)
	if !ok {
		logger.Warnw("no session to target peer", "target", targetPeer.String()[:8])
		return fmt.Errorf("no session to target peer %s", targetPeer.String()[:8])
	}

	// Get initiator's external address
	initiatorAddr, ok := m.peerProvider.GetActivePeerAddress(fromPeer)
	if !ok {
		return fmt.Errorf("no session to initiator peer %s", fromPeer.String()[:8])
	}

	// Use the initiator's request ID for correlation through the entire flow
	reqID := req.RequestId

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
		initiatorSrc: net.JoinHostPort(extractIP(initiatorAddr), strconv.FormatUint(uint64(req.LocalPort), 10)),
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
		PeerAddr:    net.JoinHostPort(extractIP(initiatorAddr), strconv.FormatUint(uint64(req.LocalPort), 10)),
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
		if tc, ok := rawConn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(punchKeepalivePeriod)
		}

		// Wrap with TLS (we're the server in TLS terms)
		cfg := tcp.NewPinnedTLSConfig(true, ownCert, peerCert)
		tlsConn := tls.Server(rawConn, cfg)

		if err := tlsConn.HandshakeContext(ctx); err != nil {
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
		RequestId: trigger.RequestId,
		LocalPort: uint32(localPort),
		CertDer:   ownCert.Certificate[0],
		Sig:       ownSig,
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

	// Send response to initiator with target's address and cert
	resp := &tcpv1.TcpPunchResponse{
		PeerAddr:    net.JoinHostPort(extractIP(targetAddr), strconv.FormatUint(uint64(ready.LocalPort), 10)),
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

// extractIP extracts the IP part from an "ip:port" address string.
func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
