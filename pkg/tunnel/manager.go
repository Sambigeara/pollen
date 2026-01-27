package tunnel

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/ed25519"

	"github.com/sambigeara/pollen/pkg/tcp"
	"github.com/sambigeara/pollen/pkg/types"

	tcpv1 "github.com/sambigeara/pollen/api/genpb/pollen/tcp/v1"
)

const (
	dialTimeout   = 6 * time.Second
	acceptTimeout = 10 * time.Second
)

type Send func(ctx context.Context, peer types.PeerKey, msg types.Envelope) error

type PeerDirectory interface {
	IdentityPub(peerNoisePub types.PeerKey) (ed25519.PublicKey, bool)
}

type Manager struct {
	peerProvider      ConnectedPeersProvider
	dir               PeerDirectory
	sessionInflight   map[uint64]sessionInflight
	sessions          *SessionManager
	connections       map[string]connectionHandler
	pendingPunch      map[uint64]pendingPunchReq
	services          map[string]serviceHandler
	punchInflight     map[uint64]punchInflight
	sender            Send
	ips               []string
	signPriv          ed25519.PrivateKey
	acceptTimeout     time.Duration
	dialTimeout       time.Duration
	connectionMu      sync.RWMutex
	serviceMu         sync.RWMutex
	sessionInflightMu sync.Mutex
	punchMu           sync.Mutex
}

type serviceHandler struct {
	fn     func(net.Conn)
	cancel context.CancelFunc
}

type connectionHandler struct {
	cancel context.CancelFunc
	peerID types.PeerKey
	remote uint32
	local  uint32
}

type ConnectionInfo struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

func connectionKey(peerID, port string) string {
	return peerID + ":" + port
}

// sessionInflight tracks an in-progress session establishment.
type sessionInflight struct {
	ch      chan *tcpv1.SessionHandshake
	peerKey types.PeerKey
}

var (
	ErrNoHandler             = errors.New("no incoming tunnel handler registered")
	ErrUnknownPeerIdentity   = errors.New("peer identity key unknown")
	ErrHandshakeResponseMiss = errors.New("timed out waiting for session handshake response")
	ErrNoDialableAddress     = errors.New("no dialable tunnel address")
	ErrServiceNotRegistered  = errors.New("service not registered")
)

func New(sender Send, dir PeerDirectory, signPriv ed25519.PrivateKey, advertisableIPs []string) *Manager {
	m := &Manager{
		dir:             dir,
		sender:          sender,
		services:        make(map[string]serviceHandler),
		connections:     make(map[string]connectionHandler),
		signPriv:        signPriv,
		ips:             advertisableIPs,
		dialTimeout:     dialTimeout,
		acceptTimeout:   acceptTimeout,
		sessionInflight: make(map[uint64]sessionInflight),
		punchInflight:   make(map[uint64]punchInflight),
		pendingPunch:    make(map[uint64]pendingPunchReq),
	}

	// Initialize session manager with dial function and stream handler
	m.sessions = NewSessionManager(m.dialSession, m.handleIncomingStream)

	return m
}

// SetPeerProvider sets the connected peers provider for TCP punch coordination.
// This should be called after the Link is created.
func (m *Manager) SetPeerProvider(p ConnectedPeersProvider) {
	m.peerProvider = p
}

// handleIncomingStream routes incoming streams to the appropriate service handler.
func (m *Manager) handleIncomingStream(stream net.Conn, servicePort uint16) {
	portStr := strconv.FormatUint(uint64(servicePort), 10)

	m.serviceMu.RLock()
	h, ok := m.services[portStr]
	m.serviceMu.RUnlock()

	if !ok {
		zap.S().Warnw("no handler for incoming stream", "port", servicePort)
		stream.Close()
		return
	}

	h.fn(stream)
}

// RegisterService registers a local service that can be accessed by remote peers.
func (m *Manager) RegisterService(port uint32) {
	m.serviceMu.Lock()
	defer m.serviceMu.Unlock()

	portStr := strconv.FormatUint(uint64(port), 10)

	curr, ok := m.services[portStr]
	if ok {
		curr.cancel()
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	m.services[portStr] = serviceHandler{
		fn: func(tunnelConn net.Conn) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", "localhost:"+portStr)
			if err != nil {
				zap.S().Warnw("failed to dial local service", "port", portStr, "err", err)
				_ = tunnelConn.Close()
				return
			}
			tcp.Bridge(tunnelConn, conn)
		},
		cancel: cancelFn,
	}

	zap.S().Infow("registered service", "port", port)
}

// ConnectService establishes a tunnel to a remote service and listens locally.
// Local connections to the port are forwarded to the remote peer's service.
// If localPort is zero, it will attempt to bind to remotePort first, then fall
// back to an ephemeral port if that fails.
func (m *Manager) ConnectService(peerID types.PeerKey, remotePort uint32, localPort uint32) (uint32, error) {
	if _, ok := m.dir.IdentityPub(peerID); !ok {
		return 0, errors.New("peerID not recognised")
	}
	if remotePort == 0 {
		return 0, errors.New("remote port missing")
	}

	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	ctx, cancelFn := context.WithCancel(context.Background())
	var ln net.Listener
	var err error
	var requestedLocal uint32

	if localPort > 0 {
		requestedLocal = localPort
	} else {
		requestedLocal = remotePort
	}

	ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+strconv.FormatUint(uint64(requestedLocal), 10))
	if err != nil && localPort == 0 {
		ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	}

	if err != nil {
		cancelFn()
		return 0, err
	}

	_, boundPortStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		cancelFn()
		_ = ln.Close()
		return 0, err
	}
	boundPort, err := strconv.ParseUint(boundPortStr, 10, 16)
	if err != nil {
		cancelFn()
		_ = ln.Close()
		return 0, err
	}

	portNum := uint16(remotePort)

	go func() {
		logger := zap.S().Named("tunnel")
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				// Get or create session to peer
				session, err := m.sessions.GetOrCreate(ctx, peerID)
				if err != nil {
					logger.Warnw("session failed", "peer", peerID.String()[:8], "err", err)
					_ = clientConn.Close()
					return
				}

				// Open stream to remote service
				stream, err := session.OpenStream(portNum)
				if err != nil {
					logger.Warnw("open stream failed", "peer", peerID.String()[:8], "port", remotePort, "err", err)
					_ = clientConn.Close()
					return
				}

				tcp.Bridge(clientConn, stream)
			}()
		}
	}()

	m.connections[connectionKey(peerID.String(), boundPortStr)] = connectionHandler{
		cancel: cancelFn,
		peerID: peerID,
		remote: remotePort,
		local:  uint32(boundPort),
	}

	zap.S().Infow("connected to service", "peer", peerID.String()[:8], "port", remotePort, "local_port", boundPort)
	return uint32(boundPort), nil
}

func (m *Manager) ListConnections() []ConnectionInfo {
	m.connectionMu.RLock()
	defer m.connectionMu.RUnlock()

	out := make([]ConnectionInfo, 0, len(m.connections))
	for _, h := range m.connections {
		out = append(out, ConnectionInfo{
			PeerID:     h.peerID,
			RemotePort: h.remote,
			LocalPort:  h.local,
		})
	}
	return out
}

func (m *Manager) DisconnectLocalPort(port uint32) bool {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	var key string
	var handler connectionHandler
	found := false
	for k, h := range m.connections {
		if h.local == port {
			key = k
			handler = h
			found = true
			break
		}
	}
	if !found {
		return false
	}
	delete(m.connections, key)
	handler.cancel()
	return true
}

func (m *Manager) UnregisterService(port uint32) bool {
	portStr := strconv.FormatUint(uint64(port), 10)

	m.serviceMu.Lock()
	defer m.serviceMu.Unlock()

	if h, ok := m.services[portStr]; ok {
		h.cancel()
		delete(m.services, portStr)
		return true
	}
	return false
}

// HandleSessionRequest handles incoming session establishment requests.
// This is called when a remote peer wants to establish a multiplexed session to us.
func (m *Manager) HandleSessionRequest(ctx context.Context, peerKey types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel.session")
	logger.Infow("handling session request", "peer", peerKey.String()[:8])

	req := &tcpv1.SessionHandshake{}
	if err := req.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal session request: %w", err)
	}
	if req.GetRequestId() == 0 {
		return errors.New("session request missing request_id")
	}

	peerIDPub, ok := m.dir.IdentityPub(peerKey)
	if !ok {
		logger.Warnw("unknown peer", "peerID", peerKey.String())
		return ErrUnknownPeerIdentity
	}

	peerCert, err := tcp.VerifyPeerAttestation(peerIDPub, req.GetCertDer(), req.GetSig())
	if err != nil {
		logger.Errorf("peer attestation failed: %v", err)
		return fmt.Errorf("peer attestation failed: %w", err)
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	if err != nil {
		logger.Errorw("listen tcp :0", "err", err)
		return fmt.Errorf("listen tcp :0: %w", err)
	}

	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		ln.Close()
		logger.Errorf("generate ephemeral cert: %v", err)
		return fmt.Errorf("generate ephemeral cert: %w", err)
	}

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		logger.Errorf("split listener addr: %v", err)
		return fmt.Errorf("split listener addr: %w", err)
	}

	addrs := make([]string, 0, len(m.ips))
	for _, ip := range m.ips {
		addrs = append(addrs, net.JoinHostPort(ip, port))
	}

	resp := &tcpv1.SessionHandshake{
		RequestId: req.GetRequestId(),
		CertDer:   ownCert.Certificate[0],
		Sig:       ownSig,
		Addr:      addrs,
	}

	respBytes, err := resp.MarshalVT()
	if err != nil {
		ln.Close()
		logger.Errorf("marshal session response: %v", err)
		return fmt.Errorf("marshal session response: %w", err)
	}

	env := types.Envelope{
		Type:    types.MsgTypeSessionResponse,
		Payload: respBytes,
	}
	if err := m.sender(ctx, peerKey, env); err != nil {
		ln.Close()
		logger.Errorf("send session response: %v", err)
		return fmt.Errorf("send session response: %w", err)
	}

	// Accept incoming connection and wrap with session
	go m.acceptSession(peerKey, ln, ownCert, peerCert)
	return nil
}

// acceptSession waits for the incoming TCP connection and wraps it as a session.
func (m *Manager) acceptSession(peerKey types.PeerKey, ln net.Listener, ourCert tls.Certificate, peerCert *x509.Certificate) {
	defer ln.Close()

	logger := zap.S().Named("tunnel.session")

	cfg := tcp.NewPinnedTLSConfig(true, ourCert, peerCert)

	// Avoid goroutine leaks: set an accept deadline if possible.
	if tl, ok := ln.(*net.TCPListener); ok {
		_ = tl.SetDeadline(time.Now().Add(m.acceptTimeout))
	}

	tlsLn := tls.NewListener(ln, cfg)
	conn, err := tlsLn.Accept()
	if err != nil {
		logger.Warnw("error accepting session connection", "err", err)
		return
	}

	// Register the session
	_, err = m.sessions.Accept(conn, peerKey)
	if err != nil {
		logger.Warnw("failed to accept session", "peer", peerKey.String()[:8], "err", err)
		conn.Close()
		return
	}

	logger.Infow("accepted session", "peer", peerKey.String()[:8])
}

// HandleSessionResponse handles session establishment responses.
func (m *Manager) HandleSessionResponse(_ context.Context, _ types.PeerKey, payload []byte) error {
	resp := &tcpv1.SessionHandshake{}
	if err := resp.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("unmarshal session response: %w", err)
	}
	rid := resp.GetRequestId()
	if rid == 0 {
		return errors.New("session response missing request_id")
	}

	m.sessionInflightMu.Lock()
	inf, ok := m.sessionInflight[rid]
	if ok {
		delete(m.sessionInflight, rid)
	}
	m.sessionInflightMu.Unlock()

	if !ok {
		return nil
	}

	select {
	case inf.ch <- resp:
	default:
	}
	return nil
}

// dialSession establishes the underlying TCP+TLS connection for a session.
// This is called by SessionManager.GetOrCreate when no session exists.
func (m *Manager) dialSession(ctx context.Context, peerID types.PeerKey) (net.Conn, error) {
	logger := zap.S().Named("tunnel.session")

	// Stage 1: Try direct connection
	conn, err := m.dialSessionDirect(ctx, peerID)
	if err == nil {
		return conn, nil
	}
	logger.Debugw("direct session dial failed, trying punch", "peer", peerID.String()[:8], "err", err)

	// Stage 2: Try TCP hole punch via a coordinator
	if m.peerProvider != nil {
		coordinator := m.findCoordinator(peerID)
		if coordinator != (types.PeerKey{}) {
			conn, err = m.dialSessionWithPunch(ctx, peerID, coordinator)
			if err == nil {
				return conn, nil
			}
			logger.Debugw("punch session dial failed", "peer", peerID.String()[:8], "err", err)
		}
	}

	return nil, ErrNoDialableAddress
}

// dialSessionDirect attempts a direct TCP+TLS connection for session establishment.
func (m *Manager) dialSessionDirect(ctx context.Context, peerNoisePub types.PeerKey) (net.Conn, error) {
	peerIDPub, ok := m.dir.IdentityPub(peerNoisePub)
	if !ok {
		return nil, ErrUnknownPeerIdentity
	}

	reqID, err := randUint64()
	if err != nil {
		return nil, err
	}

	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral cert: %w", err)
	}

	req := &tcpv1.SessionHandshake{
		RequestId: reqID,
		CertDer:   ownCert.Certificate[0],
		Sig:       ownSig,
	}
	reqBytes, err := req.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshal session request: %w", err)
	}

	ch := make(chan *tcpv1.SessionHandshake, 1)
	m.sessionInflightMu.Lock()
	m.sessionInflight[reqID] = sessionInflight{peerKey: peerNoisePub, ch: ch}
	m.sessionInflightMu.Unlock()
	defer func() {
		m.sessionInflightMu.Lock()
		delete(m.sessionInflight, reqID)
		m.sessionInflightMu.Unlock()
	}()

	zap.S().Debugw("sending session request", "peer", peerNoisePub.String()[:8])
	env := types.Envelope{
		Type:    types.MsgTypeSessionRequest,
		Payload: reqBytes,
	}
	if err := m.sender(ctx, peerNoisePub, env); err != nil {
		return nil, fmt.Errorf("session request failed: %w", err)
	}

	// Wait for response
	waitCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
	defer cancel()

	var resp *tcpv1.SessionHandshake
	select {
	case resp = <-ch:
	case <-waitCtx.Done():
		return nil, ErrHandshakeResponseMiss
	}

	peerCert, err := tcp.VerifyPeerAttestation(peerIDPub, resp.GetCertDer(), resp.GetSig())
	if err != nil {
		return nil, fmt.Errorf("peer attestation failed: %w", err)
	}

	cfg := tcp.NewPinnedTLSConfig(false, ownCert, peerCert)

	// Dial all candidates concurrently; first success wins.
	addrs := resp.GetAddr()
	if len(addrs) == 0 {
		return nil, ErrNoDialableAddress
	}

	attemptCtx, attemptCancel := context.WithTimeout(ctx, m.dialTimeout)
	defer attemptCancel()

	resCh := make(chan net.Conn, 1)
	var wg sync.WaitGroup

	for _, a := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			d := &tls.Dialer{
				NetDialer: &net.Dialer{Timeout: m.dialTimeout},
				Config:    cfg,
			}
			conn, err := d.DialContext(attemptCtx, "tcp", addr)
			if err != nil {
				return
			}
			select {
			case resCh <- conn:
				// We won; cancel remaining dials.
				attemptCancel()
			default:
				// Another goroutine already won.
				_ = conn.Close()
			}
		}(a)
	}

	// Close resCh when all attempts finish.
	go func() {
		wg.Wait()
		close(resCh)
	}()

	if conn, ok := <-resCh; ok && conn != nil {
		return conn, nil
	}

	return nil, ErrNoDialableAddress
}

// dialSessionWithPunch attempts session establishment via TCP hole punching.
func (m *Manager) dialSessionWithPunch(ctx context.Context, peerKey, coordinator types.PeerKey) (net.Conn, error) {
	logger := zap.S().Named("tunnel.session")
	logger.Infow("attempting punch session dial",
		"target", peerKey.String()[:8],
		"coordinator", coordinator.String()[:8])

	peerIDPub, ok := m.dir.IdentityPub(peerKey)
	if !ok {
		return nil, ErrUnknownPeerIdentity
	}

	// Generate ephemeral cert
	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral cert: %w", err)
	}

	// Bind TCP listener with SO_REUSEPORT
	ln, err := tcp.ListenReusePort(ctx, ":0")
	if err != nil {
		return nil, fmt.Errorf("listen reuseport: %w", err)
	}

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("split listener addr: %w", err)
	}
	localPort, _ := strconv.ParseUint(portStr, 10, 32)

	// Generate request ID
	reqID, err := randUint64()
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("generate request id: %w", err)
	}

	// Register inflight
	ch := make(chan *tcpv1.TcpPunchResponse, 1)
	m.punchMu.Lock()
	m.punchInflight[reqID] = punchInflight{
		ch:       ch,
		peerKey:  peerKey,
		listener: ln,
		cert:     ownCert,
	}
	m.punchMu.Unlock()

	defer func() {
		m.punchMu.Lock()
		delete(m.punchInflight, reqID)
		m.punchMu.Unlock()
	}()

	// Send punch request to coordinator (with empty service port - session establishment)
	req := &tcpv1.TcpPunchRequest{
		PeerId:      peerKey.Bytes(),
		LocalPort:   uint32(localPort),
		ServicePort: "", // Empty for session establishment
		CertDer:     ownCert.Certificate[0],
		Sig:         ownSig,
	}

	reqBytes, err := req.MarshalVT()
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("marshal punch request: %w", err)
	}

	logger.Infow("sending session punch request to coordinator",
		"coordinator", coordinator.String()[:8],
		"localPort", localPort)

	if err := m.sender(ctx, coordinator, types.Envelope{
		Type:    types.MsgTypeTCPPunchRequest,
		Payload: reqBytes,
	}); err != nil {
		ln.Close()
		return nil, fmt.Errorf("send punch request: %w", err)
	}

	// Wait for response
	waitCtx, cancel := context.WithTimeout(ctx, punchTimeout)
	defer cancel()

	var resp *tcpv1.TcpPunchResponse
	select {
	case resp = <-ch:
	case <-waitCtx.Done():
		ln.Close()
		return nil, fmt.Errorf("timed out waiting for punch response")
	}

	// Verify peer cert attestation
	peerCert, err := tcp.VerifyPeerAttestation(peerIDPub, resp.PeerCertDer, resp.PeerSig)
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("peer attestation failed: %w", err)
	}

	logger.Infow("starting TCP simultaneous open for session",
		"peerAddr", resp.PeerAddr,
		"localPort", localPort)

	// Start TCP simultaneous open
	rawConn, err := tcp.SimultaneousOpen(ctx, ln, resp.PeerAddr, punchTimeout)
	ln.Close() // Close listener after simultaneous open completes
	if err != nil {
		return nil, fmt.Errorf("tcp simultaneous open failed: %w", err)
	}

	// Tune socket before TLS wrap
	if tc, ok := rawConn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(punchKeepalivePeriod)
	}

	// Wrap with TLS (we're the client in TLS terms)
	cfg := tcp.NewPinnedTLSConfig(false, ownCert, peerCert)
	tlsConn := tls.Client(rawConn, cfg)

	if err := tlsConn.HandshakeContext(ctx); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("tls handshake failed: %w", err)
	}

	logger.Infow("punch session established (initiator side)",
		"target", peerKey.String()[:8])

	return tlsConn, nil
}

// findCoordinator finds a connected peer that can act as coordinator for punch.
// Returns zero value if no coordinator is available.
func (m *Manager) findCoordinator(target types.PeerKey) types.PeerKey {
	if m.peerProvider == nil {
		return types.PeerKey{}
	}

	peers := m.peerProvider.GetConnectedPeers()
	for _, p := range peers {
		// Don't use the target as coordinator
		if p == target {
			continue
		}
		return p
	}
	return types.PeerKey{}
}

// Close shuts down the manager and all sessions.
func (m *Manager) Close() {
	m.sessions.Close()

	m.serviceMu.Lock()
	for _, h := range m.services {
		h.cancel()
	}
	m.serviceMu.Unlock()

	m.connectionMu.Lock()
	for _, h := range m.connections {
		h.cancel()
	}
	m.connectionMu.Unlock()
}

func randUint64() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}
