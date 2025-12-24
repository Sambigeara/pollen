package tunnel

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/ed25519"
	"google.golang.org/protobuf/proto"

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
	dir           PeerDirectory
	sender        Send
	services      map[string]serviceHandler
	connections   map[string]connectionHandler
	inflight      map[uint64]inflight
	signPriv      ed25519.PrivateKey
	ips           []string
	dialTimeout   time.Duration
	acceptTimeout time.Duration
	serviceMu     sync.RWMutex
	connectionMu  sync.RWMutex
	inflightMu    sync.Mutex
}

type serviceHandler struct {
	fn     func(net.Conn)
	cancel context.CancelFunc
}

type connectionHandler struct {
	fn     func(net.Conn)
	cancel context.CancelFunc
}

func connectionKey(peerID, port string) string {
	return peerID + ":" + port
}

type inflight struct {
	ch      chan *tcpv1.Handshake
	peerKey types.PeerKey
}

var (
	ErrNoHandler             = errors.New("no incoming tunnel handler registered")
	ErrUnknownPeerIdentity   = errors.New("peer identity key unknown")
	ErrHandshakeResponseMiss = errors.New("timed out waiting for tunnel handshake response")
	ErrNoDialableAddress     = errors.New("no dialable tunnel address")
)

func New(sender Send, dir PeerDirectory, signPriv ed25519.PrivateKey, advertisableIPs []string) *Manager {
	return &Manager{
		dir:           dir,
		sender:        sender,
		services:      make(map[string]serviceHandler),
		connections:   make(map[string]connectionHandler),
		signPriv:      signPriv,
		ips:           advertisableIPs,
		dialTimeout:   dialTimeout,
		acceptTimeout: acceptTimeout,
		inflight:      make(map[uint64]inflight),
	}
}

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
				log.Printf("Failed to dial local service: %v", err)
				_ = tunnelConn.Close()
				return
			}
			tcp.Bridge(tunnelConn, conn)
		},
		cancel: cancelFn,
	}
}

func (m *Manager) ConnectService(peerID types.PeerKey, port string) error {
	if _, ok := m.dir.IdentityPub(peerID); !ok {
		return errors.New("peerID not recognised")
	}

	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	ctx, cancelFn := context.WithCancel(context.Background())

	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+port)
	if err != nil {
		cancelFn()
		return err
	}

	go func() {
		for {
			clientConn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				meshConn, err := m.Dial(ctx, peerID, port)
				if err != nil {
					log.Printf("Tunnel failed: %v", err)
					_ = clientConn.Close()
					return
				}
				tcp.Bridge(clientConn, meshConn)
			}()
		}
	}()

	m.connections[connectionKey(peerID.String(), port)] = connectionHandler{
		fn: func(net.Conn) {
			panic("TODO")
		},
		cancel: cancelFn,
	}

	return nil
}

func (m *Manager) HandleRequest(ctx context.Context, peerKey types.PeerKey, payload []byte) error {
	logger := zap.S().Named("tunnel")
	logger.Info("Handling connect request")

	req := &tcpv1.Handshake{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return fmt.Errorf("unmarshal tunnel request: %w", err)
	}
	if req.GetRequestId() == 0 {
		return errors.New("tunnel request missing request_id")
	}

	peerIDPub, ok := m.dir.IdentityPub(peerKey)
	if !ok {
		logger.Warnw("unknown peer", "peerID", peerKey.String())
		return ErrUnknownPeerIdentity
	}

	peerCert, err := tcp.VerifyPeerAttestation(peerIDPub, req.GetCertDer(), req.GetSig())
	if err != nil {
		logger.Errorf("peer attestation failed: %w", err)
		return fmt.Errorf("peer attestation failed: %w", err)
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	if err != nil {
		logger.Errorw("listen tcp :0", err)
		return fmt.Errorf("listen tcp :0: %w", err)
	}

	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		ln.Close()
		logger.Errorf("generate ephemeral cert: %w", err)
		return fmt.Errorf("generate ephemeral cert: %w", err)
	}

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		logger.Errorf("split listener addr: %w", err)
		return fmt.Errorf("split listener addr: %w", err)
	}

	addrs := make([]string, 0, len(m.ips))
	for _, ip := range m.ips {
		addrs = append(addrs, net.JoinHostPort(ip, port))
	}

	resp := &tcpv1.Handshake{
		RequestId: req.GetRequestId(),
		CertDer:   ownCert.Certificate[0],
		Sig:       ownSig,
		Addr:      addrs,
	}

	respBytes, err := proto.Marshal(resp)
	if err != nil {
		ln.Close()
		logger.Errorf("marshal tunnel response: %w", err)
		return fmt.Errorf("marshal tunnel response: %w", err)
	}

	env := types.Envelope{
		Type:    types.MsgTypeTCPTunnelResponse,
		Payload: respBytes,
	}
	if err := m.sender(ctx, peerKey, env); err != nil {
		ln.Close()
		logger.Errorf("send tunnel response: %w", err)
		return fmt.Errorf("send tunnel response: %w", err)
	}

	go m.acceptIncoming(req.ServicePort, ln, ownCert, peerCert)
	return nil
}

func (m *Manager) acceptIncoming(port string, ln net.Listener, ourCert tls.Certificate, peerCert *x509.Certificate) {
	defer ln.Close()

	logger := zap.S().Named("tunnel")

	m.serviceMu.RLock()
	h, ok := m.services[port]
	m.serviceMu.RUnlock()
	if !ok {
		logger.Warn("service handler not found")
		// TODO(saml) error return
		return
	}

	cfg := tcp.NewPinnedTLSConfig(true, ourCert, peerCert)

	// Avoid goroutine leaks: set an accept deadline if possible.
	if tl, ok := ln.(*net.TCPListener); ok {
		_ = tl.SetDeadline(time.Now().Add(m.acceptTimeout))
	}

	tlsLn := tls.NewListener(ln, cfg)
	conn, err := tlsLn.Accept()
	if err != nil {
		logger.Warn("error accepting incoming connection")
		return
	}

	h.fn(conn)
}

func (m *Manager) HandleResponse(_ context.Context, _ types.PeerKey, payload []byte) error {
	resp := &tcpv1.Handshake{}
	if err := proto.Unmarshal(payload, resp); err != nil {
		return fmt.Errorf("unmarshal tunnel response: %w", err)
	}
	rid := resp.GetRequestId()
	if rid == 0 {
		return errors.New("tunnel response missing request_id")
	}

	m.inflightMu.Lock()
	inf, ok := m.inflight[rid]
	if ok {
		delete(m.inflight, rid)
	}
	m.inflightMu.Unlock()

	if !ok {
		return nil
	}

	select {
	case inf.ch <- resp:
	default:
	}
	return nil
}

// Dial establishes an outgoing TCP tunnel to peerNoisePub.
// Register HandleResponse with mesh.On(...) for this to work.
func (m *Manager) Dial(ctx context.Context, peerNoisePub types.PeerKey, servicePort string) (net.Conn, error) {
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

	req := &tcpv1.Handshake{
		RequestId:   reqID,
		CertDer:     ownCert.Certificate[0],
		Sig:         ownSig,
		ServicePort: servicePort,
	}
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal tunnel request: %w", err)
	}

	ch := make(chan *tcpv1.Handshake, 1)
	m.inflightMu.Lock()
	m.inflight[reqID] = inflight{peerKey: peerNoisePub, ch: ch}
	m.inflightMu.Unlock()
	defer func() {
		m.inflightMu.Lock()
		delete(m.inflight, reqID)
		m.inflightMu.Unlock()
	}()

	zap.S().Infow("Sending tunnel request", "servicePort", servicePort)
	env := types.Envelope{
		Type:    types.MsgTypeTCPTunnelRequest,
		Payload: reqBytes,
	}
	if err := m.sender(ctx, peerNoisePub, env); err != nil {
		return nil, fmt.Errorf("tunnel request failed: %w", err)
	}

	// Wait for response
	waitCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
	defer cancel()

	var resp *tcpv1.Handshake
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

	// Try candidates in order
	// dialer := &net.Dialer{Timeout: m.dialTimeout}
	// for _, addr := range resp.GetAddr() {
	// 	d := &tls.Dialer{
	// 		NetDialer: dialer,
	// 		Config:    cfg,
	// 	}
	// 	conn, err := d.DialContext(ctx, "tcp", addr)
	// 	if err != nil {
	// 		continue
	// 	}
	// 	return conn, nil
	// }

	// Dial all candidates concurrently; first success wins.
	addrs := resp.GetAddr()
	if len(addrs) == 0 {
		return nil, ErrNoDialableAddress
	}

	attemptCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
	defer cancel()

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
				cancel()
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

func randUint64() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}
