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
	"sync"
	"time"

	"golang.org/x/crypto/ed25519"
	"google.golang.org/protobuf/proto"

	"github.com/sambigeara/pollen/pkg/tcp"
	"github.com/sambigeara/pollen/pkg/types"

	tcpv1 "github.com/sambigeara/pollen/api/genpb/pollen/tcp/v1"
)

type Send func(ctx context.Context, peer types.PeerKey, msg types.Envelope) error

type PeerDirectory interface {
	IdentityPub(peerNoisePub []byte) (ed25519.PublicKey, bool)
}

type Manager struct {
	sender Send
	dir    PeerDirectory

	signPriv ed25519.PrivateKey
	ips      []string

	dialTimeout   time.Duration
	acceptTimeout time.Duration

	incomingMu sync.RWMutex
	incoming   func(net.Conn)

	inflightMu sync.Mutex
	inflight   map[uint64]inflight

	peerLocks sync.Map // map[types.NodeID]*sync.Mutex
}

type inflight struct {
	peerKey types.NodeID
	ch      chan *tcpv1.Handshake
}

var (
	ErrNoHandler             = errors.New("no incoming tunnel handler registered")
	ErrUnknownPeerIdentity   = errors.New("peer identity key unknown")
	ErrHandshakeResponseMiss = errors.New("timed out waiting for tunnel handshake response")
	ErrNoDialableAddress     = errors.New("no dialable tunnel address")
)

func New(sender Send, dir PeerDirectory, signPriv ed25519.PrivateKey, advertisableIPs []string) *Manager {
	return &Manager{
		sender:        sender,
		dir:           dir,
		signPriv:      signPriv,
		ips:           advertisableIPs,
		dialTimeout:   6 * time.Second,
		acceptTimeout: 10 * time.Second,
		inflight:      make(map[uint64]inflight),
	}
}

func (m *Manager) SetIncomingHandler(h func(net.Conn)) {
	m.incomingMu.Lock()
	m.incoming = h
	m.incomingMu.Unlock()
}

func (m *Manager) HandleRequest(ctx context.Context, peerKey types.PeerKey, payload []byte) error {
	req := &tcpv1.Handshake{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return fmt.Errorf("unmarshal tunnel request: %w", err)
	}
	if req.GetRequestId() == 0 {
		return errors.New("tunnel request missing request_id")
	}

	peerIDPub, ok := m.dir.IdentityPub(peerKey.Bytes())
	if !ok {
		return ErrUnknownPeerIdentity
	}

	peerCert, err := tcp.VerifyPeerAttestation(peerIDPub, req.GetCertDer(), req.GetSig())
	if err != nil {
		return fmt.Errorf("peer attestation failed: %w", err)
	}

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return fmt.Errorf("listen tcp :0: %w", err)
	}

	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		ln.Close()
		return fmt.Errorf("generate ephemeral cert: %w", err)
	}

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
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
		return fmt.Errorf("marshal tunnel response: %w", err)
	}

	env := types.Envelope{
		Type:    types.MsgTypeTCPTunnelResponse,
		Payload: respBytes,
	}
	if err := m.sender(ctx, peerKey, env); err != nil {
		ln.Close()
		return fmt.Errorf("send tunnel response: %w", err)
	}

	go m.acceptIncoming(ln, ownCert, peerCert)
	return nil
}

func (m *Manager) acceptIncoming(ln net.Listener, ourCert tls.Certificate, peerCert *x509.Certificate) {
	defer ln.Close()

	m.incomingMu.RLock()
	h := m.incoming
	m.incomingMu.RUnlock()
	if h == nil {
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
		return
	}

	h(conn)
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
		// stale/duplicate/unsolicited response
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
func (m *Manager) Dial(ctx context.Context, peerNoisePub []byte) (net.Conn, error) {
	peerIDPub, ok := m.dir.IdentityPub(peerNoisePub)
	if !ok {
		return nil, ErrUnknownPeerIdentity
	}

	lock := m.getPeerLock(types.NodeID(peerNoisePub))
	lock.Lock()
	defer lock.Unlock()

	reqID, err := randUint64()
	if err != nil {
		return nil, err
	}

	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral cert: %w", err)
	}

	req := &tcpv1.Handshake{
		RequestId: reqID,
		CertDer:   ownCert.Certificate[0],
		Sig:       ownSig,
	}
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal tunnel request: %w", err)
	}

	ch := make(chan *tcpv1.Handshake, 1)
	m.inflightMu.Lock()
	m.inflight[reqID] = inflight{peerKey: types.NodeID(peerNoisePub), ch: ch}
	m.inflightMu.Unlock()
	defer func() {
		m.inflightMu.Lock()
		delete(m.inflight, reqID)
		m.inflightMu.Unlock()
	}()

	env := types.Envelope{
		Type:    types.MsgTypeTCPTunnelRequest,
		Payload: reqBytes,
	}
	if err := m.sender(ctx, types.PeerKeyFromBytes(peerNoisePub), env); err != nil {
		return nil, fmt.Errorf("send tunnel request: %w", err)
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
	dialer := &net.Dialer{Timeout: m.dialTimeout}
	for _, addr := range resp.GetAddr() {
		conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
		if err != nil {
			continue
		}
		return conn, nil
	}

	return nil, ErrNoDialableAddress
}

func (m *Manager) getPeerLock(k types.NodeID) *sync.Mutex {
	v, _ := m.peerLocks.LoadOrStore(k, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func randUint64() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}
