package quic

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	recvChanSize = 1024

	errCodeNormal     quic.ApplicationErrorCode = 0
	errCodeDisconnect quic.ApplicationErrorCode = 1
)

type PeerDirectory interface {
	IdentityPub(peerKey types.PeerKey) (ed25519.PublicKey, bool)
}

type SuperSockImpl struct {
	ctx       context.Context
	dir       PeerDirectory
	log       *zap.SugaredLogger
	conns     map[types.PeerKey]*Conn
	recvChan  chan Packet
	events    chan peer.Input
	store     *SocketStore
	transport *Transport
	cancel    context.CancelFunc
	signPriv  ed25519.PrivateKey
	invites   admission.Store
	port      int
	connsMu   sync.RWMutex
}

type Packet struct {
	Payload []byte
	Typ     types.MsgType
	Peer    types.PeerKey
}

func NewSuperSock(port int, signPriv ed25519.PrivateKey, dir PeerDirectory, invites admission.Store) *SuperSockImpl {
	return &SuperSockImpl{
		log:      zap.S().Named("sock"),
		port:     port,
		signPriv: signPriv,
		dir:      dir,
		invites:  invites,
		conns:    make(map[types.PeerKey]*Conn),
		recvChan: make(chan Packet, recvChanSize),
		events:   make(chan peer.Input),
	}
}

func (s *SuperSockImpl) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	localID := types.PeerKeyFromBytes(s.signPriv.Public().(ed25519.PublicKey))

	ss, err := NewSocketStore(s.port, localID)
	if err != nil {
		return fmt.Errorf("create socket store: %w", err)
	}
	s.store = ss

	t, err := NewTransport(s.signPriv, s.invites)
	if err != nil {
		_ = ss.Close()
		return fmt.Errorf("create transport: %w", err)
	}
	s.transport = t

	if err := t.Listen(ss.MainPacketConn()); err != nil {
		_ = ss.Close()
		return fmt.Errorf("QUIC listen: %w", err)
	}

	go s.acceptLoop(s.ctx)
	return nil
}

func (s *SuperSockImpl) Recv(ctx context.Context) (Packet, error) {
	select {
	case p := <-s.recvChan:
		return p, nil
	case <-ctx.Done():
		return Packet{}, fmt.Errorf("transport closed")
	}
}

func (s *SuperSockImpl) Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	s.connsMu.RLock()
	conn, ok := s.conns[peerKey]
	s.connsMu.RUnlock()

	if !ok || conn.IsClosed() {
		return fmt.Errorf("%w: %s", ErrNoPeer, peerKey.Short())
	}

	data := encodeDatagram(msg.Type, msg.Payload)
	if err := conn.SendDatagram(data); err != nil {
		s.log.Debugw("send datagram failed", "peer", peerKey.Short(), "type", msg.Type, "err", err)
		return err
	}
	return nil
}

func (s *SuperSockImpl) Events() <-chan peer.Input {
	return s.events
}

func (s *SuperSockImpl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr, opts *GetOrCreateOpts) error {
	if s.hasActiveConn(peerKey) {
		return nil
	}

	if _, ok := s.dir.IdentityPub(peerKey); !ok {
		return fmt.Errorf("no identity pub for peer %s", peerKey.Short())
	}

	sock, err := s.store.GetOrCreate(ctx, peerKey, addrs, opts)
	if err != nil {
		return err
	}

	qc, cleanup, dialErr := s.transport.Dial(ctx, sock, peerKey)
	if dialErr != nil {
		sock.Close()
		return fmt.Errorf("dial peer %s: %w", peerKey.Short(), dialErr)
	}

	// Build onClose hook: clean up ephemeral QUIC transport + socket store entry.
	var onClose func()
	if cleanup != nil || !sock.IsMain() {
		capturedCleanup := cleanup
		capturedSock := sock
		onClose = func() {
			if capturedCleanup != nil {
				capturedCleanup()
			}
			capturedSock.Close()
		}
	}

	addr := sock.RemoteAddr()
	conn := NewConnWithClose(qc, peerKey, onClose)
	s.registerConn(peerKey, conn)
	s.store.NoteAddress(peerKey, addr)
	go s.datagramReadLoop(s.ctx, conn)

	s.events <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           addr.IP,
		ObservedPort: addr.Port,
	}

	return nil
}

func (s *SuperSockImpl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	qc, peerKey, addr, err := s.transport.JoinWithInvite(ctx, inv)
	if err != nil {
		return err
	}

	conn := NewConn(qc, peerKey)
	s.registerConn(peerKey, conn)
	s.store.NoteAddress(peerKey, addr)
	go s.datagramReadLoop(s.ctx, conn)

	s.events <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           addr.IP,
		ObservedPort: addr.Port,
	}

	return nil
}

func (s *SuperSockImpl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	s.connsMu.RLock()
	conn, ok := s.conns[peerKey]
	s.connsMu.RUnlock()

	if !ok || conn.IsClosed() {
		return nil, false
	}

	return conn.RemoteAddr(), conn.RemoteAddr() != nil
}

func (s *SuperSockImpl) BroadcastDisconnect() {
	s.connsMu.RLock()
	peers := make([]*Conn, 0, len(s.conns))
	for _, conn := range s.conns {
		peers = append(peers, conn)
	}
	s.connsMu.RUnlock()

	for _, conn := range peers {
		_ = conn.CloseWithError(errCodeDisconnect, "shutting down")
	}
}

func (s *SuperSockImpl) Close() error {
	if s.cancel != nil {
		s.cancel()
	}

	s.connsMu.Lock()
	for k, conn := range s.conns {
		_ = conn.Close()
		delete(s.conns, k)
	}
	s.connsMu.Unlock()

	if s.transport != nil {
		_ = s.transport.Close()
	}

	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *SuperSockImpl) GetConn(peerKey types.PeerKey) (*Conn, bool) {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()

	conn, ok := s.conns[peerKey]
	if ok && conn.IsClosed() {
		return nil, false
	}
	return conn, ok
}

func (s *SuperSockImpl) hasActiveConn(peerKey types.PeerKey) bool {
	s.connsMu.RLock()
	conn, ok := s.conns[peerKey]
	s.connsMu.RUnlock()
	return ok && !conn.IsClosed()
}

func (s *SuperSockImpl) registerConn(peerKey types.PeerKey, conn *Conn) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	if old, ok := s.conns[peerKey]; ok {
		_ = old.Close()
	}
	s.conns[peerKey] = conn
}

func (s *SuperSockImpl) removeConnIfSame(peerKey types.PeerKey, conn *Conn) bool {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	current, ok := s.conns[peerKey]
	if ok && current == conn {
		delete(s.conns, peerKey)
		return true
	}
	return false
}

func (s *SuperSockImpl) acceptLoop(ctx context.Context) {
	for {
		qc, err := s.transport.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Debugw("accept failed", "err", err)
			continue
		}
		go s.handleIncomingConnection(ctx, qc)
	}
}

func (s *SuperSockImpl) handleIncomingConnection(ctx context.Context, qc *quic.Conn) {
	peerKey, err := peerKeyFromConnection(qc)
	if err != nil {
		s.log.Debugw("failed to read peer identity from certificate", "remote", qc.RemoteAddr(), "err", err)
		_ = qc.CloseWithError(errCodeNormal, "invalid peer certificate")
		return
	}

	if _, known := s.dir.IdentityPub(peerKey); !known {
		if err := s.transport.AcceptInviteStream(ctx, qc); err != nil {
			s.log.Debugw("invite handshake failed", "remote", qc.RemoteAddr(), "peer", peerKey.Short(), "err", err)
			_ = qc.CloseWithError(errCodeNormal, "invite required")
			return
		}
	}

	conn := NewConn(qc, peerKey)
	s.registerConn(peerKey, conn)
	go s.datagramReadLoop(s.ctx, conn)

	if addr, ok := qc.RemoteAddr().(*net.UDPAddr); ok {
		s.store.NoteAddress(peerKey, addr)
		s.events <- peer.ConnectPeer{
			PeerKey:      peerKey,
			IP:           addr.IP,
			ObservedPort: addr.Port,
		}
	}
}

func peerKeyFromConnection(qc *quic.Conn) (types.PeerKey, error) {
	peerCerts := qc.ConnectionState().TLS.PeerCertificates
	if len(peerCerts) == 0 {
		return types.PeerKey{}, fmt.Errorf("no peer certificate")
	}
	return peerKeyFromRawCert(peerCerts[0].Raw)
}

func (s *SuperSockImpl) datagramReadLoop(ctx context.Context, conn *Conn) {
	peerKey := conn.PeerID()

	for {
		data, err := conn.ReceiveDatagram(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			_ = conn.Close()
			s.log.Debugw("datagram read error", "peer", peerKey.Short(), "err", err)
			if s.removeConnIfSame(peerKey, conn) {
				s.events <- peer.PeerDisconnected{PeerKey: peerKey}
			}
			return
		}

		msgType, payload, err := decodeDatagram(data)
		if err != nil {
			s.log.Debugw("datagram decode error", "peer", peerKey.Short(), "err", err)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case s.recvChan <- Packet{
			Peer:    peerKey,
			Payload: payload,
			Typ:     msgType,
		}:
		}
	}
}
