package quic

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

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

	streamTypeInvite byte = 0x02

	inviteTokenMaxLen = 255
	inviteHeaderLen   = 2

	handshakeTimeout = 5 * time.Second

	maxReassemblyFragments = 4096
	maxReassemblyBytes     = 8 * 1024 * 1024
	fragmentReassemblyTTL  = 30 * time.Second
)

type PeerDirectory interface {
	IdentityPub(peerKey types.PeerKey) (ed25519.PublicKey, bool)
}

type SuperSockImpl struct {
	ctx       context.Context
	dir       PeerDirectory
	invites   admission.Store
	log       *zap.SugaredLogger
	conns     map[types.PeerKey]*Conn
	recvChan  chan Packet
	events    chan peer.Input
	transport *Transport
	cancel    context.CancelFunc
	signPriv  ed25519.PrivateKey
	port      int
	connsMu   sync.RWMutex
	fragSeq   uint32
}

type Packet struct {
	Payload []byte
	Typ     types.MsgType
	Peer    types.PeerKey
}

type dialResult struct {
	qc        *quic.Conn
	addr      *net.UDPAddr
	transport *Transport
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

	t, err := newQUICTransport(s.port, s.signPriv)
	if err != nil {
		return fmt.Errorf("create QUIC transport: %w", err)
	}
	s.transport = t

	if err := t.listen(); err != nil {
		_ = t.close()
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
	if err := conn.SendDatagram(data); err == nil {
		return nil
	} else {
		var tooLarge *quic.DatagramTooLargeError
		if !errors.As(err, &tooLarge) {
			s.log.Debugw("send datagram failed", "peer", peerKey.Short(), "type", msg.Type, "err", err)
			return err
		}

		frames, fragErr := encodeFragmentedDatagrams(msg.Type, msg.Payload, int(tooLarge.MaxDatagramPayloadSize), atomic.AddUint32(&s.fragSeq, 1))
		if fragErr != nil {
			return fmt.Errorf("fragment oversized datagram: %w", fragErr)
		}

		for _, frame := range frames {
			if sendErr := conn.SendDatagram(frame); sendErr != nil {
				s.log.Debugw("send fragmented datagram failed", "peer", peerKey.Short(), "type", msg.Type, "err", sendErr)
				return sendErr
			}
		}
	}

	return nil
}

func (s *SuperSockImpl) Events() <-chan peer.Input {
	return s.events
}

func (s *SuperSockImpl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr) error {
	if s.hasActiveConn(peerKey) {
		return nil
	}

	if _, ok := s.dir.IdentityPub(peerKey); !ok {
		return fmt.Errorf("no identity pub for peer %s", peerKey.Short())
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan dialResult, 1)
	var wg sync.WaitGroup

	for _, addr := range addrs {
		if addr == nil {
			continue
		}

		wg.Add(1)
		go func(addr *net.UDPAddr) {
			defer wg.Done()

			qc, err := s.transport.dialExpectedPeer(ctx, addr, peerKey)
			if err != nil {
				s.log.Debugw("dial failed", "peer", peerKey.Short(), "addr", addr, "err", err)
				return
			}

			select {
			case resultCh <- dialResult{qc: qc, addr: addr}:
				cancel()
			default:
				_ = qc.CloseWithError(errCodeNormal, "duplicate connection")
			}
		}(addr)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	res, ok := <-resultCh
	if !ok {
		if s.hasActiveConn(peerKey) {
			return nil
		}

		return fmt.Errorf("failed to connect to peer %s at any address", peerKey.Short())
	}

	s.registerDialResult(peerKey, res)
	return nil
}

func (s *SuperSockImpl) EnsurePeerPunch(ctx context.Context, peerKey types.PeerKey, targetAddr *net.UDPAddr) error {
	if targetAddr == nil {
		return errors.New("missing target address for punch")
	}

	if s.hasActiveConn(peerKey) {
		return nil
	}

	if _, ok := s.dir.IdentityPub(peerKey); !ok {
		return fmt.Errorf("no identity pub for peer %s", peerKey.Short())
	}

	candidates := buildPunchCandidates(targetAddr, punchRemoteRandomPortAttempts)
	if len(candidates) == 0 {
		return fmt.Errorf("failed to derive punch candidates for peer %s", peerKey.Short())
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan dialResult, 1)
	var wg sync.WaitGroup

	launchDial := func(t *Transport, addr *net.UDPAddr, ownedTransport bool) {
		if t == nil || addr == nil {
			if ownedTransport && t != nil {
				_ = t.close()
			}
			return
		}

		wg.Go(func() {
			qc, err := t.dialExpectedPeer(ctx, addr, peerKey)
			if err != nil {
				if ownedTransport {
					_ = t.close()
				}
				return
			}

			result := dialResult{qc: qc, addr: addr}
			if ownedTransport {
				result.transport = t
			}

			select {
			case resultCh <- result:
				cancel()
			default:
				_ = qc.CloseWithError(errCodeNormal, "duplicate connection")
				if ownedTransport {
					_ = t.close()
				}
			}
		})
	}

	for _, candidate := range candidates {
		launchDial(s.transport, candidate, false)
	}

	for range punchLocalSocketAttempts {
		t, err := newQUICTransport(0, s.signPriv)
		if err != nil {
			s.log.Debugw("fanout transport setup failed", "peer", peerKey.Short(), "err", err)
			continue
		}
		launchDial(t, targetAddr, true)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	res, ok := <-resultCh
	if !ok {
		if s.hasActiveConn(peerKey) {
			return nil
		}

		return fmt.Errorf("failed to punch-connect to peer %s", peerKey.Short())
	}

	s.registerDialResult(peerKey, res)
	return nil
}

func (s *SuperSockImpl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	if len(inv.Fingerprint) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid fingerprint length in invite")
	}

	if len(inv.Id) == 0 {
		return fmt.Errorf("missing invite id")
	}

	token := []byte(inv.Id)
	if len(token) > inviteTokenMaxLen {
		return fmt.Errorf("invite id too long")
	}

	peerKey := types.PeerKeyFromBytes(inv.Fingerprint)

	for _, addrStr := range inv.Addr {
		addr, err := net.ResolveUDPAddr("udp", addrStr)
		if err != nil {
			s.log.Debugw("resolve invite addr failed", "addr", addrStr, "err", err)
			continue
		}

		qc, err := s.transport.dialExpectedPeer(ctx, addr, peerKey)
		if err != nil {
			s.log.Debugw("invite dial failed", "addr", addrStr, "err", err)
			continue
		}

		stream, err := qc.OpenStreamSync(ctx)
		if err != nil {
			s.log.Debugw("open invite stream failed", "addr", addrStr, "err", err)
			_ = qc.CloseWithError(errCodeNormal, "stream open failed")
			continue
		}

		_ = stream.SetDeadline(time.Now().Add(handshakeTimeout))

		req := make([]byte, 0, inviteHeaderLen+len(token))
		req = append(req, streamTypeInvite, byte(len(token)))
		req = append(req, token...)
		if _, err := stream.Write(req); err != nil {
			_ = stream.Close()
			_ = qc.CloseWithError(errCodeNormal, "invite write failed")
			continue
		}

		var resp [1]byte
		if _, err := io.ReadFull(stream, resp[:]); err != nil {
			_ = stream.Close()
			_ = qc.CloseWithError(errCodeNormal, "invite read failed")
			continue
		}

		_ = stream.Close()

		if resp[0] != 0 {
			_ = qc.CloseWithError(errCodeNormal, "invite rejected")
			continue
		}

		conn := NewConn(qc, peerKey)
		s.registerConn(peerKey, conn)
		go s.datagramReadLoop(s.ctx, conn)

		s.events <- peer.ConnectPeer{
			PeerKey:      peerKey,
			IP:           addr.IP,
			ObservedPort: addr.Port,
		}

		return nil
	}

	return fmt.Errorf("failed to join via invite at any address")
}

func (s *SuperSockImpl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	s.connsMu.RLock()
	conn, ok := s.conns[peerKey]
	s.connsMu.RUnlock()

	if !ok || conn.IsClosed() {
		return nil, false
	}

	addr, ok := conn.qc.RemoteAddr().(*net.UDPAddr)
	return addr, ok
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

	if s.transport == nil {
		return nil
	}

	return s.transport.close()
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

func (s *SuperSockImpl) registerDialResult(peerKey types.PeerKey, res dialResult) {
	if res.qc == nil || res.addr == nil {
		return
	}

	conn := NewConn(res.qc, peerKey)
	if res.transport != nil {
		ownedTransport := res.transport
		conn = NewConnWithClose(res.qc, peerKey, func() {
			if err := ownedTransport.close(); err != nil {
				s.log.Debugw("failed closing owned transport", "peer", peerKey.Short(), "err", err)
			}
		})
	}

	s.registerConn(peerKey, conn)
	go s.datagramReadLoop(s.ctx, conn)

	s.events <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           res.addr.IP,
		ObservedPort: res.addr.Port,
	}
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
		qc, err := s.transport.accept(ctx)
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
		if err := s.acceptInviteStream(ctx, qc); err != nil {
			s.log.Debugw("invite handshake failed", "remote", qc.RemoteAddr(), "peer", peerKey.Short(), "err", err)
			_ = qc.CloseWithError(errCodeNormal, "invite required")
			return
		}
	}

	conn := NewConn(qc, peerKey)
	s.registerConn(peerKey, conn)
	go s.datagramReadLoop(s.ctx, conn)

	if addr, ok := qc.RemoteAddr().(*net.UDPAddr); ok {
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

func (s *SuperSockImpl) acceptInviteStream(ctx context.Context, qc *quic.Conn) error {
	acceptCtx, acceptCancel := context.WithTimeout(ctx, handshakeTimeout)
	defer acceptCancel()

	stream, err := qc.AcceptStream(acceptCtx)
	if err != nil {
		return err
	}
	defer func() {
		_ = stream.Close()
	}()

	_ = stream.SetDeadline(time.Now().Add(handshakeTimeout))

	var header [2]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		return fmt.Errorf("read invite header: %w", err)
	}

	if header[0] != streamTypeInvite {
		return fmt.Errorf("unexpected stream type: %d", header[0])
	}

	tokenLen := int(header[1])
	if tokenLen == 0 {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("empty invite token")
	}

	tokenBuf := make([]byte, tokenLen)
	if _, err := io.ReadFull(stream, tokenBuf); err != nil {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("read invite token: %w", err)
	}

	ok, err := s.invites.ConsumeToken(string(tokenBuf))
	if err != nil {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("consume invite token: %w", err)
	}
	if !ok {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("invalid invite token")
	}

	if _, err := stream.Write([]byte{0}); err != nil {
		return fmt.Errorf("write invite response: %w", err)
	}

	return nil
}

func (s *SuperSockImpl) datagramReadLoop(ctx context.Context, conn *Conn) {
	peerKey := conn.PeerID()
	fragments := make(map[uint32]*fragmentAccumulator)
	lastPrune := time.Now()

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

		now := time.Now()
		if now.Sub(lastPrune) >= fragmentReassemblyTTL {
			pruneFragmentAccumulators(fragments, now)
			lastPrune = now
		}

		msgType, payload, frag, err := decodeDatagram(data)
		//nolint:nestif
		if err != nil {
			if errors.Is(err, errFragmentedDatagram) && frag != nil {
				assembledType, assembledPayload, complete, asmErr := addFragment(fragments, frag, now)
				if asmErr != nil {
					s.log.Debugw("fragment assembly error", "peer", peerKey.Short(), "err", asmErr)
					continue
				}
				if !complete {
					continue
				}
				msgType = assembledType
				payload = assembledPayload
			} else {
				s.log.Debugw("datagram decode error", "peer", peerKey.Short(), "err", err)
				continue
			}
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

type fragmentAccumulator struct {
	updated  time.Time
	parts    [][]byte
	size     int
	msgType  types.MsgType
	total    uint16
	received uint16
}

func addFragment(acc map[uint32]*fragmentAccumulator, frag *datagramFragment, now time.Time) (types.MsgType, []byte, bool, error) {
	if frag.total == 0 {
		return 0, nil, false, fmt.Errorf("fragment total is zero")
	}
	if frag.total > maxReassemblyFragments {
		return 0, nil, false, fmt.Errorf("too many fragments: %d", frag.total)
	}

	entry, ok := acc[frag.msgID]
	if !ok {
		entry = &fragmentAccumulator{
			msgType: frag.msgType,
			total:   frag.total,
			parts:   make([][]byte, frag.total),
		}
		acc[frag.msgID] = entry
	}

	if entry.msgType != frag.msgType || entry.total != frag.total {
		delete(acc, frag.msgID)
		return 0, nil, false, fmt.Errorf("fragment header mismatch")
	}

	entry.updated = now

	if entry.parts[frag.index] != nil {
		return 0, nil, false, nil
	}

	chunk := append([]byte(nil), frag.payload...)
	entry.parts[frag.index] = chunk
	entry.received++
	entry.size += len(chunk)

	if entry.size > maxReassemblyBytes {
		delete(acc, frag.msgID)
		return 0, nil, false, fmt.Errorf("reassembled payload too large: %d bytes", entry.size)
	}

	if entry.received != entry.total {
		return 0, nil, false, nil
	}

	out := make([]byte, 0, entry.size)
	for _, part := range entry.parts {
		if part == nil {
			return 0, nil, false, fmt.Errorf("missing fragment in completed message")
		}
		out = append(out, part...)
	}

	delete(acc, frag.msgID)
	return entry.msgType, out, true, nil
}

func pruneFragmentAccumulators(acc map[uint32]*fragmentAccumulator, now time.Time) {
	for msgID, entry := range acc {
		if now.Sub(entry.updated) > fragmentReassemblyTTL {
			delete(acc, msgID)
		}
	}
}
