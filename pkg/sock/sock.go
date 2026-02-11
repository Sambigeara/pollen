package sock

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ SuperSock = (*impl)(nil)

var (
	ErrHandshakeIncomplete = errors.New("handshake incomplete")
	ErrNoSession           = errors.New("no session for peer")
)

const (
	udpReadBufferSize = 64 * 1024

	// Resource limits.
	ephemeralSocketCount      = 256
	searchTickerInterval      = 10 * time.Millisecond
	searchTimeout             = 3 * time.Second
	handshakeDedupTTL         = 5 * time.Second
	sessionRefreshInterval    = 120 * time.Second // new IK handshake every 2 mins
	punchAttemptDuration      = 5 * time.Second
	ensurePeerResendInterval  = 100 * time.Millisecond
	staleSessionCheckInterval = 30 * time.Second
	recvChanSize              = 1024
	readDeadlineTimeout       = 500 * time.Millisecond
	minEphemeralPort          = 1024
	maxEphemeralPort          = 64511 // 65535 - minEphemeralPort
)

type Packet struct {
	Src     string
	Payload []byte
	Typ     types.MsgType
	Peer    types.PeerKey
}

type SuperSock interface {
	Start(ctx context.Context) error
	Recv(ctx context.Context) (Packet, error)
	Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error
	Events() <-chan peer.Input
	EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr, withPeer bool) error // IK init
	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error                                     // XXpsk2 init
	GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool)
	BroadcastDisconnect() // notify all connected peers we're shutting down
	Close() error
}

type LocalCrypto interface {
	NoisePub() []byte
	IdentityPub() []byte // ed25519 pub
}

type route struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr
}

type impl struct {
	log *zap.SugaredLogger

	ctx    context.Context
	cancel context.CancelFunc

	crypto   LocalCrypto
	recvChan chan Packet
	events   chan peer.Input
	primary  *net.UDPConn

	rekeyMgr       *rekeyManager
	handshakeStore *handshakeStore
	sessionStore   *sessionStore
	peerLocks      sync.Map
	port           int

	waitPeer map[types.PeerKey]chan route
	waitMu   sync.Mutex
}

func NewTransport(port int, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission) SuperSock {
	return &impl{
		log:            zap.S().Named("sock"),
		port:           port,
		recvChan:       make(chan Packet, recvChanSize),
		crypto:         crypto,
		handshakeStore: newHandshakeStore(cs, admission, staticKey, crypto.IdentityPub()),
		sessionStore:   newSessionStore(crypto.NoisePub()),
		rekeyMgr:       newRekeyManager(),
		waitPeer:       make(map[types.PeerKey]chan route),
		events:         make(chan peer.Input),
	}
}

func (m *impl) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.ctx = ctx
	m.cancel = cancel

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
	if err != nil {
		return fmt.Errorf("failed to listen UDP: %w", err)
	}

	m.primary = conn

	go m.readLoop(ctx, conn)
	go m.staleSessionChecker(ctx)
	return nil
}

func (m *impl) Recv(ctx context.Context) (Packet, error) {
	select {
	case p := <-m.recvChan:
		return p, nil
	case <-ctx.Done():
		return Packet{}, fmt.Errorf("transport closed")
	}
}

func (m *impl) Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	log := m.log.With("peerKey", peerKey.Short(), "msg.Type", msg.Type)

	sess, ok := m.sessionStore.getByPeer(peerKey)
	if !ok {
		log.Debugw("%v", ErrNoSession)
		return fmt.Errorf("%w: %s", ErrNoSession, peerKey.String())
	}

	ct, shouldRekey, err := sess.Encrypt(msg.Payload)
	if err != nil {
		log.Errorf("encrypt: %v", err)
		return fmt.Errorf("encrypt: %w", err)
	}

	fr := &Frame{
		Payload:    ct,
		Typ:        msg.Type,
		SenderID:   sess.localSessionID,
		ReceiverID: sess.peerSessionID,
	}

	if err := m.send(sess.conn, sess.peerAddr, encodeFrame(fr)); err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(sess.peerSessionID, sessionRefreshInterval)
	}

	return nil
}

func (m *impl) send(conn *net.UDPConn, addr *net.UDPAddr, payload []byte) error {
	_, err := conn.WriteToUDP(payload, addr)
	return err
}

func (m *impl) Events() <-chan peer.Input {
	return m.events
}

func (m *impl) Close() error {
	m.cancel()
	err := m.primary.Close()
	for _, sess := range m.sessionStore.drain() {
		if sess.conn != m.primary {
			sess.conn.Close()
		}
	}
	return err
}

func (m *impl) readLoop(ctx context.Context, conn *net.UDPConn) {
	buf := make([]byte, udpReadBufferSize)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(readDeadlineTimeout)); err != nil {
			return
		}
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) {
				continue
			}
			m.log.Debugw("ReadFromUDP error", "error", err)
			return
		}

		fr, err := decodeFrame(buf[:n], src.String())
		if err != nil {
			m.log.Debugw("failed to decodeFrame", "src", src, "err", err)
			continue
		}

		switch fr.Typ { //nolint:exhaustive
		case types.MsgTypePing:
			continue
		case types.MsgTypeHandshakeIKInit,
			types.MsgTypeHandshakeIKResp,
			types.MsgTypeHandshakeXXPsk2Init,
			types.MsgTypeHandshakeXXPsk2Resp:
			if err := m.handleHandshake(conn, src.String(), fr); err != nil {
				_ = err
				// if !errors.Is(err, ErrHandshakeIncomplete) {
				// 	m.log.Debugf("failed to handle handshake: %s", err)
				// }
			}
			continue
		case types.MsgTypeUDPPunchCoordRequest:
			m.log.Debugw("received punch request", "src", fr.Src)
			if err := m.handlePunchCoordRequest(ctx, fr); err != nil {
				m.log.Debugf("failed to handle punch coord request: %s", err)
			}
			continue
		case types.MsgTypeUDPPunchCoordResponse:
			m.log.Debugw("received punch trigger", "src", fr.Src)
			// handlePunchCoordTrigger runs EnsurePeer, which blocks waiting for a handshake/up event that
			// is processed on this same loop. Run it async so the loop can keep draining socket events and
			// complete the handshake (AssociatePeerSocket/removeWaiter) that unblocks EnsurePeer.
			// Copy payload before dispatching — buf is reused by the next ReadFromUDP.
			frCopy := fr
			frCopy.Payload = append([]byte(nil), fr.Payload...)
			go func() {
				if err := m.handlePunchCoordTrigger(ctx, frCopy); err != nil {
					m.log.Debugf("failed to handle punch coord trigger: %s", err)
				}
			}()
			continue
		case types.MsgTypeDisconnect:
			m.handleDisconnect(fr)
			continue
		default:
		}

		sess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
		if !ok {
			continue
		}

		pt, shouldRekey, err := sess.Decrypt(fr.Payload)
		if err != nil {
			m.log.Debugw("decrypt failed", "err", err, "msgType", fr.Typ, "src", fr.Src, "receiverID", fr.ReceiverID, "peer", sess.peerKey.Short(), "localSessionID", sess.localSessionID, "peerSessionID", sess.peerSessionID, "payloadBytes", len(fr.Payload))
			continue
		}

		sess.touchRecv()

		if shouldRekey {
			m.rekeyMgr.resetIfExists(sess.peerSessionID, sessionRefreshInterval)
		}

		select {
		case <-ctx.Done():
			return
		case m.recvChan <- Packet{
			Peer:    sess.peerKey,
			Payload: pt,
			Src:     fr.Src,
			Typ:     fr.Typ,
		}:
		}
	}
}

func (m *impl) handlePunchCoordRequest(ctx context.Context, fr Frame) error {
	initSess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
	if !ok {
		// TODO(saml)
		return errors.New("coord req session not found")
	}

	pt, _, err := initSess.Decrypt(fr.Payload)
	if err != nil {
		return fmt.Errorf("decrypt request payload: %w", err)
	}
	initSess.touchRecv()

	req := &peerv1.PunchCoordRequest{}
	if err := req.UnmarshalVT(pt); err != nil {
		return fmt.Errorf("malformed request payload: %w", err)
	}

	recvSess, ok := m.sessionStore.getByPeer(types.PeerKeyFromBytes(req.PeerId))
	if !ok {
		// TODO(saml)
		return nil
	}

	recvPeer := types.PeerKeyFromBytes(req.PeerId)
	initPeer := initSess.peerKey

	return m.coordinateUDPPunch(ctx, initPeer, recvPeer, fr.Src, recvSess.peerAddr.String())
}

func (m *impl) coordinateUDPPunch(ctx context.Context, initiatorPeer, targetPeer types.PeerKey, initiatorAddr, targetAddr string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return m.sendUDPPunchTrigger(ctx, initiatorPeer, targetPeer, initiatorAddr, targetAddr)
	})
	g.Go(func() error {
		return m.sendUDPPunchTrigger(ctx, targetPeer, initiatorPeer, targetAddr, initiatorAddr)
	})
	return g.Wait()
}

func (m *impl) sendUDPPunchTrigger(ctx context.Context, to, peerID types.PeerKey, selfAddr, peerAddr string) error {
	resp := &peerv1.PunchCoordTrigger{
		PeerId:   peerID.Bytes(),
		SelfAddr: selfAddr,
		PeerAddr: peerAddr,
	}

	b, err := resp.MarshalVT()
	if err != nil {
		return err
	}

	return m.Send(ctx, to, types.Envelope{
		Payload: b,
		Type:    types.MsgTypeUDPPunchCoordResponse,
	})
}

func (m *impl) handlePunchCoordTrigger(ctx context.Context, fr Frame) (err error) {
	sess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
	if !ok {
		return errors.New("coord trigger session not found")
	}

	b, _, err := sess.Decrypt(fr.Payload)
	if err != nil {
		return fmt.Errorf("decrypt trigger payload: %w", err)
	}
	sess.touchRecv()

	req := &peerv1.PunchCoordTrigger{}
	if err := req.UnmarshalVT(b); err != nil {
		return fmt.Errorf("malformed trigger payload: %w", err)
	}

	peerID := types.PeerKeyFromBytes(req.PeerId)

	// TODO(saml) this shouldn't be here, ideally a central loop will publish events like this
	defer func() {
		if err != nil {
			m.events <- peer.ConnectFailed{PeerKey: peerID}
		}
	}()

	// trigger search for hole punch
	addr, err := net.ResolveUDPAddr("udp", req.PeerAddr)
	if err != nil {
		m.log.Errorw("Error resolving address", "err", err)
		return err
	}
	addrs := []*net.UDPAddr{addr}

	m.log.Debugw("attempting punch", "peer", peerID.String()[:8], "addrs", addrs)

	ensureCtx, cancel := context.WithTimeout(ctx, punchAttemptDuration)
	defer cancel()

	if err = m.EnsurePeer(ensureCtx, peerID, addrs, true); err != nil {
		m.log.Debugw("ensure peer failed", "peer", peerID.String()[:8], "err", err)
		return err
	}

	return nil
}

func (m *impl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr, withPunch bool) error {
	log := m.log.With("peer", peerKey.Short())

	if _, ok := m.sessionStore.getByPeer(peerKey); ok {
		log.Debug("session already exists")
		return nil
	}

	mu := m.peerLock(peerKey)
	mu.Lock()
	defer mu.Unlock()

	// re-check under lock
	if _, ok := m.sessionStore.getByPeer(peerKey); ok {
		log.Debug("session already exists")
		return nil
	}

	// register waiter *before* sending, so we can't miss the PeerUp
	upCh, exists := m.addWaiter(peerKey)
	if exists {
		log.Debug("handshake already in progress")
		return nil
	}
	defer m.removeWaiter(peerKey)

	res, err := m.handshakeStore.initIK(peerKey.Bytes())
	if err != nil {
		log.Errorw("unable to create IK handshake", "err", err)
		return err
	}

	payload := encodeHandshake(res)

	ctx, cancel := context.WithTimeout(ctx, searchTimeout)
	defer cancel()

	// we can respond to the same success, and guarantee that the signal
	// that needs to route receives it.
	succ := make(chan struct{})
	proxyCh := make(chan route, 1)
	go func() {
		select {
		case <-ctx.Done():
		case r := <-upCh:
			if withPunch {
				proxyCh <- r
			}
			close(succ)
		}
	}()

	// attempt direct dials
	for _, addr := range addrs {
		go func() {
			ticker := time.NewTicker(ensurePeerResendInterval)
			defer ticker.Stop()
			for {
				_ = m.send(m.primary, addr, payload)
				select {
				case <-succ:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}

	// TODO(saml) close early if a direct connection is found via ctx!
	if withPunch {
		if len(addrs) == 0 {
			return errors.New("no addresses available for punch")
		}

		go func() {
			if err := m.fanOut(ctx, addrs[0], payload, proxyCh); err != nil {
				m.log.Error(err)
			}
		}()
	}

	select {
	case <-succ:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *impl) fanOut(ctx context.Context, target *net.UDPAddr, payload []byte, winnerCh chan route) error {
	type fanOutSocket struct {
		conn   *net.UDPConn
		cancel context.CancelFunc
	}

	sockets := make([]fanOutSocket, 0, ephemeralSocketCount)
	for range ephemeralSocketCount {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: nil, Port: 0})
		if err == nil {
			loopCtx, loopCancel := context.WithCancel(m.ctx)
			sockets = append(sockets, fanOutSocket{conn: c, cancel: loopCancel})

			go m.readLoop(loopCtx, c)
		}
	}

	if len(sockets) == 0 {
		return errors.New("failed to create fanout sockets")
	}

	ticker := time.NewTicker(searchTickerInterval)
	defer ticker.Stop()

	tickCount := 0

outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		case <-ticker.C:
			// Easy-side behaviour: keep source port fixed (primary socket) and
			// probe random destination ports on the peer to discover an open mapping.
			{
				var b [2]byte
				if _, err := rand.Read(b[:]); err == nil {
					port := minEphemeralPort + int(binary.BigEndian.Uint16(b[:])%maxEphemeralPort)
					dst := &net.UDPAddr{IP: target.IP, Port: port}
					_, _ = m.primary.WriteToUDP(payload, dst)
				}
			}

			// Hard-side behaviour: vary source port by cycling ephemeral sockets
			// while sending to a fixed destination (the peer's known ip:port) to
			// create many NAT mappings on our side.
			_, _ = sockets[tickCount%len(sockets)].conn.WriteToUDP(payload, target)

			tickCount++
		}
	}

	var winnerConn *net.UDPConn
	select {
	case r := <-winnerCh:
		winnerConn = r.conn
	default:
	}

	for _, s := range sockets {
		if s.conn == winnerConn {
			continue
		}

		s.cancel()
		if err := s.conn.Close(); err != nil {
			m.log.Debugw("failed closing fanout socket", "local", s.conn.LocalAddr(), "err", err)
		}
	}

	return nil
}

func (m *impl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	res, err := m.handshakeStore.initXXPsk2(inv, m.crypto.IdentityPub())
	if err != nil {
		return fmt.Errorf("failed to create XXpsk2 handshake: %w", err)
	}

	// Send the initial packet(s)
	// For Init, receiverID is 0
	g, _ := errgroup.WithContext(ctx)
	for _, addr := range inv.Addr {
		g.Go(func() error {
			return m.sendHandshakeResult(m.primary, addr, res, 0)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to send invite init: %w", err)
	}
	m.log.Debug("join attempt complete")

	return nil
}

func (m *impl) peerLock(p types.PeerKey) *sync.Mutex {
	v, _ := m.peerLocks.LoadOrStore(p, &sync.Mutex{})
	if m, ok := v.(*sync.Mutex); ok {
		return m
	}
	mu := &sync.Mutex{}
	m.peerLocks.Store(p, mu)
	return mu
}

func (m *impl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	sess, ok := m.sessionStore.getByPeer(peerKey)
	if !ok {
		return nil, false
	}

	return sess.peerAddr, true
}

func (m *impl) addWaiter(k types.PeerKey) (chan route, bool) {
	m.waitMu.Lock()
	defer m.waitMu.Unlock()
	if _, ok := m.waitPeer[k]; ok {
		return nil, true
	}
	ch := make(chan route, 1)
	m.waitPeer[k] = ch
	return ch, false
}

func (m *impl) getWaiter(k types.PeerKey) (chan route, bool) {
	m.waitMu.Lock()
	defer m.waitMu.Unlock()
	waiter, ok := m.waitPeer[k]
	return waiter, ok
}

func (m *impl) removeWaiter(k types.PeerKey) {
	m.waitMu.Lock()
	defer m.waitMu.Unlock()
	waiter := m.waitPeer[k]
	if waiter == nil {
		return
	}
	delete(m.waitPeer, k)
	close(waiter)
}

func encodeHandshake(res HandshakeResult) []byte {
	return encodeFrame(&Frame{
		Payload:    res.Msg,
		Typ:        res.MsgType,
		SenderID:   res.LocalSessionID,
		ReceiverID: 0, // For init, receiverID is 0
	})
}

func (m *impl) handleHandshake(conn *net.UDPConn, src string, fr Frame) error {
	hs, err := m.handshakeStore.getOrCreate(fr.SenderID, fr.ReceiverID, fr.Typ)
	if err != nil {
		return err
	}
	if hs == nil {
		m.log.Debugw("handshake is nil", "src", src, "senderID", fr.SenderID, "receiverID", fr.ReceiverID)
		return nil
	}

	res, err := hs.Step(fr.Payload)
	if err != nil {
		return err
	}

	if err := m.sendHandshakeResult(conn, src, res, fr.SenderID); err != nil {
		m.log.Debugw("failed to send handshake reply", "err", err)
	}

	if res.Session == nil {
		return ErrHandshakeIncomplete
	}

	addr, err := net.ResolveUDPAddr("udp", src)
	if err != nil {
		m.log.Errorw("error resolving address", "err", err)
		return err
	}

	res.Session.peerAddr = addr
	res.Session.peerSessionID = fr.SenderID
	res.Session.conn = conn

	m.sessionStore.set(res.Session)
	m.log.Debugw("session created", "src", src, "localSessionID", res.Session.localSessionID, "peerSessionID", res.Session.peerSessionID)

	// Associate peer with the socket that successfully completed the handshake
	peerKey := types.PeerKeyFromBytes(res.PeerStaticKey)

	// Clearing after a grace period gates against old handshakes from the same "batch" overriding
	// successfully completed "quicker" ones (`handleHandshake.getOrCreate` returns handshakes with invalid
	// stages, which are dropped).
	m.handshakeStore.clear(fr.SenderID, handshakeDedupTTL)

	active, ok := m.sessionStore.getByPeer(peerKey)
	if !ok || active != res.Session {
		m.log.Debugw("active session nonexistent or mismatching", "peer", types.PeerKeyFromBytes(res.PeerIdentityPub).String()[:8])
		return nil
	}

	if upCh, ok := m.getWaiter(peerKey); ok {
		select {
		case upCh <- route{conn: conn, remoteAddr: addr}:
		default:
		}
	}

	udpAddr, err := net.ResolveUDPAddr("udp", src)
	if err != nil {
		return err
	}

	m.events <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           udpAddr.IP,
		ObservedPort: udpAddr.Port,
	}

	return nil
}

func (m *impl) handleDisconnect(fr Frame) {
	sess := m.sessionStore.removeByLocalID(fr.ReceiverID)
	if sess == nil {
		return
	}

	if sess.conn != m.primary {
		if err := sess.conn.Close(); err != nil {
			m.log.Debugw("error closing disconnected session conn", "peer", sess.peerKey.Short(), "err", err)
		}
	}

	m.log.Infow("received disconnect from peer", "peer", sess.peerKey.Short())
	m.events <- peer.PeerDisconnected{PeerKey: sess.peerKey}
}

func (m *impl) BroadcastDisconnect() {
	peers := m.sessionStore.getAllPeers()
	for _, peerKey := range peers {
		if err := m.Send(context.Background(), peerKey, types.Envelope{
			Type:    types.MsgTypeDisconnect,
			Payload: nil,
		}); err != nil {
			m.log.Debugw("failed to send disconnect", "peer", peerKey.String()[:8], "err", err)
		}
	}
}

func (m *impl) sendHandshakeResult(conn *net.UDPConn, addr string, res HandshakeResult, remoteID uint32) error {
	if len(res.Msg) == 0 {
		return ErrEmptyMsg
	}

	payload := encodeFrame(&Frame{
		Payload:    res.Msg,
		Typ:        res.MsgType,
		SenderID:   res.LocalSessionID,
		ReceiverID: remoteID,
	})

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		m.log.Errorf("Error resolving address:", err)
		return nil
	}
	_, err = conn.WriteToUDP(payload, udpAddr)
	// if !errors.Is(err, syscall.ENETUNREACH) && !errors.Is(err, syscall.EHOSTUNREACH) {
	if err != nil {
		m.log.Debugf("failed to write to candidate %s: %v", addr, err)
		return err
	}

	return nil
}

func (m *impl) staleSessionChecker(ctx context.Context) {
	ticker := time.NewTicker(staleSessionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale := m.sessionStore.getStaleAndRemove(time.Now())
			for _, sess := range stale {
				if sess.conn != m.primary {
					if err := sess.conn.Close(); err != nil {
						m.log.Infow("error closing stale connection", "peer", sess.peerKey.String()[:8])
					}
				}
				m.log.Infow("session timed out", "peer", sess.peerKey.String()[:8])
				m.events <- peer.PeerDisconnected{PeerKey: sess.peerKey}
			}
		}
	}
}
