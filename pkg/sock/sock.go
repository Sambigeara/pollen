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

var _ Socket = (*megaSock)(nil)

var ErrHandshakeIncomplete = errors.New("handshake incomplete")
var ErrNoSession = errors.New("no session for peer")

const (
	udpReadBufferSize = 64 * 1024

	// Resource limits
	ephemeralSocketCount      = 256
	searchTickerInterval      = 10 * time.Millisecond
	searchTimeout             = 3 * time.Second
	handshakeDedupTTL         = 5 * time.Second
	sessionRefreshInterval    = 120 * time.Second // new IK handshake every 2 mins
	punchAttemptDuration      = 5 * time.Second
	ensurePeerResendInterval  = 100 * time.Millisecond
	staleSessionCheckInterval = 30 * time.Second
)

type Packet struct {
	Peer    types.PeerKey
	Payload []byte
	Src     string
	Typ     types.MsgType
}

type Socket interface {
	Recv() (Packet, error)
	Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error
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

type pendingProbe struct {
	expectedIP net.IP
	result     chan route
}

type megaSock struct {
	log *zap.SugaredLogger

	primary *net.UDPConn

	// address -> working route
	sessions map[string]route
	sessMu   sync.RWMutex

	// nonce -> probe context
	pending map[uint64]pendingProbe
	pendMu  sync.Mutex

	recvChan    chan Packet
	closeCtx    context.Context
	closeCancel context.CancelFunc

	crypto         LocalCrypto
	handshakeStore *handshakeStore
	sessionStore   *sessionStore
	rekeyMgr       *rekeyManager
	peerLocks      sync.Map
	waitMu         sync.Mutex
	waitPeer       map[types.PeerKey]chan route
	events         chan peer.Input
}

func NewTransport(port int, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission, inputCh chan peer.Input) (Socket, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, fmt.Errorf("failed to listen UDP: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ms := &megaSock{
		log:            zap.S().Named("magicsock"),
		primary:        conn,
		sessions:       make(map[string]route),
		pending:        make(map[uint64]pendingProbe),
		recvChan:       make(chan Packet, 1024),
		crypto:         crypto,
		handshakeStore: newHandshakeStore(cs, admission, staticKey, crypto.IdentityPub()),
		sessionStore:   newSessionStore(crypto.NoisePub()),
		rekeyMgr:       newRekeyManager(),
		waitPeer:       make(map[types.PeerKey]chan route),
		events:         inputCh,
		closeCtx:       ctx,
		closeCancel:    cancel,
	}

	go ms.readLoop(conn)
	go ms.staleSessionChecker(ctx)
	return ms, nil
}

func (m *megaSock) Recv() (Packet, error) {
	select {
	case p := <-m.recvChan:
		return p, nil
	case <-m.closeCtx.Done():
		return Packet{}, fmt.Errorf("transport closed")
	}
}

func (m *megaSock) Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	log := m.log.Named("link.Send").With("peerKey", peerKey.Short(), "msg.Type", msg.Type)

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

	if err := m.send(ctx, sess.peerAddrValue(), encodeFrame(fr)); err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(sess.peerSessionID, sessionRefreshInterval)
	}

	return nil
}

func (m *megaSock) send(ctx context.Context, addr *net.UDPAddr, payload []byte) error {
	m.sessMu.RLock()
	r, ok := m.sessions[addr.String()]
	m.sessMu.RUnlock()
	if ok {
		_, err := r.conn.WriteToUDP(payload, r.remoteAddr)
		return err
	}

	// Try send on primary (same network, or port-preserving remote)
	// TODO(saml) can probably make smart decisions based on locality (if a commonly known local IP address)
	if _, err := m.primary.WriteToUDP(payload, addr); err != nil {
		return err
	}

	return nil
}

func (m *megaSock) Close() error {
	m.closeCancel()

	err := m.primary.Close()

	m.sessMu.Lock()
	defer m.sessMu.Unlock()
	for _, r := range m.sessions {
		if r.conn != m.primary {
			r.conn.Close()
		}
	}
	// Clear maps to aid GC
	m.sessions = make(map[string]route)

	return err
}

func (m *megaSock) readLoop(conn *net.UDPConn) {
	buf := make([]byte, udpReadBufferSize)
	for {
		select {
		case <-m.closeCtx.Done():
			return
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return
		}
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		fr, err := decodeFrame(buf[:n], src.String())
		if err != nil {
			continue
		}

		ctx := context.Background() // TODO(saml) pass up
		switch fr.Typ {             //nolint:exhaustive
		case types.MsgTypePing:
			continue
		case types.MsgTypeHandshakeIKInit,
			types.MsgTypeHandshakeIKResp,
			types.MsgTypeHandshakeXXPsk2Init,
			types.MsgTypeHandshakeXXPsk2Resp:
			if err := m.handleHandshake(ctx, conn, src.String(), fr); err != nil {
				if errors.Is(err, ErrHandshakeIncomplete) {
					continue
				}
				m.log.Debugf("failed to handle handshake: %s", err)
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
			sess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
			if !ok {
				m.log.Debug("coord trigger session not found")
			}
			b, _, err := sess.Decrypt(fr.Payload)
			if err != nil {
				m.log.Debugf("decrypt trigger payload: %w", err)
			}
			req := &peerv1.PunchCoordTrigger{}
			if err := req.UnmarshalVT(b); err != nil {
				m.log.Errorf("malformed trigger payload: %w", err)
			}
			// TODO(saml) this `ConnectingPeer` event I added during other work might not even be necessary
			//
			// host, portStr, err := net.SplitHostPort(src)
			// if err != nil {
			// 	i.log.Errorf("failed to split src: %w", err)
			// }
			// port, err := strconv.Atoi(portStr)
			// if err != nil {
			// 	i.log.Errorf("failed to get port: %w", err)
			// }
			// i.events <- peer.ConnectingPeer{
			// 	Ip:           host,
			// 	ObservedPort: port,
			// 	PeerKey:      types.PeerKeyFromBytes(req.PeerId),
			// 	IsPunch:      true,
			// }
			go func() {
				if err := m.handlePunchCoordTrigger(ctx, req); err != nil {
					m.log.Debugf("failed to handle punch coord trigger: %s", err)
				}
			}()
			continue
		case types.MsgTypeDisconnect:
			m.handleDisconnect(fr)
			continue
		default:
		}

		// TODO(saml) there's a chance that this causes problems if a peer previously resided in the same network and is now offline.
		// When we randomly open sockets for future punches, then other nodes outside of the network might land on those sockets.
		// I think here in particular is problematic, as we're establishing connections for any inbound message
		r := route{conn: conn, remoteAddr: src}
		m.sessMu.Lock()
		if _, ok := m.sessions[src.String()]; !ok {
			m.sessions[src.String()] = r
		}
		m.sessMu.Unlock()

		sess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
		if !ok {
			m.log.Debugw("session not found", "recieverID", fr.ReceiverID)
			continue
		}
		peerKey := types.PeerKeyFromBytes(sess.peerNoiseKey)

		pt, shouldRekey, err := sess.Decrypt(fr.Payload)
		if err != nil {
			if errors.Is(err, ErrReplay) || errors.Is(err, ErrTooOld) || errors.Is(err, ErrShortCiphertext) {
				m.log.Debugf("nonce error: %w", err)
				continue
			}
			m.log.Debugw("decrypt failed", "err", err, "src", "")
			continue
		}

		sess.touchRecv()
		if fr.Src != "" {
			addr, err := net.ResolveUDPAddr("udp", fr.Src)
			if err != nil {
				m.log.Errorw("error resolving address", "err", err)
			}
			sess.setPeerAddr(addr)
		}

		if shouldRekey {
			m.rekeyMgr.resetIfExists(sess.peerSessionID, sessionRefreshInterval)
		}

		select {
		case m.recvChan <- Packet{
			Peer:    peerKey,
			Payload: pt,
			Src:     fr.Src,
			Typ:     fr.Typ,
		}:
		case <-m.closeCtx.Done():
			return
		}
	}
}

func (m *megaSock) handlePunchCoordRequest(ctx context.Context, fr Frame) error {
	initSess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
	if !ok {
		// TODO(saml)
		return errors.New("coord req session not found")
	}

	pt, _, err := initSess.Decrypt(fr.Payload)
	if err != nil {
		return fmt.Errorf("decrypt request payload: %w", err)
	}

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
	initPeer := types.PeerKeyFromBytes(initSess.peerNoiseKey)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:   req.PeerId,
			SelfAddr: fr.Src,
			PeerAddr: recvSess.peerAddrValue().String(),
		}

		b, err := resp.MarshalVT()
		if err != nil {
			return err
		}

		return m.Send(ctx, initPeer, types.Envelope{
			Payload: b,
			Type:    types.MsgTypeUDPPunchCoordResponse,
		})
	})

	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:   initSess.peerNoiseKey,
			SelfAddr: recvSess.peerAddrValue().String(),
			PeerAddr: fr.Src,
		}

		b, err := resp.MarshalVT()
		if err != nil {
			return err
		}

		return m.Send(ctx, recvPeer, types.Envelope{
			Payload: b,
			Type:    types.MsgTypeUDPPunchCoordResponse,
		})
	})
	return g.Wait()
}

func (m *megaSock) handlePunchCoordTrigger(ctx context.Context, req *peerv1.PunchCoordTrigger) (err error) {
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

func (m *megaSock) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr, withPunch bool) error {
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
		log.Debug("session already exists")
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

	// attempt direct dials
	for _, addr := range addrs {
		go func() {
			ticker := time.NewTicker(ensurePeerResendInterval)
			defer ticker.Stop()
			for {
				m.send(ctx, addr, payload)
				select {
				case <-upCh:
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
		go func() {
			if err := m.fanOut(ctx, addrs[0], payload); err != nil {
				m.log.Error(err)
			}
		}()
	}

	select {
	case <-upCh:
		m.log.Debugw("received direct up event", "peer", peerKey.String()[:8])
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *megaSock) fanOut(ctx context.Context, target *net.UDPAddr, payload []byte) error {
	sockets := make([]*net.UDPConn, 0, ephemeralSocketCount)
	for range ephemeralSocketCount {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: nil, Port: 0})
		if err == nil {
			sockets = append(sockets, c)
			go m.readLoop(c)
		}
	}

	ticker := time.NewTicker(searchTickerInterval)
	defer ticker.Stop()

	tickCount := 0
search:
	for {
		select {
		case <-ctx.Done():
			break search
		case <-ticker.C:
			// Easy-side behaviour: keep source port fixed (primary socket) and
			// probe random destination ports on the peer to discover an open mapping.
			{
				var b [2]byte
				if _, err := rand.Read(b[:]); err == nil {
					port := 1024 + int(binary.BigEndian.Uint16(b[:])%64511)
					dst := &net.UDPAddr{IP: target.IP, Port: port}
					m.primary.WriteToUDP(payload, dst)
				}
			}

			// Hard-side behaviour: vary source port by cycling ephemeral sockets
			// while sending to a fixed destination (the peer’s known ip:port) to
			// create many NAT mappings on our side.
			sockets[tickCount%len(sockets)].WriteToUDP(payload, target)

			tickCount++
		}
	}

	for _, c := range sockets {
		keep := false
		m.sessMu.RLock()
		for _, r := range m.sessions {
			if r.conn == c {
				keep = true
				break
			}
		}
		m.sessMu.RUnlock()
		if !keep {
			c.Close()
		}
	}

	return nil
}

func (m *megaSock) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
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

func (m *megaSock) peerLock(p types.PeerKey) *sync.Mutex {
	v, _ := m.peerLocks.LoadOrStore(p, &sync.Mutex{})
	if m, ok := v.(*sync.Mutex); ok {
		return m
	}
	mu := &sync.Mutex{}
	m.peerLocks.Store(p, mu)
	return mu
}

func (m *megaSock) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	sess, ok := m.sessionStore.getByPeer(peerKey)
	if !ok {
		return nil, false
	}

	return sess.peerAddrValue(), true
}

func (m *megaSock) addWaiter(k types.PeerKey) (chan route, bool) {
	m.waitMu.Lock()
	defer m.waitMu.Unlock()
	if _, ok := m.waitPeer[k]; ok {
		return nil, true
	}
	ch := make(chan route)
	m.waitPeer[k] = ch
	return ch, false
}

func (m *megaSock) getWaiter(k types.PeerKey) (chan route, bool) {
	m.waitMu.Lock()
	defer m.waitMu.Unlock()
	waiter, ok := m.waitPeer[k]
	return waiter, ok
}

func (m *megaSock) removeWaiter(k types.PeerKey) {
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

func (m *megaSock) handleHandshake(ctx context.Context, conn *net.UDPConn, src string, fr Frame) error {
	m.log.Debugw("handleHandshake", "src", src, "senderID", fr.SenderID, "receiverID", fr.ReceiverID)
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
		m.log.Debugw("step failed", "src", src, "senderID", fr.SenderID, "receiverID", fr.ReceiverID)
		return err
	}

	if err := m.sendHandshakeResult(conn, src, res, fr.SenderID); err != nil {
		m.log.Debugw("failed to send handshake reply", "err", err)
	}
	m.log.Debugw("sent handshake reply", "src", src)

	if res.Session == nil {
		return ErrHandshakeIncomplete
	}

	addr, err := net.ResolveUDPAddr("udp", src)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return err
	}

	res.Session.setPeerAddr(addr)
	res.Session.peerSessionID = fr.SenderID

	m.sessionStore.set(res.Session)
	m.log.Debugw("session created", "src", src, "localSessionID", res.Session.localSessionID, "peerSessionID", res.Session.peerSessionID)

	m.sessMu.Lock()
	m.sessions[addr.String()] = route{conn: conn, remoteAddr: addr}
	m.sessMu.Unlock()

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
		upCh <- route{
			conn:       conn,
			remoteAddr: addr,
		}
	}
	m.removeWaiter(peerKey)

	udpAddr, err := net.ResolveUDPAddr("udp", src)
	if err != nil {
		return err
	}

	m.events <- peer.ConnectPeer{
		PeerKey:      peerKey,
		Ip:           udpAddr.IP,
		ObservedPort: udpAddr.Port,
	}

	return nil
}

func (m *megaSock) handleDisconnect(fr Frame) {
	sess, ok := m.sessionStore.getByLocalID(fr.ReceiverID)
	if !ok {
		return
	}

	peerKey := types.PeerKeyFromBytes(sess.peerNoiseKey)
	m.log.Infow("received disconnect from peer", "peer", peerKey.String()[:8])

	m.sessionStore.removeByPeerKey(peerKey)

	// Emit PeerDisconnected to state machine
	select {
	case m.events <- peer.PeerDisconnected{PeerKey: peerKey}:
	default:
	}
}

func (m *megaSock) BroadcastDisconnect() {
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

func (m *megaSock) sendHandshakeResult(conn *net.UDPConn, addr string, res HandshakeResult, remoteID uint32) error {
	if len(res.Msg) == 0 {
		m.log.Debug("empty message")
		return ErrEmptyMsg
	}

	payload := encodeFrame(&Frame{
		Payload:    res.Msg,
		Typ:        res.MsgType,
		SenderID:   res.LocalSessionID,
		ReceiverID: remoteID,
	})

	// m.sessMu.RLock()
	// r, ok := m.sessions[addr]
	// m.sessMu.RUnlock()
	m.log.Debugw("retrieved session lock", "addr", addr)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		m.log.Errorf("Error resolving address:", err)
		return nil
	}
	m.log.Debugw("WriteToUDP", "addr", addr)
	_, err = conn.WriteToUDP(payload, udpAddr)
	// if !errors.Is(err, syscall.ENETUNREACH) && !errors.Is(err, syscall.EHOSTUNREACH) {
	if err != nil {
		m.log.Debugf("failed to write to candidate %s: %v", addr, err)
		return err
	}

	return nil
}

func (m *megaSock) staleSessionChecker(ctx context.Context) {
	ticker := time.NewTicker(staleSessionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale := m.sessionStore.getStaleAndRemove(time.Now())
			for _, peerKey := range stale {
				m.log.Infow("session timed out", "peer", peerKey.String()[:8])
				select {
				case m.events <- peer.PeerDisconnected{PeerKey: peerKey}:
				default:
				}
			}
		}
	}
}
