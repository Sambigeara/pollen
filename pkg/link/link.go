package link

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ Link = (*impl)(nil)

const (
	defaultSessionRefreshInterval    = 120 * time.Second // new IK handshake every 2 mins
	defaultHandshakeDedupTTL         = 5 * time.Second
	defaultEnsurePeerTimeout         = 4 * time.Second
	defaultEnsurePeerInterval        = 200 * time.Millisecond
	defaultHolepunchAttempts         = 10
	defaultStaleSessionCheckInterval = 15 * time.Second // how often to check for stale sessions
)

type Option func(*config)

type config struct {
	sessionRefreshInterval    time.Duration
	handshakeDedupTTL         time.Duration
	ensurePeerTimeout         time.Duration
	ensurePeerInterval        time.Duration
	holepunchAttempts         int
	staleSessionCheckInterval time.Duration
}

func defaultConfig() config {
	return config{
		sessionRefreshInterval:    defaultSessionRefreshInterval,
		handshakeDedupTTL:         defaultHandshakeDedupTTL,
		ensurePeerTimeout:         defaultEnsurePeerTimeout,
		ensurePeerInterval:        defaultEnsurePeerInterval,
		holepunchAttempts:         defaultHolepunchAttempts,
		staleSessionCheckInterval: defaultStaleSessionCheckInterval,
	}
}

func WithEnsurePeerTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.ensurePeerTimeout = d
		}
	}
}

func WithEnsurePeerInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.ensurePeerInterval = d
		}
	}
}

func WithHolepunchAttempts(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.holepunchAttempts = n
		}
	}
}

func WithStaleSessionCheckInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.staleSessionCheckInterval = d
		}
	}
}

func WithSessionRefreshInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.sessionRefreshInterval = d
		}
	}
}

func WithHandshakeDedupTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.handshakeDedupTTL = d
		}
	}
}

var ErrNoSession = errors.New("no session for peer")

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type Link interface {
	Start(ctx context.Context) error
	Close() error

	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error                               // XXpsk2 init
	EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string, nAttempts int) error // IK init
	Events() <-chan peer.Input

	Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error
	Handle(t types.MsgType, h HandlerFn) // dispatch after decrypt

	GetActivePeerAddress(peerKey types.PeerKey) (string, bool)
	BroadcastDisconnect() // notify all connected peers we're shutting down
}

type LocalCrypto interface {
	NoisePub() []byte
	IdentityPub() []byte // ed25519 pub
	// SignIdentity(msg []byte) []byte // ed25519 sign
}

type impl struct {
	transport      transport.Transport
	crypto         LocalCrypto
	handlers       map[types.MsgType]HandlerFn
	handshakeStore *handshakeStore
	sessionStore   *sessionStore
	rekeyMgr       *rekeyManager
	log            *zap.SugaredLogger
	events         chan peer.Input
	cancel         context.CancelFunc
	pingMgrs       map[string]*pingMgr
	waitPeer       map[types.PeerKey]chan struct{}
	peerLocks      sync.Map
	handlersMu     sync.RWMutex
	pingMu         sync.RWMutex
	waitMu         sync.Mutex
	cfg            config
}

const eventBufSize = 64

func NewLink(port int, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission, opts ...Option) (Link, error) {
	tr, err := transport.NewTransport(port)
	if err != nil {
		return nil, err
	}

	return NewLinkWithTransport(tr, cs, staticKey, crypto, admission, opts...)
}

func NewLinkWithTransport(tr transport.Transport, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission, opts ...Option) (Link, error) {
	if tr == nil {
		return nil, errors.New("transport is nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &impl{
		log:            zap.S().Named("mesh"),
		crypto:         crypto,
		transport:      tr,
		handshakeStore: newHandshakeStore(cs, admission, staticKey, crypto.IdentityPub()),
		sessionStore:   newSessionStore(crypto.NoisePub()),
		rekeyMgr:       newRekeyManager(),
		handlers:       make(map[types.MsgType]HandlerFn),
		events:         make(chan peer.Input, eventBufSize),
		pingMgrs:       make(map[string]*pingMgr),
		waitPeer:       make(map[types.PeerKey]chan struct{}),
		cfg:            cfg,
	}, nil
}

func (i *impl) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel

	go i.loop(ctx)
	go i.staleSessionChecker(ctx)
	return nil
}

func (i *impl) loop(ctx context.Context) {
	for {
		src, b, err := i.transport.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			i.log.Debugw("recv failed", "err", err)
			continue
		}

		fr, err := decodeFrame(b)
		if err != nil {
			i.log.Debugw("bad frame", "src", src, "err", err)
			continue
		}

		switch fr.typ { //nolint:exhaustive
		case types.MsgTypePing:
		case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Init, types.MsgTypeHandshakeXXPsk2Resp:
			if err := i.handleHandshake(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle handshake: %s", err)
			}
		case types.MsgTypeTransportData, types.MsgTypeTCPPunchRequest, types.MsgTypeTCPPunchTrigger,
			types.MsgTypeTCPPunchReady, types.MsgTypeTCPPunchResponse, types.MsgTypeGossip, types.MsgTypeTest,
			types.MsgTypeSessionRequest, types.MsgTypeSessionResponse:
			i.handleApp(ctx, fr)
		case types.MsgTypeUDPPunchCoordRequest:
			i.log.Debugw("received punch request", "src", src)
			if err := i.handlePunchCoordRequest(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle punch coord request: %s", err)
			}
		case types.MsgTypeUDPPunchCoordResponse:
			i.log.Debugw("received punch trigger", "src", src)
			if err := i.handlePunchCoordTrigger(ctx, fr); err != nil {
				i.log.Debugf("failed to handle punch coord trigger: %s", err)
			}
		case types.MsgTypeDisconnect:
			i.handleDisconnect(fr)
		}
	}
}

func (i *impl) handlePunchCoordRequest(ctx context.Context, src string, fr frame) error {
	initSess, ok := i.sessionStore.getByLocalID(fr.receiverID)
	if !ok {
		// TODO(saml)
		return errors.New("coord req session not found")
	}

	pt, _, err := initSess.Decrypt(fr.payload)
	if err != nil {
		return fmt.Errorf("decrypt request payload: %w", err)
	}

	req := &peerv1.PunchCoordRequest{}
	if err := req.UnmarshalVT(pt); err != nil {
		return fmt.Errorf("malformed request payload: %w", err)
	}

	recvSess, ok := i.sessionStore.getByPeer(types.PeerKeyFromBytes(req.PeerId))
	if !ok {
		// TODO(saml)
		return nil
	}

	recvPeer := types.PeerKeyFromBytes(req.PeerId)
	initPeer := types.PeerKeyFromBytes(initSess.peerNoiseKey)

	initiatorAddrs := []string{src}
	if req.GetLocalPort() > 0 {
		initiatorAddrWithLocalPort, err := net.ResolveUDPAddr("udp", src)
		if err == nil {
			initiatorAddrWithLocalPort.Port = int(req.GetLocalPort())
			if addr := initiatorAddrWithLocalPort.String(); addr != src {
				initiatorAddrs = append(initiatorAddrs, addr)
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:    req.PeerId,
			SelfAddr:  src,
			PeerAddrs: []string{recvSess.peerAddr},
		}

		b, err := resp.MarshalVT()
		if err != nil {
			return err
		}

		return i.Send(ctx, initPeer, types.Envelope{
			Payload: b,
			Type:    types.MsgTypeUDPPunchCoordResponse,
		})
	})

	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:    initSess.peerNoiseKey,
			SelfAddr:  recvSess.peerAddr,
			PeerAddrs: initiatorAddrs,
		}

		b, err := resp.MarshalVT()
		if err != nil {
			return err
		}

		return i.Send(ctx, recvPeer, types.Envelope{
			Payload: b,
			Type:    types.MsgTypeUDPPunchCoordResponse,
		})
	})
	return g.Wait()
}

func (i *impl) handlePunchCoordTrigger(ctx context.Context, fr frame) error {
	sess, ok := i.sessionStore.getByLocalID(fr.receiverID)
	if !ok {
		return errors.New("coord trigger session not found")
	}

	b, _, err := sess.Decrypt(fr.payload)
	if err != nil {
		return fmt.Errorf("decrypt trigger payload: %w", err)
	}

	req := &peerv1.PunchCoordTrigger{}
	if err := req.UnmarshalVT(b); err != nil {
		return fmt.Errorf("malformed trigger payload: %w", err)
	}

	ensureCtx, cancel := context.WithTimeout(ctx, i.cfg.ensurePeerTimeout)
	defer cancel()

	peerID := types.PeerKeyFromBytes(req.PeerId)
	if err := i.EnsurePeer(ensureCtx, peerID, req.PeerAddrs, i.cfg.holepunchAttempts); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		i.log.Debugw("ensure peer failed", "peer", peerID.String()[:8], "err", err)
		return err
	}

	return nil
}

func (i *impl) handleHandshake(ctx context.Context, src string, fr frame) error {
	hs, err := i.handshakeStore.getOrCreate(fr.senderID, fr.receiverID, fr.typ)
	if err != nil {
		return err
	}
	if hs == nil {
		return nil
	}

	res, err := hs.Step(fr.payload)
	if err != nil {
		return err
	}

	if err := i.sendHandshakeResult(src, res, fr.senderID); err != nil {
		i.log.Debugw("failed to send handshake reply", "err", err)
	}

	if res.Session != nil {
		res.Session.peerAddr = src
		res.Session.peerSessionID = fr.senderID

		i.sessionStore.set(res.Session)

		// Clearing after a grace period gates against old handshakes from the same "batch" overriding
		// successfully completed "quicker" ones (`handleHandshake.getOrCreate` returns handshakes with invalid
		// stages, which are dropped).
		// TODO(saml) need to sync up the various durations/grace periods. This timeout needs to be less than the
		// handshake request TTL.
		// TODO(saml) probably need to clear on hard errors too
		i.handshakeStore.clear(fr.senderID, i.cfg.handshakeDedupTTL)

		i.removeWaiter(types.PeerKeyFromBytes(res.PeerStaticKey))
		// start only when we trust the peer
		i.bumpPinger(ctx, res.Session.peerAddr)

		select {
		case i.events <- peer.ConnectPeer{
			PeerKey:     types.PeerKeyFromBytes(res.PeerStaticKey),
			Addr:        res.Session.peerAddr,
			IdentityPub: res.PeerIdentityPub,
		}:
		default:
		}
	}

	return nil
}

func (i *impl) handleApp(ctx context.Context, fr frame) {
	sess, ok := i.sessionStore.getByLocalID(fr.receiverID)
	if !ok {
		i.log.Debugw("handleApp: session not found",
			"receiverID", fr.receiverID,
			"msgType", fr.typ)
		return
	}

	pt, shouldRekey, err := sess.Decrypt(fr.payload)
	if err != nil {
		peerKey := types.PeerKeyFromBytes(sess.peerNoiseKey)
		i.log.Debugw("handleApp: decrypt failed",
			"peer", peerKey.String()[:8],
			"msgType", fr.typ,
			"err", err)
		return
	}

	sess.touchRecv()
	i.bumpPinger(ctx, sess.peerAddr)

	if shouldRekey {
		i.rekeyMgr.resetIfExists(sess.peerSessionID, i.cfg.sessionRefreshInterval)
	}

	i.handlersMu.RLock()
	h := i.handlers[fr.typ]
	i.handlersMu.RUnlock()
	if h != nil {
		if err := h(ctx, types.PeerKeyFromBytes(sess.peerNoiseKey), pt); err != nil {
			i.log.Debugw("handler error", "err", err)
		}
	}
}

func (i *impl) handleDisconnect(fr frame) {
	sess, ok := i.sessionStore.getByLocalID(fr.receiverID)
	if !ok {
		return
	}

	peerKey := types.PeerKeyFromBytes(sess.peerNoiseKey)
	i.log.Infow("received disconnect from peer", "peer", peerKey.String()[:8])

	i.sessionStore.removeByPeerKey(peerKey)

	// Emit PeerDisconnected to state machine
	select {
	case i.events <- peer.PeerDisconnected{PeerKey: peerKey}:
	default:
	}
}

func (i *impl) staleSessionChecker(ctx context.Context) {
	ticker := time.NewTicker(i.cfg.staleSessionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale := i.sessionStore.getStaleAndRemove(time.Now())
			for _, peerKey := range stale {
				i.log.Infow("session timed out", "peer", peerKey.String()[:8])
				select {
				case i.events <- peer.PeerDisconnected{PeerKey: peerKey}:
				default:
				}
			}
		}
	}
}

func (i *impl) BroadcastDisconnect() {
	peers := i.sessionStore.getAllPeers()
	for _, peerKey := range peers {
		if err := i.Send(context.Background(), peerKey, types.Envelope{
			Type:    types.MsgTypeDisconnect,
			Payload: nil,
		}); err != nil {
			i.log.Debugw("failed to send disconnect", "peer", peerKey.String()[:8], "err", err)
		}
	}
}

func (i *impl) Close() error {
	i.cancel()
	return i.transport.Close()
}

func (i *impl) Events() <-chan peer.Input { return i.events }

func (i *impl) Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	sess, ok := i.sessionStore.getByPeer(peerKey)
	if !ok {
		i.log.Errorf("%v: %s", ErrNoSession, peerKey.String())
		return fmt.Errorf("%w: %s", ErrNoSession, peerKey.String())
	}

	ct, shouldRekey, err := sess.Encrypt(msg.Payload)
	if err != nil {
		i.log.Errorf("encrypt: %v", err)
		return fmt.Errorf("encrypt: %w", err)
	}

	fr := &frame{
		payload:    ct,
		typ:        msg.Type,
		senderID:   sess.localSessionID,
		receiverID: sess.peerSessionID,
	}

	if err := i.transport.Send(sess.peerAddr, encodeFrame(fr)); err != nil {
		i.log.Errorf("send: %v", err)
		return fmt.Errorf("send: %w", err)
	}

	i.bumpPinger(ctx, sess.peerAddr)

	if shouldRekey {
		i.rekeyMgr.resetIfExists(sess.peerSessionID, i.cfg.sessionRefreshInterval)
	}

	return nil
}

func (i *impl) Handle(t types.MsgType, h HandlerFn) {
	i.handlersMu.Lock()
	defer i.handlersMu.Unlock()
	i.handlers[t] = h
}

func (i *impl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	res, err := i.handshakeStore.initXXPsk2(inv, i.crypto.IdentityPub())
	if err != nil {
		return fmt.Errorf("failed to create XXpsk2 handshake: %w", err)
	}

	// Send the initial packet(s)
	// For Init, receiverID is 0
	g, _ := errgroup.WithContext(ctx)
	for _, addr := range inv.Addr {
		g.Go(func() error {
			return i.sendHandshakeResult(addr, res, 0)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to send invite init: %w", err)
	}
	i.log.Debug("join attempt complete")

	return nil
}

func (i *impl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string, nAttempts int) error {
	if len(addrs) == 0 {
		return errors.New("no addresses provided")
	}

	if _, ok := i.sessionStore.getByPeer(peerKey); ok {
		return nil
	}

	mu := i.peerLock(peerKey)
	mu.Lock()
	defer mu.Unlock()

	// re-check under lock
	if _, ok := i.sessionStore.getByPeer(peerKey); ok {
		return nil
	}

	// register waiter *before* sending, so we can't miss the PeerUp
	upCh, exists := i.addWaiter(peerKey)
	if exists {
		i.log.Debugf("waiter already pending for peer: %s", peerKey.String()[:8])
		return nil
	}
	defer i.removeWaiter(peerKey)

	res, err := i.handshakeStore.initIK(peerKey.Bytes())
	if err != nil {
		return err
	}

	// Happy Eyeballs
	sendToAll := func() {
		for _, addr := range addrs {
			go func(target string) {
				// Quick check to see if we should abandon sending
				if ctx.Err() != nil {
					return
				}
				if err := i.sendHandshakeResult(target, res, 0); err != nil {
					i.log.Debugw("handshake send failed", "addr", target, "err", err)
				}
			}(addr)
		}
	}

	ticker := time.NewTicker(i.cfg.ensurePeerInterval)
	defer ticker.Stop()

	for attempt := 1; attempt <= nAttempts; attempt++ {
		sendToAll()
		select {
		case <-upCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			continue
		}
	}

	return fmt.Errorf("timed out waiting for peer %s after %d attempts", peerKey.String()[:8], nAttempts)
}

func (i *impl) peerLock(p types.PeerKey) *sync.Mutex {
	v, _ := i.peerLocks.LoadOrStore(p, &sync.Mutex{})
	if m, ok := v.(*sync.Mutex); ok {
		return m
	}
	m := &sync.Mutex{}
	i.peerLocks.Store(p, m)
	return m
}

func (i *impl) GetActivePeerAddress(peerKey types.PeerKey) (string, bool) {
	sess, ok := i.sessionStore.getByPeer(peerKey)
	if !ok {
		return "", false
	}

	return sess.peerAddr, true
}

func (i *impl) addWaiter(k types.PeerKey) (chan struct{}, bool) {
	i.waitMu.Lock()
	defer i.waitMu.Unlock()
	if _, ok := i.waitPeer[k]; ok {
		return nil, true
	}
	ch := make(chan struct{})
	i.waitPeer[k] = ch
	return ch, false
}

func (i *impl) removeWaiter(k types.PeerKey) {
	i.waitMu.Lock()
	defer i.waitMu.Unlock()
	waiter := i.waitPeer[k]
	if waiter == nil {
		return
	}
	delete(i.waitPeer, k)
	close(waiter)
}

func (i *impl) sendHandshakeResult(addr string, res HandshakeResult, remoteID uint32) error {
	if len(res.Msg) == 0 {
		return ErrEmptyMsg
	}

	if err := i.transport.Send(addr, encodeFrame(&frame{
		payload:    res.Msg,
		typ:        res.MsgType,
		senderID:   res.LocalSessionID,
		receiverID: remoteID,
	})); err != nil {
		i.log.Debugf("failed to write to candidate %s: %v", addr, err)
		return err
	}

	return nil
}
