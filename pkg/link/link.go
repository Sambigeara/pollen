package link

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
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

var _ Link = (*impl)(nil)

const (
	defaultSessionRefreshInterval    = 120 * time.Second // new IK handshake every 2 mins
	defaultHandshakeDedupTTL         = 5 * time.Second
	defaultEnsurePeerTimeout         = 4 * time.Second
	defaultEnsurePeerInterval        = 200 * time.Millisecond
	defaultHolepunchAttempts         = 10
	defaultStaleSessionCheckInterval = 15 * time.Second // how often to check for stale sessions
	defaultBirthdayPunchSocketCount  = 256
)

type Option func(*config)

type config struct {
	sessionRefreshInterval    time.Duration
	handshakeDedupTTL         time.Duration
	ensurePeerTimeout         time.Duration
	ensurePeerInterval        time.Duration
	holepunchAttempts         int
	staleSessionCheckInterval time.Duration
	birthdayPunchSocketCount  int
}

func defaultConfig() config {
	return config{
		sessionRefreshInterval:    defaultSessionRefreshInterval,
		handshakeDedupTTL:         defaultHandshakeDedupTTL,
		ensurePeerTimeout:         defaultEnsurePeerTimeout,
		ensurePeerInterval:        defaultEnsurePeerInterval,
		holepunchAttempts:         defaultHolepunchAttempts,
		staleSessionCheckInterval: defaultStaleSessionCheckInterval,
		birthdayPunchSocketCount:  defaultBirthdayPunchSocketCount,
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

func WithBirthdayPunchSocketCount(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.birthdayPunchSocketCount = n
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

var networkRestrictionWarningOnce sync.Once

func logNetworkRestrictionWarning(log *zap.SugaredLogger, err error) {
	if strings.Contains(err.Error(), "no route to host") || strings.Contains(err.Error(), "operation not permitted") {
		networkRestrictionWarningOnce.Do(func() {
			log.Warnw("network access may be restricted by OS security policy",
				"hint", "on macOS, try running with sudo or signing the binary")
		})
	}
}

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type EnsurePeerOpts struct {
	SendInterval time.Duration
	Rounds       int

	// BirthdayProbe enables birthday punch mode when set.
	// In this mode:
	//   - 'sockets' parameter contains ephemeral sockets (role 1: send to known address)
	//   - BirthdayProbe configures the base socket probing (role 2: send to random ports)
	BirthdayProbe *BirthdayProbeOpts
}

type BirthdayProbeOpts struct {
	ProbeSocket Socket
	ProbeHost   string
	ProbeCount  int
}

type Link interface {
	Start(ctx context.Context) error
	Close() error

	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error                                                       // XXpsk2 init
	EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string, sockets []Socket, opts EnsurePeerOpts) error // IK init
	Events() <-chan peer.Input

	Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error
	Handle(t types.MsgType, h HandlerFn) // dispatch after decrypt

	GetActivePeerAddress(peerKey types.PeerKey) (string, bool)
	BroadcastDisconnect() // notify all connected peers we're shutting down
	SocketStore() SocketStore
}

type LocalCrypto interface {
	NoisePub() []byte
	IdentityPub() []byte // ed25519 pub
	// SignIdentity(msg []byte) []byte // ed25519 sign
}

type impl struct {
	socketStore    SocketStore
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

func NewLink(socketStore SocketStore, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission, opts ...Option) (Link, error) {
	if socketStore == nil {
		return nil, errors.New("socketStore is nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &impl{
		log:            zap.S().Named("mesh"),
		crypto:         crypto,
		socketStore:    socketStore,
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

func (i *impl) SocketStore() SocketStore {
	return i.socketStore
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
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-i.socketStore.Events():
			if !ok {
				return
			}

			fr, err := decodeFrame(evt.Payload)
			if err != nil {
				i.log.Debugw("bad frame", "src", evt.Src, "err", err)
				continue
			}

			switch fr.typ { //nolint:exhaustive
			case types.MsgTypePing:
			case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Init, types.MsgTypeHandshakeXXPsk2Resp:
				if err := i.handleHandshake(ctx, evt.Src, evt.Socket, fr); err != nil {
					i.log.Debugf("failed to handle handshake: %s", err)
				}
			case types.MsgTypeTransportData, types.MsgTypeTCPPunchRequest, types.MsgTypeTCPPunchTrigger,
				types.MsgTypeTCPPunchReady, types.MsgTypeTCPPunchResponse, types.MsgTypeGossip, types.MsgTypeTest,
				types.MsgTypeSessionRequest, types.MsgTypeSessionResponse,
				types.MsgTypeTCPPunchProbeRequest, types.MsgTypeTCPPunchProbeOffer, types.MsgTypeTCPPunchProbeResult:
				i.handleApp(ctx, fr)
			case types.MsgTypeUDPPunchCoordRequest:
				i.log.Debugw("received punch request", "src", evt.Src)
				if err := i.handlePunchCoordRequest(ctx, evt.Src, fr); err != nil {
					i.log.Debugf("failed to handle punch coord request: %s", err)
				}
			case types.MsgTypeUDPPunchCoordResponse:
				i.log.Debugw("received punch trigger", "src", evt.Src)
				// Run in goroutine so we don't block the main loop while punching
				go func() {
					if err := i.handlePunchCoordTrigger(ctx, fr); err != nil {
						i.log.Debugf("failed to handle punch coord trigger: %s", err)
					}
				}()
			case types.MsgTypeDisconnect:
				i.handleDisconnect(fr)
			}
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

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:   req.PeerId,
			SelfAddr: src,
			PeerAddr: recvSess.peerAddr,
			Mode:     req.GetMode(),
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
			PeerId:   initSess.peerNoiseKey,
			SelfAddr: recvSess.peerAddr,
			PeerAddr: src,
			Mode:     req.GetMode(),
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

	// Early exit: already have a session with this peer
	if _, ok := i.sessionStore.getByPeer(peerID); ok {
		return nil
	}

	// Early exit: another punch is already in progress for this peer
	if i.hasWaiter(peerID) {
		i.log.Debugf("punch already in progress for peer: %s", peerID.String()[:8])
		return nil
	}

	addrs := []string{req.PeerAddr}

	var sockets []Socket
	var opts EnsurePeerOpts

	switch req.Mode { //nolint:exhaustive
	case peerv1.PunchMode_PUNCH_MODE_BIRTHDAY:
		i.log.Debugf("attempting birthday punch: %s", peerID.String()[:8])

		host, _, err := net.SplitHostPort(req.PeerAddr)
		if err != nil {
			return fmt.Errorf("invalid peer address: %w", err)
		}

		birthdaySockets, err := i.socketStore.CreateBatch(i.cfg.birthdayPunchSocketCount)
		if err != nil {
			return fmt.Errorf("create birthday sockets: %w", err)
		}
		defer i.socketStore.CloseEphemeral()

		sockets = birthdaySockets
		opts = EnsurePeerOpts{
			SendInterval: 10 * time.Millisecond,
			BirthdayProbe: &BirthdayProbeOpts{
				ProbeSocket: i.socketStore.Base(),
				ProbeHost:   host,
				ProbeCount:  i.cfg.birthdayPunchSocketCount,
			},
		}
	case peerv1.PunchMode_PUNCH_MODE_DIRECT:
		sockets = []Socket{i.socketStore.Base()}
		opts = EnsurePeerOpts{
			SendInterval: i.cfg.ensurePeerInterval,
			Rounds:       i.cfg.holepunchAttempts,
		}
	}

	err = i.EnsurePeer(ensureCtx, peerID, addrs, sockets, opts)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		i.log.Debugw("ensure peer failed", "peer", peerID.String()[:8], "err", err)
		return err
	}

	return nil
}

func (i *impl) handleHandshake(ctx context.Context, src string, sock Socket, fr frame) error {
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

	if err := i.sendHandshakeResultViaSocket(src, res, fr.senderID, sock); err != nil {
		i.log.Debugw("failed to send handshake reply", "err", err)
	}

	if res.Session != nil {
		res.Session.peerAddr = src
		res.Session.peerSessionID = fr.senderID

		i.sessionStore.set(res.Session)

		// Associate peer with the socket that successfully completed the handshake
		peerKey := types.PeerKeyFromBytes(res.PeerStaticKey)
		i.socketStore.AssociatePeerSocket(peerKey, sock)

		// Clearing after a grace period gates against old handshakes from the same "batch" overriding
		// successfully completed "quicker" ones (`handleHandshake.getOrCreate` returns handshakes with invalid
		// stages, which are dropped).
		// TODO(saml) need to sync up the various durations/grace periods. This timeout needs to be less than the
		// handshake request TTL.
		// TODO(saml) probably need to clear on hard errors too
		i.handshakeStore.clear(fr.senderID, i.cfg.handshakeDedupTTL)

		i.removeWaiter(peerKey)
		// start only when we trust the peer
		i.bumpPinger(ctx, res.Session.peerAddr)

		select {
		case i.events <- peer.ConnectPeer{
			PeerKey:     peerKey,
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
		if errors.Is(err, ErrReplay) || errors.Is(err, ErrTooOld) || errors.Is(err, ErrShortCiphertext) {
			return
		}
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
	i.log.Debug("closing Link")
	i.cancel()
	return i.socketStore.Close()
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

	if err := i.socketStore.SendToPeer(peerKey, sess.peerAddr, encodeFrame(fr)); err != nil {
		i.log.Debugw("send failed", "peer", peerKey.String()[:8], "addr", sess.peerAddr, "err", err)
		logNetworkRestrictionWarning(i.log, err)
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
	baseSock := i.socketStore.Base()
	g, _ := errgroup.WithContext(ctx)
	for _, addr := range inv.Addr {
		g.Go(func() error {
			return i.sendHandshakeResultViaSocket(addr, res, 0, baseSock)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to send invite init: %w", err)
	}
	i.log.Debug("join attempt complete")

	return nil
}

func (i *impl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string, sockets []Socket, opts EnsurePeerOpts) error {
	if len(addrs) == 0 {
		return errors.New("no addresses provided")
	}
	if len(sockets) == 0 {
		return errors.New("no sockets provided")
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

	payload := encodeHandshake(res)

	// Birthday punch mode: dual-role sending
	if opts.BirthdayProbe != nil {
		b := opts.BirthdayProbe
		probeAddrs := generateRandomPorts(b.ProbeHost, b.ProbeCount)
		ephIdx, probeIdx := 0, 0

		ticker := time.NewTicker(opts.SendInterval)
		defer ticker.Stop()

		for {
			select {
			case <-upCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				// Role 1: ephemeral socket -> known address (opens our NAT)
				if ephIdx < len(sockets) {
					if err := sockets[ephIdx].Send(addrs[0], payload); err != nil {
						i.log.Debugw("ephemeral send failed", "err", err)
					}
					ephIdx++
				}
				// Role 2: probe socket -> random port (probes peer's NAT)
				if probeIdx < len(probeAddrs) {
					if err := b.ProbeSocket.Send(probeAddrs[probeIdx], payload); err != nil {
						i.log.Debugw("probe send failed", "addr", probeAddrs[probeIdx], "err", err)
					}
					probeIdx++
				}
				// Regenerate probes for next round
				if ephIdx >= len(sockets) && probeIdx >= len(probeAddrs) {
					probeIdx = 0
					probeAddrs = generateRandomPorts(b.ProbeHost, b.ProbeCount)
				}
			}
		}
	}

	// Standard mode: iterate sockets × addresses
	pairsPerRound := len(sockets) * len(addrs)
	totalSends := opts.Rounds * pairsPerRound

	tickCh := make(chan struct{})
	go func() {
		defer close(tickCh)
		for j := 0; j < totalSends; j++ {
			select {
			case tickCh <- struct{}{}:
			case <-ctx.Done():
				return
			}
			if opts.SendInterval > 0 && j < totalSends-1 {
				select {
				case <-time.After(opts.SendInterval):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	sent := 0
	for {
		select {
		case <-upCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-tickCh:
			if !ok {
				return fmt.Errorf("peer %s unreachable after %d sends", peerKey.String()[:8], sent)
			}
			pairIdx := sent % pairsPerRound
			sockIdx := pairIdx / len(addrs)
			addrIdx := pairIdx % len(addrs)
			if err := sockets[sockIdx].Send(addrs[addrIdx], payload); err != nil {
				i.log.Debugw("handshake send failed", "addr", addrs[addrIdx], "err", err)
				logNetworkRestrictionWarning(i.log, err)
			}
			sent++
		}
	}
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

func (i *impl) hasWaiter(k types.PeerKey) bool {
	i.waitMu.Lock()
	defer i.waitMu.Unlock()
	_, ok := i.waitPeer[k]
	return ok
}

func (i *impl) sendHandshakeResultViaSocket(addr string, res HandshakeResult, remoteID uint32, sock Socket) error {
	if len(res.Msg) == 0 {
		return ErrEmptyMsg
	}

	if err := sock.Send(addr, encodeFrame(&frame{
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

func encodeHandshake(res HandshakeResult) []byte {
	return encodeFrame(&frame{
		payload:    res.Msg,
		typ:        res.MsgType,
		senderID:   res.LocalSessionID,
		receiverID: 0, // For init, receiverID is 0
	})
}

func generateRandomPorts(host string, count int) []string {
	addrs := make([]string, count)
	for i := 0; i < count; i++ {
		port := 1024 + rand.Intn(65535-1024+1)
		addrs[i] = net.JoinHostPort(host, strconv.Itoa(port))
	}
	return addrs
}
