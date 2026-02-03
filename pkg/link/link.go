package link

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
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
	defaultPunchProbeCount           = 256
	defaultPunchSendInterval         = 10 * time.Millisecond
	defaultStaleSessionCheckInterval = 15 * time.Second // how often to check for stale sessions
)

type Option func(*config)

type config struct {
	sessionRefreshInterval    time.Duration
	handshakeDedupTTL         time.Duration
	punchProbeCount           int
	punchSendInterval         time.Duration
	staleSessionCheckInterval time.Duration
}

func defaultConfig() config {
	return config{
		sessionRefreshInterval:    defaultSessionRefreshInterval,
		handshakeDedupTTL:         defaultHandshakeDedupTTL,
		punchProbeCount:           defaultPunchProbeCount,
		punchSendInterval:         defaultPunchSendInterval,
		staleSessionCheckInterval: defaultStaleSessionCheckInterval,
	}
}

func WithPunchProbeCount(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.punchProbeCount = n
		}
	}
}

func WithPunchSendInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.punchSendInterval = d
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

type Link interface {
	Start(ctx context.Context) error
	Close() error

	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error                            // XXpsk2 init
	EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string, rounds int) error // IK init
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

func NewLink(tr transport.Transport, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission, opts ...Option) (Link, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &impl{
		log:            zap.S().Named("mesh"),
		transport:      tr,
		crypto:         crypto,
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
		case types.MsgTypeHandshakeIKInit,
			types.MsgTypeHandshakeIKResp,
			types.MsgTypeHandshakeXXPsk2Init,
			types.MsgTypeHandshakeXXPsk2Resp:
			if err := i.handleHandshake(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle handshake: %s", err)
			}
		case types.MsgTypeTransportData,
			types.MsgTypeTCPPunchRequest,
			types.MsgTypeTCPPunchTrigger,
			types.MsgTypeTCPPunchReady,
			types.MsgTypeTCPPunchResponse,
			types.MsgTypeGossip,
			types.MsgTypeTest,
			types.MsgTypeSessionRequest,
			types.MsgTypeSessionResponse,
			types.MsgTypeTCPPunchProbeRequest,
			types.MsgTypeTCPPunchProbeOffer,
			types.MsgTypeTCPPunchProbeResult:
			i.handleApp(ctx, src, fr)
		case types.MsgTypeUDPPunchCoordRequest:
			i.log.Debugw("received punch request", "src", src)
			if err := i.handlePunchCoordRequest(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle punch coord request: %s", err)
			}
		case types.MsgTypeUDPPunchCoordResponse:
			i.log.Debugw("received punch trigger", "src", src)
			// handlePunchCoordTrigger runs EnsurePeer, which blocks waiting for a handshake/up event that
			// is processed on this same loop. Run it async so the loop can keep draining socket events and
			// complete the handshake (AssociatePeerSocket/removeWaiter) that unblocks EnsurePeer.
			frCopy := fr
			go func() {
				if err := i.handlePunchCoordTrigger(ctx, frCopy); err != nil {
					i.log.Debugf("failed to handle punch coord trigger: %s", err)
				}
			}()
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

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:   req.PeerId,
			SelfAddr: src,
			PeerAddr: recvSess.peerAddrValue(),
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
			SelfAddr: recvSess.peerAddrValue(),
			PeerAddr: src,
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

	peerID := types.PeerKeyFromBytes(req.PeerId)

	// host, _, err := net.SplitHostPort(req.PeerAddr)
	// if err != nil {
	// 	return fmt.Errorf("invalid peer address: %w", err)
	// }

	// Generate probe addresses for the peer's host interleaved with the known address
	// randoms := generateRandomPorts(host, i.cfg.punchProbeCount)
	// addrs := make([]string, 0, len(randoms)*2)
	// for _, r := range randoms {
	// 	addrs = append(addrs, req.PeerAddr, r)
	// }

	// rounds := 2
	// var addrs []string
	// if _, ok := os.LookupEnv("GOOD_NAT"); ok {
	// addrs = generateRandomPorts(host, i.cfg.punchProbeCount)
	// } else {
	// 	addrs = []string{req.PeerAddr}
	// 	rounds *= i.cfg.punchProbeCount
	// }
	// addrs = append([]string{req.PeerAddr}, generateRandomPorts(host, i.cfg.punchProbeCount)...)

	rounds := 2
	addrs := append([]string{req.PeerAddr})

	i.log.Debugw("attempting punch", "peer", peerID.String()[:8], "addrs", addrs)

	ensureCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = i.EnsurePeer(ensureCtx, peerID, addrs, rounds)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
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
		res.Session.setPeerAddr(src)
		res.Session.peerSessionID = fr.senderID

		i.sessionStore.set(res.Session)

		// Associate peer with the socket that successfully completed the handshake
		peerKey := types.PeerKeyFromBytes(res.PeerStaticKey)

		// Clearing after a grace period gates against old handshakes from the same "batch" overriding
		// successfully completed "quicker" ones (`handleHandshake.getOrCreate` returns handshakes with invalid
		// stages, which are dropped).
		i.handshakeStore.clear(fr.senderID, i.cfg.handshakeDedupTTL)

		active, ok := i.sessionStore.getByPeer(peerKey)
		if !ok || active != res.Session {
			i.log.Debugw("active session nonexistent or mismatching", "peer", types.PeerKeyFromBytes(res.PeerIdentityPub).String()[:8])
			return nil
		}

		i.removeWaiter(peerKey)

		// start only when we trust the peer
		i.bumpPinger(ctx, src)

		host, portStr, err := net.SplitHostPort(src)
		if err != nil {
			return err
		}

		port, err := strconv.Atoi(portStr)
		if err != nil {
			return err
		}

		i.events <- peer.ConnectPeer{
			PeerKey:      peerKey,
			Ip:           host,
			ObservedPort: port,
			IdentityPub:  res.PeerIdentityPub,
		}
	}

	return nil
}

func (i *impl) handleApp(ctx context.Context, src string, fr frame) {
	sess, ok := i.sessionStore.getByLocalID(fr.receiverID)
	if !ok {
		i.log.Debugw("handleApp: session not found",
			"receiverID", fr.receiverID,
			"msgType", fr.typ)
		return
	}
	peerKey := types.PeerKeyFromBytes(sess.peerNoiseKey)

	pt, shouldRekey, err := sess.Decrypt(fr.payload)
	if err != nil {
		if errors.Is(err, ErrReplay) || errors.Is(err, ErrTooOld) || errors.Is(err, ErrShortCiphertext) {
			return
		}
		i.log.Debugw("handleApp: decrypt failed",
			"peer", peerKey.String()[:8],
			"msgType", fr.typ,
			"err", err)
		return
	}

	sess.touchRecv()
	if src != "" {
		sess.setPeerAddr(src)
	}
	addr := sess.peerAddrValue()
	if addr != "" {
		i.bumpPinger(ctx, addr)
	}

	if shouldRekey {
		i.rekeyMgr.resetIfExists(sess.peerSessionID, i.cfg.sessionRefreshInterval)
	}

	i.handlersMu.RLock()
	h := i.handlers[fr.typ]
	i.handlersMu.RUnlock()
	if h != nil {
		if err := h(ctx, peerKey, pt); err != nil {
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

	addr := sess.peerAddrValue()
	if err := i.transport.Send(addr, encodeFrame(fr)); err != nil {
		i.log.Debugw("send failed", "peer", peerKey.String()[:8], "addr", addr, "err", err)
		logNetworkRestrictionWarning(i.log, err)
		return fmt.Errorf("send: %w", err)
	}

	if addr != "" {
		i.bumpPinger(ctx, addr)
	}

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

func (i *impl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string, rounds int) error {
	if len(addrs) == 0 {
		return errors.New("no addresses provided")
	}

	if _, ok := i.sessionStore.getByPeer(peerKey); ok {
		i.log.Debugf("session already exists for peer: %s", peerKey.String()[:8])
		return nil
	}

	mu := i.peerLock(peerKey)
	mu.Lock()
	defer mu.Unlock()

	// re-check under lock
	if _, ok := i.sessionStore.getByPeer(peerKey); ok {
		i.log.Debugf("session already exists for peer: %s", peerKey.String()[:8])
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
		i.log.Errorf("unable to create IK handshake: %s", peerKey.String()[:8], "err", err)
		return err
	}

	payload := encodeHandshake(res)

	ticker := time.NewTicker(i.cfg.punchSendInterval)
	defer ticker.Stop()

	total := len(addrs) * rounds
	sent := 0
	addrIdx := 0
	for {
		select {
		case <-upCh:
			i.log.Debugw("received direct up event", "peer", peerKey.String()[:8])
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if sent == total {
				return fmt.Errorf("peer %s unreachable after %d sends", peerKey.String()[:8], sent)
			}
			if err := i.transport.Send(addrs[addrIdx], payload); err != nil {
				i.log.Errorw("EnsurePeer Send error", "err", err, "addr", addrs[addrIdx])
				// logNetworkRestrictionWarning(i.log, err)
			}
			addrIdx = (addrIdx + 1) % len(addrs)
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

	return sess.peerAddrValue(), true
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
	for i := range count {
		port := 1024 + rand.IntN(65535-1024+1)
		addrs[i] = net.JoinHostPort(host, strconv.Itoa(port))
	}
	return addrs
}
