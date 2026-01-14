package link

import (
	"context"
	"errors"
	"fmt"
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
	sessionRefreshInterval    = time.Second * 120 // new IK handshake every 2 mins
	handshakeDedupTTL         = time.Second * 5
	ensurePeerTimeout         = time.Second * 4
	staleSessionCheckInterval = time.Second * 15 // how often to check for stale sessions
)

var ErrNoSession = errors.New("no session for peer")

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type Link interface {
	Start(ctx context.Context) error
	Close() error

	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error                // XXpsk2 init
	EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string) error // IK init
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
	waitPeer       map[types.PeerKey][]chan struct{}
	peerLocks      sync.Map
	handlersMu     sync.RWMutex
	pingMu         sync.RWMutex
	waitMu         sync.Mutex
}

const eventBufSize = 64

func NewLink(port int, cs *noise.CipherSuite, staticKey noise.DHKey, crypto LocalCrypto, admission admission.Admission) (Link, error) {
	tr, err := transport.NewTransport(port)
	if err != nil {
		return nil, err
	}

	return &impl{
		log:            zap.S().Named("mesh"),
		crypto:         crypto,
		transport:      tr,
		handshakeStore: newHandshakeStore(cs, admission, staticKey, crypto.IdentityPub()),
		sessionStore:   newSessionStore(),
		rekeyMgr:       newRekeyManager(),
		handlers:       make(map[types.MsgType]HandlerFn),
		events:         make(chan peer.Input, eventBufSize),
		pingMgrs:       make(map[string]*pingMgr),
		waitPeer:       make(map[types.PeerKey][]chan struct{}),
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
		case types.MsgTypeUdpPunchCoordRequest:
			i.log.Debugw("received punch request", "src", src)
			if err := i.handlePunchCoordRequest(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle punch coord request: %s", err)
			}
		case types.MsgTypeUdpPunchCoordResponse:
			i.log.Debugw("received punch trigger", "src", src)
			if err := i.handlePunchCoordTrigger(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle punch coord trigger: %s", err)
			}
		case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Init, types.MsgTypeHandshakeXXPsk2Resp:
			if err := i.handleHandshake(ctx, src, fr); err != nil {
				i.log.Debugf("failed to handle handshake: %s", err)
			}
		case types.MsgTypeTransportData, types.MsgTypeTcpPunchRequest, types.MsgTypeTcpPunchTrigger,
			types.MsgTypeTcpPunchReady, types.MsgTypeTcpPunchResponse, types.MsgTypeGossip, types.MsgTypeTest,
			types.MsgTypeSessionRequest, types.MsgTypeSessionResponse:
			i.handleApp(ctx, fr)
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
			PeerAddr: recvSess.peerAddr,
		}

		b, err := resp.MarshalVT()
		if err != nil {
			return err
		}

		return i.Send(ctx, initPeer, types.Envelope{
			Payload: b,
			Type:    types.MsgTypeUdpPunchCoordResponse,
		})
	})

	g.Go(func() error {
		resp := &peerv1.PunchCoordTrigger{
			PeerId:   initSess.peerNoiseKey,
			SelfAddr: recvSess.peerAddr,
			PeerAddr: src,
		}

		b, err := resp.MarshalVT()
		if err != nil {
			return err
		}

		return i.Send(ctx, recvPeer, types.Envelope{
			Payload: b,
			Type:    types.MsgTypeUdpPunchCoordResponse,
		})
	})
	return g.Wait()
}

func (i *impl) handlePunchCoordTrigger(ctx context.Context, src string, fr frame) error {
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

	ensureCtx, cancel := context.WithTimeout(ctx, ensurePeerTimeout)
	defer cancel()

	peerID := types.PeerKeyFromBytes(req.PeerId)
	if err := i.EnsurePeer(ensureCtx, peerID, []string{req.PeerAddr}); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		i.log.Debugw("ensure peer failed", "peer", peerID, "err", err)
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
		// i.log.Debugw("failed to send handshake reply", "err", err)
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
		i.handshakeStore.clear(fr.senderID, handshakeDedupTTL)

		i.notifyUp(types.PeerKeyFromBytes(res.PeerStaticKey))
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
		return
	}

	pt, shouldRekey, err := sess.Decrypt(fr.payload)
	if err != nil {
		return
	}

	sess.touchRecv()
	i.bumpPinger(ctx, sess.peerAddr)

	if shouldRekey {
		i.rekeyMgr.resetIfExists(sess.peerSessionID, sessionRefreshInterval)
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

	// Emit PeerDisconnected to state machine
	select {
	case i.events <- peer.PeerDisconnected{PeerKey: peerKey}:
	default:
	}
}

func (i *impl) staleSessionChecker(ctx context.Context) {
	ticker := time.NewTicker(staleSessionCheckInterval)
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
		i.log.Errorf("%w: %s", ErrNoSession, peerKey.String())
		return fmt.Errorf("%w: %s", ErrNoSession, peerKey.String())
	}

	ct, shouldRekey, err := sess.Encrypt(msg.Payload)
	if err != nil {
		i.log.Errorf("encrypt: %w", err)
		return fmt.Errorf("encrypt: %w", err)
	}

	fr := &frame{
		payload:    ct,
		typ:        msg.Type,
		senderID:   sess.localSessionID,
		receiverID: sess.peerSessionID,
	}

	if err := i.transport.Send(sess.peerAddr, encodeFrame(fr)); err != nil {
		i.log.Errorf("send: %w", err)
		return fmt.Errorf("send: %w", err)
	}

	i.bumpPinger(ctx, sess.peerAddr)

	if shouldRekey {
		i.rekeyMgr.resetIfExists(sess.peerSessionID, sessionRefreshInterval)
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
	g, ctx := errgroup.WithContext(ctx)
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

func (i *impl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []string) error {
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
	upCh := i.addWaiter(peerKey)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	res, err := i.handshakeStore.initIK(peerKey.Bytes())
	if err != nil {
		return err
	}

	for {
		select {
		case <-upCh:
			return nil
		case <-ticker.C:
			// Send to all addresses in parallel (happy eyeballs)
			for _, addr := range addrs {
				go func(addr string) {
					if err := i.sendHandshakeResult(addr, res, 0); err != nil {
						i.log.Debugw("handshake send failed", "addr", addr, "err", err)
					}
				}(addr)
			}
		case <-ctx.Done():
			return ctx.Err()
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

func (i *impl) addWaiter(k types.PeerKey) chan struct{} {
	ch := make(chan struct{})
	i.waitMu.Lock()
	defer i.waitMu.Unlock()
	i.waitPeer[k] = append(i.waitPeer[k], ch)
	return ch
}

func (i *impl) notifyUp(p types.PeerKey) {
	i.waitMu.Lock()
	waiters := i.waitPeer[p]
	delete(i.waitPeer, p)
	i.waitMu.Unlock()

	for _, ch := range waiters {
		close(ch)
	}
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
