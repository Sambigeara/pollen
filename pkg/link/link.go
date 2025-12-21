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
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var _ Link = (*impl)(nil)

const (
	sessionRefreshInterval = time.Second * 120 // new IK handshake every 2 mins
	handshakeDedupTTL      = time.Second * 5
)

var ErrNoSession = errors.New("no session for peer")

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type Link interface {
	Start(ctx context.Context) error
	Close() error

	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error             // XXpsk2 init
	EnsurePeer(ctx context.Context, peer types.PeerKey, addrs []string) error // IK init
	Events() <-chan types.PeerEvent

	Send(ctx context.Context, peer types.PeerKey, msg types.Envelope) error
	Handle(t types.MsgType, h HandlerFn) // dispatch after decrypt
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
	events         chan types.PeerEvent
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
		events:         make(chan types.PeerEvent, eventBufSize),
		pingMgrs:       make(map[string]*pingMgr),
		waitPeer:       make(map[types.PeerKey][]chan struct{}),
	}, nil
}

func (i *impl) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel

	go i.loop(ctx)
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
		case types.MsgTypeTransportData, types.MsgTypeTCPTunnelRequest, types.MsgTypeTCPTunnelResponse, types.MsgTypeGossip, types.MsgTypeTest:
			i.handleApp(ctx, fr)
		}
	}
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

	if err := i.sendHandshakeResult([]string{src}, res, fr.senderID); err != nil {
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
		i.handshakeStore.clear(fr.senderID, handshakeDedupTTL)

		i.notifyUp(types.PeerKeyFromBytes(res.PeerStaticKey))
		// start only when we trust the peer
		i.bumpPinger(ctx, res.Session.peerAddr)

		select {
		case i.events <- types.PeerEvent{
			Peer:        types.PeerKeyFromBytes(res.PeerStaticKey),
			Kind:        types.PeerEventKindUp,
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

func (i *impl) Close() error {
	i.cancel()
	return i.transport.Close()
}

func (i *impl) Events() <-chan types.PeerEvent { return i.events }

func (i *impl) Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	sess, ok := i.sessionStore.getByPeer(peerKey)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoSession, peerKey.String())
	}

	ct, shouldRekey, err := sess.Encrypt(msg.Payload)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	fr := &frame{
		payload:    ct,
		typ:        msg.Type,
		senderID:   sess.localSessionID,
		receiverID: sess.peerSessionID,
	}

	if err := i.transport.Send(sess.peerAddr, encodeFrame(fr)); err != nil {
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
	if err := i.sendHandshakeResult(inv.Addr, res, 0); err != nil {
		return fmt.Errorf("failed to send invite init: %w", err)
	}

	return nil
}

func (i *impl) EnsurePeer(ctx context.Context, peer types.PeerKey, addrs []string) error {
	// fast path
	if _, ok := i.sessionStore.getByPeer(peer); ok {
		return nil
	}

	mu := i.peerLock(peer)
	mu.Lock()
	defer mu.Unlock()

	// re-check under lock
	if _, ok := i.sessionStore.getByPeer(peer); ok {
		return nil
	}

	// register waiter *before* sending, so we can’t miss the PeerUp
	upCh := i.addWaiter(peer)

	res, err := i.handshakeStore.initIK(peer.Bytes())
	if err != nil {
		return err
	}
	if err := i.sendHandshakeResult(addrs, res, 0); err != nil {
		return err
	}

	select {
	case <-upCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

func (i *impl) sendHandshakeResult(addrs []string, res HandshakeResult, remoteID uint32) error {
	if len(addrs) == 0 {
		return ErrNoResolvableIP
	}
	if len(res.Msg) == 0 {
		return nil
	}

	var errs error
	success := false

	enc := encodeFrame(&frame{
		payload:    res.Msg,
		typ:        res.MsgType,
		senderID:   res.LocalSessionID,
		receiverID: remoteID,
	})

	// happy eyeballs
	for _, addr := range addrs {
		if err := i.transport.Send(addr, enc); err != nil {
			i.log.Debugf("failed to write to candidate %s: %v", addr, err)
			errs = multierr.Append(errs, fmt.Errorf("%s: %w", addr, err))
		} else {
			success = true
		}
	}

	if !success {
		return errs
	}
	return nil
}
