package link

import (
	"context"
	"crypto/ed25519"
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
)

var ErrNoSession = errors.New("no session for peer")

type Link interface {
	Start(ctx context.Context) error
	Close() error

	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error             // XXpsk2 init
	EnsurePeer(ctx context.Context, peer types.PeerKey, hints []string) error // IK init
	Events() <-chan types.PeerEvent

	Send(ctx context.Context, peer types.PeerKey, msg types.Envelope) error
	Handle(t types.MsgType, h func(from types.PeerKey, payload []byte)) // dispatch after decrypt
}

type LocalCrypto interface {
	NoiseStatic() noise.DHKey
	IdentityPub() []byte            // ed25519 pub
	SignIdentity(msg []byte) []byte // ed25519 sign
}

type impl struct {
	log *zap.SugaredLogger

	crypto    LocalCrypto
	admission admission.Admission
	transport transport.Transport

	handshakeStore *handshakeStore
	sessionStore   *sessionStore
	rekeyMgr       *rekeyManager

	handlersMu sync.RWMutex
	handlers   map[types.MsgType]func(from types.PeerKey, payload []byte)

	events chan types.PeerEvent

	cancel context.CancelFunc

	pingMgrs map[string]*pingMgr
	pingMu   sync.RWMutex

	peerLocks sync.Map // map[types.PeerKey]*sync.Mutex

	waitMu   sync.Mutex
	waitPeer map[types.PeerKey][]chan struct{}
}

func NewLink(port int, crypto LocalCrypto, cs *noise.CipherSuite, staticKey noise.DHKey, pub ed25519.PublicKey, admission admission.Admission) (Link, error) {
	tr, err := transport.NewTransport(port)
	if err != nil {
		return nil, err
	}

	return &impl{
		log:            zap.S().Named("mesh"),
		crypto:         crypto,
		transport:      tr,
		handshakeStore: newHandshakeStore(cs, admission, staticKey, pub),
		sessionStore:   newSessionStore(),
		rekeyMgr:       newRekeyManager(),
		handlers:       make(map[types.MsgType]func(from types.PeerKey, payload []byte)),
		events:         make(chan types.PeerEvent, 64),
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
		src, b, err := i.transport.Recv(ctx)
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

		switch fr.typ {
		case types.MsgTypePing:
		case types.MsgTypeHandshakeIKInit, types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Init, types.MsgTypeHandshakeXXPsk2Resp:
			i.handleHandshake(ctx, src, fr)
		default:
			i.handleApp(ctx, src, fr)
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

	if err := i.sendHandshakeResult(ctx, []string{src}, res, fr.senderID); err != nil {
		i.log.Debugw("failed to send handshake reply", "err", err)
	}

	if res.Session != nil {
		res.Session.peerAddr = src
		res.Session.peerSessionID = fr.senderID

		i.sessionStore.set(res.Session)
		// TODO(saml) probably need to clear on hard errors too
		i.handshakeStore.clear(fr.senderID)

		i.notifyUp(types.PeerKeyFromBytes(res.PeerStaticKey))
		// start only when we trust the peer
		i.bumpPinger(ctx, res.Session.peerAddr)

		select {
		case i.events <- types.PeerEvent{Peer: types.PeerKeyFromBytes(res.PeerStaticKey), Kind: types.PeerEventKindUp}:
		default:
		}
	}

	return nil
}

func (i *impl) handleApp(ctx context.Context, src string, fr frame) {
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
		h(types.PeerKeyFromBytes(sess.peerNoiseKey), pt)
	}
}

func (i *impl) Close() error {
	i.cancel()
	return i.transport.Close()
}

func (i *impl) Events() <-chan types.PeerEvent { return i.events }

func (i *impl) Send(ctx context.Context, peer types.PeerKey, msg types.Envelope) error {
	sess, ok := i.sessionStore.getByPeer(peer)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoSession, peer.String())
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

func (i *impl) Handle(t types.MsgType, h func(from types.PeerKey, payload []byte)) {
	i.handlersMu.Lock()
	defer i.handlersMu.Unlock()
	i.handlers[t] = h
}

func (i *impl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	res, err := i.handshakeStore.initXXPsk2(inv, i.crypto.IdentityPub())
	if err != nil {
		return fmt.Errorf("failed to create XXpsk2 handshake: %v", err)
	}

	// Send the initial packet(s)
	// For Init, receiverID is 0
	if err := i.sendHandshakeResult(ctx, inv.Addr, res, 0); err != nil {
		return fmt.Errorf("failed to send invite init: %v", err)
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
	if err := i.sendHandshakeResult(ctx, addrs, res, 0); err != nil {
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
	return v.(*sync.Mutex)
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

func (i *impl) sendHandshakeResult(ctx context.Context, addrs []string, res HandshakeResult, remoteID uint32) error {
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
