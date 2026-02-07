package link

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/sock"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

// TODO(saml) the link abstraction can probably go now, and node should just hold the SuperSock

var _ Link = (*impl)(nil)

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

type sendFn func(dst string, b []byte, isPunch bool) error

type Link interface {
	Start(ctx context.Context) error
	Close() error
	Events() <-chan peer.Input
	Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error
	Handle(t types.MsgType, h HandlerFn)                                                              // dispatch after decrypt
	EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr, withPeer bool) error // IK init
	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error                                     // XXpsk2 init
	GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool)
	BroadcastDisconnect() // notify all connected peers we're shutting down
}

type impl struct {
	transport  sock.Socket
	crypto     sock.LocalCrypto
	handlers   map[types.MsgType]HandlerFn
	log        *zap.SugaredLogger
	events     chan peer.Input
	cancel     context.CancelFunc
	handlersMu sync.RWMutex
}

func NewLink(cs *noise.CipherSuite, port int, noiseKey noise.DHKey, crypto sock.LocalCrypto, admission admission.Admission) (Link, error) {
	events := make(chan peer.Input)

	tr, err := sock.NewTransport(port, cs, noiseKey, crypto, admission, events)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	return &impl{
		log:       zap.S().Named("mesh"),
		transport: tr,
		crypto:    crypto,
		handlers:  make(map[types.MsgType]HandlerFn),
		events:    events,
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
		fr, err := i.transport.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			i.log.Debugw("recv failed", "err", err)
			continue
		}
		i.handleApp(ctx, fr)
	}
}

func (i *impl) handleApp(ctx context.Context, p sock.Packet) {
	i.handlersMu.RLock()
	h := i.handlers[p.Typ]
	i.handlersMu.RUnlock()
	if h != nil {
		if err := h(ctx, p.Peer, p.Payload); err != nil {
			i.log.Debugf("handler error: %w", err)
		}
	}
}

func (i *impl) Send(ctx context.Context, peerKey types.PeerKey, msg types.Envelope) error {
	return i.transport.Send(ctx, peerKey, msg)
}

func (i *impl) Events() <-chan peer.Input { return i.events }

func (i *impl) Handle(t types.MsgType, h HandlerFn) {
	i.handlersMu.Lock()
	defer i.handlersMu.Unlock()
	i.handlers[t] = h
}

func (i *impl) EnsurePeer(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr, withPunch bool) error {
	return i.transport.EnsurePeer(ctx, peerKey, addrs, withPunch)
}

func (i *impl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	return i.transport.JoinWithInvite(ctx, inv)
}

func (i *impl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	return i.transport.GetActivePeerAddress(peerKey)
}

func (i *impl) BroadcastDisconnect() {
	i.transport.BroadcastDisconnect()
}

func (i *impl) Close() error {
	i.log.Debug("closing Link")
	i.cancel()
	return i.transport.Close()
}
