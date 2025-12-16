package mesh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/ed25519"
	"golang.org/x/sync/errgroup"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	tcpHandshakeTimeout = time.Second * 5
)

var (
	handshakePrologue = []byte("pollenv1")

	ErrNotConnected = errors.New("peer not connected")
)

type Config struct {
	Port          int
	AdvertisedIPs []string
}

type Handler func(peerNoisePub []byte, plaintext []byte) error

type handlerRegistry struct {
	mu sync.RWMutex
	m  map[MessageType]Handler
}

func newHandlerRegistry() handlerRegistry {
	return handlerRegistry{m: make(map[MessageType]Handler)}
}

func (r *handlerRegistry) On(t MessageType, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[t] = h
}

func (r *handlerRegistry) get(t MessageType) Handler {
	r.mu.RLock()
	h := r.m[t]
	r.mu.RUnlock()
	return h
}

type PeerJoinHandler func(peerStaticKey []byte, peerSigPub []byte, peerAddr *net.UDPAddr)

type Mesh struct {
	log             *zap.SugaredLogger
	conf            *Config
	invites         *invites.InviteStore
	signingKey      ed25519.PrivateKey
	signingPubKey   ed25519.PublicKey
	handshakeStore  *handshakeStore
	sessionStore    *sessionStore
	rekeyMgr        *rekeyManager
	Conn            *UDPConn
	handshakeMu     sync.Mutex
	advertisableIPs []net.IP
	handlers        handlerRegistry
	peerJoinHandler PeerJoinHandler
}

func New(conf *Config, cs *noise.CipherSuite, staticKey noise.DHKey, priv ed25519.PrivateKey, pub ed25519.PublicKey, invites *invites.InviteStore) (*Mesh, error) {
	var ips []net.IP

	// TODO(saml) currently this is only used for injecting IPs in tests, there's probably a better way of doing it
	if len(conf.AdvertisedIPs) > 0 {
		for _, s := range conf.AdvertisedIPs {
			if ip := net.ParseIP(s); ip != nil {
				ips = append(ips, ip)
			}
		}
	}

	if len(ips) == 0 {
		var err error
		ips, err = GetAdvertisableIPs()
		if err != nil {
			return nil, err
		}
	}

	return &Mesh{
		log:             zap.S().Named("mesh"),
		conf:            conf,
		invites:         invites,
		signingKey:      priv,
		signingPubKey:   pub,
		handshakeStore:  newHandshakeStore(cs, invites, staticKey, pub),
		sessionStore:    newSessionStore(),
		rekeyMgr:        newRekeyManager(),
		advertisableIPs: ips,
		handlers:        newHandlerRegistry(),
	}, nil
}

func (m *Mesh) Start(ctx context.Context, token *peerv1.Invite) error {
	var err error
	m.Conn, err = newUDPConn(ctx, m.conf.Port)
	if err != nil {
		return err
	}

	go m.listen(ctx)

	// Handle the invite token exactly once (immediately)
	if token != nil {
		res, err := m.handshakeStore.initXXPsk2(token, m.signingPubKey)
		if err != nil {
			m.log.Errorf("failed to create XXpsk2 handshake: %v", err)
		} else {
			// Send the initial packet(s)
			// For Init, receiverID is 0
			if err := m.sendHandshakeResult(res, 0); err != nil {
				m.log.Errorf("failed to send invite init: %v", err)
			}
		}
	}

	return nil
}

func (m *Mesh) SetPeerJoinHandler(h PeerJoinHandler) {
	m.handshakeMu.Lock()
	defer m.handshakeMu.Unlock()
	m.peerJoinHandler = h
}

func (m *Mesh) listen(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// TODO(saml) there will be a lot of IO so we can probably tune up aggressively
	g.SetLimit(runtime.NumCPU() + 4)

	for {
		select {
		case <-ctx.Done():
			return g.Wait()
		default:
		}

		dg, err := read(m.Conn, nil)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return g.Wait()
			}
			m.log.Errorf("read UDP: %v", err)
			continue
		}

		g.Go(func() error {
			switch dg.tp {
			case MessageTypePing:
				m.log.Debugw("received ping", "peer", dg.senderUDPAddr)
				return nil
			case MessageTypeTransportData:
				return m.handleTransportDataMsg(dg.receiverID, dg.msg)
			case MessageTypeGossip, MessageTypeTest, MessageTypeTCPTunnelRequest, MessageTypeTCPTunnelResponse:
				return m.handleAppMsg(dg.receiverID, dg.tp, dg.msg)
			}

			hs, err := m.handshakeStore.get(dg.tp, dg.senderID, dg.receiverID)
			if err != nil {
				m.log.Errorf("get hs: %v", err)
				return nil
			}
			if hs == nil {
				// This is common during "Reconciliation Wars" - a packet for a
				// handshake we already finished or haven't started arrives. Ignore it.
				m.log.Debugf("dropping packet for unrecognised/stale session: %d", dg.senderID)
				return nil
			}

			res, err := hs.Step(dg.msg)
			if err != nil {
				m.log.Debugw("handshake step failed (ignoring)", "peer", dg.senderUDPAddr, "err", err)
				return nil
			}

			if len(res.Targets) == 0 {
				res.Targets = []*net.UDPAddr{dg.senderUDPAddr}
			}
			if err := m.sendHandshakeResult(res, dg.senderID); err != nil {
				m.log.Errorf("failed to send handshake response: %v", err)
			}

			if res.Session != nil {
				// bind the address that successfully completed the handshake
				res.Session.peerAddr = dg.senderUDPAddr

				var tp string
				switch hs.(type) {
				case *handshakeIKInit: // receiver of a pre-existing join
					m.scheduleRekey(dg.senderID, res.PeerStaticKey, dg.senderUDPAddr.String())
					tp = "IKInit"
				case *handshakeXXPsk2Init: // receiver of a token join
					m.scheduleRekey(dg.senderID, res.PeerStaticKey, dg.senderUDPAddr.String())
					tp = "XXPsk2Init"
				case *handshakeIKResp: // initiator of a known join
					m.rekeyMgr.resetIfExists(dg.senderID, sessionRefreshInterval)
					tp = "IKResp"
				case *handshakeXXPsk2Resp: // initiator of a token join
					tp = "XXPsk2Resp"
				}

				m.log.Infow("established session", "type", tp, "peer", dg.senderUDPAddr)

				// Register session under both our local and the peer's session IDs.
				// - res.LocalID: our local session identifier
				// - dg.senderID: the peer's local session identifier (as seen in the last handshake frame)
				m.sessionStore.set(res.PeerStaticKey, res.LocalSessionID, dg.senderID, res.Session)

				m.handshakeMu.Lock()
				handler := m.peerJoinHandler
				m.handshakeMu.Unlock()
				if handler != nil {
					go handler(res.PeerStaticKey, res.PeerIdentityPub, dg.senderUDPAddr)
				}
			}

			return nil
		})
	}
}

func (m *Mesh) handleAppMsg(receiverID uint32, tp MessageType, ciphertext []byte) error {
	sess, ok := m.sessionStore.get(receiverID)
	if !ok {
		return nil // not connected (or stale)
	}

	pt, shouldRekey, err := sess.Decrypt(ciphertext)
	if err != nil {
		return err
	}
	if shouldRekey {
		if peerID, ok := m.sessionStore.getID(sess.peerNoiseKey); ok {
			m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
		}
	}

	h := m.handlers.get(tp)
	if h == nil {
		return nil
	}

	return h(sess.peerNoiseKey, pt)
}

func (m *Mesh) sendHandshakeResult(res HandshakeResult, remoteID uint32) error {
	if len(res.Msg) == 0 {
		return nil
	}

	var errs error
	success := false

	for _, addr := range res.Targets {
		if err := write(m.Conn, addr, res.MsgType, res.LocalSessionID, remoteID, res.Msg); err != nil {
			m.log.Debugf("failed to write to candidate %s: %v", addr, err)
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

func (m *Mesh) handleTransportDataMsg(receiverID uint32, msg []byte) error {
	sess, ok := m.sessionStore.get(receiverID)
	if !ok {
		return nil
	}

	_, shouldRekey, err := sess.Decrypt(msg)
	if err != nil {
		return err
	}

	if shouldRekey {
		if peerID, ok := m.sessionStore.getID(sess.peerNoiseKey); ok {
			m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
		}
	}

	return nil
}

func (m *Mesh) scheduleRekey(peerSessionID uint32, staticKey []byte, address string) {
	t := time.AfterFunc(sessionRefreshInterval, func() {
		res, err := m.handshakeStore.initIK(staticKey, []string{address})
		if err != nil {
			m.log.Error(err)
			return
		}
		if err := m.sendHandshakeResult(res, 0); err != nil {
			m.log.Error(err)
		}

		m.log.Info("successfully rekeyed")
	})

	m.rekeyMgr.set(peerSessionID, staticKey, t)
}

func (m *Mesh) RejoinPeer(node *statev1.Node) error {
	if _, exists := m.sessionStore.getID(node.Keys.NoisePub); exists {
		return nil
	}

	m.log.Infow("dialing new peer found in state", "peer_id", node.Id[:8], "addr", node.Addresses[0])

	res, err := m.handshakeStore.initIK(node.Keys.NoisePub, node.Addresses)
	if err != nil {
		m.log.Errorf("failed to create IK handshake for peer %s: %v", node.Addresses[0], err)
		return err
	}

	// Send Init packet
	if err := m.sendHandshakeResult(res, 0); err != nil {
		m.log.Errorf("failed to send IK init: %v", err)
	}

	return nil
}

func (m *Mesh) sendByPeerID(peerID uint32, msg []byte, typ MessageType) error {
	sess, ok := m.sessionStore.get(peerID)
	if !ok {
		return nil
	}

	enc, shouldRekey, err := sess.Encrypt(m.Conn, msg)
	if err != nil {
		return err
	}

	if err := write(m.Conn, sess.peerAddr, typ, 0, peerID, enc); err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
	}

	return nil
}

func (m *Mesh) Send(peerNoisePub []byte, payload []byte, typ MessageType) error {
	sessID, ok := m.sessionStore.getID(peerNoisePub)
	if !ok {
		return ErrNotConnected
	}
	return m.sendByPeerID(sessID, payload, typ)
}

func (m *Mesh) On(t MessageType, h Handler) {
	m.handlers.On(t, h)
}

func (m *Mesh) Shutdown() error {
	if m.Conn != nil {
		m.Conn.Close()
	}

	if err := m.invites.Save(); err != nil {
		return err
	}

	return nil
}

func (m *Mesh) GetAdvertisableAddrs() []string {
	res := make([]string, len(m.advertisableIPs))
	for i, ip := range m.advertisableIPs {
		res[i] = net.JoinHostPort(ip.String(), strconv.Itoa(m.conf.Port))
	}
	return res
}
