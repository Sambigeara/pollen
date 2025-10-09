package mesh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/peers"
	"go.uber.org/zap"
)

const nodePingInternal = time.Second * 25

var handshakePrologue = []byte("pollenv1")

type Mesh struct {
	log            *zap.SugaredLogger
	peers          *peers.PeerStore
	invites        *invites.InviteStore
	localStaticKey *noise.DHKey
	conn           *UDPConn
	handshakeStore *handshakeStore
	sessionStore   *sessionStore
	rekeyMgr       *rekeyManager
	port           int
}

func New(cs *noise.CipherSuite, staticKey *noise.DHKey, peers *peers.PeerStore, invites *invites.InviteStore, port int) (*Mesh, error) {
	return &Mesh{
		log:            zap.S().Named("mesh"),
		port:           port,
		peers:          peers,
		invites:        invites,
		localStaticKey: staticKey,
		handshakeStore: newHandshakeStore(cs, invites, staticKey),
		sessionStore:   newSessionStore(),
		rekeyMgr:       newRekeyManager(),
	}, nil
}

func (m *Mesh) Start(ctx context.Context, token *peerv1.Invite) error {
	var err error
	m.conn, err = newUDPConn(ctx, m.port)
	if err != nil {
		return err
	}

	go m.listen(ctx)
	go m.handleInitiators(ctx, token)

	<-ctx.Done()

	m.log.Debug("closing down...")
	m.conn.Close()

	return nil
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

		dg, err := read(m.conn, nil)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return g.Wait()
			}
			m.log.Errorf("read UDP: %v", err)
			continue
		}

		g.Go(func() error {
			switch dg.tp {
			case messageTypePing:
				zap.L().Named("session").Debug("...pinged!")
				return nil
			case messageTypeTransportData:
				return m.handleTransportDataMsg(dg.receiverID, dg.msg)
			}

			hs, err := m.handshakeStore.get(dg.tp, dg.senderID, dg.receiverID)
			if err != nil {
				m.log.Errorf("get hs: %v", err)
				return nil
			}
			if hs == nil {
				m.log.Debugf("unrecognised senderID: %d", dg.senderID)
				return nil
			}

			sess, peerStaticKey, err := hs.progress(m.conn, dg.msg, dg.senderUDPAddr, dg.senderID)
			if err != nil {
				m.log.Errorf("failed to progress for senderID (%d), kind (%d): %v", dg.senderID, dg.tp, err)
				return nil // TODO(saml) return err to cancel others?
			}

			if sess != nil {
				m.log.Infof("established connection for: %d", dg.tp)
				m.handshakeStore.clear(dg.senderID)

				switch t := hs.(type) {
				case *handshakeIKInit:
					m.scheduleRekey(dg.senderID, peerStaticKey)
				case *handshakeXXPsk2Init:
					m.scheduleRekey(dg.senderID, peerStaticKey)
					m.peers.Add(&peerv1.Known{
						StaticKey: peerStaticKey,
						Addr:      t.peerUDPAddr.String(),
					})
				case *handshakeIKResp:
					m.rekeyMgr.resetIfExists(dg.senderID, sessionRefreshInterval)
				case *handshakeXXPsk2Resp:
					m.peers.PromoteToPeer(peerStaticKey, t.peerUDPAddr.String())
				}

				m.sessionStore.set(peerStaticKey, dg.senderID, sess)
			}

			return nil
		})
	}
}

func (m *Mesh) handleTransportDataMsg(receiverID uint32, msg []byte) error {
	sess, ok := m.sessionStore.get(receiverID)
	if !ok {
		return nil
	}

	pt, shouldRekey, err := sess.Decrypt(msg)
	if err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(receiverID, sessionRefreshInterval)
	}

	fmt.Println(string(pt))
	return nil
}

func (m *Mesh) scheduleRekey(peerSessionID uint32, staticKey []byte) {
	t := time.AfterFunc(sessionRefreshInterval, func() {
		p, ok := m.peers.Get(staticKey)
		if !ok {
			return
		}

		if err := m.handshakeStore.initIK(m.conn, staticKey, p.Addr); err != nil {
			m.log.Error(err)
		}
		m.log.Info("successfully rekeyed")
	})

	m.rekeyMgr.set(peerSessionID, staticKey, t)
}

func (m *Mesh) handleInitiators(ctx context.Context, token *peerv1.Invite) error {
	if token != nil {
		if err := m.handshakeStore.initXXPsk2(m.conn, token); err != nil {
			m.log.Error("failed to create XXpsk2 handshake")
			return err
		}
	}

	for _, k := range m.peers.GetAllKnown() {
		if err := m.handshakeStore.initIK(m.conn, k.StaticKey, k.Addr); err != nil {
			m.log.Error("failed to create IK handshake")
			return err
		}
	}

	return nil
}

func (m *Mesh) Send(peerID uint32, msg []byte) error {
	sess, ok := m.sessionStore.get(peerID)
	if !ok {
		return nil
	}

	enc, shouldRekey, err := sess.Encrypt(m.conn, msg)
	if err != nil {
		return err
	}

	if err := write(m.conn, sess.peerAddr, messageTypeTransportData, 0, peerID, enc); err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
	}

	return nil
}

func (m *Mesh) Shutdown(ctx context.Context) error {
	// TODO(saml) use multierr to gather?
	if m.conn != nil {
		m.conn.Close()
	}

	if err := m.peers.Save(); err != nil {
		return err
	}

	if err := m.invites.Save(); err != nil {
		return err
	}

	return ctx.Err()
}
