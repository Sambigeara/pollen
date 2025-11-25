package mesh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"time"

	"golang.org/x/crypto/ed25519"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	tcpv1 "github.com/sambigeara/pollen/api/genpb/pollen/tcp/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/peers"
	"github.com/sambigeara/pollen/pkg/tcp"
	"go.uber.org/zap"
)

const (
	nodePingInternal    = time.Second * 25
	tcpHandshakeTimeout = time.Second * 5
)

var handshakePrologue = []byte("pollenv1")

type Mesh struct {
	log             *zap.SugaredLogger
	advertisableIPs []net.IP
	Peers           *peers.PeerStore
	invites         *invites.InviteStore
	noiseKey        noise.DHKey
	signingKey      ed25519.PrivateKey
	signingPubKey   ed25519.PublicKey
	conn            *UDPConn
	handshakeStore  *handshakeStore
	sessionStore    *sessionStore
	listenerStore   *tcp.Store
	rekeyMgr        *rekeyManager
	port            int
}

func New(cs *noise.CipherSuite, staticKey noise.DHKey, priv ed25519.PrivateKey, pub ed25519.PublicKey, peers *peers.PeerStore, invites *invites.InviteStore, port int) (*Mesh, error) {
	ips, err := GetAdvertisableIPs()
	if err != nil {
		return nil, err
	}

	return &Mesh{
		log:             zap.S().Named("mesh"),
		advertisableIPs: ips,
		port:            port,
		Peers:           peers,
		invites:         invites,
		noiseKey:        staticKey,
		signingKey:      priv,
		signingPubKey:   pub,
		handshakeStore:  newHandshakeStore(cs, invites, staticKey, pub),
		sessionStore:    newSessionStore(),
		listenerStore:   tcp.NewStore(),
		rekeyMgr:        newRekeyManager(),
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
				m.log.Debugw("received ping", "peer", dg.senderUDPAddr)
				return nil
			case messageTypeTransportData:
				return m.handleTransportDataMsg(dg.receiverID, dg.msg)
			case messageTypeTCPTunnelRequest:
				return m.handleTCPTunnelRequest(dg.receiverID, dg.msg)
			case messageTypeTCPTunnelResponse:
				// Tunnel frames mirror transport data: we always address the peer by receiverID, and
				// we never stash our own session ID, so senderID stays zero.
				if err := m.listenerStore.AckInflight(dg.receiverID, dg.msg); err != nil {
					return err
				}
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

			sess, peerStaticKey, peerSigPub, err := hs.progress(m.conn, dg.msg, dg.senderUDPAddr, dg.senderID)
			if err != nil {
				m.log.Errorw("failed to progress", "type", "error", err, dg.tp, "peer", dg.senderUDPAddr)
				return nil // TODO(saml) return err to cancel others?
			}

			if sess != nil {
				m.handshakeStore.clear(dg.senderID)

				var tp string
				switch t := hs.(type) {
				case *handshakeIKInit:
					m.scheduleRekey(dg.senderID, peerStaticKey)
					tp = "IKInit"
				case *handshakeXXPsk2Init:
					m.scheduleRekey(dg.senderID, peerStaticKey)
					m.Peers.Add(&peerv1.Known{
						NoisePub: peerStaticKey,
						SigPub:   peerSigPub,
						Addr:     sess.peerAddr.String()})
					tp = "XXPsk2Init"
				case *handshakeIKResp:
					m.rekeyMgr.resetIfExists(dg.senderID, sessionRefreshInterval)
					tp = "IKResp"
				case *handshakeXXPsk2Resp:
					m.Peers.PromoteToPeer(peerStaticKey, peerSigPub, t.peerUDPAddr.String())
					tp = "XXPsk2Resp"
				}

				m.log.Infow("established session", "type", tp, "peer", dg.senderUDPAddr)

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

	_, shouldRekey, err := sess.Decrypt(msg)
	if err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(receiverID, sessionRefreshInterval)
	}

	return nil
}

func (m *Mesh) scheduleRekey(peerSessionID uint32, staticKey []byte) {
	t := time.AfterFunc(sessionRefreshInterval, func() {
		p, ok := m.Peers.Get(staticKey)
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
		if err := m.handshakeStore.initXXPsk2(m.conn, token, m.signingPubKey); err != nil {
			m.log.Errorf("failed to create XXpsk2 handshake: %v", err)
			return err
		}
	}

	for _, k := range m.Peers.GetAllKnown() {
		if len(k.NoisePub) == 0 || k.Addr == "" {
			m.log.Warnf("skipping invalid peer (missing key/addr): %s", k.Addr)
			continue
		}

		if err := m.handshakeStore.initIK(m.conn, k.NoisePub, k.Addr); err != nil {
			m.log.Errorf("failed to create IK handshake for peer %s: %v", k.Addr, err)
			continue
		}
	}

	return nil
}

func (m *Mesh) Send(peerID uint32, msg []byte, typ messageType) error {
	sess, ok := m.sessionStore.get(peerID)
	if !ok {
		return nil
	}

	enc, shouldRekey, err := sess.Encrypt(m.conn, msg)
	if err != nil {
		return err
	}

	if err := write(m.conn, sess.peerAddr, typ, 0, peerID, enc); err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
	}

	return nil
}

func (m *Mesh) handleTCPTunnelRequest(peerID uint32, msg []byte) error {
	sess, ok := m.sessionStore.get(peerID)
	if !ok {
		return errors.New("peerID not recognised")
	}

	reqPlaintext, shouldRekey, err := sess.Decrypt(msg)
	if err != nil {
		return err
	}

	if shouldRekey {
		m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
	}

	reqHs := &tcpv1.Handshake{}
	if err := proto.Unmarshal(reqPlaintext, reqHs); err != nil {
		return err
	}

	peer, ok := m.Peers.Get(sess.peerNoiseKey)
	if !ok {
		// TODO(saml) this should not raise
		return errors.New("noise key not recognised")
	}

	peerCert, err := tcp.VerifyPeerAttestation(peer.SigPub, reqHs.CertDer, reqHs.Sig)
	if err != nil {
		return err
	}

	tcpSess, err := m.listenerStore.CreateSession(sess.peerNoiseKey)
	if err != nil {
		return err
	}

	ownCert, ownSig, err := tcp.GenerateEphemeralCert(m.signingKey)
	if err != nil {
		return err
	}

	addrs := make([]string, len(m.advertisableIPs))
	for i, ip := range m.advertisableIPs {
		addrs[i] = net.JoinHostPort(ip.String(), tcpSess.GetAddrPort())
	}

	respHs := &tcpv1.Handshake{
		CertDer: ownCert.Certificate[0],
		Sig:     ownSig,
		Addr:    addrs,
	}

	respBytes, err := proto.Marshal(respHs)
	if err != nil {
		return err
	}

	if err := m.Send(peerID, respBytes, messageTypeTCPTunnelResponse); err != nil {
		return err
	}

	tlsConfig := tcp.NewPinnedTLSConfig(true, ownCert, peerCert)
	if err := tcpSess.UpgradeToTLS(tlsConfig); err != nil {
		return err
	}

	// TODO(saml) remove proof of life receiver
	// go func() {
	// 	buf := make([]byte, 1024)
	// 	n, err := tcpSess.Conn.Read(buf)
	// 	if err != nil {
	// 		return
	// 	}
	// 	m.log.Infof("TCP Tunnel Accept received: %s", string(buf[:n]))

	// 	fmt.Fprintf(tcpSess.Conn, "ACK_FROM_RESPONDER\n")
	// }()

	return nil
}

func (m *Mesh) InitTCPTunnel(peerStaticKey []byte) error {
	peerID, ok := m.sessionStore.getID(peerStaticKey)
	if !ok {
		return fmt.Errorf("no active UDP session found for peer: %x", peerStaticKey)
	}

	ownTlsCert, ownSig, err := tcp.GenerateEphemeralCert(m.signingKey)
	if err != nil {
		return err
	}

	reqHs := &tcpv1.Handshake{
		CertDer: ownTlsCert.Certificate[0],
		Sig:     ownSig,
	}

	reqBytes, err := proto.Marshal(reqHs)
	if err != nil {
		return err
	}

	respCh := make(chan []byte)
	if err := m.listenerStore.SetInflight(peerID, respCh); err != nil {
		return err
	}
	defer m.listenerStore.ExpireInflight(peerID)

	if err := m.Send(peerID, reqBytes, messageTypeTCPTunnelRequest); err != nil {
		return err
	}

	m.log.Info("Waiting for TCP handshake response...")
	t := time.NewTimer(tcpHandshakeTimeout)
	var respMsg []byte
	select {
	case respMsg = <-respCh:
		t.Stop()
	case <-t.C:
		return errors.New("timed out waiting for TCP handshake response")
	}

	sess, ok := m.sessionStore.get(peerID)
	if !ok {
		return errors.New("peer session lost during handshake")
	}

	respPlaintext, shouldRekey, err := sess.Decrypt(respMsg)
	if err != nil {
		return err
	}
	if shouldRekey {
		m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
	}

	respHs := &tcpv1.Handshake{}
	if err := proto.Unmarshal(respPlaintext, respHs); err != nil {
		return err
	}

	peer, ok := m.Peers.Get(sess.peerNoiseKey)
	if !ok {
		return errors.New("peer not recognized in store") // Should be impossible if we got here
	}

	peerCert, err := tcp.VerifyPeerAttestation(peer.SigPub, respHs.CertDer, respHs.Sig)
	if err != nil {
		return fmt.Errorf("peer attestation failed: %w", err)
	}

	m.log.Infof("Dialing TCP Tunnel to %s", respHs.Addr)
	tlsConfig := tcp.NewPinnedTLSConfig(false, ownTlsCert, peerCert)

	// iterate over candidates until one works
	var hasMatchingCandidate bool
	for _, addr := range respHs.Addr {
		if err := m.listenerStore.Dial(sess.peerNoiseKey, addr, tlsConfig); err != nil {
			// return fmt.Errorf("TCP dial failed: %w", err)
			m.log.Warnf("TCP dial failed: %w", err)
			continue
		}
		hasMatchingCandidate = true
	}

	if !hasMatchingCandidate {
		return ErrNoResolvableIP
	}

	// TODO(saml) remove proof of life
	// go func() {
	// 	conn, ok := m.listenerStore.GetConn(sess.peerNoiseKey)
	// 	if !ok {
	// 		return
	// 	}

	// 	fmt.Fprintf(conn, "HELLO_FROM_TCP_TUNNEL\n")

	// 	buf := make([]byte, 1024)
	// 	// Set a read deadline so we don't hang forever if they don't reply
	// 	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	// 	n, err := conn.Read(buf)
	// 	if err == nil {
	// 		m.log.Infof("SUCCESS: Received on TCP Tunnel: %s", string(buf[:n]))
	// 	} else {
	// 		m.log.Warnf("TCP Tunnel connected, but read failed: %v", err)
	// 	}
	// 	conn.SetReadDeadline(time.Time{}) // Clear deadline
	// }()

	return nil
}

func (m *Mesh) Shutdown(ctx context.Context) error {
	// TODO(saml) use multierr to gather?
	if m.conn != nil {
		m.conn.Close()
	}

	if err := m.Peers.Save(); err != nil {
		return err
	}

	if err := m.invites.Save(); err != nil {
		return err
	}

	m.listenerStore.Shutdown()

	return ctx.Err()
}
