package mesh

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/ed25519"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	tcpv1 "github.com/sambigeara/pollen/api/genpb/pollen/tcp/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/tcp"
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

type Mesh struct {
	log             *zap.SugaredLogger
	conf            *Config
	Cluster         *state.Cluster
	invites         *invites.InviteStore
	signingKey      ed25519.PrivateKey
	signingPubKey   ed25519.PublicKey
	handshakeStore  *handshakeStore
	sessionStore    *sessionStore
	listenerStore   *tcp.Store
	rekeyMgr        *rekeyManager
	Conn            *UDPConn
	tunnelHandler   func(net.Conn)
	gossipHandler   func([]byte) error
	testHandler     func([]byte)
	handshakeMu     sync.Mutex
	advertisableIPs []net.IP
}

type Config struct {
	PeerReconcileInterval time.Duration
	GossipInterval        time.Duration
	Port                  int
	AdvertisedIPs         []string
}

func New(cs *noise.CipherSuite, staticKey noise.DHKey, priv ed25519.PrivateKey, pub ed25519.PublicKey, cluster *state.Cluster, invites *invites.InviteStore, conf *Config) (*Mesh, error) {
	var ips []net.IP

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
		Cluster:         cluster,
		invites:         invites,
		signingKey:      priv,
		signingPubKey:   pub,
		handshakeStore:  newHandshakeStore(cs, invites, staticKey, pub),
		sessionStore:    newSessionStore(),
		listenerStore:   tcp.NewStore(),
		rekeyMgr:        newRekeyManager(),
		advertisableIPs: ips,
	}, nil
}

func (m *Mesh) SetGossipHandler(h func([]byte) error) {
	m.gossipHandler = h
}

func (m *Mesh) SetTestHandler(h func([]byte)) {
	m.testHandler = h
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

	go m.startGossip(ctx)

	go m.maintainConnections(ctx)

	<-ctx.Done()

	m.log.Debug("closing down...")
	m.Conn.Close()

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
			case MessageTypeTest:
				return m.handleTestMsg(dg.receiverID, dg.msg)
			case MessageTypeTransportData:
				return m.handleTransportDataMsg(dg.receiverID, dg.msg)
			case MessageTypeGossip:
				return m.handleGossipMsg(dg.receiverID, dg.msg)
			case MessageTypeTCPTunnelRequest:
				return m.handleTCPTunnelRequest(dg.receiverID, dg.msg)
			case MessageTypeTCPTunnelResponse:
				// tunnel frames mirror transport data: we always address the peer by receiverID, and
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
					m.scheduleRekey(dg.senderID, res.PeerStaticKey)
					tp = "IKInit"
				case *handshakeXXPsk2Init: // receiver of a token join
					m.scheduleRekey(dg.senderID, res.PeerStaticKey)
					tp = "XXPsk2Init"
				case *handshakeIKResp: // initiator of a known join
					m.rekeyMgr.resetIfExists(dg.senderID, sessionRefreshInterval)
					tp = "IKResp"
				case *handshakeXXPsk2Resp: // initiator of a token join
					tp = "XXPsk2Resp"
				}

				// TODO(saml): we currently rely on the Node state to determine which peers
				// are reachable, we set a temp placeholder to ensure they can receive gossip messages.
				// This needs to be made better.
				peerNodeID := hex.EncodeToString(res.PeerStaticKey)
				m.Cluster.Nodes.SetPlaceholder(peerNodeID, &statev1.Node{
					Id:        peerNodeID,
					Addresses: []string{dg.senderUDPAddr.String()},
					Keys: &statev1.Keys{
						NoisePub:    res.PeerStaticKey,
						IdentityPub: res.PeerSigPub,
					},
				})

				m.log.Infow("established session", "type", tp, "peer", dg.senderUDPAddr)

				// Register session under both our local and the peer's session IDs.
				// - res.LocalID: our local session identifier
				// - dg.senderID: the peer's local session identifier (as seen in the last handshake frame)
				m.sessionStore.set(res.PeerStaticKey, res.LocalSessionID, dg.senderID, res.Session)
			}

			return nil
		})
	}
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

func (m *Mesh) handleTestMsg(receiverID uint32, msg []byte) error {
	sess, ok := m.sessionStore.get(receiverID)
	if !ok {
		return nil
	}

	pt, shouldRekey, err := sess.Decrypt(msg)
	if err != nil {
		return err
	}

	if shouldRekey {
		if peerID, ok := m.sessionStore.getID(sess.peerNoiseKey); ok {
			m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
		}
	}

	if m.testHandler != nil {
		m.testHandler(pt)
	}
	return nil
}

func (m *Mesh) handleGossipMsg(receiverID uint32, msg []byte) error {
	sess, ok := m.sessionStore.get(receiverID)
	if !ok {
		return nil
	}

	pt, shouldRekey, err := sess.Decrypt(msg)
	if err != nil {
		return err
	}

	if shouldRekey {
		if peerID, ok := m.sessionStore.getID(sess.peerNoiseKey); ok {
			m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
		}
	}

	if m.gossipHandler != nil {
		if err := m.gossipHandler(pt); err != nil {
			m.log.Warnf("failed to handle gossip: %v", err)
		}
	}
	return nil
}

func (m *Mesh) scheduleRekey(peerSessionID uint32, staticKey []byte) {
	t := time.AfterFunc(sessionRefreshInterval, func() {
		n, ok := m.Cluster.Nodes.Get(hex.EncodeToString(staticKey))
		if !ok {
			return
		}

		res, err := m.handshakeStore.initIK(staticKey, n.Value.Addresses)
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

func (m *Mesh) startGossip(ctx context.Context) {
	ticker := time.NewTicker(m.conf.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.gossip()
		}
	}
}

func (m *Mesh) gossip() {
	// TODO(saml) for now we're just gossiping to all, but down the line, this'll be much more selective
	peers := m.Cluster.Nodes.GetAll()

	// we're the only peer
	if len(peers) == 1 {
		return
	}

	delta := &statev1.DeltaState{
		Nodes: state.ToNodeDelta(m.Cluster.Nodes),
	}

	payload, err := delta.MarshalVT()
	if err != nil {
		m.log.Errorf("failed to marshal gossip: %v", err)
		return
	}

	for _, peer := range peers {
		if peer.Id == m.Cluster.LocalID {
			continue
		}
		m.Send(peer.Keys.NoisePub, payload, MessageTypeGossip)
	}
}

func (m *Mesh) maintainConnections(ctx context.Context) {
	ticker := time.NewTicker(m.conf.PeerReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcilePeers()
		}
	}
}

func (m *Mesh) reconcilePeers() {
	for _, node := range m.Cluster.Nodes.GetAll() {
		if node.Id == m.Cluster.LocalID {
			continue
		}

		if len(node.Keys.NoisePub) == 0 || len(node.Addresses) == 0 {
			continue
		}

		if _, exists := m.sessionStore.getID(node.Keys.NoisePub); exists {
			continue // already connected
		}

		m.log.Infow("dialing new peer found in state", "peer_id", node.Id[:8], "addr", node.Addresses[0])

		res, err := m.handshakeStore.initIK(node.Keys.NoisePub, node.Addresses)
		if err != nil {
			m.log.Errorf("failed to create IK handshake for peer %s: %v", node.Addresses[0], err)
			continue
		}

		// Send Init packet
		if err := m.sendHandshakeResult(res, 0); err != nil {
			m.log.Errorf("failed to send IK init: %v", err)
		}
	}
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

func (m *Mesh) Send(staticKey []byte, msg []byte, t MessageType) error {
	sessID, ok := m.sessionStore.getID(staticKey)
	if !ok {
		return ErrNotConnected
	}
	return m.sendByPeerID(sessID, msg, t)
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

	peer, ok := m.Cluster.Nodes.Get(hex.EncodeToString(sess.peerNoiseKey))
	if !ok {
		// TODO(saml) this should not raise
		return errors.New("noise key not recognised")
	}

	peerCert, err := tcp.VerifyPeerAttestation(peer.Value.Keys.IdentityPub, reqHs.CertDer, reqHs.Sig)
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

	if err := m.sendByPeerID(peerID, respBytes, MessageTypeTCPTunnelResponse); err != nil {
		return err
	}

	tlsConfig := tcp.NewPinnedTLSConfig(true, ownCert, peerCert)
	if err := tcpSess.UpgradeToTLS(tlsConfig); err != nil {
		return err
	}

	if m.tunnelHandler != nil {
		go m.tunnelHandler(tcpSess.Conn)
	} else {
		m.log.Warn("Incoming TCP tunnel established, but no handler registered")
		tcpSess.Conn.Close()
	}
	return nil
}

func (m *Mesh) SetTunnelHandler(h func(net.Conn)) {
	m.tunnelHandler = h
}

func (m *Mesh) InitTCPTunnel(peerStaticKey []byte) (net.Conn, error) {
	m.handshakeMu.Lock()
	defer m.handshakeMu.Unlock()

	peerID, ok := m.sessionStore.getID(peerStaticKey)

	if !ok {
		return nil, fmt.Errorf("no active UDP session found for peer: %x", peerStaticKey)
	}

	ownTlsCert, ownSig, err := tcp.GenerateEphemeralCert(m.signingKey)
	if err != nil {
		return nil, err
	}

	reqHs := &tcpv1.Handshake{
		CertDer: ownTlsCert.Certificate[0],
		Sig:     ownSig,
	}

	reqBytes, err := proto.Marshal(reqHs)
	if err != nil {
		return nil, err
	}

	respCh := make(chan []byte)
	if err := m.listenerStore.SetInflight(peerID, respCh); err != nil {
		return nil, err
	}
	defer m.listenerStore.ExpireInflight(peerID)

	if err := m.sendByPeerID(peerID, reqBytes, MessageTypeTCPTunnelRequest); err != nil {
		return nil, err
	}

	m.log.Info("Waiting for TCP handshake response...")
	t := time.NewTimer(tcpHandshakeTimeout)
	var respMsg []byte
	select {
	case respMsg = <-respCh:
		t.Stop()
	case <-t.C:
		return nil, errors.New("timed out waiting for TCP handshake response")
	}

	sess, ok := m.sessionStore.get(peerID)
	if !ok {
		return nil, errors.New("peer session lost during handshake")
	}

	respPlaintext, shouldRekey, err := sess.Decrypt(respMsg)
	if err != nil {
		return nil, err
	}
	if shouldRekey {
		m.rekeyMgr.resetIfExists(peerID, sessionRefreshInterval)
	}

	respHs := &tcpv1.Handshake{}
	if err := proto.Unmarshal(respPlaintext, respHs); err != nil {
		return nil, err
	}

	// peer, ok := m.Peers.Get(sess.peerNoiseKey)
	peer, ok := m.Cluster.Nodes.Get(hex.EncodeToString(sess.peerNoiseKey))
	if !ok {
		return nil, errors.New("peer not recognized in store") // Should be impossible if we got here
	}

	peerCert, err := tcp.VerifyPeerAttestation(peer.Value.Keys.IdentityPub, respHs.CertDer, respHs.Sig)
	if err != nil {
		return nil, fmt.Errorf("peer attestation failed: %w", err)
	}

	m.log.Infof("Dialing TCP Tunnel to %s", respHs.Addr)
	tlsConfig := tcp.NewPinnedTLSConfig(false, ownTlsCert, peerCert)

	// iterate over candidates until one works
	var finalConn net.Conn
	for _, addr := range respHs.Addr {
		if err := m.listenerStore.Dial(sess.peerNoiseKey, addr, tlsConfig); err != nil {
			// return fmt.Errorf("TCP dial failed: %w", err)
			m.log.Warnf("TCP dial failed: %v", err)
			continue
		}
		if conn, ok := m.listenerStore.GetConn(sess.peerNoiseKey); ok {
			finalConn = conn
			break
		}
	}

	if finalConn == nil {
		return nil, ErrNoResolvableIP
	}

	return finalConn, nil
}

func (m *Mesh) Shutdown(ctx context.Context) error {
	// TODO(saml) use multierr to gather?
	if m.Conn != nil {
		m.Conn.Close()
	}

	if err := m.invites.Save(); err != nil {
		return err
	}

	m.listenerStore.Shutdown()

	return nil
}

func (m *Mesh) GetAdvertisableAddrs() []string {
	res := make([]string, len(m.advertisableIPs))
	for i, ip := range m.advertisableIPs {
		res[i] = net.JoinHostPort(ip.String(), strconv.Itoa(m.conf.Port))
	}
	return res
}
