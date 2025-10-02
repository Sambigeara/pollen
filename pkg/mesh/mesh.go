package mesh

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/peers"
	"github.com/sambigeara/pollen/pkg/workspace"
	"go.uber.org/zap"
)

const nodePingInternal = time.Second * 25

var handshakePrologue = []byte("pollenv1")

type Mesh struct {
	log            *zap.SugaredLogger
	port           int
	peers          *peers.PeerStore
	localStaticKey *noise.DHKey
	noiseCS        *noise.CipherSuite
	conn           *net.UDPConn
	hsStore        *handshakeStore
}

func New(peers *peers.PeerStore, pollenDir string, port int) (*Mesh, error) {
	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

	staticKey, err := GenStaticKey(cs, filepath.Join(pollenDir, workspace.CredsDir))
	if err != nil {
		return nil, fmt.Errorf("failed to load noise static key: %w", err)
	}

	return &Mesh{
		log:            zap.S().Named("mesh"),
		port:           port,
		peers:          peers,
		localStaticKey: staticKey,
		noiseCS:        &cs,
		hsStore:        newHandshakeStore(&cs, peers, staticKey),
	}, nil
}

func (m *Mesh) Start(ctx context.Context, token *peerv1.Invite) error {
	var err error
	m.conn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: m.port})
	if err != nil {
		return err
	}

	go m.listen()
	go m.handleInitiators(ctx, token)

	<-ctx.Done()

	m.conn.Close()
	return nil
}

func (m *Mesh) listen() error {
	for {
		dg, err := read(m.conn, nil)
		if err != nil {
			return err
		}
		m.log.Debugf("handling datagram from senderID (%d), kind (%d)", dg.senderID, dg.kind)

		hs, err := m.hsStore.get(dg.kind, dg.senderID)
		if err != nil {
			return err
		}

		if hs == nil {
			m.log.Debugf("unrecognised senderID: %d", dg.senderID)
			continue
		}

		// TODO(saml) can probably go in a goroutine or similar concurrency pattern when we're confident
		noiseConn, err := hs.progress(m.conn, dg.msg, dg.senderUDPAddr)
		if err != nil {
			m.log.Errorf("failed to progress for senderID (%d), kind (%d): %v", dg.senderID, dg.kind, err)
			return err
		}

		if noiseConn != nil {
			m.log.Infof("established connection for: %d", dg.kind)
			m.hsStore.clear(dg.senderID)
		}

	}
}

func (m *Mesh) handleInitiators(ctx context.Context, token *peerv1.Invite) error {
	if token != nil {
		hs, err := m.hsStore.create(handshakeKindXXPsk2Init, nil, token)
		if err != nil {
			m.log.Error("failed to create XXpsk2 handshake")
			return err
		}

		if _, err := hs.progress(m.conn, nil, nil); err != nil {
			m.log.Error("failed to start XXpsk2 handshake")
			return err
		}
	}

	// ticker := time.NewTicker(nodePingInternal)
	// defer ticker.Stop()
	// for {
	// 	select {
	// 	case <-ctx.Done():
	// 		return ctx.Err()
	// 	case <-ticker.C:

	for _, k := range m.peers.GetAllKnown() {
		hs, err := m.hsStore.create(handshakeKindIKInit, k.StaticKey, nil)
		if err != nil {
			m.log.Error("failed to create IK handshake")
			return err
		}

		if _, err := hs.progress(m.conn, nil, nil); err != nil {
			m.log.Error("failed to start IK handshake")
			return err
		}
	}

	// 	}
	// }

	return nil
}

func read(conn *net.UDPConn, buf []byte) (*datagram, error) {
	if buf == nil {
		buf = make([]byte, 2048)
	}
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	if n < 8 { // 4 bytes kind + 4 bytes senderID
		return nil, fmt.Errorf("handshake frame too short: %d", n)
	}

	return &datagram{
		kind:          handshakeKind(binary.BigEndian.Uint32(buf[:4])),
		senderID:      binary.BigEndian.Uint32(buf[4:8]),
		senderUDPAddr: addr,
		msg:           append([]byte(nil), buf[8:n]...), // copy to decouple from shared buffer
	}, nil
}

type datagram struct {
	kind          handshakeKind
	senderID      uint32
	senderUDPAddr *net.UDPAddr
	msg           []byte
}

func (m *Mesh) Shutdown(ctx context.Context) error {
	// TODO(saml) use multierr to gather?
	if m.conn != nil {
		m.conn.Close()
	}

	if err := m.peers.Save(); err != nil {
		return err
	}

	return ctx.Err()
}

type noiseConn struct {
	peerStatic []byte
	// TODO(saml) implement send.Rekey() on both every 1<<20 bytes or 1000 messages or whatever Wireguards standard is
	send *noise.CipherState
	recv *noise.CipherState
}
