package mesh

import (
	"context"
	"errors"
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
	peers          *peers.PeerStore
	localStaticKey *noise.DHKey
	noiseCS        *noise.CipherSuite
	conn           *net.UDPConn
	hsStore        *handshakeStore
	port           int
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

	go m.listen(ctx)
	go m.handleInitiators(ctx, token)

	<-ctx.Done()

	m.log.Debug("closing down...")
	m.conn.Close()
	return nil
}

func (m *Mesh) listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dg, err := read(m.conn, nil)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			m.log.Errorf("failed to read UDP packet: %v", err)
		}

		go func() {
			hs, err := m.hsStore.get(dg.tp, dg.senderID)
			if err != nil {
				m.log.Errorf("failed to get handshake state: %v", err)
			}

			if hs == nil {
				m.log.Debugf("unrecognised senderID: %d", dg.senderID)
				return
			}

			noiseConn, err := hs.progress(m.conn, dg.msg, dg.senderUDPAddr)
			if err != nil {
				m.log.Errorf("failed to progress for senderID (%d), kind (%d): %v", dg.senderID, dg.tp, err)
			}

			if noiseConn != nil {
				m.log.Infof("established connection for: %d", dg.tp)
				m.hsStore.clear(dg.senderID)
			}
		}()
	}
}

func (m *Mesh) handleInitiators(ctx context.Context, token *peerv1.Invite) error {
	if token != nil {
		hs, err := m.hsStore.initXXPsk2(token)
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
		hs, err := m.hsStore.initIK(k.StaticKey)
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

// TODO(saml) implement send.Rekey() on both every 1<<20 bytes or 1000 messages or whatever Wireguards standard is.
type noiseConn struct {
	send       *noise.CipherState
	recv       *noise.CipherState
	peerStatic []byte
}
