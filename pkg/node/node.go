package node

import (
	"context"
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

type Node struct {
	log  *zap.SugaredLogger
	addr string
	mesh *noiseMesh
}

func New(addr string, pollenDir string) (*Node, error) {
	log := zap.S().Named("node")

	peersStore, err := peers.Load(filepath.Join(pollenDir, workspace.PeersDir))
	if err != nil {
		log.Error("failed to load peers", zap.Error(err))
		return nil, err
	}

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

	noiseKey, err := GenLocalStaticKey(cs, filepath.Join(pollenDir, workspace.CredsDir))
	if err != nil {
		return nil, fmt.Errorf("failed to load noise static key: %w", err)
	}

	mesh, err := newNoiseMesh(peersStore, cs, noiseKey, addr)
	if err != nil {
		return nil, err
	}

	return &Node{
		log:  log,
		addr: addr,
		mesh: mesh,
	}, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	go n.listen(ctx)
	go n.connectPeers(ctx, token)

	<-ctx.Done()
	return n.shutdown(ctx)
}

func (n *Node) listen(ctx context.Context) error {
	// TODO(saml) change to UDP
	ln, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		insecureConn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				n.log.Errorf("Accept error: %v", err)
				continue
			}
		}

		go n.handleInitiatorConn(insecureConn)
	}
}

func (n *Node) handleInitiatorConn(conn net.Conn) {
	noiseConn, err := n.mesh.performResponderNoiseHandshake(conn)
	if err != nil {
		n.log.Errorf("Noise handshake failed: %v", err)
		conn.Close()
		return
	}

	n.mesh.updateConn(string(noiseConn.peerStatic), noiseConn)

	if err := n.consumePeers(noiseConn); err != nil {
		n.log.Errorf("Failed to consume peer data: %v", err)
	}
}

func (n *Node) consumePeers(_ net.Conn) error {
	return nil
}

func (n *Node) connectPeers(ctx context.Context, token *peerv1.Invite) error {
	n.mesh.establishPublishConns(token)

	ticker := time.NewTicker(nodePingInternal)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n.mesh.establishPublishConns(nil)
		}
	}
}

func (n *Node) shutdown(ctx context.Context) error {
	if err := n.mesh.shutdown(ctx); err != nil {
		return err
	}

	return ctx.Err()
}
