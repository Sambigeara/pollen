package node

import (
	"context"
	"path/filepath"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/peers"
	"github.com/sambigeara/pollen/pkg/workspace"
	"go.uber.org/zap"
)

type Node struct {
	log  *zap.SugaredLogger
	mesh *mesh.Mesh
}

func New(port int, pollenDir string) (*Node, error) {
	log := zap.S().Named("node")

	peersStore, err := peers.Load(filepath.Join(pollenDir, workspace.PeersDir))
	if err != nil {
		log.Error("failed to load peers", zap.Error(err))
		return nil, err
	}

	mesh, err := mesh.New(peersStore, pollenDir, port)
	if err != nil {
		return nil, err
	}

	return &Node{
		log:  log,
		mesh: mesh,
	}, nil
}

func (m *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	// go m.mesh.Listen(ctx)
	// go m.mesh.ConnectPeers(ctx, token)
	if err := m.mesh.Start(ctx, token); err != nil {
		return err
	}

	<-ctx.Done()

	return m.shutdown(ctx)
}

func (m *Node) shutdown(ctx context.Context) error {
	if err := m.mesh.Shutdown(ctx); err != nil {
		return err
	}

	return ctx.Err()
}
