package node

import (
	"context"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

var _ controlv1.ControlServiceServer = (*NodeService)(nil)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node *Node
}

func NewNodeService(n *Node) *NodeService {
	return &NodeService{node: n}
}

func (s *NodeService) ListPeers(ctx context.Context, req *controlv1.ListPeersRequest) (*controlv1.ListPeersResponse, error) {
	known := s.node.Mesh.Peers.GetAllKnown()

	keys := make([][]byte, len(known))

	for i, k := range known {
		keys[i] = k.NoisePub
	}

	return &controlv1.ListPeersResponse{
		Keys: keys,
	}, nil
}
