package node

import (
	"context"
	"net"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

var _ controlv1.ControlServiceServer = (*NodeService)(nil)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node *Node
}

func NewNodeService(n *Node) *NodeService {
	return &NodeService{node: n}
}

func (s *NodeService) JoinCluster(ctx context.Context, req *controlv1.JoinClusterRequest) (*controlv1.JoinClusterResponse, error) {
	token, err := DecodeToken(req.Token)
	if err != nil {
		return nil, err
	}

	if err := s.node.Link.JoinWithInvite(ctx, token); err != nil {
		return nil, err
	}

	return &controlv1.JoinClusterResponse{
		Self: &controlv1.NodeRef{PeerId: s.node.Store.Cluster.LocalID.Bytes()},
	}, nil
}

func (s *NodeService) CreateInvite(_ context.Context, _ *controlv1.CreateInviteRequest) (*controlv1.CreateInviteResponse, error) {
	local := s.node.Store.Cluster.LocalID
	rec, ok := s.node.Store.Cluster.Nodes.Get(local)
	if !ok || rec.Tombstone || rec.Value == nil {
		return &controlv1.CreateInviteResponse{}, nil
	}

	var ips []string
	port := ""
	for _, a := range rec.Value.Addresses {
		host, p, err := splitHostPort(a)
		if err != nil {
			continue
		}
		if host != "" {
			ips = append(ips, host)
		}
		if port == "" && p != "" {
			port = p
		}
	}

	if port == "" {
		// defensive fallback (matches your defaultUDPPort)
		port = "60611"
	}

	inv, err := NewInvite(ips, port)
	if err != nil {
		return nil, err
	}

	enc, err := EncodeToken(inv)
	if err != nil {
		return nil, err
	}

	s.node.AdmissionStore.AddInvite(inv)
	if err := s.node.AdmissionStore.Save(); err != nil {
		return nil, err
	}

	return &controlv1.CreateInviteResponse{
		Token: enc,
	}, nil
}

func (s *NodeService) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	nodes := s.node.Store.Cluster.Nodes.GetAll()

	localID := s.node.Store.Cluster.LocalID

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status: controlv1.NodeStatus_NODE_STATUS_ONLINE,
		},
		Services: []*controlv1.ServiceSummary{},
		Nodes:    make([]*controlv1.NodeSummary, 0, len(nodes)),
	}

	for key := range nodes {
		if key == localID {
			continue
		}

		addr, _ := s.node.Link.GetActivePeerAddress(key)
		out.Nodes = append(out.Nodes, &controlv1.NodeSummary{
			Node:     &controlv1.NodeRef{PeerId: key.Bytes()},
			Status:   controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:     addr,
			LastSeen: nil,
		})
	}

	return out, nil
}

func (s *NodeService) RegisterService(ctx context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	s.node.Tunnel.RegisterService(req.Port)
	return nil, nil
}

func (s *NodeService) ConnectService(ctx context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	if err := s.node.Tunnel.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.Port); err != nil {
		return nil, err
	}

	return nil, nil
}

func splitHostPort(s string) (string, string, error) {
	h, p, err := net.SplitHostPort(s)
	if err == nil {
		return h, p, nil
	}
	// tolerate "ip" without port
	if ip := net.ParseIP(s); ip != nil {
		return s, "", nil
	}
	return "", "", err
}
