package node

import (
	"context"
	"net"
	"sort"
	"strconv"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
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
	if !ok || rec.Tombstone || rec.Node == nil {
		return &controlv1.CreateInviteResponse{}, nil
	}

	var ips []string
	port := ""
	for _, a := range rec.Node.Addresses {
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

	selfAddr := ""
	if rec, ok := s.node.Store.Cluster.Nodes.Get(localID); ok {
		if rec.Node != nil && len(rec.Node.Addresses) > 0 {
			selfAddr = rec.Node.Addresses[0]
		}
	}

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status: controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:   selfAddr,
		},
		Services: []*controlv1.ServiceSummary{},
		Nodes:    make([]*controlv1.NodeSummary, 0, len(nodes)),
	}

	for key, node := range nodes {
		if key == localID {
			continue
		}

		addr, online := s.node.Link.GetActivePeerAddress(key)
		status := controlv1.NodeStatus_NODE_STATUS_OFFLINE
		if online {
			status = controlv1.NodeStatus_NODE_STATUS_ONLINE
		}
		if !online && node != nil && len(node.Addresses) > 0 {
			addr = node.Addresses[0]
		}

		out.Nodes = append(out.Nodes, &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: key.Bytes()},
			Status: status,
			Addr:   addr,
		})
	}

	for key, node := range nodes {
		if node == nil {
			continue
		}
		for _, svc := range node.Services {
			name := svc.GetName()
			if name == "" {
				name = strconv.FormatUint(uint64(svc.GetPort()), 10)
			}
			out.Services = append(out.Services, &controlv1.ServiceSummary{
				Name:     name,
				Provider: &controlv1.NodeRef{PeerId: key.Bytes()},
				Port:     svc.GetPort(),
			})
		}
	}

	sort.Slice(out.Nodes, func(i, j int) bool {
		li := out.Nodes[i].Status == controlv1.NodeStatus_NODE_STATUS_ONLINE
		lj := out.Nodes[j].Status == controlv1.NodeStatus_NODE_STATUS_ONLINE
		if li != lj {
			return li
		}
		return types.PeerKeyFromBytes(out.Nodes[i].Node.PeerId).Less(types.PeerKeyFromBytes(out.Nodes[j].Node.PeerId))
	})

	sort.Slice(out.Services, func(i, j int) bool {
		a := out.Services[i]
		b := out.Services[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Port != b.Port {
			return a.Port < b.Port
		}
		return types.PeerKeyFromBytes(a.Provider.PeerId).Less(types.PeerKeyFromBytes(b.Provider.PeerId))
	})

	return out, nil
}

func (s *NodeService) RegisterService(ctx context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	s.node.Tunnel.RegisterService(req.Port)

	name := req.GetName()
	if name == "" {
		name = strconv.FormatUint(uint64(req.Port), 10)
	}

	localID := s.node.Store.Cluster.LocalID
	if rec, ok := s.node.Store.Cluster.Nodes.Get(localID); ok {
		if rec.Node != nil {
			node := &statev1.Node{
				Id:        rec.Node.Id,
				Name:      rec.Node.Name,
				Addresses: append([]string(nil), rec.Node.Addresses...),
				Keys:      rec.Node.Keys,
			}
			services := make([]*statev1.Service, 0, len(rec.Node.Services)+1)
			seen := false
			for _, svc := range rec.Node.Services {
				if svc.GetPort() == req.Port {
					services = append(services, &statev1.Service{Name: name, Port: req.Port})
					seen = true
					continue
				}
				services = append(services, svc)
			}
			if !seen {
				services = append(services, &statev1.Service{Name: name, Port: req.Port})
			}
			node.Services = services
			s.node.Store.Cluster.Nodes.Set(localID, node)
			trigger(s.node.gossipNow)
		}
	}

	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *NodeService) ConnectService(ctx context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	localPort, err := s.node.Tunnel.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.RemotePort, req.LocalPort)
	if err != nil {
		return nil, err
	}

	return &controlv1.ConnectServiceResponse{LocalPort: localPort}, nil
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
