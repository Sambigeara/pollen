package node

import (
	"context"
	"errors"
	"fmt"
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

	if err := s.node.sock.JoinWithInvite(ctx, token); err != nil {
		return nil, err
	}

	return &controlv1.JoinClusterResponse{
		Self: &controlv1.NodeRef{PeerId: s.node.storage.Cluster.LocalID.Bytes()},
	}, nil
}

func (s *NodeService) CreateInvite(_ context.Context, _ *controlv1.CreateInviteRequest) (*controlv1.CreateInviteResponse, error) {
	local := s.node.storage.Cluster.LocalID
	rec, ok := s.node.storage.Cluster.Nodes.Get(local)
	if !ok || rec.Tombstone || rec.Node == nil {
		return &controlv1.CreateInviteResponse{}, nil
	}

	port := strconv.Itoa(int(rec.Node.LocalPort))
	ips := make([]string, 0, len(rec.Node.Ips))
	ips = append(ips, rec.Node.Ips...)

	inv, err := NewInvite(ips, port)
	if err != nil {
		return nil, err
	}

	enc, err := EncodeToken(inv)
	if err != nil {
		return nil, err
	}

	s.node.invites.AddInvite(inv)
	if err := s.node.invites.Save(); err != nil {
		return nil, err
	}

	return &controlv1.CreateInviteResponse{
		Token: enc,
	}, nil
}

func (s *NodeService) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	nodes := s.node.storage.Cluster.Nodes.GetAll()
	localID := s.node.storage.Cluster.LocalID

	selfAddr := ""
	if rec, ok := s.node.storage.Cluster.Nodes.Get(localID); ok {
		if rec.Node != nil && len(rec.Node.Ips) > 0 {
			selfAddr = net.JoinHostPort(rec.Node.Ips[0], fmt.Sprintf("%d", rec.Node.LocalPort))
		}
	}

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status: controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:   selfAddr,
		},
		Services:    []*controlv1.ServiceSummary{},
		Nodes:       make([]*controlv1.NodeSummary, 0, len(nodes)),
		Connections: []*controlv1.ConnectionSummary{},
	}

	for key, node := range nodes {
		if key == localID {
			continue
		}

		addr, online := s.node.sock.GetActivePeerAddress(key)
		status := controlv1.NodeStatus_NODE_STATUS_OFFLINE
		if online {
			status = controlv1.NodeStatus_NODE_STATUS_ONLINE
		}
		if !online && node != nil && len(node.Ips) > 0 {
			ip := net.ParseIP(node.Ips[0])
			if ip == nil {
				return nil, errors.New("invalid IP address")
			}
			addr = &net.UDPAddr{
				IP:   ip,
				Port: int(node.LocalPort),
			}
		}

		out.Nodes = append(out.Nodes, &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: key.Bytes()},
			Status: status,
			Addr:   addr.String(),
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

	connections := s.node.tun.ListConnections()
	for _, c := range connections {
		name := ""
		if node, ok := nodes[c.PeerID]; ok && node != nil {
			for _, svc := range node.Services {
				if svc.GetPort() == c.RemotePort {
					name = svc.GetName()
					break
				}
			}
		}
		out.Connections = append(out.Connections, &controlv1.ConnectionSummary{
			Peer:        &controlv1.NodeRef{PeerId: c.PeerID.Bytes()},
			RemotePort:  c.RemotePort,
			LocalPort:   c.LocalPort,
			ServiceName: name,
		})
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

	sort.Slice(out.Connections, func(i, j int) bool {
		if out.Connections[i].LocalPort != out.Connections[j].LocalPort {
			return out.Connections[i].LocalPort < out.Connections[j].LocalPort
		}
		return types.PeerKeyFromBytes(out.Connections[i].Peer.PeerId).Less(types.PeerKeyFromBytes(out.Connections[j].Peer.PeerId))
	})

	return out, nil
}

func (s *NodeService) RegisterService(ctx context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	s.node.tun.RegisterService(req.Port)

	name := req.GetName()
	if name == "" {
		name = serviceNameForPort(req.Port)
	}

	localID := s.node.storage.Cluster.LocalID
	rec, ok := s.node.storage.Cluster.Nodes.Get(localID)
	if !ok || rec.Node == nil {
		return &controlv1.RegisterServiceResponse{}, nil
	}

	node := cloneNode(rec.Node)
	node.Services = upsertService(node.Services, req.Port, name)
	s.node.storage.Cluster.Nodes.Set(localID, node)

	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *NodeService) UnregisterService(ctx context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	localID := s.node.storage.Cluster.LocalID
	rec, ok := s.node.storage.Cluster.Nodes.Get(localID)
	if !ok || rec.Node == nil {
		return &controlv1.UnregisterServiceResponse{}, nil
	}

	port := req.GetPort()
	name := req.GetName()
	updated, removed := removeServices(rec.Node.Services, port, name)
	if len(removed) == 0 {
		return &controlv1.UnregisterServiceResponse{}, nil
	}
	for _, p := range removed {
		s.node.tun.UnregisterService(p)
	}

	node := cloneNode(rec.Node)
	node.Services = updated
	s.node.storage.Cluster.Nodes.Set(localID, node)

	return &controlv1.UnregisterServiceResponse{}, nil
}

func cloneNode(node *statev1.Node) *statev1.Node {
	if node == nil {
		return &statev1.Node{}
	}
	return &statev1.Node{
		Id:        node.Id,
		Name:      node.Name,
		Ips:       append([]string(nil), node.Ips...),
		LocalPort: node.LocalPort,
		Keys:      node.Keys,
		Services:  append([]*statev1.Service(nil), node.Services...),
	}
}

func upsertService(services []*statev1.Service, port uint32, name string) []*statev1.Service {
	updated := make([]*statev1.Service, 0, len(services)+1)
	seen := false
	for _, svc := range services {
		if svc.GetPort() == port {
			updated = append(updated, &statev1.Service{Name: name, Port: port})
			seen = true
			continue
		}
		updated = append(updated, svc)
	}
	if !seen {
		updated = append(updated, &statev1.Service{Name: name, Port: port})
	}
	return updated
}

func removeServices(services []*statev1.Service, port uint32, name string) ([]*statev1.Service, []uint32) {
	updated := make([]*statev1.Service, 0, len(services))
	removed := make([]uint32, 0, len(services))
	for _, svc := range services {
		if matchesService(svc, port, name) {
			removed = append(removed, svc.GetPort())
			continue
		}
		updated = append(updated, svc)
	}
	return updated, removed
}

func matchesService(svc *statev1.Service, port uint32, name string) bool {
	if svc == nil {
		return false
	}
	if port != 0 && svc.GetPort() == port {
		return true
	}
	if name == "" {
		return false
	}
	if svc.GetName() == name {
		return true
	}
	return svc.GetName() == "" && serviceNameForPort(svc.GetPort()) == name
}

func serviceNameForPort(port uint32) string {
	return strconv.FormatUint(uint64(port), 10)
}

func (s *NodeService) ConnectService(ctx context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	localPort, err := s.node.tun.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.RemotePort, req.LocalPort)
	if err != nil {
		return nil, err
	}

	return &controlv1.ConnectServiceResponse{LocalPort: localPort}, nil
}
