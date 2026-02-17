package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"

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

	if err := s.node.mesh.JoinWithInvite(ctx, token); err != nil {
		return nil, err
	}

	return &controlv1.JoinClusterResponse{
		Self: &controlv1.NodeRef{PeerId: s.node.store.LocalID.Bytes()},
	}, nil
}

func (s *NodeService) CreateInvite(_ context.Context, _ *controlv1.CreateInviteRequest) (*controlv1.CreateInviteResponse, error) {
	local := s.node.store.LocalID
	rec, ok := s.node.store.Get(local)
	if !ok {
		return &controlv1.CreateInviteResponse{}, nil
	}

	port := strconv.Itoa(int(rec.LocalPort))
	ips := append([]string(nil), rec.IPs...)

	inv, err := NewInvite(ips, port, s.node.identityPub)
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
	nodes := s.node.store.CloneNodes()
	localID := s.node.store.LocalID

	selfAddr := ""
	if rec, ok := s.node.store.Get(localID); ok {
		if len(rec.IPs) > 0 {
			selfAddr = net.JoinHostPort(rec.IPs[0], fmt.Sprintf("%d", rec.LocalPort))
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

		addr, online := s.node.mesh.GetActivePeerAddress(key)
		status := controlv1.NodeStatus_NODE_STATUS_OFFLINE
		if online {
			status = controlv1.NodeStatus_NODE_STATUS_ONLINE
		}

		addrStr := ""
		if addr != nil {
			addrStr = addr.String()
		}
		if addrStr == "" && len(node.IPs) > 0 {
			ip := net.ParseIP(node.IPs[0])
			if ip == nil {
				return nil, errors.New("invalid IP address")
			}
			addrStr = (&net.UDPAddr{
				IP:   ip,
				Port: int(node.LocalPort),
			}).String()
		}

		out.Nodes = append(out.Nodes, &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: key.Bytes()},
			Status: status,
			Addr:   addrStr,
		})
	}

	for key, node := range nodes {
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
		if node, ok := nodes[c.PeerID]; ok {
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
		const (
			nodeStatusRankOnline = iota
			nodeStatusRankRelay
			nodeStatusRankOffline
		)

		rank := func(status controlv1.NodeStatus) int {
			switch status {
			case controlv1.NodeStatus_NODE_STATUS_ONLINE:
				return nodeStatusRankOnline
			case controlv1.NodeStatus_NODE_STATUS_RELAY:
				return nodeStatusRankRelay
			default:
				return nodeStatusRankOffline
			}
		}
		ri := rank(out.Nodes[i].Status)
		rj := rank(out.Nodes[j].Status)
		if ri != rj {
			return ri < rj
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

func (s *NodeService) RegisterService(_ context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	s.node.tun.RegisterService(req.Port)

	name := req.GetName()
	if name == "" {
		name = serviceNameForPort(req.Port)
	}
	s.node.queueGossipNode(s.node.store.UpsertLocalService(req.Port, name))

	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *NodeService) UnregisterService(_ context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	port := req.GetPort()
	name := req.GetName()
	update := s.node.store.RemoveLocalServices(port, name)
	s.node.tun.UnregisterService(port)
	s.node.queueGossipNode(update)

	return &controlv1.UnregisterServiceResponse{}, nil
}

func serviceNameForPort(port uint32) string {
	return strconv.FormatUint(uint64(port), 10)
}

func (s *NodeService) ConnectService(ctx context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	localPort, err := s.node.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.RemotePort, req.LocalPort)
	if err != nil {
		return nil, err
	}

	return &controlv1.ConnectServiceResponse{LocalPort: localPort}, nil
}
