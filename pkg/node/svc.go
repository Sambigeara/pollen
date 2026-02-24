package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

var _ controlv1.ControlServiceServer = (*NodeService)(nil)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node        *Node
	shutdown    func()
	adminSigner *auth.AdminSigner
	creds       *auth.NodeCredentials
}

func NewNodeService(n *Node, shutdown func(), adminSigner *auth.AdminSigner, creds *auth.NodeCredentials) *NodeService {
	return &NodeService{node: n, shutdown: shutdown, adminSigner: adminSigner, creds: creds}
}

func (s *NodeService) Shutdown(_ context.Context, _ *controlv1.ShutdownRequest) (*controlv1.ShutdownResponse, error) {
	if s.shutdown == nil {
		return &controlv1.ShutdownResponse{}, errors.New("shutdown callback not configured")
	}

	go s.shutdown()

	return &controlv1.ShutdownResponse{}, nil
}

func (s *NodeService) GetBootstrapInfo(_ context.Context, _ *controlv1.GetBootstrapInfoRequest) (*controlv1.GetBootstrapInfoResponse, error) {
	local := s.node.store.LocalID
	rec, ok := s.node.store.Get(local)
	if !ok {
		return &controlv1.GetBootstrapInfoResponse{}, nil
	}

	addrs := make([]string, 0, len(rec.IPs))
	for _, ip := range rec.IPs {
		addrs = append(addrs, net.JoinHostPort(ip, strconv.Itoa(int(rec.LocalPort))))
	}

	return &controlv1.GetBootstrapInfoResponse{
		Self: &controlv1.BootstrapPeerInfo{
			Peer:  &controlv1.NodeRef{PeerId: local.Bytes()},
			Addrs: addrs,
		},
	}, nil
}

func (s *NodeService) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	nodes := s.node.store.AllNodes()
	localID := s.node.store.LocalID

	selfAddr := ""
	if rec, ok := s.node.store.Get(localID); ok {
		if len(rec.IPs) > 0 {
			port := rec.LocalPort
			ip := net.ParseIP(rec.IPs[0])
			if ip != nil && rec.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				port = rec.ExternalPort
			}
			selfAddr = net.JoinHostPort(rec.IPs[0], fmt.Sprintf("%d", port))
		}
	}

	var certs []*controlv1.CertificateInfo
	if s.creds != nil && s.creds.Cert != nil && s.creds.Cert.GetClaims() != nil {
		claims := s.creds.Cert.GetClaims()
		certs = append(certs, &controlv1.CertificateInfo{
			NotBeforeUnix: claims.GetNotBeforeUnix(),
			NotAfterUnix:  claims.GetNotAfterUnix(),
			CertType:      "membership",
		})
	}

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:   &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status: controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:   selfAddr,
		},
		Services:     []*controlv1.ServiceSummary{},
		Nodes:        make([]*controlv1.NodeSummary, 0, len(nodes)),
		Connections:  []*controlv1.ConnectionSummary{},
		Certificates: certs,
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
			port := int(node.LocalPort)
			if node.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				port = int(node.ExternalPort)
			}
			addrStr = (&net.UDPAddr{
				IP:   ip,
				Port: port,
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
	s.node.queueGossipEvents(s.node.store.UpsertLocalService(req.Port, name))

	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *NodeService) UnregisterService(_ context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	name := req.GetName()
	events := s.node.store.RemoveLocalServices(name)
	s.node.tun.UnregisterService(req.GetPort())
	s.node.queueGossipEvents(events)

	return &controlv1.UnregisterServiceResponse{}, nil
}

func serviceNameForPort(port uint32) string {
	return strconv.FormatUint(uint64(port), 10)
}

func (s *NodeService) ConnectPeer(ctx context.Context, req *controlv1.ConnectPeerRequest) (*controlv1.ConnectPeerResponse, error) {
	peerKey := types.PeerKeyFromBytes(req.PeerId)
	addrs := make([]*net.UDPAddr, 0, len(req.Addrs))
	for _, a := range req.Addrs {
		addr, err := net.ResolveUDPAddr("udp", a)
		if err != nil {
			return nil, fmt.Errorf("resolve address %q: %w", a, err)
		}
		addrs = append(addrs, addr)
	}
	if err := s.node.mesh.Connect(ctx, peerKey, addrs); err != nil {
		return nil, err
	}
	return &controlv1.ConnectPeerResponse{}, nil
}

func (s *NodeService) ConnectService(ctx context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	localPort, err := s.node.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.RemotePort, req.LocalPort)
	if err != nil {
		return nil, err
	}

	return &controlv1.ConnectServiceResponse{LocalPort: localPort}, nil
}

func (s *NodeService) RevokePeer(_ context.Context, req *controlv1.RevokePeerRequest) (*controlv1.RevokePeerResponse, error) {
	if s.adminSigner == nil {
		return nil, errors.New("admin key not available on this node")
	}

	rev, err := auth.SignRevocation(s.adminSigner.Priv, s.adminSigner.Trust.GetClusterId(), req.GetPeerId())
	if err != nil {
		return nil, fmt.Errorf("sign revocation: %w", err)
	}

	events := s.node.store.AddRevocation(rev)
	s.node.queueGossipEvents(events)

	return &controlv1.RevokePeerResponse{}, nil
}
