package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

var _ controlv1.ControlServiceServer = (*NodeService)(nil)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node     *Node
	shutdown func()
	creds    *auth.NodeCredentials
}

func NewNodeService(n *Node, shutdown func(), creds *auth.NodeCredentials) *NodeService {
	return &NodeService{node: n, shutdown: shutdown, creds: creds}
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

	resp := &controlv1.GetBootstrapInfoResponse{
		Self: nodeBootstrapInfo(local, rec.IPs, rec.LocalPort),
	}

	resp.Recommended = s.pickRecommendedPeer(local)

	return resp, nil
}

func (s *NodeService) pickRecommendedPeer(localID types.PeerKey) *controlv1.BootstrapPeerInfo {
	nodes := s.node.store.AllNodes()

	var candidates []types.PeerKey
	for peerID, rec := range nodes {
		if peerID == localID || !rec.PubliclyAccessible {
			continue
		}
		if len(rec.IPs) == 0 || rec.LocalPort == 0 {
			continue
		}
		candidates = append(candidates, peerID)
	}

	if len(candidates) > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Less(candidates[j])
		})
		best := candidates[0]
		rec := nodes[best]
		return nodeBootstrapInfo(best, rec.IPs, rec.LocalPort)
	}

	localRec, ok := nodes[localID]
	if !ok || len(localRec.IPs) == 0 || localRec.LocalPort == 0 {
		return nil
	}
	return nodeBootstrapInfo(localID, localRec.IPs, localRec.LocalPort)
}

func nodeBootstrapInfo(peerID types.PeerKey, ips []string, port uint32) *controlv1.BootstrapPeerInfo {
	addrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, net.JoinHostPort(ip, strconv.Itoa(int(port))))
	}
	return &controlv1.BootstrapPeerInfo{
		Peer:  &controlv1.NodeRef{PeerId: peerID.Bytes()},
		Addrs: addrs,
	}
}

func (s *NodeService) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	nodes := s.node.store.AllNodes()
	localID := s.node.store.LocalID

	selfAddr := ""
	selfPubliclyAccessible := false
	if rec, ok := s.node.store.Get(localID); ok {
		selfPubliclyAccessible = rec.PubliclyAccessible
		if len(rec.IPs) > 0 {
			port := rec.LocalPort
			ip := net.ParseIP(rec.IPs[0])
			if ip != nil && rec.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				port = rec.ExternalPort
			}
			selfAddr = net.JoinHostPort(rec.IPs[0], fmt.Sprintf("%d", port))
		}
	}

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:               &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status:             controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:               selfAddr,
			PubliclyAccessible: selfPubliclyAccessible,
		},
		Services:    []*controlv1.ServiceSummary{},
		Nodes:       make([]*controlv1.NodeSummary, 0, len(nodes)),
		Connections: []*controlv1.ConnectionSummary{},
	}

	if s.creds != nil && s.creds.Cert != nil {
		claims := s.creds.Cert.GetClaims()
		health := controlv1.CertHealth_CERT_HEALTH_OK
		remaining := time.Until(auth.CertExpiresAt(s.creds.Cert))
		switch {
		case remaining <= 0:
			health = controlv1.CertHealth_CERT_HEALTH_EXPIRED
		case remaining <= certWarnThreshold:
			health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
		}
		out.Certificates = append(out.Certificates, &controlv1.CertInfo{
			NotBeforeUnix: claims.GetNotBeforeUnix(),
			NotAfterUnix:  claims.GetNotAfterUnix(),
			Serial:        claims.GetSerial(),
			Health:        health,
		})
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
			Node:               &controlv1.NodeRef{PeerId: key.Bytes()},
			Status:             status,
			Addr:               addrStr,
			PubliclyAccessible: node.PubliclyAccessible,
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
		name = strconv.FormatUint(uint64(req.Port), 10)
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
	if s.creds.InviteSigner == nil {
		return nil, errors.New("admin signer not available; this node cannot revoke peers")
	}

	signerPub, ok := s.creds.InviteSigner.Priv.Public().(ed25519.PublicKey)
	if !ok || !bytes.Equal(signerPub, s.creds.InviteSigner.Trust.GetRootPub()) {
		return nil, errors.New("only the root admin can revoke peers")
	}

	peerKey := types.PeerKeyFromBytes(req.GetPeerId())
	now := time.Now()

	rev, err := auth.IssueRevocation(
		s.creds.InviteSigner.Priv,
		s.creds.InviteSigner.Trust.GetClusterId(),
		peerKey.Bytes(),
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("issue revocation: %w", err)
	}

	events := s.node.store.PublishRevocation(rev)
	s.node.queueGossipEvents(events)

	return &controlv1.RevokePeerResponse{}, nil
}
