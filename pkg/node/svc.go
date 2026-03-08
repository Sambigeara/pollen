package node

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"net"
	"slices"
	"strconv"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		return &controlv1.ShutdownResponse{}, status.Error(codes.FailedPrecondition, "shutdown callback not configured")
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
		slices.SortFunc(candidates, types.PeerKey.Compare)
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
			selfAddr = net.JoinHostPort(rec.IPs[0], strconv.Itoa(int(port)))
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
		case remaining <= certCriticalThreshold:
			health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
		case remaining <= certWarnThreshold:
			if claims.GetNonRenewable() || s.node.renewalFailed.Load() {
				health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
			} else {
				health = controlv1.CertHealth_CERT_HEALTH_RENEWING
			}
		}
		out.Certificates = append(out.Certificates, &controlv1.CertInfo{
			NotBeforeUnix: claims.GetNotBeforeUnix(),
			NotAfterUnix:  claims.GetNotAfterUnix(),
			Serial:        claims.GetSerial(),
			Health:        health,
			NonRenewable:  claims.GetNonRenewable(),
		})
	}

	connections := s.node.tun.ListConnections()
	tunnelCounts := make(map[types.PeerKey]uint32, len(connections))
	for _, c := range connections {
		tunnelCounts[c.PeerID]++
	}

	if rec, ok := s.node.store.Get(localID); ok {
		out.Self.CpuPercent = rec.CPUPercent
		out.Self.MemPercent = rec.MemPercent
	}
	out.Self.TunnelCount = uint32(len(connections))

	// Determine which peers have direct QUIC sessions (ONLINE).
	// Store the address so we don't call GetActivePeerAddress again per peer
	// (avoids a TOCTOU race where a session appears/disappears between calls).
	directPeers := make(map[types.PeerKey]*net.UDPAddr)
	for key := range nodes {
		if key == localID {
			continue
		}
		if addr, ok := s.node.mesh.GetActivePeerAddress(key); ok {
			directPeers[key] = addr
		}
	}

	// BFS from directly-connected peers through gossip Reachable edges to
	// find the indirectly-reachable component. Only trust edges from nodes
	// that are themselves proven reachable (direct or already discovered
	// via traversal), so stale edges from partitioned nodes are ignored.
	indirectPeers := make(map[types.PeerKey]struct{})
	queue := make([]types.PeerKey, 0, len(directPeers))
	for p := range directPeers {
		queue = append(queue, p)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rec, ok := nodes[cur]
		if !ok {
			continue
		}
		for peer := range rec.Reachable {
			if peer == localID {
				continue
			}
			if _, ok := directPeers[peer]; ok {
				continue
			}
			if _, ok := indirectPeers[peer]; ok {
				continue
			}
			indirectPeers[peer] = struct{}{}
			queue = append(queue, peer)
		}
	}

	for key, node := range nodes {
		if key == localID {
			continue
		}

		addr, isDirect := directPeers[key]
		status := controlv1.NodeStatus_NODE_STATUS_OFFLINE
		if isDirect {
			status = controlv1.NodeStatus_NODE_STATUS_ONLINE
		} else if _, ok := indirectPeers[key]; ok {
			status = controlv1.NodeStatus_NODE_STATUS_INDIRECT
		}

		addrStr := ""
		if addr != nil {
			addrStr = addr.String()
		}
		if addrStr == "" && len(node.IPs) > 0 {
			ip := net.ParseIP(node.IPs[0])
			if ip == nil {
				s.node.log.Warnw("skipping unparseable peer IP", "peer", key.Short(), "ip", node.IPs[0])
			} else {
				port := int(node.LocalPort)
				if node.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
					port = int(node.ExternalPort)
				}
				addrStr = (&net.UDPAddr{
					IP:   ip,
					Port: port,
				}).String()
			}
		}

		ns := &controlv1.NodeSummary{
			Node:               &controlv1.NodeRef{PeerId: key.Bytes()},
			Status:             status,
			Addr:               addrStr,
			PubliclyAccessible: node.PubliclyAccessible,
			TunnelCount:        tunnelCounts[key],
			CpuPercent:         node.CPUPercent,
			MemPercent:         node.MemPercent,
		}

		if isDirect {
			if conn, ok := s.node.mesh.GetConn(key); ok {
				rtt := conn.ConnectionStats().SmoothedRTT
				if rtt > 0 {
					ns.LatencyMs = float64(rtt.Microseconds()) / 1000 //nolint:mnd
				}
			}
		}

		out.Nodes = append(out.Nodes, ns)
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

	slices.SortFunc(out.Nodes, func(a, b *controlv1.NodeSummary) int {
		if ra, rb := nodeStatusRank(a.Status), nodeStatusRank(b.Status); ra != rb {
			return ra - rb
		}
		return types.PeerKeyFromBytes(a.Node.PeerId).Compare(types.PeerKeyFromBytes(b.Node.PeerId))
	})

	slices.SortFunc(out.Services, func(a, b *controlv1.ServiceSummary) int {
		if a.Name != b.Name {
			return cmp.Compare(a.Name, b.Name)
		}
		if a.Port != b.Port {
			return cmp.Compare(a.Port, b.Port)
		}
		return types.PeerKeyFromBytes(a.Provider.PeerId).Compare(types.PeerKeyFromBytes(b.Provider.PeerId))
	})

	slices.SortFunc(out.Connections, func(a, b *controlv1.ConnectionSummary) int {
		if a.LocalPort != b.LocalPort {
			return cmp.Compare(a.LocalPort, b.LocalPort)
		}
		return types.PeerKeyFromBytes(a.Peer.PeerId).Compare(types.PeerKeyFromBytes(b.Peer.PeerId))
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
			return nil, status.Errorf(codes.InvalidArgument, "invalid address %q", a)
		}
		addrs = append(addrs, addr)
	}
	if err := s.node.mesh.Connect(ctx, peerKey, addrs); err != nil {
		s.node.log.Warnw("connect peer failed", "peer", peerKey.Short(), "err", err)
		return nil, status.Error(codes.Internal, "failed to connect to peer")
	}
	return &controlv1.ConnectPeerResponse{}, nil
}

func (s *NodeService) ConnectService(_ context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	localPort, err := s.node.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.RemotePort, req.LocalPort)
	if err != nil {
		s.node.log.Warnw("connect service failed", "err", err)
		return nil, status.Error(codes.Internal, "failed to connect service")
	}

	return &controlv1.ConnectServiceResponse{LocalPort: localPort}, nil
}

func (s *NodeService) DisconnectService(_ context.Context, req *controlv1.DisconnectServiceRequest) (*controlv1.DisconnectServiceResponse, error) {
	if err := s.node.DisconnectService(req.GetLocalPort()); err != nil {
		s.node.log.Warnw("disconnect service failed", "err", err)
		return nil, status.Error(codes.Internal, "failed to disconnect service")
	}
	return &controlv1.DisconnectServiceResponse{}, nil
}

func (s *NodeService) RevokePeer(_ context.Context, req *controlv1.RevokePeerRequest) (*controlv1.RevokePeerResponse, error) {
	if s.creds.InviteSigner == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin signer not available; this node cannot revoke peers")
	}

	signerPub, ok := s.creds.InviteSigner.Priv.Public().(ed25519.PublicKey)
	if !ok || !bytes.Equal(signerPub, s.creds.InviteSigner.Trust.GetRootPub()) {
		return nil, status.Error(codes.PermissionDenied, "only the root admin can revoke peers")
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
		s.node.log.Errorw("failed to issue revocation", "peer", peerKey.Short(), "err", err)
		return nil, status.Error(codes.Internal, "failed to issue revocation")
	}

	events := s.node.store.PublishRevocation(rev)
	s.node.queueGossipEvents(events)

	return &controlv1.RevokePeerResponse{}, nil
}

func (s *NodeService) GetMetrics(_ context.Context, _ *controlv1.GetMetricsRequest) (*controlv1.GetMetricsResponse, error) {
	counts := s.node.peers.StateCounts()

	nm := s.node.nodeMetrics
	gm := s.node.store.GossipMetrics()
	gossipApplied := uint64(gm.EventsApplied.Value()) //nolint:gosec
	gossipStale := uint64(gm.EventsStale.Value())     //nolint:gosec

	certExpiry := nm.CertExpirySeconds.Value()
	certRenewals := uint64(nm.CertRenewals.Value())             //nolint:gosec
	certRenewalsFailed := uint64(nm.CertRenewalsFailed.Value()) //nolint:gosec
	punchAttempts := uint64(nm.PunchAttempts.Value())           //nolint:gosec
	punchFailures := uint64(nm.PunchFailures.Value())           //nolint:gosec

	health := controlv1.HealthStatus_HEALTH_STATUS_HEALTHY
	switch {
	case certExpiry <= 0 && s.creds != nil && s.creds.Cert != nil:
		health = controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
	case counts.Connected == 0:
		health = controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
	case s.node.smoothedErr.Value() > vivaldiDegradedThreshold:
		health = controlv1.HealthStatus_HEALTH_STATUS_DEGRADED
	}

	return &controlv1.GetMetricsResponse{
		PeersDiscovered:    counts.Discovered,
		PeersConnecting:    counts.Connecting,
		PeersConnected:     counts.Connected,
		PeersUnreachable:   counts.Unreachable,
		EventsApplied:      gossipApplied,
		EventsStale:        gossipStale,
		VivaldiError:       s.node.smoothedErr.Value(),
		CertExpirySeconds:  certExpiry,
		CertRenewals:       certRenewals,
		CertRenewalsFailed: certRenewalsFailed,
		PunchAttempts:      punchAttempts,
		PunchFailures:      punchFailures,
		Health:             health,
	}, nil
}

const vivaldiDegradedThreshold = 0.9

const offlineRank = 3

func nodeStatusRank(status controlv1.NodeStatus) int {
	switch status {
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return 0
	case controlv1.NodeStatus_NODE_STATUS_INDIRECT:
		return 1
	case controlv1.NodeStatus_NODE_STATUS_RELAY:
		return 2 //nolint:mnd
	case controlv1.NodeStatus_NODE_STATUS_OFFLINE,
		controlv1.NodeStatus_NODE_STATUS_UNSPECIFIED:
		return offlineRank
	}
	return offlineRank
}
