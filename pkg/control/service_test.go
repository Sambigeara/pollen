package control_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- Test Harness ---

type harness struct {
	t          *testing.T
	membership *fakeMembership
	placement  *fakePlacement
	tunneling  *fakeTunneling
	state      *fakeState
	transport  *fakeTransport
	metrics    *fakeMetrics
	connector  *fakeConnector
	svc        *control.Service
}

func newHarness(t *testing.T, opts ...control.Option) *harness { //nolint:thelper
	h := &harness{
		t:          t,
		membership: &fakeMembership{},
		placement:  &fakePlacement{},
		tunneling:  &fakeTunneling{},
		state:      &fakeState{snap: state.Snapshot{Nodes: make(map[types.PeerKey]state.NodeView)}},
		transport:  &fakeTransport{activeAddrs: make(map[types.PeerKey]*net.UDPAddr), rtts: make(map[types.PeerKey]time.Duration)},
		metrics:    &fakeMetrics{},
		connector:  &fakeConnector{},
	}
	h.svc = control.NewService(h.membership, h.placement, h.tunneling, h.state, append([]control.Option{
		control.WithTransportInfo(h.transport),
		control.WithMetricsSource(h.metrics),
		control.WithMeshConnector(h.connector),
	}, opts...)...)
	return h
}

// --- Tests ---

func TestShutdownInvokesCallback(t *testing.T) {
	done := make(chan struct{})
	h := newHarness(t, control.WithShutdown(func() { close(done) }))

	_, err := h.svc.Shutdown(context.Background(), &controlv1.ShutdownRequest{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestShutdownNoCallback(t *testing.T) {
	svc := control.NewService(nil, nil, nil, nil)
	_, err := svc.Shutdown(context.Background(), &controlv1.ShutdownRequest{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestRegisterService(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.RegisterService(context.Background(), &controlv1.RegisterServiceRequest{
		Port: 8080,
		Name: new("web"),
	})
	require.NoError(t, err)
	require.Equal(t, uint32(8080), h.tunneling.exposedPort)
	require.Equal(t, "web", h.tunneling.exposedName)
}

func TestRegisterServiceError(t *testing.T) {
	h := newHarness(t)
	h.tunneling.exposeErr = errors.New("boom")
	_, err := h.svc.RegisterService(context.Background(), &controlv1.RegisterServiceRequest{Port: 8080})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestRegisterServiceFallbackName(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.RegisterService(context.Background(), &controlv1.RegisterServiceRequest{Port: 9090})
	require.NoError(t, err)
	require.Equal(t, "9090", h.tunneling.exposedName)
}

func TestUnregisterService(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.UnregisterService(context.Background(), &controlv1.UnregisterServiceRequest{
		Name: new("web"),
	})
	require.NoError(t, err)
	require.Equal(t, "web", h.tunneling.unexposedName)
}

func TestUnregisterServiceError(t *testing.T) {
	h := newHarness(t)
	h.tunneling.unexposeErr = errors.New("boom")
	_, err := h.svc.UnregisterService(context.Background(), &controlv1.UnregisterServiceRequest{
		Name: new("web"),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestConnectPeer(t *testing.T) {
	h := newHarness(t)
	pk := testPeerKey(1)

	_, err := h.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: pk.Bytes(),
		Addrs:   []string{"1.2.3.4:5000"},
	})
	require.NoError(t, err)
	require.Equal(t, pk, h.connector.calledPeer)
	require.Len(t, h.connector.calledAddr, 1)
}

func TestConnectPeerNoConnector(t *testing.T) {
	svc := control.NewService(nil, nil, nil, nil)
	_, err := svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: testPeerKey(1).Bytes(),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConnectPeerInvalidAddr(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: testPeerKey(1).Bytes(),
		Addrs:   []string{"not-an-addr"},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestConnectPeerError(t *testing.T) {
	h := newHarness(t)
	h.connector.err = errors.New("boom")
	_, err := h.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: testPeerKey(1).Bytes(),
		Addrs:   []string{"1.2.3.4:5000"},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestSeedWorkload(t *testing.T) {
	h := newHarness(t)
	wasmBytes := []byte("test-wasm")
	hash := hex.EncodeToString(func() []byte { h := sha256.Sum256(wasmBytes); return h[:] }())

	resp, err := h.svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: wasmBytes,
	})
	require.NoError(t, err)
	require.Equal(t, hash, resp.Hash)
	require.Equal(t, hash, h.placement.seededHash)
	require.Equal(t, wasmBytes, h.placement.seededBinary)
}

func TestSeedWorkloadCompileError(t *testing.T) {
	h := newHarness(t)
	h.placement.seedErr = placement.ErrCompile
	_, err := h.svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: []byte("bad-wasm"),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestSeedWorkloadAlreadyRunning(t *testing.T) {
	h := newHarness(t)
	h.placement.seedErr = placement.ErrAlreadyRunning
	resp, err := h.svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: []byte("wasm"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Hash)
}

func TestSeedWorkloadInternalError(t *testing.T) {
	h := newHarness(t)
	h.placement.seedErr = errors.New("disk full")
	_, err := h.svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: []byte("wasm"),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestUnseedWorkload(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.UnseedWorkload(context.Background(), &controlv1.UnseedWorkloadRequest{Hash: "abc123"})
	require.NoError(t, err)
	require.Equal(t, "abc123", h.placement.unseededHash)
}

func TestUnseedWorkloadError(t *testing.T) {
	h := newHarness(t)
	h.placement.unseedErr = errors.New("boom")
	_, err := h.svc.UnseedWorkload(context.Background(), &controlv1.UnseedWorkloadRequest{Hash: "abc123"})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestCallWorkload(t *testing.T) {
	h := newHarness(t)
	h.placement.callOut = []byte("result")

	resp, err := h.svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash:     "h1",
		Function: "run",
		Input:    []byte("input"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("result"), resp.Output)
	require.Equal(t, "h1", h.placement.calledHash)
	require.Equal(t, "run", h.placement.calledFn)
	require.Equal(t, []byte("input"), h.placement.calledInput)
}

func TestCallWorkloadNotRunning(t *testing.T) {
	h := newHarness(t)
	h.placement.callErr = placement.ErrNotRunning
	_, err := h.svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash: "h1", Function: "run",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestCallWorkloadDeadline(t *testing.T) {
	h := newHarness(t)
	h.placement.callErr = context.DeadlineExceeded
	_, err := h.svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash: "h1", Function: "run",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
}

func TestCallWorkloadInternalError(t *testing.T) {
	h := newHarness(t)
	h.placement.callErr = errors.New("boom")
	_, err := h.svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash: "h1", Function: "run",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestDenyPeer(t *testing.T) {
	dc := dummyCreds(t)
	h := newHarness(t, control.WithCredentials(&dc))
	pk := testPeerKey(5)

	_, err := h.svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerPub: pk.Bytes()})
	require.NoError(t, err)
	require.Equal(t, pk, h.membership.deniedKey)
}

func TestDenyPeerNoCreds(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerPub: testPeerKey(5).Bytes()})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestDenyPeerError(t *testing.T) {
	dc := dummyCreds(t)
	h := newHarness(t, control.WithCredentials(&dc))
	h.membership.denyErr = errors.New("boom")

	_, err := h.svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerPub: testPeerKey(5).Bytes()})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestConnectService(t *testing.T) {
	h := newHarness(t)
	pk := testPeerKey(2)
	h.state.snap.Nodes[pk] = state.NodeView{
		Services: map[string]*state.Service{
			"web": {Port: 80, Name: "web"},
		},
	}

	_, err := h.svc.ConnectService(context.Background(), &controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerPub: pk.Bytes()},
		RemotePort: 80,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(80), h.tunneling.connectedRemotePort)
	require.Equal(t, pk, h.tunneling.connectedPeer)
}

func TestDisconnectService(t *testing.T) {
	h := newHarness(t)
	pk := testPeerKey(3)
	h.tunneling.connections = []tunneling.ConnectionInfo{
		{PeerID: pk, RemotePort: 80, LocalPort: 3000},
	}
	h.state.snap.Nodes[pk] = state.NodeView{
		Services: map[string]*state.Service{
			"web": {Port: 80, Name: "web"},
		},
	}

	_, err := h.svc.DisconnectService(context.Background(), &controlv1.DisconnectServiceRequest{
		LocalPort: 3000,
	})
	require.NoError(t, err)
	require.Equal(t, "web", h.tunneling.disconnectedName)
}

func TestDisconnectServiceNotFound(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.DisconnectService(context.Background(), &controlv1.DisconnectServiceRequest{
		LocalPort: 9999,
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestGetMetricsHealthy(t *testing.T) {
	h := newHarness(t)
	h.transport.counts = transport.PeerStateCounts{Connected: 3, Connecting: 1, Backoff: 2}
	h.metrics.m = control.Metrics{GossipApplied: 100, GossipStale: 5}

	resp, err := h.svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_HEALTHY, resp.Health)
	require.Equal(t, uint32(3), resp.PeersConnected)
	require.Equal(t, uint32(1), resp.PeersConnecting)
	require.Equal(t, uint32(2), resp.PeersDiscovered)
	require.Equal(t, uint64(100), resp.EventsApplied)
	require.Equal(t, uint64(5), resp.EventsStale)
}

func TestGetMetricsUnhealthyNoPeers(t *testing.T) {
	h := newHarness(t)
	h.transport.counts = transport.PeerStateCounts{Connected: 0}

	resp, err := h.svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY, resp.Health)
}

func TestGetMetricsDegradedVivaldi(t *testing.T) {
	h := newHarness(t)
	h.transport.counts = transport.PeerStateCounts{Connected: 1}
	h.metrics.m = control.Metrics{SmoothedVivaldiErr: 1.5}

	resp, err := h.svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_DEGRADED, resp.Health)
}

func TestGetBootstrapInfoEmpty(t *testing.T) {
	h := newHarness(t)
	resp, err := h.svc.GetBootstrapInfo(context.Background(), &controlv1.GetBootstrapInfoRequest{})
	require.NoError(t, err)
	require.Nil(t, resp.Self)
}

func TestGetBootstrapInfoWithSelf(t *testing.T) {
	h := newHarness(t)
	pk := testPeerKey(1)
	h.state.snap.LocalID = pk
	h.state.snap.Nodes[pk] = state.NodeView{IPs: []string{"1.2.3.4"}, LocalPort: 5000}

	resp, err := h.svc.GetBootstrapInfo(context.Background(), &controlv1.GetBootstrapInfoRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Self)
	require.Equal(t, pk.Bytes(), resp.Self.Peer.PeerPub)
	require.Equal(t, []string{"1.2.3.4:5000"}, resp.Self.Addrs)
}

func TestGetBootstrapInfoRecommendedPeer(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	remote := testPeerKey(2)
	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{IPs: []string{"10.0.0.1"}, LocalPort: 5000}
	h.state.snap.Nodes[remote] = state.NodeView{IPs: []string{"1.2.3.4"}, LocalPort: 5000, PubliclyAccessible: true}

	resp, err := h.svc.GetBootstrapInfo(context.Background(), &controlv1.GetBootstrapInfoRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Recommended)
	require.Equal(t, remote.Bytes(), resp.Recommended.Peer.PeerPub)
}

func TestGetStatusBasic(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	peer := testPeerKey(2)
	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{IPs: []string{"10.0.0.1"}, LocalPort: 5000, CPUPercent: 50, MemPercent: 60, NumCPU: 4}
	h.state.snap.Nodes[peer] = state.NodeView{IPs: []string{"1.2.3.4"}, LocalPort: 5000}
	h.transport.activeAddrs[peer] = &net.UDPAddr{IP: net.ParseIP("1.2.3.4"), Port: 5000}
	h.transport.rtts[peer] = 10 * time.Millisecond

	resp, err := h.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)

	require.Equal(t, local.Bytes(), resp.Self.Node.PeerPub)
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, resp.Self.Status)
	require.Equal(t, uint32(50), resp.Self.CpuPercent)
	require.Equal(t, uint32(60), resp.Self.MemPercent)
	require.Equal(t, uint32(4), resp.Self.NumCpu)

	require.Len(t, resp.Nodes, 1)
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, resp.Nodes[0].Status)
	require.Equal(t, "1.2.3.4:5000", resp.Nodes[0].Addr)
	require.InDelta(t, 10.0, resp.Nodes[0].LatencyMs, 0.1)
}

func TestGetStatusOfflinePeer(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	peer := testPeerKey(2)
	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{IPs: []string{"10.0.0.1"}, LocalPort: 5000}
	h.state.snap.Nodes[peer] = state.NodeView{IPs: []string{"5.6.7.8"}, LocalPort: 5000}

	resp, err := h.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 1)
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_OFFLINE, resp.Nodes[0].Status)
}

func TestGetStatusWorkloads(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{}
	h.state.snap.Specs = map[string]state.WorkloadSpecView{
		"abc": {Spec: &statev1.WorkloadSpecChange{Replicas: 3}},
		"def": {Spec: &statev1.WorkloadSpecChange{Replicas: 2}},
	}
	h.state.snap.Claims = map[string]map[types.PeerKey]struct{}{
		"abc": {local: {}},
		"def": {testPeerKey(2): {}},
	}
	h.placement.statuses = []placement.WorkloadSummary{
		{Hash: "abc", Status: placement.StatusRunning, CompiledAt: time.Unix(1000, 0)},
	}

	resp, err := h.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Workloads, 2)

	var localW, remoteW *controlv1.WorkloadSummary
	for _, w := range resp.Workloads {
		if w.Hash == "abc" {
			localW = w
		} else {
			remoteW = w
		}
	}
	require.NotNil(t, localW)
	require.True(t, localW.Local)
	require.Equal(t, controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, localW.Status)
	require.Equal(t, uint32(3), localW.DesiredReplicas)
	require.Equal(t, uint32(1), localW.ActiveReplicas)

	require.NotNil(t, remoteW)
	require.False(t, remoteW.Local)
	require.Equal(t, uint32(2), remoteW.DesiredReplicas)
	require.Equal(t, uint32(1), remoteW.ActiveReplicas)
}

func TestGetStatus_PeerTrafficFromGossip(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	peerA := testPeerKey(2)
	peerB := testPeerKey(3)

	h.transport.activeAddrs[peerA] = &net.UDPAddr{IP: net.ParseIP("1.2.3.4"), Port: 5000}
	h.transport.activeAddrs[peerB] = &net.UDPAddr{IP: net.ParseIP("5.6.7.8"), Port: 5000}

	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{IPs: []string{"10.0.0.1"}, LocalPort: 5000}
	h.state.snap.Nodes[peerA] = state.NodeView{IPs: []string{"1.2.3.4"}, LocalPort: 5000, TrafficRates: map[types.PeerKey]state.TrafficSnapshot{
		peerB: {BytesIn: 1000, BytesOut: 2000},
	}}
	h.state.snap.Nodes[peerB] = state.NodeView{IPs: []string{"5.6.7.8"}, LocalPort: 5000, TrafficRates: map[types.PeerKey]state.TrafficSnapshot{
		peerA: {BytesIn: 3000, BytesOut: 4000},
	}}

	resp, err := h.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 2)

	nodeByID := make(map[string]*controlv1.NodeSummary)
	for _, n := range resp.Nodes {
		nodeByID[string(n.Node.PeerPub)] = n
	}

	a := nodeByID[string(peerA.Bytes())]
	require.Equal(t, uint64(1000), a.TrafficBytesIn)
	require.Equal(t, uint64(2000), a.TrafficBytesOut)

	b := nodeByID[string(peerB.Bytes())]
	require.Equal(t, uint64(3000), b.TrafficBytesIn)
	require.Equal(t, uint64(4000), b.TrafficBytesOut)
}

func TestGetStatus_SelfTrafficAggregation(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	peerA := testPeerKey(2)
	peerB := testPeerKey(3)

	h.transport.activeAddrs[peerA] = &net.UDPAddr{IP: net.ParseIP("1.2.3.4"), Port: 5000}
	h.transport.activeAddrs[peerB] = &net.UDPAddr{IP: net.ParseIP("5.6.7.8"), Port: 5000}

	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{IPs: []string{"10.0.0.1"}, LocalPort: 5000, TrafficRates: map[types.PeerKey]state.TrafficSnapshot{
		peerA: {BytesIn: 100, BytesOut: 200},
		peerB: {BytesIn: 300, BytesOut: 400},
	}}
	h.state.snap.Nodes[peerA] = state.NodeView{IPs: []string{"1.2.3.4"}, LocalPort: 5000}
	h.state.snap.Nodes[peerB] = state.NodeView{IPs: []string{"5.6.7.8"}, LocalPort: 5000}

	resp, err := h.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)

	require.Equal(t, uint64(400), resp.Self.TrafficBytesIn)
	require.Equal(t, uint64(600), resp.Self.TrafficBytesOut)
}

func TestGetStatus_ExpiredPeerNotIndirect(t *testing.T) {
	h := newHarness(t)
	local := testPeerKey(1)
	peerA := testPeerKey(2)
	peerB := testPeerKey(3)

	expiredCertTime := time.Now().Add(-time.Hour).Unix()

	h.state.snap.LocalID = local
	h.state.snap.Nodes[local] = state.NodeView{}
	h.state.snap.Nodes[peerA] = state.NodeView{
		IPs:       []string{"1.2.3.4"},
		LocalPort: 4000,
		Reachable: map[types.PeerKey]struct{}{peerB: {}},
	}
	h.state.snap.Nodes[peerB] = state.NodeView{
		IPs:        []string{"5.6.7.8"},
		LocalPort:  5000,
		CertExpiry: expiredCertTime,
	}

	h.transport.activeAddrs[peerA] = &net.UDPAddr{IP: net.ParseIP("1.2.3.4"), Port: 4000}

	resp, err := h.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)

	for _, n := range resp.Nodes {
		pk := types.PeerKeyFromBytes(n.Node.PeerPub)
		if pk == peerB {
			t.Fatalf("expired peer %s should not appear in status output", pk.Short())
		}
	}
}

// --- Fakes ---

type fakeMembership struct {
	denyErr   error
	deniedKey types.PeerKey
}

func (f *fakeMembership) DenyPeer(key types.PeerKey) error {
	f.deniedKey = key
	return f.denyErr
}

type fakePlacement struct {
	seedErr   error
	unseedErr error
	callErr   error
	callOut   []byte
	statuses  []placement.WorkloadSummary

	seededHash   string
	seededBinary []byte
	unseededHash string
	calledHash   string
	calledFn     string
	calledInput  []byte
}

func (f *fakePlacement) Seed(hash string, binary []byte, _, _, _ uint32) error {
	f.seededHash = hash
	f.seededBinary = binary
	return f.seedErr
}

func (f *fakePlacement) Unseed(hash string) error {
	f.unseededHash = hash
	return f.unseedErr
}

func (f *fakePlacement) Call(_ context.Context, hash, fn string, input []byte) ([]byte, error) {
	f.calledHash = hash
	f.calledFn = fn
	f.calledInput = input
	return f.callOut, f.callErr
}

func (f *fakePlacement) Status() []placement.WorkloadSummary {
	return f.statuses
}

type fakeTunneling struct {
	exposeErr     error
	unexposeErr   error
	connectErr    error
	disconnectErr error
	connections   []tunneling.ConnectionInfo

	exposedPort         uint32
	exposedName         string
	unexposedName       string
	connectedPeer       types.PeerKey
	connectedRemotePort uint32
	connectedLocalPort  uint32
	disconnectedName    string
}

func (f *fakeTunneling) Connect(_ context.Context, peer types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	f.connectedPeer = peer
	f.connectedRemotePort = remotePort
	f.connectedLocalPort = localPort
	if localPort == 0 {
		localPort = remotePort
	}
	return localPort, f.connectErr
}

func (f *fakeTunneling) Disconnect(service string) error {
	f.disconnectedName = service
	return f.disconnectErr
}

func (f *fakeTunneling) ExposeService(port uint32, name string) error {
	f.exposedPort = port
	f.exposedName = name
	return f.exposeErr
}

func (f *fakeTunneling) UnexposeService(name string) error {
	f.unexposedName = name
	return f.unexposeErr
}

func (f *fakeTunneling) ListConnections() []tunneling.ConnectionInfo {
	return f.connections
}

type fakeState struct {
	snap state.Snapshot
}

func (f *fakeState) Snapshot() state.Snapshot {
	return f.snap
}

type fakeTransport struct {
	counts          transport.PeerStateCounts
	activeAddrs     map[types.PeerKey]*net.UDPAddr
	rtts            map[types.PeerKey]time.Duration
	reconnectWindow time.Duration
}

func (f *fakeTransport) PeerStateCounts() transport.PeerStateCounts {
	return f.counts
}

func (f *fakeTransport) GetActivePeerAddress(key types.PeerKey) (*net.UDPAddr, bool) {
	addr, ok := f.activeAddrs[key]
	return addr, ok
}

func (f *fakeTransport) PeerRTT(key types.PeerKey) (time.Duration, bool) {
	rtt, ok := f.rtts[key]
	return rtt, ok
}

func (f *fakeTransport) ReconnectWindowDuration() time.Duration {
	return f.reconnectWindow
}

type fakeMetrics struct {
	m control.Metrics
}

func (f *fakeMetrics) ControlMetrics() control.Metrics {
	return f.m
}

type fakeConnector struct {
	err        error
	calledPeer types.PeerKey
	calledAddr []netip.AddrPort
}

func (f *fakeConnector) Connect(_ context.Context, peer types.PeerKey, addrs []netip.AddrPort) error {
	f.calledPeer = peer
	f.calledAddr = addrs
	return f.err
}

// --- Helpers ---

func testPeerKey(b byte) types.PeerKey {
	var raw [32]byte
	raw[0] = b
	return types.PeerKeyFromBytes(raw[:])
}

func dummyCreds(t *testing.T) auth.NodeCredentials {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	issuer, err := auth.IssueDelegationCert(priv, nil, pub, auth.FullCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(pub, issuer)))
	signer, err := auth.NewDelegationSigner(dir, priv, 24*time.Hour)
	require.NoError(t, err)
	creds := auth.NewNodeCredentials(nil, nil)
	creds.SetDelegationKey(signer)
	return *creds
}
