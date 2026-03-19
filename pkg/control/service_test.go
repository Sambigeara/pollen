package control_test

import (
	"context"
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

// --- Fakes ---

type fakeMembership struct {
	denyErr   error
	inviteErr error
	inviteRet string
	deniedKey types.PeerKey
}

func (f *fakeMembership) DenyPeer(key types.PeerKey) error {
	f.deniedKey = key
	return f.denyErr
}

func (f *fakeMembership) Invite(subject string) (string, error) {
	return f.inviteRet, f.inviteErr
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

func (f *fakePlacement) Call(ctx context.Context, hash, fn string, input []byte) ([]byte, error) {
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

	exposedPort      uint32
	exposedName      string
	unexposedName    string
	connectedService string
	connectedPeer    string
	disconnectedName string
}

func (f *fakeTunneling) Connect(_ context.Context, service, peer string) error {
	f.connectedService = service
	f.connectedPeer = peer
	return f.connectErr
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

func testPeerKey(b byte) types.PeerKey {
	var raw [32]byte
	raw[0] = b
	return types.PeerKeyFromBytes(raw[:])
}

// --- Tests ---

func TestShutdownInvokesCallback(t *testing.T) {
	done := make(chan struct{})
	svc := control.NewService(nil, nil, nil, nil, control.WithShutdown(func() {
		close(done)
	}))

	_, err := svc.Shutdown(context.Background(), &controlv1.ShutdownRequest{})
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
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestRegisterService(t *testing.T) {
	ft := &fakeTunneling{}
	svc := control.NewService(nil, nil, ft, &fakeState{})

	_, err := svc.RegisterService(context.Background(), &controlv1.RegisterServiceRequest{
		Port: 8080,
		Name: new("web"),
	})
	require.NoError(t, err)
	require.Equal(t, uint32(8080), ft.exposedPort)
	require.Equal(t, "web", ft.exposedName)
}

func TestRegisterServiceError(t *testing.T) {
	ft := &fakeTunneling{exposeErr: errors.New("boom")}
	svc := control.NewService(nil, nil, ft, &fakeState{})

	_, err := svc.RegisterService(context.Background(), &controlv1.RegisterServiceRequest{Port: 8080})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestRegisterServiceFallbackName(t *testing.T) {
	ft := &fakeTunneling{}
	svc := control.NewService(nil, nil, ft, &fakeState{})

	_, err := svc.RegisterService(context.Background(), &controlv1.RegisterServiceRequest{Port: 9090})
	require.NoError(t, err)
	require.Equal(t, "9090", ft.exposedName)
}

func TestUnregisterService(t *testing.T) {
	ft := &fakeTunneling{}
	svc := control.NewService(nil, nil, ft, &fakeState{})

	_, err := svc.UnregisterService(context.Background(), &controlv1.UnregisterServiceRequest{
		Name: new("web"),
	})
	require.NoError(t, err)
	require.Equal(t, "web", ft.unexposedName)
}

func TestUnregisterServiceError(t *testing.T) {
	ft := &fakeTunneling{unexposeErr: errors.New("boom")}
	svc := control.NewService(nil, nil, ft, &fakeState{})

	_, err := svc.UnregisterService(context.Background(), &controlv1.UnregisterServiceRequest{
		Name: new("web"),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestConnectPeer(t *testing.T) {
	fc := &fakeConnector{}
	pk := testPeerKey(1)
	svc := control.NewService(nil, nil, nil, nil, control.WithMeshConnector(fc))

	_, err := svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: pk.Bytes(),
		Addrs:  []string{"1.2.3.4:5000"},
	})
	require.NoError(t, err)
	require.Equal(t, pk, fc.calledPeer)
	require.Len(t, fc.calledAddr, 1)
}

func TestConnectPeerNoConnector(t *testing.T) {
	svc := control.NewService(nil, nil, nil, nil)
	_, err := svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: testPeerKey(1).Bytes(),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestConnectPeerInvalidAddr(t *testing.T) {
	fc := &fakeConnector{}
	svc := control.NewService(nil, nil, nil, nil, control.WithMeshConnector(fc))

	_, err := svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: testPeerKey(1).Bytes(),
		Addrs:  []string{"not-an-addr"},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestConnectPeerError(t *testing.T) {
	fc := &fakeConnector{err: errors.New("boom")}
	svc := control.NewService(nil, nil, nil, nil, control.WithMeshConnector(fc))

	_, err := svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: testPeerKey(1).Bytes(),
		Addrs:  []string{"1.2.3.4:5000"},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestSeedWorkload(t *testing.T) {
	fp := &fakePlacement{}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	wasmBytes := []byte("test-wasm")
	h := sha256.Sum256(wasmBytes)
	expectedHash := hex.EncodeToString(h[:])

	resp, err := svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: wasmBytes,
	})
	require.NoError(t, err)
	require.Equal(t, expectedHash, resp.Hash)
	require.Equal(t, expectedHash, fp.seededHash)
	require.Equal(t, wasmBytes, fp.seededBinary)
}

func TestSeedWorkloadCompileError(t *testing.T) {
	fp := &fakePlacement{seedErr: placement.ErrCompile}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: []byte("bad-wasm"),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestSeedWorkloadAlreadyRunning(t *testing.T) {
	fp := &fakePlacement{seedErr: placement.ErrAlreadyRunning}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	resp, err := svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: []byte("wasm"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Hash)
}

func TestSeedWorkloadInternalError(t *testing.T) {
	fp := &fakePlacement{seedErr: errors.New("disk full")}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.SeedWorkload(context.Background(), &controlv1.SeedWorkloadRequest{
		WasmBytes: []byte("wasm"),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestUnseedWorkload(t *testing.T) {
	fp := &fakePlacement{}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.UnseedWorkload(context.Background(), &controlv1.UnseedWorkloadRequest{Hash: "abc123"})
	require.NoError(t, err)
	require.Equal(t, "abc123", fp.unseededHash)
}

func TestUnseedWorkloadError(t *testing.T) {
	fp := &fakePlacement{unseedErr: errors.New("boom")}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.UnseedWorkload(context.Background(), &controlv1.UnseedWorkloadRequest{Hash: "abc123"})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestCallWorkload(t *testing.T) {
	fp := &fakePlacement{callOut: []byte("result")}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	resp, err := svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash:     "h1",
		Function: "run",
		Input:    []byte("input"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("result"), resp.Output)
	require.Equal(t, "h1", fp.calledHash)
	require.Equal(t, "run", fp.calledFn)
	require.Equal(t, []byte("input"), fp.calledInput)
}

func TestCallWorkloadNotRunning(t *testing.T) {
	fp := &fakePlacement{callErr: placement.ErrNotRunning}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash: "h1", Function: "run",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestCallWorkloadDeadline(t *testing.T) {
	fp := &fakePlacement{callErr: context.DeadlineExceeded}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash: "h1", Function: "run",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.DeadlineExceeded, st.Code())
}

func TestCallWorkloadInternalError(t *testing.T) {
	fp := &fakePlacement{callErr: errors.New("boom")}
	svc := control.NewService(nil, fp, &fakeTunneling{}, &fakeState{})

	_, err := svc.CallWorkload(context.Background(), &controlv1.CallWorkloadRequest{
		Hash: "h1", Function: "run",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestDenyPeer(t *testing.T) {
	fm := &fakeMembership{}
	pk := testPeerKey(5)
	svc := control.NewService(fm, nil, nil, nil, control.WithCredentials(&dummyCreds))

	_, err := svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerId: pk.Bytes()})
	require.NoError(t, err)
	require.Equal(t, pk, fm.deniedKey)
}

func TestDenyPeerNoCreds(t *testing.T) {
	fm := &fakeMembership{}
	svc := control.NewService(fm, nil, nil, nil)

	_, err := svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerId: testPeerKey(5).Bytes()})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestDenyPeerError(t *testing.T) {
	fm := &fakeMembership{denyErr: errors.New("boom")}
	svc := control.NewService(fm, nil, nil, nil, control.WithCredentials(&dummyCreds))

	_, err := svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerId: testPeerKey(5).Bytes()})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Internal, st.Code())
}

func TestConnectService(t *testing.T) {
	pk := testPeerKey(2)
	ft := &fakeTunneling{}
	fs := &fakeState{snap: state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			pk: {
				Services: map[string]*statev1.Service{
					"web": {Port: 80, Name: "web"},
				},
			},
		},
	}}
	svc := control.NewService(nil, nil, ft, fs)

	_, err := svc.ConnectService(context.Background(), &controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerId: pk.Bytes()},
		RemotePort: 80,
	})
	require.NoError(t, err)
	require.Equal(t, "web", ft.connectedService)
	require.Equal(t, pk.String(), ft.connectedPeer)
}

func TestConnectServiceFallbackName(t *testing.T) {
	pk := testPeerKey(2)
	ft := &fakeTunneling{}
	fs := &fakeState{snap: state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			pk: {},
		},
	}}
	svc := control.NewService(nil, nil, ft, fs)

	_, err := svc.ConnectService(context.Background(), &controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerId: pk.Bytes()},
		RemotePort: 9999,
	})
	require.NoError(t, err)
	require.Equal(t, "9999", ft.connectedService)
}

func TestDisconnectService(t *testing.T) {
	pk := testPeerKey(3)
	ft := &fakeTunneling{
		connections: []tunneling.ConnectionInfo{
			{PeerID: pk, RemotePort: 80, LocalPort: 3000},
		},
	}
	fs := &fakeState{snap: state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			pk: {
				Services: map[string]*statev1.Service{
					"web": {Port: 80, Name: "web"},
				},
			},
		},
	}}
	svc := control.NewService(nil, nil, ft, fs)

	_, err := svc.DisconnectService(context.Background(), &controlv1.DisconnectServiceRequest{
		LocalPort: 3000,
	})
	require.NoError(t, err)
	require.Equal(t, "web", ft.disconnectedName)
}

func TestDisconnectServiceNotFound(t *testing.T) {
	ft := &fakeTunneling{}
	fs := &fakeState{snap: state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{},
	}}
	svc := control.NewService(nil, nil, ft, fs)

	_, err := svc.DisconnectService(context.Background(), &controlv1.DisconnectServiceRequest{
		LocalPort: 9999,
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestGetMetricsHealthy(t *testing.T) {
	ft := &fakeTransport{
		counts: transport.PeerStateCounts{Connected: 3, Connecting: 1, Backoff: 2},
	}
	fm := &fakeMetrics{m: control.Metrics{
		GossipApplied: 100,
		GossipStale:   5,
	}}
	svc := control.NewService(nil, nil, nil, nil,
		control.WithTransportInfo(ft),
		control.WithMetricsSource(fm),
	)

	resp, err := svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_HEALTHY, resp.Health)
	require.Equal(t, uint32(3), resp.PeersConnected)
	require.Equal(t, uint32(1), resp.PeersConnecting)
	require.Equal(t, uint32(2), resp.PeersDiscovered)
	require.Equal(t, uint64(100), resp.EventsApplied)
	require.Equal(t, uint64(5), resp.EventsStale)
}

func TestGetMetricsUnhealthyNoPeers(t *testing.T) {
	ft := &fakeTransport{
		counts: transport.PeerStateCounts{Connected: 0},
	}
	svc := control.NewService(nil, nil, nil, nil,
		control.WithTransportInfo(ft),
	)

	resp, err := svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY, resp.Health)
}

func TestGetMetricsDegradedVivaldi(t *testing.T) {
	ft := &fakeTransport{
		counts: transport.PeerStateCounts{Connected: 1},
	}
	fm := &fakeMetrics{m: control.Metrics{SmoothedVivaldiErr: 1.5}}
	svc := control.NewService(nil, nil, nil, nil,
		control.WithTransportInfo(ft),
		control.WithMetricsSource(fm),
	)

	resp, err := svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_DEGRADED, resp.Health)
}

func TestGetMetricsNoTransport(t *testing.T) {
	svc := control.NewService(nil, nil, nil, nil)
	resp, err := svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY, resp.Health)
}

func TestGetBootstrapInfoEmpty(t *testing.T) {
	fs := &fakeState{snap: state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{},
	}}
	svc := control.NewService(nil, nil, nil, fs)

	resp, err := svc.GetBootstrapInfo(context.Background(), &controlv1.GetBootstrapInfoRequest{})
	require.NoError(t, err)
	require.Nil(t, resp.Self)
}

func TestGetBootstrapInfoWithSelf(t *testing.T) {
	pk := testPeerKey(1)
	fs := &fakeState{snap: state.Snapshot{
		LocalID: pk,
		Nodes: map[types.PeerKey]state.NodeView{
			pk: {IPs: []string{"1.2.3.4"}, LocalPort: 5000},
		},
	}}
	svc := control.NewService(nil, nil, nil, fs)

	resp, err := svc.GetBootstrapInfo(context.Background(), &controlv1.GetBootstrapInfoRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Self)
	require.Equal(t, pk.Bytes(), resp.Self.Peer.PeerId)
	require.Equal(t, []string{"1.2.3.4:5000"}, resp.Self.Addrs)
}

func TestGetBootstrapInfoRecommendedPeer(t *testing.T) {
	local := testPeerKey(1)
	remote := testPeerKey(2)
	fs := &fakeState{snap: state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"10.0.0.1"}, LocalPort: 5000},
			remote: {IPs: []string{"1.2.3.4"}, LocalPort: 5000, PubliclyAccessible: true},
		},
	}}
	svc := control.NewService(nil, nil, nil, fs)

	resp, err := svc.GetBootstrapInfo(context.Background(), &controlv1.GetBootstrapInfoRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Recommended)
	require.Equal(t, remote.Bytes(), resp.Recommended.Peer.PeerId)
}

func TestGetStatusBasic(t *testing.T) {
	local := testPeerKey(1)
	peer := testPeerKey(2)
	ft := &fakeTunneling{}
	fp := &fakePlacement{}
	ftr := &fakeTransport{
		activeAddrs: map[types.PeerKey]*net.UDPAddr{
			peer: {IP: net.ParseIP("1.2.3.4"), Port: 5000},
		},
		rtts: map[types.PeerKey]time.Duration{
			peer: 10 * time.Millisecond,
		},
	}
	fs := &fakeState{snap: state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {IPs: []string{"10.0.0.1"}, LocalPort: 5000, CPUPercent: 50, MemPercent: 60, NumCPU: 4},
			peer:  {IPs: []string{"1.2.3.4"}, LocalPort: 5000},
		},
		Heatmaps: map[types.PeerKey]map[types.PeerKey]state.TrafficSnapshot{},
	}}

	svc := control.NewService(nil, fp, ft, fs,
		control.WithTransportInfo(ftr),
	)

	resp, err := svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)

	require.Equal(t, local.Bytes(), resp.Self.Node.PeerId)
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
	local := testPeerKey(1)
	peer := testPeerKey(2)
	ft := &fakeTunneling{}
	fp := &fakePlacement{}
	ftr := &fakeTransport{
		activeAddrs: map[types.PeerKey]*net.UDPAddr{},
	}
	fs := &fakeState{snap: state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {IPs: []string{"10.0.0.1"}, LocalPort: 5000},
			peer:  {IPs: []string{"5.6.7.8"}, LocalPort: 5000},
		},
		Heatmaps: map[types.PeerKey]map[types.PeerKey]state.TrafficSnapshot{},
	}}

	svc := control.NewService(nil, fp, ft, fs,
		control.WithTransportInfo(ftr),
	)

	resp, err := svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 1)
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_OFFLINE, resp.Nodes[0].Status)
}

func TestGetStatusWorkloads(t *testing.T) {
	local := testPeerKey(1)
	fp := &fakePlacement{
		statuses: []placement.WorkloadSummary{
			{Hash: "abc", Status: placement.StatusRunning, CompiledAt: time.Unix(1000, 0)},
		},
	}
	fs := &fakeState{snap: state.Snapshot{
		LocalID: local,
		Nodes:   map[types.PeerKey]state.NodeView{local: {}},
		Specs: map[string]state.WorkloadSpecView{
			"abc": {Spec: &statev1.WorkloadSpecChange{Replicas: 3}},
			"def": {Spec: &statev1.WorkloadSpecChange{Replicas: 2}},
		},
		Claims: map[string]map[types.PeerKey]struct{}{
			"abc": {local: {}},
			"def": {testPeerKey(2): {}},
		},
		Heatmaps: map[types.PeerKey]map[types.PeerKey]state.TrafficSnapshot{},
	}}

	svc := control.NewService(nil, fp, &fakeTunneling{}, fs)

	resp, err := svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
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

func TestOptionWiring(t *testing.T) {
	ft := &fakeTransport{
		counts: transport.PeerStateCounts{Connected: 5},
	}
	fm := &fakeMetrics{m: control.Metrics{GossipApplied: 42}}
	svc := control.NewService(nil, nil, nil, nil,
		control.WithTransportInfo(ft),
		control.WithMetricsSource(fm),
	)

	resp, err := svc.GetMetrics(context.Background(), &controlv1.GetMetricsRequest{})
	require.NoError(t, err)
	require.Equal(t, uint32(5), resp.PeersConnected)
	require.Equal(t, uint64(42), resp.EventsApplied)
}

func TestGetStatus_PeerTrafficFromGossip(t *testing.T) {
	local := testPeerKey(1)
	peerA := testPeerKey(2)
	peerB := testPeerKey(3)

	ft := &fakeTunneling{}
	fp := &fakePlacement{}
	ftr := &fakeTransport{
		activeAddrs: map[types.PeerKey]*net.UDPAddr{
			peerA: {IP: net.ParseIP("1.2.3.4"), Port: 5000},
			peerB: {IP: net.ParseIP("5.6.7.8"), Port: 5000},
		},
	}
	// peerA gossiped its heatmap: it has traffic with peerB.
	// The local node has no tunnel/placement traffic of its own.
	fs := &fakeState{snap: state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {IPs: []string{"10.0.0.1"}, LocalPort: 5000},
			peerA: {IPs: []string{"1.2.3.4"}, LocalPort: 5000},
			peerB: {IPs: []string{"5.6.7.8"}, LocalPort: 5000},
		},
		Heatmaps: map[types.PeerKey]map[types.PeerKey]state.TrafficSnapshot{
			peerA: {
				peerB: {BytesIn: 1000, BytesOut: 2000},
			},
			peerB: {
				peerA: {BytesIn: 3000, BytesOut: 4000},
			},
		},
	}}

	svc := control.NewService(nil, fp, ft, fs,
		control.WithTransportInfo(ftr),
	)

	resp, err := svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 2)

	nodeByID := make(map[string]*controlv1.NodeSummary)
	for _, n := range resp.Nodes {
		nodeByID[string(n.Node.PeerId)] = n
	}

	// peerA gossiped traffic with peerB: aggregate = 1000 in, 2000 out.
	a := nodeByID[string(peerA.Bytes())]
	require.Equal(t, uint64(1000), a.TrafficBytesIn, "peerA traffic IN from gossip")
	require.Equal(t, uint64(2000), a.TrafficBytesOut, "peerA traffic OUT from gossip")

	// peerB gossiped traffic with peerA: aggregate = 3000 in, 4000 out.
	b := nodeByID[string(peerB.Bytes())]
	require.Equal(t, uint64(3000), b.TrafficBytesIn, "peerB traffic IN from gossip")
	require.Equal(t, uint64(4000), b.TrafficBytesOut, "peerB traffic OUT from gossip")
}

func TestGetStatus_SelfTrafficAggregation(t *testing.T) {
	local := testPeerKey(1)
	peerA := testPeerKey(2)
	peerB := testPeerKey(3)

	ft := &fakeTunneling{}
	fp := &fakePlacement{}
	ftr := &fakeTransport{
		activeAddrs: map[types.PeerKey]*net.UDPAddr{
			peerA: {IP: net.ParseIP("1.2.3.4"), Port: 5000},
			peerB: {IP: net.ParseIP("5.6.7.8"), Port: 5000},
		},
	}
	fs := &fakeState{snap: state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {IPs: []string{"10.0.0.1"}, LocalPort: 5000},
			peerA: {IPs: []string{"1.2.3.4"}, LocalPort: 5000},
			peerB: {IPs: []string{"5.6.7.8"}, LocalPort: 5000},
		},
		Heatmaps: map[types.PeerKey]map[types.PeerKey]state.TrafficSnapshot{
			local: {
				peerA: {BytesIn: 100, BytesOut: 200},
				peerB: {BytesIn: 300, BytesOut: 400},
			},
		},
	}}

	svc := control.NewService(nil, fp, ft, fs,
		control.WithTransportInfo(ftr),
	)

	resp, err := svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)

	// Self row: aggregate of local heatmap = (100+300) in, (200+400) out.
	require.Equal(t, uint64(400), resp.Self.TrafficBytesIn, "self aggregated IN")
	require.Equal(t, uint64(600), resp.Self.TrafficBytesOut, "self aggregated OUT")
}

// --- Helpers ---

// dummyCreds is a NodeCredentials with a non-nil DelegationKey for DenyPeer precondition tests.
var dummyCreds = dummyCredsValue()

func dummyCredsValue() auth.NodeCredentials {
	return auth.NodeCredentials{
		DelegationKey: &auth.DelegationSigner{},
	}
}
