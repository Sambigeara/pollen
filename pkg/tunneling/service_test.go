package tunneling

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type fakeState struct {
	mu      sync.Mutex
	snap    state.Snapshot
	traffic map[types.PeerKey]peerTraffic
}

func newFakeState(self types.PeerKey) *fakeState {
	return &fakeState{
		snap: state.Snapshot{
			Nodes:   make(map[types.PeerKey]state.NodeView),
			LocalID: self,
		},
		traffic: make(map[types.PeerKey]peerTraffic),
	}
}

func (f *fakeState) Snapshot() state.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

func (f *fakeState) SetService(port uint32, name string) []state.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	nv := f.snap.Nodes[f.snap.LocalID]
	if nv.Services == nil {
		nv.Services = make(map[string]*statev1.Service)
	}
	nv.Services[name] = &statev1.Service{Port: port, Name: name}
	f.snap.Nodes[f.snap.LocalID] = nv
	return nil
}

func (f *fakeState) RemoveService(name string) []state.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	nv := f.snap.Nodes[f.snap.LocalID]
	delete(nv.Services, name)
	f.snap.Nodes[f.snap.LocalID] = nv
	return nil
}

func (f *fakeState) SetLocalTraffic(peer types.PeerKey, in, out uint64) []state.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.traffic[peer] = peerTraffic{BytesIn: in, BytesOut: out}
	return nil
}

func (f *fakeState) addPeer(pk types.PeerKey, services map[string]uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	nv := state.NodeView{
		IdentityPub: []byte{1},
		Services:    make(map[string]*statev1.Service),
	}
	for name, port := range services {
		nv.Services[name] = &statev1.Service{Port: port, Name: name}
	}
	f.snap.Nodes[pk] = nv
}

func (f *fakeState) removePeerService(pk types.PeerKey, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	nv := f.snap.Nodes[pk]
	delete(nv.Services, name)
	f.snap.Nodes[pk] = nv
}

type fakeStreams struct {
	mu     sync.Mutex
	openFn func(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

func (f *fakeStreams) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	f.mu.Lock()
	fn := f.openFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, peer, st)
	}
	return nil, errors.New("not implemented")
}

type fakeRouter struct {
	hops map[types.PeerKey]types.PeerKey
}

func (r *fakeRouter) NextHop(dest types.PeerKey) (types.PeerKey, bool) {
	next, ok := r.hops[dest]
	return next, ok
}

func pk(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func newTestService(self types.PeerKey, st *fakeState) *Service {
	return New(self, st, &fakeStreams{}, &fakeRouter{})
}

func TestExposeService(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	require.NoError(t, svc.ExposeService(8080, "http"))

	svc.serviceMu.RLock()
	_, ok := svc.services[8080]
	svc.serviceMu.RUnlock()
	require.True(t, ok, "service handler should be registered")

	snap := st.Snapshot()
	port, found := findServicePort(snap, self, "http")
	require.True(t, found)
	require.Equal(t, uint32(8080), port)
}

func TestUnexposeService(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	require.NoError(t, svc.ExposeService(8080, "http"))
	require.NoError(t, svc.UnexposeService("http"))

	svc.serviceMu.RLock()
	_, ok := svc.services[8080]
	svc.serviceMu.RUnlock()
	require.False(t, ok, "service handler should be removed")

	snap := st.Snapshot()
	_, found := findServicePort(snap, self, "http")
	require.False(t, found)
}

func TestUnexposeServiceNotFound(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	err := svc.UnexposeService("nope")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestHandleIncomingStream_Dispatch(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	var received []byte
	done := make(chan struct{})

	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	svc.serviceMu.Lock()
	svc.services[8080] = serviceHandler{
		fn: func(stream io.ReadWriteCloser) {
			data, _ := io.ReadAll(stream)
			received = data
			_ = stream.Close()
			close(done)
		},
		cancel: cancel,
	}
	svc.serviceMu.Unlock()

	// Build stream: 2-byte port header + payload.
	var buf bytes.Buffer
	require.NoError(t, writeServicePort(&buf, 8080))
	buf.WriteString("hello")

	svc.handleIncomingStream(pk(1), &rwcBuf{Buffer: &buf})
	<-done

	require.Equal(t, []byte("hello"), received)
}

func TestHandleIncomingStream_UnknownPort(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	var buf bytes.Buffer
	require.NoError(t, writeServicePort(&buf, 9999))
	buf.WriteString("data")

	closed := make(chan struct{})
	stream := &notifyCloser{Reader: &buf, closeCh: closed}

	svc.handleIncomingStream(pk(1), stream)

	<-closed
}

type notifyCloser struct {
	io.Reader
	closeCh chan struct{}
	once    sync.Once
}

func (n *notifyCloser) Write(p []byte) (int, error) { return len(p), nil }
func (n *notifyCloser) Close() error {
	n.once.Do(func() { close(n.closeCh) })
	return nil
}

func TestHandlePeerDenied(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	st.addPeer(peer, map[string]uint32{"http": 8080})
	svc := New(self, st, &fakeStreams{}, &fakeRouter{})

	// Create a connection via connectService.
	port, err := svc.connectService(peer, 8080, 0)
	require.NoError(t, err)
	require.NotZero(t, port)
	t.Cleanup(func() { _ = svc.Stop() })

	// Seed a desired connection.
	svc.SeedDesiredConnection(peer, 8080, port)
	require.Len(t, svc.ListDesiredConnections(), 1)
	require.Len(t, svc.ListConnections(), 1)

	// Deny the peer.
	svc.HandlePeerDenied(peer)

	require.Empty(t, svc.ListConnections())
	require.Empty(t, svc.ListDesiredConnections())
}

func TestDesiredPeers(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	peerA := pk(1)
	peerB := pk(2)

	svc.SeedDesiredConnection(peerA, 8080, 9080)
	svc.SeedDesiredConnection(peerA, 8081, 9081)
	svc.SeedDesiredConnection(peerB, 8080, 9080)

	peers := svc.DesiredPeers()
	require.Len(t, peers, 2)
	require.ElementsMatch(t, []types.PeerKey{peerA, peerB}, peers)
}

func TestSeedAndListDesiredConnections(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	peer := pk(1)
	svc.SeedDesiredConnection(peer, 8080, 9080)
	svc.SeedDesiredConnection(peer, 8081, 9081)

	conns := svc.ListDesiredConnections()
	require.Len(t, conns, 2)

	ports := make(map[uint32]uint32)
	for _, c := range conns {
		require.Equal(t, peer, c.PeerID)
		ports[c.RemotePort] = c.LocalPort
	}
	require.Equal(t, uint32(9080), ports[8080])
	require.Equal(t, uint32(9081), ports[8081])
}

func TestListConnections(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	svc := New(self, st, &fakeStreams{}, &fakeRouter{})

	port, err := svc.connectService(peer, 8080, 0)
	require.NoError(t, err)
	require.NotZero(t, port)
	t.Cleanup(func() { _ = svc.Stop() })

	conns := svc.ListConnections()
	require.Len(t, conns, 1)
	require.Equal(t, peer, conns[0].PeerID)
	require.Equal(t, uint32(8080), conns[0].RemotePort)
	require.Equal(t, port, conns[0].LocalPort)
}

func TestConnectServiceDuplicate(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	svc := New(self, st, &fakeStreams{}, &fakeRouter{})

	_, err := svc.connectService(peer, 8080, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Stop() })

	_, err = svc.connectService(peer, 8080, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already connected")
}

func TestTrafficRecorder_Enabled(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := New(self, st, &fakeStreams{}, &fakeRouter{}, WithTrafficTracking())

	rec := svc.TrafficRecorder()
	require.NotNil(t, rec)
}

func TestTrafficRecorder_Disabled(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	rec := svc.TrafficRecorder()
	require.Nil(t, rec)
}

func TestReadWriteServicePort(t *testing.T) {
	tests := []struct {
		name string
		port uint32
	}{
		{"min", 1},
		{"common", 8080},
		{"max", 65535},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, writeServicePort(&buf, tt.port))
			got, err := readServicePort(&buf)
			require.NoError(t, err)
			require.Equal(t, tt.port, got)
		})
	}
}

func TestWriteServicePort_Invalid(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, writeServicePort(&buf, 0))
	require.Error(t, writeServicePort(&buf, 0x10000))
}

func TestReadServicePort_ZeroPort(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0, 0})
	_, err := readServicePort(buf)
	require.ErrorIs(t, err, errNoServicePort)
}

func TestReconcileConnections_RemovesStale(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	st.addPeer(peer, map[string]uint32{"http": 8080})
	svc := New(self, st, &fakeStreams{}, &fakeRouter{})

	port, err := svc.connectService(peer, 8080, 0)
	require.NoError(t, err)
	require.NotZero(t, port)
	t.Cleanup(func() { _ = svc.Stop() })

	svc.addDesiredConnection(peer, 8080, port)
	require.Len(t, svc.ListConnections(), 1)

	// Remove the service from the peer.
	st.removePeerService(peer, "http")

	snap := st.Snapshot()
	svc.reconcileConnections(snap)

	require.Empty(t, svc.ListConnections())
	require.Empty(t, svc.ListDesiredConnections())
}

func TestReconcileDesiredConnections_RestoresMissing(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	st.addPeer(peer, map[string]uint32{"http": 8080})
	svc := New(self, st, &fakeStreams{}, &fakeRouter{})
	t.Cleanup(func() { _ = svc.Stop() })

	// Seed a desired connection without establishing it.
	svc.SeedDesiredConnection(peer, 8080, 0)

	snap := st.Snapshot()
	svc.reconcileDesiredConnections(snap)

	conns := svc.ListConnections()
	require.Len(t, conns, 1)
	require.Equal(t, peer, conns[0].PeerID)
	require.Equal(t, uint32(8080), conns[0].RemotePort)
}

func TestSampleTraffic(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	svc := New(self, st, &fakeStreams{}, &fakeRouter{}, WithTrafficTracking())

	svc.trafficTracker.Record(peer, 100, 200)
	svc.sampleTraffic()

	st.mu.Lock()
	pt, ok := st.traffic[peer]
	st.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, uint64(100), pt.BytesIn)
	require.Equal(t, uint64(200), pt.BytesOut)
}

func TestSampleTraffic_NoTracker(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	// Should not panic.
	svc.sampleTraffic()
}

func TestSnapshotHelpers(t *testing.T) {
	self := pk(0)
	peer := pk(1)
	st := newFakeState(self)
	st.addPeer(peer, map[string]uint32{"http": 8080, "grpc": 9090})
	snap := st.Snapshot()

	t.Run("findServicePort", func(t *testing.T) {
		port, ok := findServicePort(snap, peer, "http")
		require.True(t, ok)
		require.Equal(t, uint32(8080), port)

		_, ok = findServicePort(snap, peer, "nope")
		require.False(t, ok)

		_, ok = findServicePort(snap, pk(99), "http")
		require.False(t, ok)
	})

	t.Run("peerHasServicePort", func(t *testing.T) {
		require.True(t, peerHasServicePort(snap, peer, 8080))
		require.True(t, peerHasServicePort(snap, peer, 9090))
		require.False(t, peerHasServicePort(snap, peer, 1234))
		require.False(t, peerHasServicePort(snap, pk(99), 8080))
	})

	t.Run("matchesServiceName", func(t *testing.T) {
		require.True(t, matchesServiceName(snap, peer, 8080, "http"))
		require.False(t, matchesServiceName(snap, peer, 8080, "grpc"))
		require.False(t, matchesServiceName(snap, peer, 1234, "http"))
	})
}

func TestActiveStreamTracking(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	stream := &rwcBuf{Buffer: &bytes.Buffer{}}

	svc.addActiveStream(8080, stream)
	svc.streamMu.Lock()
	require.Len(t, svc.activeStreams[8080], 1)
	svc.streamMu.Unlock()

	svc.removeActiveStream(8080, stream)
	svc.streamMu.Lock()
	require.Empty(t, svc.activeStreams)
	svc.streamMu.Unlock()
}

func TestTrackedStream_CloseOnceSemantics(t *testing.T) {
	var closeCalls int
	inner := &countCloser{onClose: func() { closeCalls++ }}

	var onCloseCalls int
	ts := &trackedStream{
		ReadWriteCloser: inner,
		onClose:         func() { onCloseCalls++ },
	}

	_ = ts.Close()
	_ = ts.Close()

	require.Equal(t, 1, onCloseCalls, "onClose should fire once")
	require.Equal(t, 2, closeCalls, "inner Close called each time")
}

type rwcBuf struct {
	*bytes.Buffer
}

func (r *rwcBuf) Close() error { return nil }

type countCloser struct {
	onClose func()
}

func (c *countCloser) Read(p []byte) (int, error)  { return 0, io.EOF }
func (c *countCloser) Write(p []byte) (int, error) { return len(p), nil }
func (c *countCloser) Close() error                { c.onClose(); return nil }

func TestConnectServiceInvalidPort(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)
	svc := newTestService(self, st)

	_, err := svc.connectService(pk(1), 0, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote port missing")

	_, err = svc.connectService(pk(1), 0x10000, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote port missing")
}

func TestStartRegistersExistingServices(t *testing.T) {
	self := pk(0)
	st := newFakeState(self)

	// Pre-populate a service in the snapshot.
	st.SetService(8080, "http")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so tickers don't run

	svc := newTestService(self, st)
	require.NoError(t, svc.Start(ctx))

	svc.serviceMu.RLock()
	_, ok := svc.services[8080]
	svc.serviceMu.RUnlock()
	require.True(t, ok, "Start should register pre-existing services")
}
