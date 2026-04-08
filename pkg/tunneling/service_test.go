package tunneling

import (
	"context"
	"io"
	"sync"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type mockStore struct {
	mu   sync.Mutex
	snap state.Snapshot
}

func (m *mockStore) Snapshot() state.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap
}

func (m *mockStore) SetService(port uint32, name string, protocol statev1.ServiceProtocol) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	nv := m.snap.Nodes[m.snap.LocalID]
	if nv.Services == nil {
		nv.Services = make(map[string]*state.Service)
	}
	nv.Services[name] = &state.Service{Port: port, Name: name, Protocol: protocol}
	m.snap.Nodes[m.snap.LocalID] = nv
	return nil
}

func (m *mockStore) RemoveService(name string) []state.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	nv := m.snap.Nodes[m.snap.LocalID]
	delete(nv.Services, name)
	m.snap.Nodes[m.snap.LocalID] = nv
	return nil
}

func (m *mockStore) SetLocalTraffic(peer types.PeerKey, in, out uint64) []state.Event { return nil }

type mockTransport struct {
	openFn func() (io.ReadWriteCloser, error)
}

func (m *mockTransport) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	return m.openFn()
}

type mockRouter struct{}

func (r *mockRouter) NextHop(dest types.PeerKey) (types.PeerKey, bool) { return dest, true }

type mockDatagramTransport struct{}

func (m *mockDatagramTransport) SendTunnelDatagram(_ context.Context, _ types.PeerKey, _ []byte) error {
	return nil
}

func TestServiceLifecycle(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	require.NoError(t, svc.ExposeService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP))
	require.Len(t, store.Snapshot().Nodes[self].Services, 1)

	require.NoError(t, svc.UnexposeService("web"))
	require.Empty(t, store.Snapshot().Nodes[self].Services)
}

func TestReconciliation(t *testing.T) {
	self := types.PeerKey{1}
	peer := types.PeerKey{2}
	store := &mockStore{snap: state.Snapshot{
		LocalID: self,
		Nodes: map[types.PeerKey]state.NodeView{
			self: {},
			peer: {Services: map[string]*state.Service{"api": {Port: 9000, Name: "api", Protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}}},
		},
	}}

	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	port, err := svc.Connect(context.Background(), peer, 9000, 0, statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	require.NoError(t, err)
	require.NotZero(t, port)

	require.Len(t, svc.ListConnections(), 1)

	store.mu.Lock()
	delete(store.snap.Nodes[peer].Services, "api")
	store.mu.Unlock()

	svc.reconcile()
	require.Empty(t, svc.ListConnections())
}

func TestHandlePeerDenied(t *testing.T) {
	self := types.PeerKey{1}
	peer := types.PeerKey{2}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}, peer: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	_, _ = svc.Connect(context.Background(), peer, 80, 8080, statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	require.Len(t, svc.ListDesiredConnections(), 1)

	svc.HandlePeerDenied(peer)
	require.Empty(t, svc.ListDesiredConnections())
}
