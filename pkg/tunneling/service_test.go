// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package tunneling

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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

func (m *mockStore) SetService(port uint32, name string, protocol statev1.ServiceProtocol, properties *structpb.Struct, _ *admissionv1.Predicate) ([]state.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nv := m.snap.Nodes[m.snap.LocalID]
	if nv.Services == nil {
		nv.Services = make(map[string]*state.Service)
	}
	next := &state.Service{Port: port, Name: name, Protocol: protocol, Properties: properties}
	if existing, ok := nv.Services[name]; ok && existing.Port == next.Port && existing.Protocol == next.Protocol && proto.Equal(existing.Properties, next.Properties) {
		return nil, nil
	}
	nv.Services[name] = next
	m.snap.Nodes[m.snap.LocalID] = nv
	return []state.Event{state.ServiceChanged{Peer: m.snap.LocalID, Name: name}}, nil
}

func (m *mockStore) RemoveService(name string) ([]state.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nv := m.snap.Nodes[m.snap.LocalID]
	if _, ok := nv.Services[name]; !ok {
		return nil, nil
	}
	delete(nv.Services, name)
	m.snap.Nodes[m.snap.LocalID] = nv
	return []state.Event{state.ServiceChanged{Peer: m.snap.LocalID, Name: name}}, nil
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

	require.NoError(t, svc.ExposeService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil))
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

func TestRevokeService_ClosesActiveServe(t *testing.T) {
	self := types.PeerKey{1}
	peer := types.PeerKey{2}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	app, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = app.Close() })
	appPort := uint32(app.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert

	require.NoError(t, svc.ExposeService(appPort, "samflix", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil))

	appAccepted := make(chan net.Conn, 1)
	go func() {
		c, _ := app.Accept()
		appAccepted <- c
	}()

	streamSrv, streamPeer := net.Pipe()
	t.Cleanup(func() { _ = streamPeer.Close() })

	svc.Serve(streamSrv, peer, appPort)

	select {
	case c := <-appAccepted:
		t.Cleanup(func() { _ = c.Close() })
	case <-time.After(2 * time.Second):
		require.FailNow(t, "bridge did not dial local app")
	}

	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return len(svc.activeServes["samflix"]) == 1
	}, time.Second, 10*time.Millisecond, "Serve should register active session")

	svc.mu.Lock()
	revokeStreams := svc.detachServesLocked("samflix")
	svc.mu.Unlock()
	closeAll(revokeStreams)

	_ = streamPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, readErr := streamPeer.Read(buf)
	require.Error(t, readErr)
	require.True(t,
		errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrClosedPipe),
		"unexpected error after revoke: %v", readErr,
	)

	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return len(svc.activeServes["samflix"]) == 0
	}, time.Second, 10*time.Millisecond, "active session should deregister after bridge exit")
}

func TestStop_DrainsInflightServe(t *testing.T) {
	self := types.PeerKey{1}
	peer := types.PeerKey{2}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	app, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = app.Close() })
	appPort := uint32(app.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert

	require.NoError(t, svc.ExposeService(appPort, "samflix", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil))

	appAccepted := make(chan net.Conn, 1)
	go func() {
		c, _ := app.Accept()
		appAccepted <- c
	}()

	streamSrv, streamPeer := net.Pipe()
	t.Cleanup(func() { _ = streamPeer.Close() })

	svc.Serve(streamSrv, peer, appPort)

	select {
	case c := <-appAccepted:
		t.Cleanup(func() { _ = c.Close() })
	case <-time.After(2 * time.Second):
		require.FailNow(t, "bridge did not dial local app")
	}

	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return len(svc.activeServes["samflix"]) == 1
	}, time.Second, 10*time.Millisecond, "Serve should register active session")

	stopped := make(chan error, 1)
	go func() { stopped <- svc.Stop() }()

	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Stop hung waiting for in-flight Serve")
	}

	_ = streamPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = streamPeer.Read(buf)
	require.Error(t, err)
}

func TestExposeService_RejectsConflictingNameOnSamePort(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	require.NoError(t, svc.ExposeService(8080, "foo", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil))

	err := svc.ExposeService(8080, "bar", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), `already exposed as "foo"`)

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	require.Equal(t, "foo", svc.services[serviceKey{port: 8080, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}].name,
		"original name must remain owner of the port")
}

func TestExposeService_NameMoveTearsDownOldRuntimeKey(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	require.NoError(t, svc.ExposeService(8080, "samflix", statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP, nil, nil))

	svc.mu.RLock()
	oldKey := serviceKey{port: 8080, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP}
	oldEntry := svc.services[oldKey]
	svc.mu.RUnlock()
	require.NotNil(t, oldEntry)
	require.NotNil(t, oldEntry.proxy)

	require.NoError(t, svc.ExposeService(9090, "samflix", statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP, nil, nil))

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	require.NotContains(t, svc.services, oldKey, "stale runtime entry under old port should be torn down")
	require.Contains(t, svc.services, serviceKey{port: 9090, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP})
}

func TestStart_RehydratesSeededServices(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{
		LocalID: self,
		Nodes: map[types.PeerKey]state.NodeView{
			self: {Services: map[string]*state.Service{
				"tcp-svc": {Port: 8080, Name: "tcp-svc", Protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP},
				"udp-svc": {Port: 9090, Name: "udp-svc", Protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP},
			}},
		},
	}}

	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})
	require.NoError(t, svc.Start(context.Background()))
	t.Cleanup(func() { _ = svc.Stop() })

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	tcpEntry := svc.services[serviceKey{port: 8080, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP}]
	require.NotNil(t, tcpEntry, "TCP runtime entry should be rehydrated from snapshot")
	require.Equal(t, "tcp-svc", tcpEntry.name)
	require.Nil(t, tcpEntry.proxy)

	udpEntry := svc.services[serviceKey{port: 9090, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP}]
	require.NotNil(t, udpEntry, "UDP runtime entry should be rehydrated from snapshot")
	require.Equal(t, "udp-svc", udpEntry.name)
	require.NotNil(t, udpEntry.proxy, "UDP proxy must be built so HandleTunnelDatagram can route")
}

func TestExposeService_NoOpReExposePreservesActiveServes(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	require.NoError(t, svc.ExposeService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil))

	stream := &spyCloser{}
	svc.mu.Lock()
	svc.registerServeLocked("web", &activeServe{stream: stream})
	svc.mu.Unlock()

	require.NoError(t, svc.ExposeService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil, nil))

	require.False(t, stream.closed.Load(), "no-op re-expose must not close active streams")
	svc.mu.RLock()
	require.Len(t, svc.activeServes["web"], 1, "session should still be registered")
	svc.mu.RUnlock()
}

func TestExposeService_NoOpReExposePreservesUDPProxy(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	require.NoError(t, svc.ExposeService(9090, "dns", statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP, nil, nil))

	svc.mu.RLock()
	first := svc.services[serviceKey{port: 9090, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP}]
	svc.mu.RUnlock()
	require.NotNil(t, first)
	require.NotNil(t, first.proxy, "UDP entry should have a proxy")

	require.NoError(t, svc.ExposeService(9090, "dns", statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP, nil, nil))

	svc.mu.RLock()
	second := svc.services[serviceKey{port: 9090, protocol: statev1.ServiceProtocol_SERVICE_PROTOCOL_UDP}]
	svc.mu.RUnlock()
	require.Same(t, first, second, "no-op re-expose must keep the same entry/proxy")
}

func TestRevokeService_OtherServiceUnaffected(t *testing.T) {
	self := types.PeerKey{1}
	store := &mockStore{snap: state.Snapshot{LocalID: self, Nodes: map[types.PeerKey]state.NodeView{self: {}}}}
	svc := New(self, store, &mockTransport{}, &mockDatagramTransport{}, &mockRouter{})

	samflixStream := &spyCloser{}
	otherStream := &spyCloser{}
	svc.mu.Lock()
	svc.registerServeLocked("samflix", &activeServe{stream: samflixStream})
	svc.registerServeLocked("other", &activeServe{stream: otherStream})
	toClose := svc.detachServesLocked("samflix")
	svc.mu.Unlock()
	closeAll(toClose)

	require.True(t, samflixStream.closed.Load(), "revoked stream not closed")
	require.False(t, otherStream.closed.Load(), "untouched stream should stay open")

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	require.Empty(t, svc.activeServes["samflix"])
	require.Len(t, svc.activeServes["other"], 1)
}

type spyCloser struct {
	closed atomic.Bool
}

func (s *spyCloser) Read([]byte) (int, error)    { return 0, io.EOF }
func (s *spyCloser) Write(p []byte) (int, error) { return len(p), nil }
func (s *spyCloser) Close() error                { s.closed.Store(true); return nil }
