package membership

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"sync"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"

	"github.com/quic-go/quic-go"
)

func peerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

type fakeNetwork struct {
	mu             sync.Mutex
	connectedPeers []types.PeerKey
	peerEvents     chan transport.PeerEvent
	sendErr        error
	recvQueue      chan transport.Packet
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{
		peerEvents: make(chan transport.PeerEvent, 16),
		recvQueue:  make(chan transport.Packet, 16),
	}
}

func (f *fakeNetwork) Connect(_ context.Context, _ types.PeerKey, _ []netip.AddrPort) error {
	return nil
}

func (f *fakeNetwork) Disconnect(_ types.PeerKey) error {
	return nil
}

func (f *fakeNetwork) Send(_ context.Context, _ types.PeerKey, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendErr
}

func (f *fakeNetwork) Recv(ctx context.Context) (transport.Packet, error) {
	select {
	case <-ctx.Done():
		return transport.Packet{}, ctx.Err()
	case p := <-f.recvQueue:
		return p, nil
	}
}

func (f *fakeNetwork) ConnectedPeers() []types.PeerKey {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.PeerKey(nil), f.connectedPeers...)
}

func (f *fakeNetwork) PeerEvents() <-chan transport.PeerEvent {
	return f.peerEvents
}

func (f *fakeNetwork) setConnectedPeers(peers ...types.PeerKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectedPeers = peers
}

type fakeClusterState struct {
	mu             sync.Mutex
	snapshot       state.Snapshot
	applyDeltaFn   func(from types.PeerKey, data []byte) ([]state.Event, []byte, error)
	encodeDeltaFn  func(since state.Clock) []byte
	fullState      []byte
	pendingNotify  chan struct{}
	flushedEvents  []*statev1.GossipEvent
	localReachable []types.PeerKey
	localCoord     coords.Coord
}

func newFakeClusterState(localID types.PeerKey) *fakeClusterState {
	return &fakeClusterState{
		snapshot: state.Snapshot{
			LocalID:  localID,
			Nodes:    make(map[types.PeerKey]state.NodeView),
			PeerKeys: []types.PeerKey{localID},
		},
		pendingNotify: make(chan struct{}, 16),
	}
}

func (f *fakeClusterState) Snapshot() state.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot
}

func (f *fakeClusterState) ApplyDelta(from types.PeerKey, data []byte) ([]state.Event, []byte, error) {
	if f.applyDeltaFn != nil {
		return f.applyDeltaFn(from, data)
	}
	return nil, nil, nil
}

func (f *fakeClusterState) EncodeDelta(since state.Clock) []byte {
	if f.encodeDeltaFn != nil {
		return f.encodeDeltaFn(since)
	}
	return nil
}

func (f *fakeClusterState) FullState() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fullState
}

func (f *fakeClusterState) PendingNotify() <-chan struct{} {
	return f.pendingNotify
}

func (f *fakeClusterState) FlushPendingGossip() []*statev1.GossipEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	events := f.flushedEvents
	f.flushedEvents = nil
	return events
}

func (f *fakeClusterState) DenyPeer(_ types.PeerKey) []state.Event {
	return []state.Event{state.PeerDenied{}}
}

func (f *fakeClusterState) SetLocalAddresses(_ []netip.AddrPort) []state.Event {
	return nil
}

func (f *fakeClusterState) SetLocalCoord(c coords.Coord) []state.Event {
	f.mu.Lock()
	f.localCoord = c
	f.mu.Unlock()
	return nil
}

func (f *fakeClusterState) SetLocalNAT(_ nat.Type) []state.Event {
	return nil
}

func (f *fakeClusterState) SetLocalReachable(peers []types.PeerKey) []state.Event {
	f.mu.Lock()
	f.localReachable = peers
	f.mu.Unlock()
	return nil
}

func (f *fakeClusterState) SetLocalObservedExternalIP(_ string) []state.Event {
	return nil
}

func (f *fakeClusterState) SetLocalExternalPort(_ uint32) []state.Event {
	return nil
}

type fakeStreamOpener struct{}

func (f *fakeStreamOpener) OpenStream(_ context.Context, _ types.PeerKey, _ transport.StreamType) (transport.Stream, error) {
	return transport.Stream{}, errors.New("fake: no stream")
}

type fakeRTTSource struct{}

func (f *fakeRTTSource) GetConn(_ types.PeerKey) (*quic.Conn, bool) {
	return nil, false
}

type fakeCertManager struct {
	mu              sync.Mutex
	renewalResponse *admissionv1.DelegationCert
	renewalErr      error
	peerCerts       map[types.PeerKey]*admissionv1.DelegationCert
	updatedCert     *tls.Certificate
}

func newFakeCertManager() *fakeCertManager {
	return &fakeCertManager{
		peerCerts: make(map[types.PeerKey]*admissionv1.DelegationCert),
	}
}

func (f *fakeCertManager) UpdateMeshCert(cert tls.Certificate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updatedCert = &cert
}

func (f *fakeCertManager) RequestCertRenewal(_ context.Context, _ types.PeerKey) (*admissionv1.DelegationCert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewalResponse, f.renewalErr
}

func (f *fakeCertManager) PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.peerCerts[peer]
	return c, ok
}

type fakePeerAddressSource struct {
	addrs map[types.PeerKey]*net.UDPAddr
}

func (f *fakePeerAddressSource) GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool) {
	a, ok := f.addrs[peer]
	return a, ok
}

type fakePeerSessionCloser struct {
	mu     sync.Mutex
	closed map[types.PeerKey]string
}

func newFakePeerSessionCloser() *fakePeerSessionCloser {
	return &fakePeerSessionCloser{
		closed: make(map[types.PeerKey]string),
	}
}

func (f *fakePeerSessionCloser) ClosePeerSession(peer types.PeerKey, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed[peer] = reason
}

type sendRecorder struct {
	Network
	mu        sync.Mutex
	calls     map[types.PeerKey]int
	failPeer  types.PeerKey
	failAfter int
	err       error
}

func (m *sendRecorder) Send(_ context.Context, peerKey types.PeerKey, _ []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls == nil {
		m.calls = make(map[types.PeerKey]int)
	}
	m.calls[peerKey]++
	if peerKey == m.failPeer && m.calls[peerKey] >= m.failAfter {
		return m.err
	}
	return nil
}
