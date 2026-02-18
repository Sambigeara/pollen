package store

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

type nodeRecord struct {
	Reachable   map[types.PeerKey]struct{}
	Services    map[string]*statev1.Service
	IdentityPub []byte
	IPs         []string
	Counter     uint64
	LocalPort   uint32
}

type KnownPeer struct {
	IdentityPub []byte
	IPs         []string
	LocalPort   uint32
	PeerID      types.PeerKey
}

type Connection struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

type ApplyResult struct {
	Rebroadcast *statev1.GossipNode
	Updated     bool
}

type Store struct {
	disk               *disk
	nodes              map[types.PeerKey]nodeRecord
	desiredConnections map[string]Connection
	mu                 sync.RWMutex
	LocalID            types.PeerKey
}

func Load(pollenDir string, identityPub []byte) (*Store, error) {
	d, err := openDisk(pollenDir)
	if err != nil {
		return nil, err
	}

	onDisk, err := d.load()
	if err != nil {
		_ = d.close()
		return nil, err
	}

	localID := types.PeerKeyFromBytes(identityPub)

	s := &Store{
		LocalID: localID,
		disk:    d,
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				Counter:     0,
				IdentityPub: append([]byte(nil), identityPub...),
				Reachable:   make(map[types.PeerKey]struct{}),
				Services:    make(map[string]*statev1.Service),
			},
		},
		desiredConnections: make(map[string]Connection),
	}

	for _, p := range onDisk.Peers {
		peerID, err := types.PeerKeyFromString(p.IdentityPublic)
		if err != nil {
			continue
		}
		if peerID == localID {
			continue
		}

		identityPub, err := decodeHex(p.IdentityPublic)
		if err != nil {
			identityPub = nil
		}

		s.nodes[peerID] = nodeRecord{
			IdentityPub: append([]byte(nil), identityPub...),
			IPs:         append([]string(nil), p.Addresses...),
			LocalPort:   p.Port,
			Reachable:   make(map[types.PeerKey]struct{}),
			Services:    make(map[string]*statev1.Service),
		}
	}

	for _, svc := range onDisk.Services {
		provider, err := types.PeerKeyFromString(svc.Provider)
		if err != nil {
			continue
		}

		rec := s.nodes[provider]
		if rec.Reachable == nil {
			rec.Reachable = make(map[types.PeerKey]struct{})
		}
		rec.Services[svc.Name] = &statev1.Service{
			Name: svc.Name,
			Port: svc.Port,
		}
		s.nodes[provider] = rec
	}

	for _, conn := range onDisk.Connections {
		peerID, err := types.PeerKeyFromString(conn.Peer)
		if err != nil {
			continue
		}
		s.desiredConnections[connectionKey(peerID, conn.RemotePort, conn.LocalPort)] = Connection{
			PeerID:     peerID,
			RemotePort: conn.RemotePort,
			LocalPort:  conn.LocalPort,
		}
	}

	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.disk == nil {
		return nil
	}
	return s.disk.close()
}

func (s *Store) Save() error {
	s.mu.RLock()
	nodes := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	for peerID, rec := range s.nodes {
		nodes[peerID] = cloneRecord(rec)
	}
	connections := make([]Connection, 0, len(s.desiredConnections))
	for _, conn := range s.desiredConnections {
		connections = append(connections, conn)
	}
	s.mu.RUnlock()

	peers := make([]diskPeer, 0, len(nodes))
	for peerID, rec := range nodes {
		if peerID == s.LocalID {
			continue
		}
		peers = append(peers, diskPeer{
			IdentityPublic: encodeHex(rec.IdentityPub),
			Addresses:      append([]string(nil), rec.IPs...),
			Port:           rec.LocalPort,
		})
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].IdentityPublic < peers[j].IdentityPublic
	})

	services := toDiskServices(nodes)

	sort.Slice(connections, func(i, j int) bool {
		a := connections[i]
		b := connections[j]
		if a.PeerID != b.PeerID {
			return a.PeerID.Less(b.PeerID)
		}
		if a.RemotePort != b.RemotePort {
			return a.RemotePort < b.RemotePort
		}
		return a.LocalPort < b.LocalPort
	})

	diskConns := make([]diskConnection, 0, len(connections))
	for _, conn := range connections {
		diskConns = append(diskConns, diskConnection{
			Peer:       conn.PeerID.String(),
			RemotePort: conn.RemotePort,
			LocalPort:  conn.LocalPort,
		})
	}

	localRec := nodes[s.LocalID]
	st := diskState{
		Local: diskLocal{
			IdentityPublic: encodeHex(localRec.IdentityPub),
		},
		Peers:       peers,
		Services:    services,
		Connections: diskConns,
	}

	return s.disk.save(st)
}

func (s *Store) ZeroClock() *statev1.GossipVectorClock {
	return &statev1.GossipVectorClock{Counters: map[string]uint64{}}
}

func (s *Store) Clock() *statev1.GossipVectorClock {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clock := make(map[string]uint64, len(s.nodes))
	for peerID, rec := range s.nodes {
		clock[peerID.String()] = rec.Counter
	}

	return &statev1.GossipVectorClock{Counters: clock}
}

func (s *Store) MissingFor(clock *statev1.GossipVectorClock) []*statev1.GossipNode {
	requested := map[string]uint64{}
	if clock != nil {
		requested = clock.GetCounters()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	updates := make([]*statev1.GossipNode, 0, len(s.nodes))
	for peerID, rec := range s.nodes {
		if rec.Counter == 0 {
			continue
		}
		if rec.Counter <= requested[peerID.String()] {
			continue
		}
		updates = append(updates, toGossipNode(peerID, rec))
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].GetPeerId() < updates[j].GetPeerId()
	})

	return updates
}

func (s *Store) ApplyNode(update *statev1.GossipNode) ApplyResult {
	if update == nil {
		return ApplyResult{}
	}

	peerID, err := types.PeerKeyFromString(update.GetPeerId())
	if err != nil {
		return ApplyResult{}
	}

	incoming := fromGossipNode(update)

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.nodes[peerID]
	if current.Reachable == nil {
		current.Reachable = make(map[types.PeerKey]struct{})
	}

	if peerID == s.LocalID {
		if incoming.Counter > current.Counter {
			current.Counter = incoming.Counter
			s.nodes[peerID] = current
			return ApplyResult{Rebroadcast: s.bumpLocalLocked()}
		}

		if incoming.Counter == current.Counter {
			localPayload := toGossipNode(peerID, current)
			if !samePayload(localPayload, update) {
				return ApplyResult{Rebroadcast: s.bumpLocalLocked()}
			}
		}

		return ApplyResult{}
	}

	if incoming.Counter <= current.Counter {
		return ApplyResult{}
	}

	if len(incoming.IdentityPub) == 0 && len(current.IdentityPub) > 0 {
		incoming.IdentityPub = append([]byte(nil), current.IdentityPub...)
	}

	s.nodes[peerID] = incoming
	return ApplyResult{Updated: true}
}

func (s *Store) SetLocalNetwork(ips []string, port uint32) *statev1.GossipNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if slices.Compare(local.IPs, ips) == 0 && local.LocalPort == port {
		return nil
	}

	local.IPs = ips
	local.LocalPort = port

	s.nodes[s.LocalID] = local
	return s.bumpLocalLocked()
}

func (s *Store) SetLocalConnected(peerID types.PeerKey, connected bool) *statev1.GossipNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.Reachable == nil {
		local.Reachable = make(map[types.PeerKey]struct{})
	}

	_, exists := local.Reachable[peerID]
	if connected && exists {
		return nil
	}
	if !connected && !exists {
		return nil
	}

	if connected {
		local.Reachable[peerID] = struct{}{}
	} else {
		delete(local.Reachable, peerID)
	}

	s.nodes[s.LocalID] = local
	return s.bumpLocalLocked()
}

func (s *Store) UpsertLocalService(port uint32, name string) *statev1.GossipNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.Reachable == nil {
		local.Reachable = make(map[types.PeerKey]struct{})
	}

	s.nodes[s.LocalID].Services[name] = &statev1.Service{
		Name: name,
		Port: port,
	}
	return s.bumpLocalLocked()
}

func (s *Store) RemoveLocalServices(port uint32, name string) *statev1.GossipNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.Reachable == nil {
		local.Reachable = make(map[types.PeerKey]struct{})
	}

	delete(local.Services, name)

	s.nodes[s.LocalID] = local

	return s.bumpLocalLocked()
}

func (s *Store) LocalServices() map[string]*statev1.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()

	local := s.nodes[s.LocalID]
	return maps.Clone(local.Services)
}

func (s *Store) CloneNodes() map[types.PeerKey]nodeRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.nodes)
}

func (s *Store) Get(peerID types.PeerKey) (nodeRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	return rec, ok
}

func (s *Store) NodeIPs(peerID types.PeerKey) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok || len(rec.IPs) == 0 {
		return nil
	}

	return append([]string(nil), rec.IPs...)
}

func (s *Store) IsConnected(source, target types.PeerKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[source]
	if !ok {
		return false
	}

	_, ok = rec.Reachable[target]
	return ok
}

func (s *Store) ConnectedPeers(source types.PeerKey) []types.PeerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[source]
	if !ok {
		return nil
	}

	peers := make([]types.PeerKey, 0, len(rec.Reachable))
	for k := range rec.Reachable {
		peers = append(peers, k)
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Less(peers[j])
	})

	return peers
}

func (s *Store) KnownPeers() []KnownPeer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	known := make([]KnownPeer, 0, len(s.nodes))
	for peerID, rec := range s.nodes {
		if peerID == s.LocalID {
			continue
		}
		if len(rec.IPs) == 0 || rec.LocalPort == 0 {
			continue
		}
		known = append(known, KnownPeer{
			PeerID:      peerID,
			LocalPort:   rec.LocalPort,
			IdentityPub: append([]byte(nil), rec.IdentityPub...),
			IPs:         append([]string(nil), rec.IPs...),
		})
	}

	sort.Slice(known, func(i, j int) bool {
		return known[i].PeerID.Less(known[j].PeerID)
	})

	return known
}

func (s *Store) IdentityPub(peerID types.PeerKey) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok || len(rec.IdentityPub) == 0 {
		return nil, false
	}

	return append([]byte(nil), rec.IdentityPub...), true
}

func (s *Store) AddDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.desiredConnections[connectionKey(peerID, remotePort, localPort)] = Connection{
		PeerID:     peerID,
		RemotePort: remotePort,
		LocalPort:  localPort,
	}
}

func (s *Store) RemoveDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, conn := range s.desiredConnections {
		if conn.PeerID != peerID {
			continue
		}
		if remotePort != 0 && conn.RemotePort != remotePort {
			continue
		}
		if localPort != 0 && conn.LocalPort != localPort {
			continue
		}
		delete(s.desiredConnections, key)
	}
}

func (s *Store) DesiredConnections() []Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()

	connections := make([]Connection, 0, len(s.desiredConnections))
	for _, conn := range s.desiredConnections {
		connections = append(connections, conn)
	}

	sort.Slice(connections, func(i, j int) bool {
		a := connections[i]
		b := connections[j]
		if a.PeerID != b.PeerID {
			return a.PeerID.Less(b.PeerID)
		}
		if a.RemotePort != b.RemotePort {
			return a.RemotePort < b.RemotePort
		}
		return a.LocalPort < b.LocalPort
	})

	return connections
}

func (s *Store) bumpLocalLocked() *statev1.GossipNode {
	local := s.nodes[s.LocalID]
	local.Counter++
	s.nodes[s.LocalID] = local
	return toGossipNode(s.LocalID, local)
}

func toGossipNode(peerID types.PeerKey, rec nodeRecord) *statev1.GossipNode {
	reachable := make([]string, 0, len(rec.Reachable))
	for peer := range rec.Reachable {
		reachable = append(reachable, peer.String())
	}
	sort.Strings(reachable)

	services := make([]*statev1.Service, 0, len(rec.Services))
	for v := range maps.Values(rec.Services) {
		services = append(services, v)
	}
	return &statev1.GossipNode{
		PeerId:         peerID.String(),
		Counter:        rec.Counter,
		IdentityPub:    append([]byte(nil), rec.IdentityPub...),
		Ips:            append([]string(nil), rec.IPs...),
		LocalPort:      rec.LocalPort,
		Services:       services,
		ReachablePeers: reachable,
	}
}

func fromGossipNode(update *statev1.GossipNode) nodeRecord {
	reachable := make(map[types.PeerKey]struct{}, len(update.GetReachablePeers()))
	for _, peer := range update.GetReachablePeers() {
		id, err := types.PeerKeyFromString(peer)
		if err != nil {
			continue
		}
		reachable[id] = struct{}{}
	}

	services := make(map[string]*statev1.Service)
	for _, s := range update.Services {
		services[s.Name] = s
	}
	return nodeRecord{
		Counter:     update.GetCounter(),
		IdentityPub: append([]byte(nil), update.GetIdentityPub()...),
		IPs:         append([]string(nil), update.GetIps()...),
		LocalPort:   update.GetLocalPort(),
		Services:    services,
		Reachable:   reachable,
	}
}

func cloneRecord(rec nodeRecord) nodeRecord {
	services := make(map[string]*statev1.Service)
	for _, s := range rec.Services {
		services[s.Name] = s
	}
	cloned := nodeRecord{
		Counter:     rec.Counter,
		IdentityPub: append([]byte(nil), rec.IdentityPub...),
		IPs:         append([]string(nil), rec.IPs...),
		LocalPort:   rec.LocalPort,
		Services:    services,
		Reachable:   make(map[types.PeerKey]struct{}, len(rec.Reachable)),
	}
	for peer := range rec.Reachable {
		cloned.Reachable[peer] = struct{}{}
	}
	return cloned
}

func samePayload(a, b *statev1.GossipNode) bool {
	if a == nil || b == nil {
		return a == b
	}

	if a.GetPeerId() != b.GetPeerId() {
		return false
	}
	if a.GetLocalPort() != b.GetLocalPort() {
		return false
	}
	if !bytes.Equal(a.GetIdentityPub(), b.GetIdentityPub()) {
		return false
	}
	if slices.Compare(a.GetIps(), b.GetIps()) != 0 {
		return false
	}
	if slices.Compare(a.GetReachablePeers(), b.GetReachablePeers()) != 0 {
		return false
	}
	return sameServices(a.GetServices(), b.GetServices())
}

func sameServices(a, b []*statev1.Service) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] == nil && b[i] == nil {
			continue
		}
		if a[i] == nil || b[i] == nil {
			return false
		}
		if a[i].GetName() != b[i].GetName() || a[i].GetPort() != b[i].GetPort() {
			return false
		}
	}
	return true
}

func connectionKey(peerID types.PeerKey, remotePort, localPort uint32) string {
	return fmt.Sprintf("%s:%d:%d", peerID.String(), remotePort, localPort)
}
