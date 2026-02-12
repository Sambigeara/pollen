package store

import (
	"fmt"
	"sort"
	"sync"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

type nodeRecord struct {
	Reachable   map[types.PeerKey]struct{}
	IdentityPub []byte
	IPs         []string
	Services    []*statev1.Service
	Counter     uint64
	LocalPort   uint32
}

type NodeSnapshot struct {
	IdentityPub []byte
	IPs         []string
	Services    []*statev1.Service
	Reachable   []types.PeerKey
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

func Load(pollenDir string, localID types.PeerKey, identityPub []byte) (*Store, error) {
	d, err := openDisk(pollenDir)
	if err != nil {
		return nil, err
	}

	onDisk, err := d.load()
	if err != nil {
		_ = d.close()
		return nil, err
	}

	s := &Store{
		LocalID:            localID,
		disk:               d,
		nodes:              make(map[types.PeerKey]nodeRecord),
		desiredConnections: make(map[string]Connection),
	}

	local := nodeRecord{
		Counter:     0,
		IdentityPub: append([]byte(nil), identityPub...),
		Reachable:   make(map[types.PeerKey]struct{}),
	}
	s.nodes[localID] = local

	for _, p := range onDisk.Peers {
		peerID, err := types.PeerKeyFromString(p.NoisePublic)
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

		rec := s.nodes[peerID]
		rec.IdentityPub = append([]byte(nil), identityPub...)
		rec.IPs = append([]string(nil), p.Addresses...)
		rec.LocalPort = p.Port
		if rec.Reachable == nil {
			rec.Reachable = make(map[types.PeerKey]struct{})
		}
		s.nodes[peerID] = rec
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
		rec.Services = upsertService(rec.Services, svc.Port, svc.Name)
		s.nodes[provider] = rec
	}

	for _, conn := range onDisk.Connections {
		peerID, err := types.PeerKeyFromString(conn.Peer)
		if err != nil {
			continue
		}
		s.addDesiredConnectionLocked(Connection{
			PeerID:     peerID,
			RemotePort: conn.RemotePort,
			LocalPort:  conn.LocalPort,
		})
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
			NoisePublic:    peerID.String(),
			IdentityPublic: encodeHex(rec.IdentityPub),
			Addresses:      append([]string(nil), rec.IPs...),
			Port:           rec.LocalPort,
		})
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].NoisePublic < peers[j].NoisePublic
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
			NoisePublic:    s.LocalID.String(),
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
	if local.Reachable == nil {
		local.Reachable = make(map[types.PeerKey]struct{})
	}

	changed := !sameStrings(local.IPs, ips) || local.LocalPort != port
	if !changed {
		return nil
	}

	local.IPs = append([]string(nil), ips...)
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

	updated := upsertService(local.Services, port, name)
	if sameServices(local.Services, updated) {
		return nil
	}

	local.Services = updated
	s.nodes[s.LocalID] = local
	return s.bumpLocalLocked()
}

func (s *Store) RemoveLocalServices(port uint32, name string) ([]uint32, *statev1.GossipNode) {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.Reachable == nil {
		local.Reachable = make(map[types.PeerKey]struct{})
	}

	updated, removed := removeServices(local.Services, port, name)
	if len(removed) == 0 {
		return nil, nil
	}

	local.Services = updated
	s.nodes[s.LocalID] = local
	return removed, s.bumpLocalLocked()
}

func (s *Store) LocalServices() []*statev1.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()

	local := s.nodes[s.LocalID]
	return cloneServices(local.Services)
}

func (s *Store) Nodes() map[types.PeerKey]NodeSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[types.PeerKey]NodeSnapshot, len(s.nodes))
	for peerID, rec := range s.nodes {
		reachable := make([]types.PeerKey, 0, len(rec.Reachable))
		for k := range rec.Reachable {
			reachable = append(reachable, k)
		}
		sort.Slice(reachable, func(i, j int) bool {
			return reachable[i].Less(reachable[j])
		})

		out[peerID] = NodeSnapshot{
			Counter:     rec.Counter,
			LocalPort:   rec.LocalPort,
			IdentityPub: append([]byte(nil), rec.IdentityPub...),
			IPs:         append([]string(nil), rec.IPs...),
			Services:    cloneServices(rec.Services),
			Reachable:   reachable,
		}
	}

	return out
}

func (s *Store) Get(peerID types.PeerKey) (NodeSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return NodeSnapshot{}, false
	}

	reachable := make([]types.PeerKey, 0, len(rec.Reachable))
	for k := range rec.Reachable {
		reachable = append(reachable, k)
	}
	sort.Slice(reachable, func(i, j int) bool {
		return reachable[i].Less(reachable[j])
	})

	return NodeSnapshot{
		Counter:     rec.Counter,
		LocalPort:   rec.LocalPort,
		IdentityPub: append([]byte(nil), rec.IdentityPub...),
		IPs:         append([]string(nil), rec.IPs...),
		Services:    cloneServices(rec.Services),
		Reachable:   reachable,
	}, true
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

	s.addDesiredConnectionLocked(Connection{
		PeerID:     peerID,
		RemotePort: remotePort,
		LocalPort:  localPort,
	})
}

func (s *Store) addDesiredConnectionLocked(conn Connection) {
	if conn.PeerID == (types.PeerKey{}) || conn.RemotePort == 0 || conn.LocalPort == 0 {
		return
	}
	s.desiredConnections[connectionKey(conn.PeerID, conn.RemotePort, conn.LocalPort)] = conn
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

func (s *Store) ReplaceDesiredConnections(connections []Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[string]Connection, len(connections))
	for _, conn := range connections {
		if conn.PeerID == (types.PeerKey{}) || conn.RemotePort == 0 || conn.LocalPort == 0 {
			continue
		}
		next[connectionKey(conn.PeerID, conn.RemotePort, conn.LocalPort)] = conn
	}

	s.desiredConnections = next
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

	return &statev1.GossipNode{
		PeerId:         peerID.String(),
		Counter:        rec.Counter,
		IdentityPub:    append([]byte(nil), rec.IdentityPub...),
		Ips:            append([]string(nil), rec.IPs...),
		LocalPort:      rec.LocalPort,
		Services:       cloneServices(rec.Services),
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

	return nodeRecord{
		Counter:     update.GetCounter(),
		IdentityPub: append([]byte(nil), update.GetIdentityPub()...),
		IPs:         append([]string(nil), update.GetIps()...),
		LocalPort:   update.GetLocalPort(),
		Services:    cloneServices(update.GetServices()),
		Reachable:   reachable,
	}
}

func cloneRecord(rec nodeRecord) nodeRecord {
	cloned := nodeRecord{
		Counter:     rec.Counter,
		IdentityPub: append([]byte(nil), rec.IdentityPub...),
		IPs:         append([]string(nil), rec.IPs...),
		LocalPort:   rec.LocalPort,
		Services:    cloneServices(rec.Services),
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
	if !sameBytes(a.GetIdentityPub(), b.GetIdentityPub()) {
		return false
	}
	if !sameStrings(a.GetIps(), b.GetIps()) {
		return false
	}
	if !sameStrings(a.GetReachablePeers(), b.GetReachablePeers()) {
		return false
	}
	return sameServices(a.GetServices(), b.GetServices())
}

func sameBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

func upsertService(services []*statev1.Service, port uint32, name string) []*statev1.Service {
	updated := make([]*statev1.Service, 0, len(services)+1)
	seen := false
	for _, svc := range services {
		if svc.GetPort() == port {
			updated = append(updated, &statev1.Service{Name: name, Port: port})
			seen = true
			continue
		}
		updated = append(updated, &statev1.Service{Name: svc.GetName(), Port: svc.GetPort()})
	}
	if !seen {
		updated = append(updated, &statev1.Service{Name: name, Port: port})
	}

	sort.Slice(updated, func(i, j int) bool {
		if updated[i].GetPort() != updated[j].GetPort() {
			return updated[i].GetPort() < updated[j].GetPort()
		}
		return updated[i].GetName() < updated[j].GetName()
	})

	return updated
}

func removeServices(services []*statev1.Service, port uint32, name string) ([]*statev1.Service, []uint32) {
	updated := make([]*statev1.Service, 0, len(services))
	removed := make([]uint32, 0, len(services))
	for _, svc := range services {
		if matchesService(svc, port, name) {
			removed = append(removed, svc.GetPort())
			continue
		}
		updated = append(updated, &statev1.Service{Name: svc.GetName(), Port: svc.GetPort()})
	}

	sort.Slice(updated, func(i, j int) bool {
		if updated[i].GetPort() != updated[j].GetPort() {
			return updated[i].GetPort() < updated[j].GetPort()
		}
		return updated[i].GetName() < updated[j].GetName()
	})

	return updated, removed
}

func matchesService(svc *statev1.Service, port uint32, name string) bool {
	if svc == nil {
		return false
	}
	if port != 0 && svc.GetPort() == port {
		return true
	}
	if name == "" {
		return false
	}
	if svc.GetName() == name {
		return true
	}
	return svc.GetName() == "" && serviceNameForPort(svc.GetPort()) == name
}

func serviceNameForPort(port uint32) string {
	return fmt.Sprintf("%d", port)
}
