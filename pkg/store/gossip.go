package store

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// Attribute key constants and helpers.
const (
	attrNet = "net"
	attrExt = "ext"
	attrID  = "id"

	prefixService   = "s/"
	prefixReachable = "r/"
)

func serviceKey(name string) string        { return prefixService + name }
func reachableKey(pk types.PeerKey) string  { return prefixReachable + pk.String() }

func isServiceKey(key string) (name string, ok bool) {
	if len(key) > len(prefixService) && key[:len(prefixService)] == prefixService {
		return key[len(prefixService):], true
	}
	return "", false
}

func isReachableKey(key string) (suffix string, ok bool) {
	if len(key) > len(prefixReachable) && key[:len(prefixReachable)] == prefixReachable {
		return key[len(prefixReachable):], true
	}
	return "", false
}

// logEntry tracks the counter (and deletion state) of a single attribute.
type logEntry struct {
	Counter uint64
	Deleted bool
}

type nodeRecord struct {
	// Materialized view — used by consumers (svc.go, peer logic, etc.)
	Reachable    map[types.PeerKey]struct{}
	Services     map[string]*statev1.Service
	IdentityPub  []byte
	IPs          []string
	LocalPort    uint32
	ExternalPort uint32

	// Event log metadata.
	MaxCounter uint64
	Log        map[string]logEntry // attr key → latest logEntry
}

type KnownPeer struct {
	IdentityPub  []byte
	IPs          []string
	LocalPort    uint32
	ExternalPort uint32
	PeerID       types.PeerKey
}

type Connection struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

type ApplyResult struct {
	Rebroadcast []*statev1.GossipEvent
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
				MaxCounter:  0,
				IdentityPub: append([]byte(nil), identityPub...),
				Reachable:   make(map[types.PeerKey]struct{}),
				Services:    make(map[string]*statev1.Service),
				Log:         make(map[string]logEntry),
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
			IdentityPub:  append([]byte(nil), identityPub...),
			IPs:          append([]string(nil), p.Addresses...),
			LocalPort:    p.Port,
			ExternalPort: p.ExternalPort,
			Reachable:    make(map[types.PeerKey]struct{}),
			Services:     make(map[string]*statev1.Service),
			Log:          make(map[string]logEntry),
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
			ExternalPort:   rec.ExternalPort,
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
		clock[peerID.String()] = rec.MaxCounter
	}

	return &statev1.GossipVectorClock{Counters: clock}
}

// MissingFor returns the individual events that the remote peer is missing,
// based on the provided vector clock.
func (s *Store) MissingFor(clock *statev1.GossipVectorClock) []*statev1.GossipEvent {
	requested := map[string]uint64{}
	if clock != nil {
		requested = clock.GetCounters()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []*statev1.GossipEvent
	for peerID, rec := range s.nodes {
		if rec.MaxCounter == 0 {
			continue
		}
		remoteCounter := requested[peerID.String()]
		if rec.MaxCounter <= remoteCounter {
			continue
		}
		events = append(events, s.buildEventsAbove(peerID, rec, remoteCounter)...)
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].GetPeerId() != events[j].GetPeerId() {
			return events[i].GetPeerId() < events[j].GetPeerId()
		}
		return events[i].GetCounter() < events[j].GetCounter()
	})

	return events
}

// ApplyEvent applies a single incoming gossip event.
func (s *Store) ApplyEvent(event *statev1.GossipEvent) ApplyResult {
	if event == nil {
		return ApplyResult{}
	}

	peerID, err := types.PeerKeyFromString(event.GetPeerId())
	if err != nil {
		return ApplyResult{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := s.nodes[peerID]
	if rec.Reachable == nil {
		rec.Reachable = make(map[types.PeerKey]struct{})
	}
	if rec.Services == nil {
		rec.Services = make(map[string]*statev1.Service)
	}
	if rec.Log == nil {
		rec.Log = make(map[string]logEntry)
	}

	key := event.GetKey()

	// Self-state conflict handling: receiving our own events from peers.
	// Only the higher-counter check is needed here. The old full-node model
	// also compared payloads at the same counter, but with per-attribute
	// events our own events regularly bounce back via gossip — triggering on
	// same-counter would cause a rebroadcast storm. The higher-counter check
	// catches the real split-brain case (another instance ran longer).
	if peerID == s.LocalID {
		if event.GetCounter() > rec.MaxCounter {
			rec.MaxCounter = event.GetCounter()
			s.nodes[peerID] = rec
			return ApplyResult{Rebroadcast: s.bumpAndBroadcastAllLocked()}
		}
		return ApplyResult{}
	}

	// Per-key stale check: only accept if this event is newer for this key.
	if existing, ok := rec.Log[key]; ok && event.GetCounter() <= existing.Counter {
		return ApplyResult{}
	}

	// Update materialized view.
	if event.GetDeleted() {
		s.applyDeleteLocked(&rec, key)
	} else {
		s.applyValueLocked(&rec, event)
	}

	// Update log entry.
	rec.Log[key] = logEntry{Counter: event.GetCounter(), Deleted: event.GetDeleted()}

	// Update MaxCounter.
	if event.GetCounter() > rec.MaxCounter {
		rec.MaxCounter = event.GetCounter()
	}

	s.nodes[peerID] = rec
	return ApplyResult{Updated: true}
}

// applyDeleteLocked removes the attribute from the materialized view.
func (s *Store) applyDeleteLocked(rec *nodeRecord, key string) {
	if name, ok := isServiceKey(key); ok {
		delete(rec.Services, name)
	} else if suffix, ok := isReachableKey(key); ok {
		pk, err := types.PeerKeyFromString(suffix)
		if err == nil {
			delete(rec.Reachable, pk)
		}
	}
}

// applyValueLocked updates the materialized view from an event's value.
func (s *Store) applyValueLocked(rec *nodeRecord, event *statev1.GossipEvent) {
	switch v := event.GetValue().(type) {
	case *statev1.GossipEvent_Network:
		if v.Network != nil {
			rec.IPs = append([]string(nil), v.Network.GetIps()...)
			rec.LocalPort = v.Network.GetLocalPort()
		}
	case *statev1.GossipEvent_ExternalPort:
		rec.ExternalPort = v.ExternalPort
	case *statev1.GossipEvent_IdentityPub:
		rec.IdentityPub = append([]byte(nil), v.IdentityPub...)
	case *statev1.GossipEvent_Service:
		if v.Service != nil {
			rec.Services[v.Service.GetName()] = v.Service
		}
	case *statev1.GossipEvent_Reachable:
		if suffix, ok := isReachableKey(event.GetKey()); ok {
			pk, err := types.PeerKeyFromString(suffix)
			if err == nil {
				if v.Reachable {
					rec.Reachable[pk] = struct{}{}
				} else {
					delete(rec.Reachable, pk)
				}
			}
		}
	}
}

// --- Local mutation methods (return events for broadcasting) ---

func (s *Store) SetLocalNetwork(ips []string, port uint32) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if slices.Equal(local.IPs, ips) && local.LocalPort == port {
		return nil
	}

	local.IPs = ips
	local.LocalPort = port

	counter := s.bumpCounterLocked(&local)
	local.Log[attrNet] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Key:     attrNet,
		Value: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkInfo{
				Ips:       append([]string(nil), ips...),
				LocalPort: port,
			},
		},
	}}
}

func (s *Store) SetExternalPort(port uint32) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.ExternalPort == port {
		return nil
	}

	local.ExternalPort = port

	counter := s.bumpCounterLocked(&local)
	local.Log[attrExt] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Key:     attrExt,
		Value:   &statev1.GossipEvent_ExternalPort{ExternalPort: port},
	}}
}

func (s *Store) SetLocalConnected(peerID types.PeerKey, connected bool) []*statev1.GossipEvent {
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

	key := reachableKey(peerID)
	counter := s.bumpCounterLocked(&local)
	local.Log[key] = logEntry{Counter: counter, Deleted: !connected}
	s.nodes[s.LocalID] = local

	event := &statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Key:     key,
		Deleted: !connected,
	}
	if connected {
		event.Value = &statev1.GossipEvent_Reachable{Reachable: true}
	}

	return []*statev1.GossipEvent{event}
}

func (s *Store) UpsertLocalService(port uint32, name string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if existing, ok := local.Services[name]; ok && existing.GetPort() == port {
		return nil
	}

	local.Services[name] = &statev1.Service{
		Name: name,
		Port: port,
	}

	key := serviceKey(name)
	counter := s.bumpCounterLocked(&local)
	local.Log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Key:     key,
		Value: &statev1.GossipEvent_Service{
			Service: &statev1.Service{Name: name, Port: port},
		},
	}}
}

func (s *Store) RemoveLocalServices(port uint32, name string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if _, ok := local.Services[name]; !ok {
		return nil
	}

	delete(local.Services, name)

	key := serviceKey(name)
	counter := s.bumpCounterLocked(&local)
	local.Log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Key:     key,
		Deleted: true,
	}}
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
			PeerID:       peerID,
			LocalPort:    rec.LocalPort,
			ExternalPort: rec.ExternalPort,
			IdentityPub:  append([]byte(nil), rec.IdentityPub...),
			IPs:          append([]string(nil), rec.IPs...),
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

// --- Internal helpers ---

// bumpCounterLocked increments the MaxCounter on a local record and returns the new value.
// Caller must hold s.mu.
func (s *Store) bumpCounterLocked(rec *nodeRecord) uint64 {
	rec.MaxCounter++
	return rec.MaxCounter
}

// bumpAndBroadcastAllLocked returns events for ALL current local attributes,
// each with its own incremented counter. Used on self-state conflict (restart
// recovery). Caller must hold s.mu.
func (s *Store) bumpAndBroadcastAllLocked() []*statev1.GossipEvent {
	local := s.nodes[s.LocalID]

	var events []*statev1.GossipEvent
	peerIDStr := s.LocalID.String()

	nextCounter := func() uint64 {
		local.MaxCounter++
		return local.MaxCounter
	}

	// net
	if len(local.IPs) > 0 || local.LocalPort != 0 {
		c := nextCounter()
		local.Log[attrNet] = logEntry{Counter: c}
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerIDStr,
			Counter: c,
			Key:     attrNet,
			Value: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkInfo{
					Ips:       append([]string(nil), local.IPs...),
					LocalPort: local.LocalPort,
				},
			},
		})
	}

	// ext
	if local.ExternalPort != 0 {
		c := nextCounter()
		local.Log[attrExt] = logEntry{Counter: c}
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerIDStr,
			Counter: c,
			Key:     attrExt,
			Value:   &statev1.GossipEvent_ExternalPort{ExternalPort: local.ExternalPort},
		})
	}

	// id
	if len(local.IdentityPub) > 0 {
		c := nextCounter()
		local.Log[attrID] = logEntry{Counter: c}
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerIDStr,
			Counter: c,
			Key:     attrID,
			Value:   &statev1.GossipEvent_IdentityPub{IdentityPub: append([]byte(nil), local.IdentityPub...)},
		})
	}

	// services
	for name, svc := range local.Services {
		key := serviceKey(name)
		c := nextCounter()
		local.Log[key] = logEntry{Counter: c}
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerIDStr,
			Counter: c,
			Key:     key,
			Value: &statev1.GossipEvent_Service{
				Service: &statev1.Service{Name: svc.GetName(), Port: svc.GetPort()},
			},
		})
	}

	// reachable peers
	for pk := range local.Reachable {
		key := reachableKey(pk)
		c := nextCounter()
		local.Log[key] = logEntry{Counter: c}
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerIDStr,
			Counter: c,
			Key:     key,
			Value:   &statev1.GossipEvent_Reachable{Reachable: true},
		})
	}

	s.nodes[s.LocalID] = local
	return events
}

// buildEventsAbove constructs GossipEvent messages for all log entries with
// counter > minCounter. Caller must hold s.mu (at least RLock).
func (s *Store) buildEventsAbove(peerID types.PeerKey, rec nodeRecord, minCounter uint64) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	peerIDStr := peerID.String()

	for key, entry := range rec.Log {
		if entry.Counter <= minCounter {
			continue
		}
		event := s.buildEventFromLog(peerIDStr, key, entry, rec)
		if event != nil {
			events = append(events, event)
		}
	}

	return events
}

// buildEventFromLog constructs a single GossipEvent from a log entry and the
// materialized view. Caller must hold s.mu (at least RLock).
func (s *Store) buildEventFromLog(peerIDStr, key string, entry logEntry, rec nodeRecord) *statev1.GossipEvent {
	event := &statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: entry.Counter,
		Key:     key,
		Deleted: entry.Deleted,
	}

	if entry.Deleted {
		return event
	}

	switch key {
	case attrNet:
		event.Value = &statev1.GossipEvent_Network{
			Network: &statev1.NetworkInfo{
				Ips:       append([]string(nil), rec.IPs...),
				LocalPort: rec.LocalPort,
			},
		}
	case attrExt:
		event.Value = &statev1.GossipEvent_ExternalPort{ExternalPort: rec.ExternalPort}
	case attrID:
		event.Value = &statev1.GossipEvent_IdentityPub{IdentityPub: append([]byte(nil), rec.IdentityPub...)}
	default:
		if name, ok := isServiceKey(key); ok {
			svc, exists := rec.Services[name]
			if !exists {
				return nil
			}
			event.Value = &statev1.GossipEvent_Service{
				Service: &statev1.Service{Name: svc.GetName(), Port: svc.GetPort()},
			}
		} else if _, ok := isReachableKey(key); ok {
			event.Value = &statev1.GossipEvent_Reachable{Reachable: true}
		}
	}

	return event
}

func cloneRecord(rec nodeRecord) nodeRecord {
	services := make(map[string]*statev1.Service)
	for _, s := range rec.Services {
		services[s.Name] = s
	}
	log := make(map[string]logEntry, len(rec.Log))
	for k, v := range rec.Log {
		log[k] = v
	}
	cloned := nodeRecord{
		MaxCounter:   rec.MaxCounter,
		IdentityPub:  append([]byte(nil), rec.IdentityPub...),
		IPs:          append([]string(nil), rec.IPs...),
		LocalPort:    rec.LocalPort,
		ExternalPort: rec.ExternalPort,
		Services:     services,
		Reachable:    make(map[types.PeerKey]struct{}, len(rec.Reachable)),
		Log:          log,
	}
	for peer := range rec.Reachable {
		cloned.Reachable[peer] = struct{}{}
	}
	return cloned
}

func connectionKey(peerID types.PeerKey, remotePort, localPort uint32) string {
	return fmt.Sprintf("%s:%d:%d", peerID.String(), remotePort, localPort)
}

