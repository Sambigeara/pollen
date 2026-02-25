package store

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

type attrKind uint8

const (
	attrNetwork attrKind = iota + 1
	attrExternalPort
	attrIdentity
	attrService
	attrReachability
	attrRevocation
)

type attrKey struct {
	name string
	peer types.PeerKey
	kind attrKind
}

func networkAttrKey() attrKey {
	return attrKey{kind: attrNetwork}
}

func externalPortAttrKey() attrKey {
	return attrKey{kind: attrExternalPort}
}

func identityAttrKey() attrKey {
	return attrKey{kind: attrIdentity}
}

func serviceAttrKey(name string) attrKey {
	return attrKey{kind: attrService, name: name}
}

func reachabilityAttrKey(peerID types.PeerKey) attrKey {
	return attrKey{kind: attrReachability, peer: peerID}
}

func revocationAttrKey(subjectPubHex string) attrKey {
	return attrKey{kind: attrRevocation, name: subjectPubHex}
}

// logEntry tracks the counter (and deletion state) of a single attribute.
type logEntry struct {
	Counter uint64
	Deleted bool
}

type nodeRecord struct {
	Reachable    map[types.PeerKey]struct{}
	Services     map[string]*statev1.Service
	log          map[attrKey]logEntry
	LastAddr     string
	IdentityPub  []byte
	IPs          []string
	maxCounter   uint64
	LocalPort    uint32
	ExternalPort uint32
}

type KnownPeer struct {
	LastAddr     string
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

func (c Connection) Key() string {
	return fmt.Sprintf("%s:%d:%d", c.PeerID.String(), c.RemotePort, c.LocalPort)
}

type ApplyResult struct {
	Rebroadcast     []*statev1.GossipEvent
	revokedSubjects []types.PeerKey
}

type Store struct {
	disk               *disk
	nodes              map[types.PeerKey]nodeRecord
	revocations        map[types.PeerKey]*admissionv1.SignedRevocation
	trustBundle        *admissionv1.TrustBundle
	desiredConnections map[string]Connection
	onRevocation       func(types.PeerKey)
	mu                 sync.RWMutex
	LocalID            types.PeerKey
}

func Load(pollenDir string, identityPub []byte, trustBundle *admissionv1.TrustBundle) (*Store, error) {
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
				maxCounter:  1,
				IdentityPub: append([]byte(nil), identityPub...),
				Reachable:   make(map[types.PeerKey]struct{}),
				Services:    make(map[string]*statev1.Service),
				log: map[attrKey]logEntry{
					identityAttrKey(): {Counter: 1},
				},
			},
		},
		revocations:        make(map[types.PeerKey]*admissionv1.SignedRevocation),
		trustBundle:        trustBundle,
		desiredConnections: make(map[string]Connection),
	}

	s.revocations = unmarshalDiskRevocations(onDisk.Revocations, trustBundle)

	// Inject disk-loaded revocations into the local log so that
	// bumpAndBroadcastAllLocked can re-publish them after restart.
	local := s.nodes[localID]
	for subjectKey := range s.revocations {
		local.maxCounter++
		local.log[revocationAttrKey(subjectKey.String())] = logEntry{Counter: local.maxCounter}
	}
	s.nodes[localID] = local

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
			LastAddr:     p.LastAddr,
			LocalPort:    p.Port,
			ExternalPort: p.ExternalPort,
			Reachable:    make(map[types.PeerKey]struct{}),
			Services:     make(map[string]*statev1.Service),
			log:          make(map[attrKey]logEntry),
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
		if provider == localID {
			rec.maxCounter++
			rec.log[serviceAttrKey(svc.Name)] = logEntry{Counter: rec.maxCounter}
		}
		s.nodes[provider] = rec
	}

	for _, conn := range onDisk.Connections {
		peerID, err := types.PeerKeyFromString(conn.Peer)
		if err != nil {
			continue
		}
		c := Connection{PeerID: peerID, RemotePort: conn.RemotePort, LocalPort: conn.LocalPort}
		s.desiredConnections[c.Key()] = c
	}

	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.disk == nil {
		return nil
	}
	return s.disk.close()
}

func (s *Store) OnRevocation(fn func(types.PeerKey)) {
	s.onRevocation = fn
}

func (s *Store) Save() error {
	s.mu.RLock()
	nodes := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	maps.Copy(nodes, s.nodes)
	connections := make([]Connection, 0, len(s.desiredConnections))
	for _, conn := range s.desiredConnections {
		connections = append(connections, conn)
	}
	revocations := maps.Clone(s.revocations)
	s.mu.RUnlock()

	peers := make([]diskPeer, 0, len(nodes))
	for peerID, rec := range nodes {
		if peerID == s.LocalID {
			continue
		}
		peers = append(peers, diskPeer{
			IdentityPublic: encodeHex(rec.IdentityPub),
			Addresses:      rec.IPs,
			Port:           rec.LocalPort,
			ExternalPort:   rec.ExternalPort,
			LastAddr:       rec.LastAddr,
		})
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].IdentityPublic < peers[j].IdentityPublic
	})

	services := toDiskServices(nodes)

	sortConnections(connections)

	diskConns := make([]diskConnection, 0, len(connections))
	for _, conn := range connections {
		diskConns = append(diskConns, diskConnection{
			Peer:       conn.PeerID.String(),
			RemotePort: conn.RemotePort,
			LocalPort:  conn.LocalPort,
		})
	}

	diskRevs := marshalDiskRevocations(revocations)

	localRec := nodes[s.LocalID]
	st := diskState{
		Local: diskLocal{
			IdentityPublic: encodeHex(localRec.IdentityPub),
		},
		Peers:       peers,
		Services:    services,
		Connections: diskConns,
		Revocations: diskRevs,
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
		clock[peerID.String()] = rec.maxCounter
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
		if rec.maxCounter == 0 {
			continue
		}
		remoteCounter := requested[peerID.String()]
		if rec.maxCounter <= remoteCounter {
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

// ApplyEvents applies a batch of incoming gossip events under a single lock acquisition.
func (s *Store) ApplyEvents(events []*statev1.GossipEvent) ApplyResult {
	s.mu.Lock()

	var result ApplyResult
	var revokedSubjects []types.PeerKey
	for _, event := range events {
		if event == nil {
			continue
		}
		r := s.applyEventLocked(event)
		result.Rebroadcast = append(result.Rebroadcast, r.Rebroadcast...)
		revokedSubjects = append(revokedSubjects, r.revokedSubjects...)
	}

	s.mu.Unlock()

	if s.onRevocation != nil {
		for _, subject := range revokedSubjects {
			s.onRevocation(subject)
		}
	}

	return result
}

func (s *Store) applyEventLocked(event *statev1.GossipEvent) ApplyResult {
	peerID, err := types.PeerKeyFromString(event.GetPeerId())
	if err != nil {
		return ApplyResult{}
	}

	rec := s.nodes[peerID]
	if rec.Reachable == nil {
		rec.Reachable = make(map[types.PeerKey]struct{})
	}
	if rec.Services == nil {
		rec.Services = make(map[string]*statev1.Service)
	}
	if rec.log == nil {
		rec.log = make(map[attrKey]logEntry)
	}

	key, ok := eventAttrKey(event)
	if !ok {
		return ApplyResult{}
	}

	// Self-state conflict handling: receiving our own events from peers.
	// Only the higher-counter check is needed here. The old full-node model
	// also compared payloads at the same counter, but with per-attribute
	// events our own events regularly bounce back via gossip — triggering on
	// same-counter would cause a rebroadcast storm. The higher-counter check
	// catches the real split-brain case (another instance ran longer).
	if peerID == s.LocalID {
		if event.GetCounter() > rec.maxCounter {
			rec.maxCounter = event.GetCounter()
			s.nodes[peerID] = rec
			return ApplyResult{Rebroadcast: s.bumpAndBroadcastAllLocked()}
		}
		return ApplyResult{}
	}

	// Per-key stale check: only accept if this event is newer for this key.
	if existing, ok := rec.log[key]; ok && event.GetCounter() <= existing.Counter {
		return ApplyResult{}
	}

	deleted := event.GetDeleted()

	switch {
	case key.kind == attrRevocation:
		if deleted {
			return ApplyResult{}
		}
		v := event.GetChange().(*statev1.GossipEvent_Revocation) //nolint:forcetypeassert
		var rev *admissionv1.SignedRevocation
		if v.Revocation != nil {
			rev = v.Revocation.GetRevocation()
		}
		applied, subject := s.applyRevocationLocked(rev)
		if !applied {
			return ApplyResult{}
		}
		var revoked []types.PeerKey
		if (subject != types.PeerKey{}) {
			revoked = []types.PeerKey{subject}
		}

		rec.log[key] = logEntry{Counter: event.GetCounter(), Deleted: deleted}
		if event.GetCounter() > rec.maxCounter {
			rec.maxCounter = event.GetCounter()
		}
		s.nodes[peerID] = rec
		return ApplyResult{revokedSubjects: revoked}
	case deleted:
		applyDeleteLocked(&rec, key)
	default:
		applyValueLocked(&rec, event, key)
	}

	rec.log[key] = logEntry{Counter: event.GetCounter(), Deleted: deleted}

	// Update maxCounter.
	if event.GetCounter() > rec.maxCounter {
		rec.maxCounter = event.GetCounter()
	}

	s.nodes[peerID] = rec
	return ApplyResult{}
}

func eventAttrKey(event *statev1.GossipEvent) (attrKey, bool) {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		return networkAttrKey(), true
	case *statev1.GossipEvent_ExternalPort:
		return externalPortAttrKey(), true
	case *statev1.GossipEvent_IdentityPub:
		return identityAttrKey(), true
	case *statev1.GossipEvent_Service:
		if v.Service == nil || v.Service.GetName() == "" {
			return attrKey{}, false
		}
		return serviceAttrKey(v.Service.GetName()), true
	case *statev1.GossipEvent_Reachability:
		pk, err := types.PeerKeyFromString(v.Reachability.GetPeerId())
		if err != nil {
			return attrKey{}, false
		}
		return reachabilityAttrKey(pk), true
	case *statev1.GossipEvent_Revocation:
		subjectKey := types.PeerKeyFromBytes(v.Revocation.GetRevocation().GetEntry().GetSubjectPub())
		return revocationAttrKey(subjectKey.String()), true
	default:
		return attrKey{}, false
	}
}

// applyRevocationLocked stores a verified revocation. Returns (true, subjectKey)
// if the revocation was newly stored, (true, zero) if it was already known, or
// (false, zero) if the event should be silently dropped (e.g. invalid signature).
func (s *Store) applyRevocationLocked(rev *admissionv1.SignedRevocation) (applied bool, subject types.PeerKey) {
	if s.trustBundle == nil {
		return false, types.PeerKey{}
	}
	if err := auth.VerifyRevocation(rev, s.trustBundle); err != nil {
		return false, types.PeerKey{}
	}
	subjectKey := types.PeerKeyFromBytes(rev.GetEntry().GetSubjectPub())
	if _, exists := s.revocations[subjectKey]; exists {
		return true, types.PeerKey{}
	}
	s.revocations[subjectKey] = rev
	return true, subjectKey
}

func applyDeleteLocked(rec *nodeRecord, key attrKey) {
	switch key.kind {
	case attrNetwork:
		rec.IPs = nil
		rec.LocalPort = 0
	case attrExternalPort:
		rec.ExternalPort = 0
	case attrIdentity:
		rec.IdentityPub = nil
	case attrService:
		m := make(map[string]*statev1.Service, len(rec.Services))
		for k, v := range rec.Services {
			if k != key.name {
				m[k] = v
			}
		}
		rec.Services = m
	case attrReachability:
		delete(rec.Reachable, key.peer)
	case attrRevocation:
		// Revocations are cluster-wide, not per-node; deletion is a no-op.
	default:
		panic("unknown attr kind")
	}
}

// applyValueLocked updates the materialized view from an event's value.
func applyValueLocked(rec *nodeRecord, event *statev1.GossipEvent, key attrKey) {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		if v.Network == nil {
			return
		}
		rec.IPs = append([]string(nil), v.Network.GetIps()...)
		rec.LocalPort = v.Network.GetLocalPort()
	case *statev1.GossipEvent_ExternalPort:
		if v.ExternalPort != nil {
			rec.ExternalPort = v.ExternalPort.GetExternalPort()
		}
	case *statev1.GossipEvent_IdentityPub:
		if v.IdentityPub != nil {
			rec.IdentityPub = append([]byte(nil), v.IdentityPub.GetIdentityPub()...)
		}
	case *statev1.GossipEvent_Service:
		if v.Service != nil {
			m := make(map[string]*statev1.Service, len(rec.Services)+1)
			maps.Copy(m, rec.Services)
			m[key.name] = &statev1.Service{Name: key.name, Port: v.Service.GetPort()}
			rec.Services = m
		}
	case *statev1.GossipEvent_Reachability:
		rec.Reachable[key.peer] = struct{}{}
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

	local.IPs = append([]string(nil), ips...)
	local.LocalPort = port

	local.maxCounter++
	counter := local.maxCounter
	local.log[networkAttrKey()] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{
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

	local.maxCounter++
	counter := local.maxCounter
	local.log[externalPortAttrKey()] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: port},
		},
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

	key := reachabilityAttrKey(peerID)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: !connected}
	s.nodes[s.LocalID] = local

	event := &statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: !connected,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{
				PeerId: peerID.String(),
			},
		},
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

	key := serviceAttrKey(name)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: name, Port: port},
		},
	}}
}

func (s *Store) RemoveLocalServices(name string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	if _, ok := local.Services[name]; !ok {
		return nil
	}

	delete(local.Services, name)

	key := serviceAttrKey(name)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: true,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: name},
		},
	}}
}

func (s *Store) LocalServices() map[string]*statev1.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()

	local := s.nodes[s.LocalID]
	return maps.Clone(local.Services)
}

func (s *Store) AllNodes() map[types.PeerKey]nodeRecord {
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

	return rec.IPs
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

func (s *Store) HasServicePort(peerID types.PeerKey, port uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	for _, svc := range rec.Services {
		if svc.GetPort() == port {
			return true
		}
	}
	return false
}

func (s *Store) KnownPeers() []KnownPeer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	known := make([]KnownPeer, 0, len(s.nodes))
	for peerID, rec := range s.nodes {
		if peerID == s.LocalID {
			continue
		}
		if rec.LastAddr == "" && (len(rec.IPs) == 0 || rec.LocalPort == 0) {
			continue
		}
		known = append(known, KnownPeer{
			PeerID:       peerID,
			LocalPort:    rec.LocalPort,
			ExternalPort: rec.ExternalPort,
			IdentityPub:  rec.IdentityPub,
			IPs:          rec.IPs,
			LastAddr:     rec.LastAddr,
		})
	}

	sort.Slice(known, func(i, j int) bool {
		return known[i].PeerID.Less(known[j].PeerID)
	})

	return known
}

func (s *Store) SetLastAddr(peerID types.PeerKey, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return
	}
	rec.LastAddr = addr
	s.nodes[peerID] = rec
}

func (s *Store) IdentityPub(peerID types.PeerKey) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok || len(rec.IdentityPub) == 0 {
		return nil, false
	}

	return rec.IdentityPub, true
}

func (s *Store) AddDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c := Connection{PeerID: peerID, RemotePort: remotePort, LocalPort: localPort}
	s.desiredConnections[c.Key()] = c
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

	sortConnections(connections)

	return connections
}

func sortConnections(cs []Connection) {
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].PeerID != cs[j].PeerID {
			return cs[i].PeerID.Less(cs[j].PeerID)
		}
		if cs[i].RemotePort != cs[j].RemotePort {
			return cs[i].RemotePort < cs[j].RemotePort
		}
		return cs[i].LocalPort < cs[j].LocalPort
	})
}

func (s *Store) IsSubjectRevoked(subjectPub []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subjectKey := types.PeerKeyFromBytes(subjectPub)
	_, ok := s.revocations[subjectKey]
	return ok
}

func (s *Store) PublishRevocation(rev *admissionv1.SignedRevocation) []*statev1.GossipEvent {
	subjectKey := types.PeerKeyFromBytes(rev.GetEntry().GetSubjectPub())

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.revocations[subjectKey]; ok {
		return nil
	}

	s.revocations[subjectKey] = rev

	local := s.nodes[s.LocalID]
	key := revocationAttrKey(subjectKey.String())
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	}}
}

// --- Internal helpers ---

// bumpAndBroadcastAllLocked returns events for ALL current local attributes,
// each with its own incremented counter. Used on self-state conflict (restart
// recovery). Caller must hold s.mu.
func (s *Store) bumpAndBroadcastAllLocked() []*statev1.GossipEvent {
	local := s.nodes[s.LocalID]

	for key, entry := range local.log {
		local.maxCounter++
		entry.Counter = local.maxCounter
		local.log[key] = entry
	}

	s.nodes[s.LocalID] = local
	return s.buildEventsAbove(s.LocalID, local, 0)
}

// buildEventsAbove constructs GossipEvent messages for all log entries with
// counter > minCounter. Caller must hold s.mu (at least RLock).
func (s *Store) buildEventsAbove(peerID types.PeerKey, rec nodeRecord, minCounter uint64) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	peerIDStr := peerID.String()

	for key, entry := range rec.log {
		if entry.Counter <= minCounter {
			continue
		}
		if key.kind == attrRevocation {
			events = append(events, s.buildRevocationEvent(peerIDStr, key, entry))
			continue
		}
		events = append(events, buildEventFromLog(peerIDStr, key, entry, rec))
	}

	return events
}

func (s *Store) buildRevocationEvent(peerIDStr string, key attrKey, entry logEntry) *statev1.GossipEvent {
	subjectKey, err := types.PeerKeyFromString(key.name)
	if err != nil {
		panic("invalid revocation key in log")
	}
	rev, ok := s.revocations[subjectKey]
	if !ok {
		panic("revocation log entry missing revocation payload")
	}
	return &statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: entry.Counter,
		Deleted: entry.Deleted,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	}
}

// buildEventFromLog constructs a single GossipEvent from a log entry and the
// materialized view. Caller must hold the store's mu (at least RLock).
func buildEventFromLog(peerIDStr string, key attrKey, entry logEntry, rec nodeRecord) *statev1.GossipEvent {
	event := &statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: entry.Counter,
		Deleted: entry.Deleted,
	}

	switch key.kind {
	case attrNetwork:
		change := &statev1.NetworkChange{}
		if !entry.Deleted {
			change.Ips = append([]string(nil), rec.IPs...)
			change.LocalPort = rec.LocalPort
		}
		event.Change = &statev1.GossipEvent_Network{Network: change}
	case attrExternalPort:
		change := &statev1.ExternalPortChange{}
		if !entry.Deleted {
			change.ExternalPort = rec.ExternalPort
		}
		event.Change = &statev1.GossipEvent_ExternalPort{ExternalPort: change}
	case attrIdentity:
		change := &statev1.IdentityChange{}
		if !entry.Deleted {
			change.IdentityPub = append([]byte(nil), rec.IdentityPub...)
		}
		event.Change = &statev1.GossipEvent_IdentityPub{IdentityPub: change}
	case attrService:
		change := &statev1.ServiceChange{Name: key.name}
		if !entry.Deleted {
			svc, exists := rec.Services[key.name]
			if !exists {
				panic("service log entry missing service payload")
			}
			change.Port = svc.GetPort()
		}
		event.Change = &statev1.GossipEvent_Service{Service: change}
	case attrReachability:
		event.Change = &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: key.peer.String()},
		}
	case attrRevocation:
		panic("buildEventFromLog must not be called for revocation events")
	default:
		panic("unknown attr kind in log")
	}

	return event
}
