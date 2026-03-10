package store

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

type attrKind uint8

const (
	attrNetwork attrKind = iota + 1
	attrExternalPort
	attrObservedExternalIP
	attrIdentity
	attrService
	attrReachability
	attrRevocation
	attrPubliclyAccessible
	attrVivaldi
	attrNatType
	attrResourceTelemetry
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

func observedExternalIPAttrKey() attrKey {
	return attrKey{kind: attrObservedExternalIP}
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

func publiclyAccessibleAttrKey() attrKey {
	return attrKey{kind: attrPubliclyAccessible}
}

func vivaldiAttrKey() attrKey {
	return attrKey{kind: attrVivaldi}
}

func natTypeAttrKey() attrKey {
	return attrKey{kind: attrNatType}
}

func resourceTelemetryAttrKey() attrKey {
	return attrKey{kind: attrResourceTelemetry}
}

func tombstoneStaleAttrs(rec *nodeRecord) {
	for _, key := range []attrKey{publiclyAccessibleAttrKey(), natTypeAttrKey(), observedExternalIPAttrKey(), resourceTelemetryAttrKey()} {
		rec.maxCounter++
		rec.log[key] = logEntry{Counter: rec.maxCounter, Deleted: true}
	}
}

type logEntry struct {
	Counter uint64
	Deleted bool
}

type nodeRecord struct {
	Reachable          map[types.PeerKey]struct{}
	Services           map[string]*statev1.Service
	log                map[attrKey]logEntry
	VivaldiCoord       *topology.Coord
	LastAddr           string
	ObservedExternalIP string
	IPs                []string
	IdentityPub        []byte
	maxCounter         uint64
	CertExpiry         int64
	MemTotalBytes      uint64
	NatType            nat.Type
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	PubliclyAccessible bool
}

func (r nodeRecord) clone() nodeRecord {
	c := r
	if r.Services != nil {
		c.Services = make(map[string]*statev1.Service, len(r.Services))
		maps.Copy(c.Services, r.Services)
	}
	if r.Reachable != nil {
		c.Reachable = make(map[types.PeerKey]struct{}, len(r.Reachable))
		maps.Copy(c.Reachable, r.Reachable)
	}
	c.IPs = append([]string(nil), r.IPs...)
	return c
}

type KnownPeer struct {
	VivaldiCoord       *topology.Coord
	LastAddr           string
	ObservedExternalIP string
	IdentityPub        []byte
	IPs                []string
	NatType            nat.Type
	LocalPort          uint32
	ExternalPort       uint32
	PeerID             types.PeerKey
	PubliclyAccessible bool
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
	metrics            *metrics.GossipMetrics
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
		metrics: metrics.NewGossipMetrics(nil),
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
		revocations:        unmarshalDiskRevocations(onDisk.Revocations, trustBundle),
		trustBundle:        trustBundle,
		desiredConnections: make(map[string]Connection),
	}

	// Correct stale state held by peers from a prior session.
	// Vivaldi state is not tombstoned here because node startup immediately
	// publishes the current local coordinate.
	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)

	// Inject disk-loaded revocations into the local log so that
	// bumpAndBroadcastAllLocked can re-publish them after restart.
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

		s.nodes[peerID] = nodeRecord{
			IdentityPub:        append([]byte(nil), peerID[:]...),
			IPs:                append([]string(nil), p.Addresses...),
			LastAddr:           p.LastAddr,
			LocalPort:          p.Port,
			ExternalPort:       p.ExternalPort,
			ObservedExternalIP: p.ExternalIP,
			PubliclyAccessible: p.PubliclyAccessible,
			Reachable:          make(map[types.PeerKey]struct{}),
			Services:           make(map[string]*statev1.Service),
			log:                make(map[attrKey]logEntry),
		}
	}

	for _, svc := range onDisk.Services {
		provider, err := types.PeerKeyFromString(svc.Provider)
		if err != nil {
			continue
		}

		rec := s.nodes[provider]
		ensureNodeInit(&rec, provider)
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

// SetGossipMetrics replaces the no-op metrics with wired instruments.
func (s *Store) SetGossipMetrics(m *metrics.GossipMetrics) {
	s.metrics = m
}

// GossipMetrics returns the store's gossip metrics for direct value reads.
func (s *Store) GossipMetrics() *metrics.GossipMetrics {
	return s.metrics
}

func (s *Store) Save() error {
	s.mu.RLock()
	nodes := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	for k, v := range s.nodes {
		nodes[k] = v.clone()
	}
	connections := slices.Collect(maps.Values(s.desiredConnections))
	revocations := maps.Clone(s.revocations)
	s.mu.RUnlock()

	peers := make([]diskPeer, 0, len(nodes))
	for peerID, rec := range nodes {
		if peerID == s.LocalID {
			continue
		}
		peers = append(peers, diskPeer{
			IdentityPublic:     peerID.String(),
			Addresses:          rec.IPs,
			Port:               rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			ExternalIP:         rec.ObservedExternalIP,
			LastAddr:           rec.LastAddr,
			PubliclyAccessible: rec.PubliclyAccessible,
		})
	}

	slices.SortFunc(peers, func(a, b diskPeer) int {
		return cmp.Compare(a.IdentityPublic, b.IdentityPublic)
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

	st := diskState{
		Local: diskLocal{
			IdentityPublic: s.LocalID.String(),
		},
		Peers:       peers,
		Services:    services,
		Connections: diskConns,
		Revocations: diskRevs,
	}

	return s.disk.save(st)
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// computePeerHashLocked returns an order-independent XOR of FNV-1a hashes
// over a peer's compacted log. Two peers with identical log contents produce
// identical hashes regardless of iteration order.
func computePeerHashLocked(rec nodeRecord) uint64 {
	var digest uint64
	for key, entry := range rec.log {
		h := uint64(fnvOffset64)
		mix := func(b []byte) {
			for _, v := range b {
				h ^= uint64(v)
				h *= fnvPrime64
			}
		}
		mix([]byte{byte(key.kind)})
		mix([]byte(key.name))
		mix(key.peer[:])
		var buf [9]byte
		binary.LittleEndian.PutUint64(buf[:8], entry.Counter)
		if entry.Deleted {
			buf[8] = 1
		}
		mix(buf[:])
		digest ^= h
	}
	return digest
}

// EagerSyncClock returns an empty digest when no remote peer has state yet
// (so the responder sends everything), or the real digest otherwise. Both the
// remote-state check and digest construction happen under a single RLock to
// avoid a TOCTOU race with concurrent ApplyEvents calls.
func (s *Store) EagerSyncClock() *statev1.GossipStateDigest {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for peerID, rec := range s.nodes {
		if peerID != s.LocalID && rec.maxCounter > 0 {
			return s.digestLocked()
		}
	}
	return &statev1.GossipStateDigest{}
}

func (s *Store) Clock() *statev1.GossipStateDigest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.digestLocked()
}

func (s *Store) digestLocked() *statev1.GossipStateDigest {
	peers := make(map[string]*statev1.PeerDigest, len(s.nodes))
	for peerID, rec := range s.nodes {
		peers[peerID.String()] = &statev1.PeerDigest{
			MaxCounter: rec.maxCounter,
			StateHash:  computePeerHashLocked(rec),
		}
	}
	return &statev1.GossipStateDigest{Peers: peers}
}

// MissingFor returns the individual events that the remote peer is missing,
// based on the provided state digest.
//
// For each local peer:
//   - unknown to remote          → send all (full dump)
//   - hash mismatch              → send all (full dump)
//   - hash match, counter ahead  → send delta above remote counter
//   - hash match, counter match  → skip (fully synced)
func (s *Store) MissingFor(digest *statev1.GossipStateDigest) []*statev1.GossipEvent {
	remotePeers := digest.GetPeers()

	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []*statev1.GossipEvent
	for peerID, rec := range s.nodes {
		if rec.maxCounter == 0 {
			continue
		}
		rd := remotePeers[peerID.String()]
		if rd == nil {
			events = append(events, s.buildEventsAbove(peerID, rec, 0)...)
			continue
		}
		localHash := computePeerHashLocked(rec)
		if localHash != rd.GetStateHash() {
			events = append(events, s.buildEventsAbove(peerID, rec, 0)...)
			continue
		}
		if rec.maxCounter > rd.GetMaxCounter() {
			events = append(events, s.buildEventsAbove(peerID, rec, rd.GetMaxCounter())...)
		}
	}

	slices.SortFunc(events, func(a, b *statev1.GossipEvent) int {
		if c := cmp.Compare(a.GetPeerId(), b.GetPeerId()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetCounter(), b.GetCounter())
	})

	return events
}

// ApplyEvents applies a batch of incoming gossip events under a single lock
// acquisition. When isPullResponse is true, the batch outcome updates the
// EWMA stale ratio used for health checks; push-rebroadcast events are
// excluded because their inherently high staleness is normal gossip behavior.
// The EWMA is updated once per batch (not per event) so that large batches
// with a few fresh events aren't drowned out by individually-counted stale ones.
func (s *Store) ApplyEvents(events []*statev1.GossipEvent, isPullResponse bool) ApplyResult {
	s.mu.Lock()

	s.metrics.BatchSize.Set(float64(len(events)))
	s.metrics.EventsReceived.Add(int64(len(events)))

	var result ApplyResult
	anyApplied := false
	for _, event := range events {
		if event == nil {
			continue
		}
		r := s.applyEventLocked(event)
		if len(r.Rebroadcast) > 0 {
			anyApplied = true
		}
		result.Rebroadcast = append(result.Rebroadcast, r.Rebroadcast...)
		result.revokedSubjects = append(result.revokedSubjects, r.revokedSubjects...)
	}

	// Update stale ratio once per pull-response batch: a batch with any
	// fresh events is "useful" (0.0); a fully-stale batch is not (1.0).
	if isPullResponse && len(events) > 0 {
		if anyApplied {
			s.metrics.StaleRatio.Update(0.0)
		} else {
			s.metrics.StaleRatio.Update(1.0)
		}
	}

	s.mu.Unlock()

	if s.onRevocation != nil {
		for _, subject := range result.revokedSubjects {
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
	ensureNodeInit(&rec, peerID)

	key, ok := eventAttrKey(event)
	if !ok {
		return ApplyResult{}
	}

	// Self-state conflict handling: receiving our own events from peers.
	// Only the higher-counter check matters — same-counter events bounce
	// back routinely via gossip and would cause a rebroadcast storm.
	if peerID == s.LocalID {
		if event.GetCounter() > rec.maxCounter {
			rec.maxCounter = event.GetCounter()
			s.nodes[peerID] = rec
			s.metrics.SelfConflicts.Inc()
			return ApplyResult{Rebroadcast: s.bumpAndBroadcastAllLocked()}
		}
		return ApplyResult{}
	}

	// Per-key stale check: only accept if this event is newer for this key.
	if existing, ok := rec.log[key]; ok && event.GetCounter() <= existing.Counter {
		s.metrics.EventsStale.Inc()
		if event.GetCounter() > rec.maxCounter {
			rec.maxCounter = event.GetCounter()
			s.nodes[peerID] = rec
		}
		return ApplyResult{}
	}

	deleted := event.GetDeleted()

	var result ApplyResult
	switch {
	case key.kind == attrRevocation:
		if deleted {
			return ApplyResult{}
		}
		v := event.GetChange().(*statev1.GossipEvent_Revocation) //nolint:forcetypeassert
		rev := v.Revocation.GetRevocation()
		applied, subject := s.applyRevocationLocked(rev)
		if !applied {
			return ApplyResult{}
		}
		if (subject != types.PeerKey{}) {
			result.revokedSubjects = []types.PeerKey{subject}
			s.metrics.Revocations.Inc()
		}
	case deleted:
		applyDeleteLocked(&rec, key)
	default:
		applyValueLocked(&rec, event, key)
	}

	rec.log[key] = logEntry{Counter: event.GetCounter(), Deleted: deleted}
	if event.GetCounter() > rec.maxCounter {
		rec.maxCounter = event.GetCounter()
	}
	s.nodes[peerID] = rec
	if key.kind != attrRevocation || len(result.revokedSubjects) > 0 {
		s.metrics.EventsApplied.Inc()
		result.Rebroadcast = append(result.Rebroadcast, event)
	}
	return result
}

func eventAttrKey(event *statev1.GossipEvent) (attrKey, bool) {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		return networkAttrKey(), true
	case *statev1.GossipEvent_ExternalPort:
		return externalPortAttrKey(), true
	case *statev1.GossipEvent_ObservedExternalIp:
		return observedExternalIPAttrKey(), true
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
	case *statev1.GossipEvent_PubliclyAccessible:
		return publiclyAccessibleAttrKey(), true
	case *statev1.GossipEvent_Vivaldi:
		return vivaldiAttrKey(), true
	case *statev1.GossipEvent_NatType:
		return natTypeAttrKey(), true
	case *statev1.GossipEvent_ResourceTelemetry:
		return resourceTelemetryAttrKey(), true
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
	case attrObservedExternalIP:
		rec.ObservedExternalIP = ""
	case attrIdentity:
		rec.IdentityPub = nil
		rec.CertExpiry = 0
	case attrService:
		delete(rec.Services, key.name)
	case attrReachability:
		delete(rec.Reachable, key.peer)
	case attrRevocation:
		// Revocations are cluster-wide, not per-node; deletion is a no-op.
	case attrPubliclyAccessible:
		rec.PubliclyAccessible = false
	case attrVivaldi:
		rec.VivaldiCoord = nil
	case attrNatType:
		rec.NatType = nat.Unknown
	case attrResourceTelemetry:
		rec.CPUPercent = 0
		rec.MemPercent = 0
		rec.MemTotalBytes = 0
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
	case *statev1.GossipEvent_ObservedExternalIp:
		if v.ObservedExternalIp != nil {
			rec.ObservedExternalIP = v.ObservedExternalIp.GetIp()
		}
	case *statev1.GossipEvent_IdentityPub:
		if v.IdentityPub != nil {
			rec.IdentityPub = append([]byte(nil), v.IdentityPub.GetIdentityPub()...)
			rec.CertExpiry = v.IdentityPub.GetCertExpiryUnix()
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
	case *statev1.GossipEvent_PubliclyAccessible:
		rec.PubliclyAccessible = true
	case *statev1.GossipEvent_Vivaldi:
		if v.Vivaldi != nil {
			rec.VivaldiCoord = &topology.Coord{
				X:      v.Vivaldi.GetX(),
				Y:      v.Vivaldi.GetY(),
				Height: v.Vivaldi.GetHeight(),
			}
		}
	case *statev1.GossipEvent_NatType:
		if v.NatType != nil {
			rec.NatType = nat.TypeFromUint32(v.NatType.GetNatType())
		}
	case *statev1.GossipEvent_ResourceTelemetry:
		if v.ResourceTelemetry != nil {
			rec.CPUPercent = v.ResourceTelemetry.GetCpuPercent()
			rec.MemPercent = v.ResourceTelemetry.GetMemPercent()
			rec.MemTotalBytes = v.ResourceTelemetry.GetMemTotalBytes()
		}
	default:
		return
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

func (s *Store) SetObservedExternalIP(ip string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.ObservedExternalIP == ip {
		return nil
	}

	local.ObservedExternalIP = ip

	local.maxCounter++
	counter := local.maxCounter
	deleted := ip == ""
	local.log[observedExternalIPAttrKey()] = logEntry{Counter: counter, Deleted: deleted}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: deleted,
		Change: &statev1.GossipEvent_ObservedExternalIp{
			ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: ip},
		},
	}}
}

func (s *Store) SetLocalConnected(peerID types.PeerKey, connected bool) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]

	_, exists := local.Reachable[peerID]
	if connected == exists {
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

func (s *Store) SetLocalPubliclyAccessible(accessible bool) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.PubliclyAccessible == accessible {
		return nil
	}

	local.PubliclyAccessible = accessible

	key := publiclyAccessibleAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: !accessible}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: !accessible,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	}}
}

func (s *Store) SetLocalNatType(natType nat.Type) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.NatType == natType {
		return nil
	}

	local.NatType = natType

	key := natTypeAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	deleted := natType == nat.Unknown
	local.log[key] = logEntry{Counter: counter, Deleted: deleted}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: deleted,
		Change: &statev1.GossipEvent_NatType{
			NatType: &statev1.NatTypeChange{NatType: natType.ToUint32()},
		},
	}}
}

const resourceTelemetryDeadband = 2

func (s *Store) SetLocalResourceTelemetry(cpuPercent, memPercent uint32, memTotalBytes uint64) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	cpuDelta := absDiff(local.CPUPercent, cpuPercent)
	memDelta := absDiff(local.MemPercent, memPercent)
	if cpuDelta < resourceTelemetryDeadband && memDelta < resourceTelemetryDeadband && local.MemTotalBytes == memTotalBytes {
		return nil
	}

	local.CPUPercent = cpuPercent
	local.MemPercent = memPercent
	local.MemTotalBytes = memTotalBytes

	key := resourceTelemetryAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_ResourceTelemetry{
			ResourceTelemetry: &statev1.ResourceTelemetryChange{
				CpuPercent:    cpuPercent,
				MemPercent:    memPercent,
				MemTotalBytes: memTotalBytes,
			},
		},
	}}
}

func absDiff(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}

func (s *Store) NatType(peerID types.PeerKey) nat.Type {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return nat.Unknown
	}
	return rec.NatType
}

func (s *Store) SetLocalCertExpiry(expiry int64) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.CertExpiry == expiry {
		return nil
	}

	local.CertExpiry = expiry

	local.maxCounter++
	counter := local.maxCounter
	local.log[identityAttrKey()] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{
				IdentityPub:    append([]byte(nil), local.IdentityPub...),
				CertExpiryUnix: expiry,
			},
		},
	}}
}

func (s *Store) IsPubliclyAccessible(peerID types.PeerKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	return rec.PubliclyAccessible
}

func (s *Store) SetLocalVivaldiCoord(coord topology.Coord) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.LocalID]
	if local.VivaldiCoord != nil && topology.MovementDistance(*local.VivaldiCoord, coord) <= topology.PublishEpsilon {
		return nil
	}

	local.VivaldiCoord = &coord

	key := vivaldiAttrKey()
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{
				X:      coord.X,
				Y:      coord.Y,
				Height: coord.Height,
			},
		},
	}}
}

func (s *Store) PeerVivaldiCoord(peerID types.PeerKey) (*topology.Coord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return nil, false
	}
	return rec.VivaldiCoord, rec.VivaldiCoord != nil
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

// validNodesLocked returns nodes that are neither revoked nor expired.
// CertExpiry == 0 (unset/legacy peers) are NOT filtered.
// Caller must hold s.mu (at least RLock).
func (s *Store) validNodesLocked() map[types.PeerKey]nodeRecord {
	now := time.Now()
	out := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	for k, v := range s.nodes {
		if _, revoked := s.revocations[k]; revoked {
			continue
		}
		if v.CertExpiry != 0 && auth.IsCertExpiredAt(time.Unix(v.CertExpiry, 0), now) {
			continue
		}
		out[k] = v.clone()
	}
	return out
}

func (s *Store) AllNodes() map[types.PeerKey]nodeRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validNodesLocked()
}

func (s *Store) Get(peerID types.PeerKey) (nodeRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.nodes[peerID]
	if !ok {
		return nodeRecord{}, false
	}
	return rec.clone(), true
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

	valid := s.validNodesLocked()
	known := make([]KnownPeer, 0, len(valid))
	for peerID, rec := range valid {
		if peerID == s.LocalID {
			continue
		}
		if rec.LastAddr == "" && (len(rec.IPs) == 0 || rec.LocalPort == 0) {
			continue
		}
		known = append(known, KnownPeer{
			PeerID:             peerID,
			LocalPort:          rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			ObservedExternalIP: rec.ObservedExternalIP,
			NatType:            rec.NatType,
			IdentityPub:        rec.IdentityPub,
			IPs:                rec.IPs,
			LastAddr:           rec.LastAddr,
			PubliclyAccessible: rec.PubliclyAccessible,
			VivaldiCoord:       rec.VivaldiCoord,
		})
	}

	slices.SortFunc(known, func(a, b KnownPeer) int {
		return a.PeerID.Compare(b.PeerID)
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

	connections := slices.Collect(maps.Values(s.desiredConnections))
	sortConnections(connections)

	return connections
}

func sortConnections(cs []Connection) {
	slices.SortFunc(cs, func(a, b Connection) int {
		if c := a.PeerID.Compare(b.PeerID); c != 0 {
			return c
		}
		if c := cmp.Compare(a.RemotePort, b.RemotePort); c != 0 {
			return c
		}
		return cmp.Compare(a.LocalPort, b.LocalPort)
	})
}

func (s *Store) IsSubjectRevoked(subjectPub []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.revocations[types.PeerKeyFromBytes(subjectPub)]
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

func ensureNodeInit(rec *nodeRecord, peerID types.PeerKey) {
	if rec.Reachable == nil {
		rec.Reachable = make(map[types.PeerKey]struct{})
	}
	if rec.Services == nil {
		rec.Services = make(map[string]*statev1.Service)
	}
	if rec.log == nil {
		rec.log = make(map[attrKey]logEntry)
	}
	if len(rec.IdentityPub) == 0 {
		rec.IdentityPub = append([]byte(nil), peerID[:]...)
	}
}

// --- Internal helpers ---

// LocalEvents returns all current local events without bumping counters.
// Used at startup to broadcast the full local state so that peers receive
// every event and maintain a correct maxCounter (no counter gaps).
func (s *Store) LocalEvents() []*statev1.GossipEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	local := s.nodes[s.LocalID]
	return s.buildEventsAbove(s.LocalID, local, 0)
}

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
	case attrObservedExternalIP:
		change := &statev1.ObservedExternalIPChange{}
		if !entry.Deleted {
			change.Ip = rec.ObservedExternalIP
		}
		event.Change = &statev1.GossipEvent_ObservedExternalIp{ObservedExternalIp: change}
	case attrIdentity:
		change := &statev1.IdentityChange{}
		if !entry.Deleted {
			change.IdentityPub = append([]byte(nil), rec.IdentityPub...)
			change.CertExpiryUnix = rec.CertExpiry
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
	case attrPubliclyAccessible:
		event.Change = &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		}
	case attrVivaldi:
		change := &statev1.VivaldiCoordinateChange{}
		if !entry.Deleted && rec.VivaldiCoord != nil {
			change.X = rec.VivaldiCoord.X
			change.Y = rec.VivaldiCoord.Y
			change.Height = rec.VivaldiCoord.Height
		}
		event.Change = &statev1.GossipEvent_Vivaldi{Vivaldi: change}
	case attrNatType:
		change := &statev1.NatTypeChange{}
		if !entry.Deleted {
			change.NatType = rec.NatType.ToUint32()
		}
		event.Change = &statev1.GossipEvent_NatType{NatType: change}
	case attrResourceTelemetry:
		change := &statev1.ResourceTelemetryChange{}
		if !entry.Deleted {
			change.CpuPercent = rec.CPUPercent
			change.MemPercent = rec.MemPercent
			change.MemTotalBytes = rec.MemTotalBytes
		}
		event.Change = &statev1.GossipEvent_ResourceTelemetry{ResourceTelemetry: change}
	case attrRevocation:
		panic("buildEventFromLog must not be called for revocation events")
	default:
		panic("unknown attr kind in log")
	}

	return event
}
