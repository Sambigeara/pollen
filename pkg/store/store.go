package store

import (
	"bytes"
	"cmp"
	"slices"
	"sync"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
)

type Store struct {
	disk               *disk
	nodes              map[types.PeerKey]nodeRecord
	denied             map[types.PeerKey]struct{}
	consumedInvites    map[string]consumedInviteEntry
	desiredConnections map[Connection]struct{}
	onDeny             func(types.PeerKey)
	onRouteInvalidate  func()
	onWorkloadChange   func()
	onTrafficChange    func()
	metrics            *metrics.GossipMetrics
	mu                 sync.RWMutex
	localID            types.PeerKey
}

// LocalID returns the identity key of the local node.
func (s *Store) LocalID() types.PeerKey {
	return s.localID
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

	denied := make(map[types.PeerKey]struct{})
	for _, pub := range onDisk.GetDeniedPeers() {
		denied[types.PeerKeyFromBytes(pub)] = struct{}{}
	}

	s := &Store{
		localID: localID,
		disk:    d,
		metrics: metrics.NewGossipMetrics(nil),
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				maxCounter:     1,
				IdentityPub:    append([]byte(nil), identityPub...),
				Reachable:      make(map[types.PeerKey]struct{}),
				Services:       make(map[string]*statev1.Service),
				WorkloadSpecs:  make(map[string]*statev1.WorkloadSpecChange),
				WorkloadClaims: make(map[string]struct{}),
				log: map[attrKey]logEntry{
					identityAttrKey(): {Counter: 1},
				},
			},
		},
		denied:             denied,
		consumedInvites:    loadConsumedInvites(onDisk.GetConsumedInvites(), time.Now()),
		desiredConnections: make(map[Connection]struct{}),
	}

	// Correct stale state held by peers from a prior session.
	// Vivaldi state is not tombstoned here because node startup immediately
	// publishes the current local coordinate.
	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)

	// Inject disk-loaded denied peers into the local log so that
	// bumpAndBroadcastAllLocked can re-publish them after restart.
	for subjectKey := range s.denied {
		local.maxCounter++
		local.log[denyAttrKey(subjectKey.String())] = logEntry{Counter: local.maxCounter}
	}

	// Inject disk-loaded workload specs into the local log so that
	// bumpAndBroadcastAllLocked can re-publish them after restart.
	for _, spec := range onDisk.GetWorkloadSpecs() {
		hash := spec.GetHash()
		if hash == "" {
			continue
		}
		local.WorkloadSpecs[hash] = spec
		local.maxCounter++
		local.log[workloadSpecAttrKey(hash)] = logEntry{Counter: local.maxCounter}
	}

	s.nodes[localID] = local

	for _, p := range onDisk.GetPeers() {
		peerID := types.PeerKeyFromBytes(p.GetIdentityPub())
		if peerID == localID || peerID == (types.PeerKey{}) {
			continue
		}

		s.nodes[peerID] = nodeRecord{
			IdentityPub:        append([]byte(nil), p.GetIdentityPub()...),
			IPs:                append([]string(nil), p.GetAddresses()...),
			LastAddr:           p.GetLastAddr(),
			LocalPort:          p.GetPort(),
			ExternalPort:       p.GetExternalPort(),
			ObservedExternalIP: p.GetExternalIp(),
			PubliclyAccessible: p.GetPubliclyAccessible(),
			Reachable:          make(map[types.PeerKey]struct{}),
			Services:           make(map[string]*statev1.Service),
			WorkloadSpecs:      make(map[string]*statev1.WorkloadSpecChange),
			WorkloadClaims:     make(map[string]struct{}),
			log:                make(map[attrKey]logEntry),
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
	snapshot := s.snapshotStateLocked()
	s.mu.RUnlock()

	return s.disk.save(snapshot)
}

// snapshotStateLocked builds a RuntimeState from in-memory data.
// The caller must hold s.mu (read or write).
func (s *Store) snapshotStateLocked() *statev1.RuntimeState {
	peers := make([]*statev1.PeerState, 0, len(s.nodes))
	for peerID, rec := range s.nodes {
		if peerID == s.localID {
			continue
		}
		peers = append(peers, &statev1.PeerState{
			IdentityPub:        append([]byte(nil), peerID[:]...),
			Addresses:          append([]string(nil), rec.IPs...),
			Port:               rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			ExternalIp:         rec.ObservedExternalIP,
			LastAddr:           rec.LastAddr,
			PubliclyAccessible: rec.PubliclyAccessible,
		})
	}

	slices.SortFunc(peers, func(a, b *statev1.PeerState) int {
		return bytes.Compare(a.GetIdentityPub(), b.GetIdentityPub())
	})

	deniedPeers := make([][]byte, 0, len(s.denied))
	for pk := range s.denied {
		deniedPeers = append(deniedPeers, append([]byte(nil), pk[:]...))
	}
	slices.SortFunc(deniedPeers, bytes.Compare)

	invites := make([]*statev1.ConsumedInvite, 0, len(s.consumedInvites))
	for _, entry := range s.consumedInvites {
		invites = append(invites, &statev1.ConsumedInvite{
			TokenId:      entry.TokenID,
			ExpiryUnix:   entry.ExpiresAtUnix,
			ConsumedUnix: entry.ConsumedAtUnix,
		})
	}
	slices.SortFunc(invites, func(a, b *statev1.ConsumedInvite) int {
		return cmp.Compare(a.GetTokenId(), b.GetTokenId())
	})

	local := s.nodes[s.localID]
	specs := make([]*statev1.WorkloadSpecChange, 0, len(local.WorkloadSpecs))
	for _, spec := range local.WorkloadSpecs {
		specs = append(specs, &statev1.WorkloadSpecChange{
			Hash:        spec.GetHash(),
			Replicas:    spec.GetReplicas(),
			MemoryPages: spec.GetMemoryPages(),
			TimeoutMs:   spec.GetTimeoutMs(),
		})
	}
	slices.SortFunc(specs, func(a, b *statev1.WorkloadSpecChange) int {
		return cmp.Compare(a.GetHash(), b.GetHash())
	})

	return &statev1.RuntimeState{
		Peers:           peers,
		DeniedPeers:     deniedPeers,
		ConsumedInvites: invites,
		WorkloadSpecs:   specs,
	}
}

// OnDenyPeer registers a callback invoked (outside the lock) when a deny
// event is applied via gossip. Only one callback may be registered.
func (s *Store) OnDenyPeer(fn func(types.PeerKey)) {
	s.onDeny = fn
}

// OnRouteInvalidate registers a callback invoked (outside the lock) when a
// reachability or Vivaldi event is applied via gossip. Only one callback may
// be registered.
func (s *Store) OnRouteInvalidate(fn func()) {
	s.onRouteInvalidate = fn
}

// OnWorkloadChange registers a callback invoked (outside the lock) when a
// workload spec or claim event is applied via gossip.
func (s *Store) OnWorkloadChange(fn func()) {
	s.onWorkloadChange = fn
}

// OnTrafficChange registers a callback invoked (outside the lock) when a
// traffic heatmap event is applied via gossip.
func (s *Store) OnTrafficChange(fn func()) {
	s.onTrafficChange = fn
}

// SetGossipMetrics replaces the no-op metrics with wired instruments.
func (s *Store) SetGossipMetrics(m *metrics.GossipMetrics) {
	s.metrics = m
}
