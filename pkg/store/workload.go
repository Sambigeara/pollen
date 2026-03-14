package store

import (
	"errors"
	"maps"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

// ErrSpecOwnedRemotely is returned by SetLocalWorkloadSpec when a valid remote
// node already publishes a spec for the same hash.
var ErrSpecOwnedRemotely = errors.New("spec published by another node")

// isValidOwnerLocked reports whether a peer is a valid scheduling participant
// (not denied, not expired). Caller must hold s.mu.
func (s *Store) isValidOwnerLocked(peerID types.PeerKey) bool {
	if _, denied := s.denied[peerID]; denied {
		return false
	}
	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	if rec.CertExpiry != 0 && auth.IsCertExpiredAt(time.Unix(rec.CertExpiry, 0), time.Now()) {
		return false
	}
	return true
}

func (s *Store) SetLocalWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) ([]*statev1.GossipEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reject if any valid remote node already publishes this hash.
	for pk, rec := range s.nodes {
		if pk == s.localID {
			continue
		}
		if _, has := rec.WorkloadSpecs[hash]; !has {
			continue
		}
		if s.isValidOwnerLocked(pk) {
			return nil, ErrSpecOwnedRemotely
		}
	}

	local := s.nodes[s.localID]

	if existing, ok := local.WorkloadSpecs[hash]; ok &&
		existing.GetReplicas() == replicas && existing.GetMemoryPages() == memoryPages &&
		existing.GetTimeoutMs() == timeoutMs {
		return nil, nil
	}

	m := make(map[string]*statev1.WorkloadSpecChange, len(local.WorkloadSpecs)+1)
	maps.Copy(m, local.WorkloadSpecs)
	m[hash] = &statev1.WorkloadSpecChange{Hash: hash, Replicas: replicas, MemoryPages: memoryPages, TimeoutMs: timeoutMs}
	local.WorkloadSpecs = m

	key := workloadSpecAttrKey(hash)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter}
	s.nodes[s.localID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.localID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{
				Hash:        hash,
				Replicas:    replicas,
				MemoryPages: memoryPages,
				TimeoutMs:   timeoutMs,
			},
		},
	}}, nil
}

// tombstoneLosingLocalSpec checks whether the local node holds a spec for hash
// that loses to the remote publisher (lower PeerKey wins). If so, it deletes
// the local spec and returns the tombstone event for rebroadcast.
// Caller must hold s.mu.
func (s *Store) tombstoneLosingLocalSpec(remotePeer types.PeerKey, hash string) []*statev1.GossipEvent {
	if remotePeer == s.localID {
		return nil
	}
	// Ignore invalid peers — denied or expired nodes cannot win ownership.
	if !s.isValidOwnerLocked(remotePeer) {
		return nil
	}
	// Only tombstone if remote peer wins (lower PeerKey).
	if s.localID.Compare(remotePeer) < 0 {
		return nil
	}
	local := s.nodes[s.localID]
	if _, has := local.WorkloadSpecs[hash]; !has {
		return nil
	}

	delete(local.WorkloadSpecs, hash)

	key := workloadSpecAttrKey(hash)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.localID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.localID.String(),
		Counter: counter,
		Deleted: true,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash},
		},
	}}
}

func (s *Store) RemoveLocalWorkloadSpec(hash string) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.localID]

	if _, ok := local.WorkloadSpecs[hash]; !ok {
		return nil
	}

	delete(local.WorkloadSpecs, hash)

	key := workloadSpecAttrKey(hash)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.localID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.localID.String(),
		Counter: counter,
		Deleted: true,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash},
		},
	}}
}

func (s *Store) SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	local := s.nodes[s.localID]

	_, exists := local.WorkloadClaims[hash]
	if claimed == exists {
		return nil
	}

	if claimed {
		m := make(map[string]struct{}, len(local.WorkloadClaims)+1)
		maps.Copy(m, local.WorkloadClaims)
		m[hash] = struct{}{}
		local.WorkloadClaims = m
	} else {
		delete(local.WorkloadClaims, hash)
	}

	key := workloadClaimAttrKey(hash)
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: !claimed}
	s.nodes[s.localID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.localID.String(),
		Counter: counter,
		Deleted: !claimed,
		Change: &statev1.GossipEvent_WorkloadClaim{
			WorkloadClaim: &statev1.WorkloadClaimChange{Hash: hash},
		},
	}}
}
