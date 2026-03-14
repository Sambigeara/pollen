package store

import (
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

// validNodesLocked returns nodes that are neither denied nor expired.
// CertExpiry == 0 (unset/legacy peers) are NOT filtered.
// Caller must hold s.mu (at least RLock).
func (s *Store) validNodesLocked() map[types.PeerKey]nodeRecord {
	now := time.Now()
	out := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	for k, v := range s.nodes {
		if _, denied := s.denied[k]; denied {
			continue
		}
		if v.CertExpiry != 0 && auth.IsCertExpiredAt(time.Unix(v.CertExpiry, 0), now) {
			continue
		}
		out[k] = v.clone()
	}
	return out
}

// liveComponentLocked returns the set of valid peers reachable from the local
// node via BFS over the reachability graph. This ensures that stale
// reachability entries from dead peers (e.g. rack-level failure) cannot keep
// other dead peers' claims alive, while still trusting multi-hop observations
// through live intermediaries.
func (s *Store) liveComponentLocked(valid map[types.PeerKey]nodeRecord) map[types.PeerKey]struct{} {
	component := map[types.PeerKey]struct{}{s.LocalID: {}}
	queue := []types.PeerKey{s.LocalID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rec, ok := valid[cur]
		if !ok {
			continue
		}
		for neighbor := range rec.Reachable {
			if _, seen := component[neighbor]; seen {
				continue
			}
			if _, isValid := valid[neighbor]; !isValid {
				continue
			}
			component[neighbor] = struct{}{}
			queue = append(queue, neighbor)
		}
	}
	return component
}
