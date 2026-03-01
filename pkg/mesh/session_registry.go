package mesh

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/sambigeara/pollen/pkg/types"
)

type sessionRegistry struct {
	peers   map[types.PeerKey]*peerSession
	waiters map[types.PeerKey][]chan struct{}
	mu      sync.RWMutex
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		peers:   make(map[types.PeerKey]*peerSession),
		waiters: make(map[types.PeerKey][]chan struct{}),
	}
}

func (r *sessionRegistry) get(peerKey types.PeerKey) (*peerSession, bool) {
	r.mu.RLock()
	s, ok := r.peers[peerKey]
	r.mu.RUnlock()
	return s, ok
}

func (r *sessionRegistry) waitFor(ctx context.Context, peerKey types.PeerKey) (*peerSession, error) {
	for {
		r.mu.RLock()
		s, ok := r.liveSessionForPeer(peerKey)
		r.mu.RUnlock()
		if ok {
			return s, nil
		}

		ch := make(chan struct{}, 1)

		r.mu.Lock()
		if s, ok = r.liveSessionForPeer(peerKey); ok {
			r.mu.Unlock()
			return s, nil
		}
		r.waiters[peerKey] = append(r.waiters[peerKey], ch)
		r.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			r.removeWaiter(peerKey, ch)
			return nil, ctx.Err()
		}
	}
}

func (r *sessionRegistry) liveSessionForPeer(peerKey types.PeerKey) (*peerSession, bool) {
	s, ok := r.peers[peerKey]
	if !ok || s.conn.Context().Err() != nil {
		return nil, false
	}
	return s, true
}

func (r *sessionRegistry) removeWaiter(peerKey types.PeerKey, ch chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	waiters := slices.DeleteFunc(r.waiters[peerKey], func(c chan struct{}) bool { return c == ch })
	if len(waiters) == 0 {
		delete(r.waiters, peerKey)
	} else {
		r.waiters[peerKey] = waiters
	}
}

func (r *sessionRegistry) add(peerKey types.PeerKey, next *peerSession, shouldReplace func(current *peerSession) bool) (*peerSession, bool) {
	r.mu.Lock()

	var previous *peerSession
	if current, ok := r.peers[peerKey]; ok {
		if !shouldReplace(current) {
			r.mu.Unlock()
			return nil, false
		}
		previous = current
	}

	r.peers[peerKey] = next
	waiters := r.waiters[peerKey]
	delete(r.waiters, peerKey)
	r.mu.Unlock()

	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	return previous, true
}

func (r *sessionRegistry) removeIfCurrent(peerKey types.PeerKey, current *peerSession) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	active, ok := r.peers[peerKey]
	if !ok || active != current {
		return false
	}

	delete(r.peers, peerKey)
	return true
}

func (r *sessionRegistry) connectedPeers() []types.PeerKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Collect(maps.Keys(r.peers))
}

func (r *sessionRegistry) drainPeers() map[types.PeerKey]*peerSession {
	r.mu.Lock()
	peers := r.peers
	r.peers = make(map[types.PeerKey]*peerSession)
	r.mu.Unlock()

	return peers
}
