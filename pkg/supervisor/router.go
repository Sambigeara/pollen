package supervisor

import (
	"sync"

	"github.com/sambigeara/pollen/pkg/routing"
	"github.com/sambigeara/pollen/pkg/types"
)

type atomicRouter struct {
	table    routing.Table
	changeCh chan struct{}
	mu       sync.RWMutex
}

func newAtomicRouter() *atomicRouter {
	return &atomicRouter{changeCh: make(chan struct{})}
}

func (r *atomicRouter) NextHop(dest types.PeerKey) (types.PeerKey, bool) {
	r.mu.RLock()
	t := r.table
	r.mu.RUnlock()
	return t.NextHop(dest)
}

func (r *atomicRouter) Changed() <-chan struct{} {
	r.mu.RLock()
	ch := r.changeCh
	r.mu.RUnlock()
	return ch
}

func (r *atomicRouter) set(t routing.Table) {
	r.mu.Lock()
	r.table = t
	close(r.changeCh)
	r.changeCh = make(chan struct{})
	r.mu.Unlock()
}
