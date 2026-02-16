package sock

import (
	"net"
	"sync"
)

type ConnList struct {
	mu    sync.RWMutex
	order []*Conn // index 0 = highest priority
	byKey map[string]*Conn
	index map[*Conn]int
}

func newConnList() *ConnList {
	return &ConnList{
		byKey: make(map[string]*Conn),
		index: make(map[*Conn]int),
	}
}

func (l *ConnList) Get(addr *net.UDPAddr) (*Conn, bool) {
	l.mu.RLock()
	c, ok := l.byKey[addr.String()]
	l.mu.RUnlock()
	return c, ok
}

// Append adds as lowest priority. If already present, returns existing.
func (l *ConnList) Append(addr *net.UDPAddr, c *Conn) (*Conn, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	k := addr.String()
	if existing, ok := l.byKey[k]; ok {
		return existing, false
	}
	l.byKey[k] = c
	l.order = append(l.order, c)
	l.index[c] = len(l.order) - 1
	return c, true
}

func (l *ConnList) Snapshot() []*Conn {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]*Conn, len(l.order))
	copy(out, l.order)
	return out
}

func (l *ConnList) Remove(addr *net.UDPAddr) (*Conn, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	k := addr.String()
	c, ok := l.byKey[k]
	if !ok {
		return nil, false
	}
	i := l.index[c]

	// delete from maps
	delete(l.byKey, k)
	delete(l.index, c)

	// remove from order, preserving order
	copy(l.order[i:], l.order[i+1:])
	l.order[len(l.order)-1] = nil
	l.order = l.order[:len(l.order)-1]

	// fix indices for shifted tail
	for j := i; j < len(l.order); j++ {
		l.index[l.order[j]] = j
	}
	return c, true
}
