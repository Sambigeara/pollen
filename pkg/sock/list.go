package sock

import (
	"net"
	"sync"
)

type connList struct {
	byKey map[string]*Conn
	mu    sync.Mutex
}

func newConnList() *connList {
	return &connList{
		byKey: make(map[string]*Conn),
	}
}

// Append adds a connection keyed by address. If already present, returns the existing connection.
func (l *connList) Append(addr *net.UDPAddr, c *Conn) (*Conn, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	k := addr.String()
	if existing, ok := l.byKey[k]; ok {
		return existing, false
	}
	l.byKey[k] = c
	return c, true
}

func (l *connList) Remove(addr *net.UDPAddr) {
	l.mu.Lock()
	delete(l.byKey, addr.String())
	l.mu.Unlock()
}
