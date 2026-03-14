package store

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/sambigeara/pollen/pkg/types"
)

type Connection struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

func (c Connection) Key() string {
	return fmt.Sprintf("%s:%d:%d", c.PeerID.String(), c.RemotePort, c.LocalPort)
}

func (s *Store) DesiredConnections() []Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()

	connections := slices.Collect(maps.Values(s.desiredConnections))
	sortConnections(connections)

	return connections
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
