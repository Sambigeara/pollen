package store

import (
	"cmp"
	"slices"

	"github.com/sambigeara/pollen/pkg/types"
)

type Connection struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

func (s *Store) DesiredConnections() []Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()

	connections := make([]Connection, 0, len(s.desiredConnections))
	for conn := range s.desiredConnections {
		connections = append(connections, conn)
	}
	slices.SortFunc(connections, func(a, b Connection) int {
		if c := a.PeerID.Compare(b.PeerID); c != 0 {
			return c
		}
		if c := cmp.Compare(a.RemotePort, b.RemotePort); c != 0 {
			return c
		}
		return cmp.Compare(a.LocalPort, b.LocalPort)
	})
	return connections
}

func (s *Store) AddDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.desiredConnections[Connection{PeerID: peerID, RemotePort: remotePort, LocalPort: localPort}] = struct{}{}
}

func (s *Store) RemoveDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for conn := range s.desiredConnections {
		if conn.PeerID != peerID {
			continue
		}
		if remotePort != 0 && conn.RemotePort != remotePort {
			continue
		}
		if localPort != 0 && conn.LocalPort != localPort {
			continue
		}
		delete(s.desiredConnections, conn)
	}
}
