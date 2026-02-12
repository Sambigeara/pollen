package state

import (
	"sync"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

type Record struct {
	Node      *statev1.Node
	Timestamp Timestamp
	Tombstone bool
}

type NodeMap struct {
	Data        map[types.PeerKey]Record
	clock       int64
	LocalNodeID types.PeerKey
	mu          sync.RWMutex
}

func NewNodeMap(localNodeID types.PeerKey) *NodeMap {
	return &NodeMap{
		Data:        make(map[types.PeerKey]Record),
		LocalNodeID: localNodeID,
		clock:       0,
	}
}

func (m *NodeMap) tick() Timestamp {
	m.clock++
	return Timestamp{
		Counter: m.clock,
		NodeID:  m.LocalNodeID,
	}
}

func (m *NodeMap) Set(key types.PeerKey, node *statev1.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ts := m.tick()
	m.Data[key] = Record{
		Node:      node,
		Timestamp: ts,
		Tombstone: false,
	}
}

func (m *NodeMap) Update(key types.PeerKey, fn func(*statev1.Node)) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.Data[key]
	if !ok || rec.Tombstone {
		return false
	}

	fn(rec.Node)

	m.Data[key] = Record{
		Node:      rec.Node,
		Timestamp: m.tick(),
		Tombstone: false,
	}
	return true
}

func (m *NodeMap) Get(key types.PeerKey) (Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.Data[key]
	return v, ok
}

func (m *NodeMap) ConnectedPeers(key types.PeerKey) []types.PeerKey {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.Data[key]
	if !ok || rec.Tombstone || len(rec.Node.Connected) == 0 {
		return nil
	}

	out := make([]types.PeerKey, 0, len(rec.Node.Connected))
	for peerID := range rec.Node.Connected {
		peerKey, err := types.PeerKeyFromString(peerID)
		if err != nil {
			continue
		}
		out = append(out, peerKey)
	}

	return out
}

func (m *NodeMap) IsConnected(source, target types.PeerKey) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.Data[source]
	if !ok || rec.Tombstone || len(rec.Node.Connected) == 0 {
		return false
	}

	_, ok = rec.Node.Connected[target.String()]
	return ok
}

func (m *NodeMap) NodeIPs(key types.PeerKey) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.Data[key]
	if !ok || rec.Tombstone || len(rec.Node.Ips) == 0 {
		return nil
	}

	return append([]string(nil), rec.Node.Ips...)
}

func (m *NodeMap) GetAll() map[types.PeerKey]*statev1.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[types.PeerKey]*statev1.Node, len(m.Data))
	for k, rec := range m.Data {
		if !rec.Tombstone {
			out[k] = rec.Node
		}
	}

	return out
}

func (m *NodeMap) Merge(remoteData map[types.PeerKey]Record) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, remoteRec := range remoteData {
		if remoteRec.Timestamp.Counter > m.clock {
			m.clock = remoteRec.Timestamp.Counter
		}

		if localRec, exists := m.Data[k]; !exists || localRec.Timestamp.Less(remoteRec.Timestamp) {
			m.Data[k] = remoteRec
		}
	}
}
