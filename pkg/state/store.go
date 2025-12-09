package state

import (
	"sync"
)

type Record[T any] struct {
	Value     T
	Timestamp Timestamp
	Tombstone bool
}

type Map[T any] struct {
	Data        map[string]Record[T]
	Clock       int64
	LocalNodeID string
	mu          sync.RWMutex
}

func NewMap[T any](localNodeID string) *Map[T] {
	return &Map[T]{
		Data:        make(map[string]Record[T]),
		LocalNodeID: localNodeID,
		Clock:       0,
	}
}

func (m *Map[T]) tick() Timestamp {
	m.Clock++
	return Timestamp{
		Counter: m.Clock,
		NodeID:  m.LocalNodeID,
	}
}

func (m *Map[T]) Set(key string, val T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ts := m.tick()
	m.Data[key] = Record[T]{
		Value:     val,
		Timestamp: ts,
		Tombstone: false,
	}
}

// SetPlaceholder sets the record with a zeroed clock, to ensure
// a later call to `Set` overrides it.
func (m *Map[T]) SetPlaceholder(key string, val T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.Data[key]; ok {
		return
	}

	m.Data[key] = Record[T]{
		Value:     val,
		Tombstone: false,
	}
}

func (m *Map[T]) Get(key string) (Record[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.Data[key]
	return v, ok
}

func (m *Map[T]) GetAll() map[string]T {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]T, len(m.Data))
	for k, rec := range m.Data {
		if !rec.Tombstone {
			out[k] = rec.Value
		}
	}

	return out
}

func (m *Map[T]) Merge(remoteData map[string]Record[T]) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, remoteRec := range remoteData {
		if remoteRec.Timestamp.Counter > m.Clock {
			m.Clock = remoteRec.Timestamp.Counter
		}

		if localRec, exists := m.Data[k]; !exists || localRec.Timestamp.Less(remoteRec.Timestamp) {
			m.Data[k] = remoteRec
		}
	}
}
