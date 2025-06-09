package store

import (
	"time"
)

type LWWElement struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Hash      string    `json:"hash"`
	Deleted   bool      `json:"deleted"`
}

type LWWSet struct {
	Elements map[string]LWWElement `json:"elements"`
}

func NewLWWSet() *LWWSet {
	return &LWWSet{
		Elements: make(map[string]LWWElement),
	}
}

func (s *LWWSet) Add(id, hash string) {
	elem := LWWElement{
		ID:        id,
		Hash:      hash,
		Timestamp: time.Now(),
		Deleted:   false,
	}

	if existing, ok := s.Elements[id]; !ok || elem.Timestamp.After(existing.Timestamp) {
		s.Elements[id] = elem
	}
}

func (s *LWWSet) Merge(other *LWWSet) {
	for id, elem := range other.Elements {
		if existing, ok := s.Elements[id]; !ok || elem.Timestamp.After(existing.Timestamp) {
			s.Elements[id] = elem
		}
	}
}
