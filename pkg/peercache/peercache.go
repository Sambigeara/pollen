// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package peercache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	fileName   = "peers.json"
	maxEntries = 128
)

type Entry struct {
	LastSeen time.Time
	Addrs    []string
	PeerKey  types.PeerKey
}

type Store struct {
	entries map[types.PeerKey]Entry
	path    string
	mu      sync.Mutex
	dirty   bool
}

type wireEntry struct {
	LastSeen time.Time `json:"lastSeen"`
	PeerPub  string    `json:"peerPub"`
	Addrs    []string  `json:"addrs"`
}

func Open(pollenDir string) (*Store, error) {
	s := &Store{
		path:    filepath.Join(pollenDir, fileName),
		entries: make(map[types.PeerKey]Entry),
	}

	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, nil
		}
		return nil, fmt.Errorf("read peers.json: %w", err)
	}
	if len(raw) == 0 {
		return s, nil
	}

	var wire []wireEntry
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("parse peers.json: %w", err)
	}
	for _, we := range wire {
		pk, err := types.PeerKeyFromString(we.PeerPub)
		if err != nil || len(we.Addrs) == 0 {
			continue
		}
		s.entries[pk] = Entry{
			PeerKey:  pk,
			Addrs:    slices.Clone(we.Addrs),
			LastSeen: we.LastSeen,
		}
	}
	return s, nil
}

func (s *Store) Snapshot() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, Entry{
			PeerKey:  e.PeerKey,
			Addrs:    slices.Clone(e.Addrs),
			LastSeen: e.LastSeen,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out
}

func (s *Store) Upsert(peerKey types.PeerKey, addrs []string, now time.Time) {
	if len(addrs) == 0 {
		return
	}
	cloned := slices.Clone(addrs)

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.entries[peerKey]
	if ok && slices.Equal(existing.Addrs, cloned) && existing.LastSeen.Equal(now) {
		return
	}
	s.entries[peerKey] = Entry{
		PeerKey:  peerKey,
		Addrs:    cloned,
		LastSeen: now,
	}
	for len(s.entries) > maxEntries {
		s.evictOldestLocked()
	}
	s.dirty = true
}

func (s *Store) Forget(peerKey types.PeerKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[peerKey]; !ok {
		return
	}
	delete(s.entries, peerKey)
	s.dirty = true
}

func (s *Store) Flush() error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	wire := make([]wireEntry, 0, len(s.entries))
	for _, e := range s.entries {
		wire = append(wire, wireEntry{
			PeerPub:  e.PeerKey.String(),
			Addrs:    slices.Clone(e.Addrs),
			LastSeen: e.LastSeen,
		})
	}
	s.dirty = false
	s.mu.Unlock()

	sort.Slice(wire, func(i, j int) bool {
		return wire[i].LastSeen.After(wire[j].LastSeen)
	})

	data, err := json.MarshalIndent(wire, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal peers.json: %w", err)
	}
	return plnfs.WriteGroupReadable(s.path, data)
}

func (s *Store) evictOldestLocked() {
	var oldestKey types.PeerKey
	var oldestTime time.Time
	first := true
	for k, e := range s.entries {
		if first || e.LastSeen.Before(oldestTime) {
			oldestKey = k
			oldestTime = e.LastSeen
			first = false
		}
	}
	delete(s.entries, oldestKey)
}
