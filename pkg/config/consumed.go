package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

const (
	consumedInvitesFile = "consumed_invites.json"
	consumedExpirySkew  = time.Minute
)

type consumedInviteRecord struct {
	TokenID        string `json:"tokenID"`
	ExpiresAtUnix  int64  `json:"expiresAtUnix"`
	ConsumedAtUnix int64  `json:"consumedAtUnix"`
}

type consumedInviteState struct {
	Invites []consumedInviteRecord `json:"invites"`
}

type ConsumedInvites struct {
	entries map[string]consumedInviteRecord
	path    string
	mu      sync.Mutex
}

func LoadConsumedInvites(pollenDir string, now time.Time) (*ConsumedInvites, error) {
	path := filepath.Join(pollenDir, consumedInvitesFile)
	entries, err := readConsumedEntries(path, now)
	if err != nil {
		return nil, err
	}

	return &ConsumedInvites{path: path, entries: entries}, nil
}

func (c *ConsumedInvites) TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error) {
	if c == nil {
		return false, errors.New("consumed invite state not configured")
	}

	claims := token.GetClaims()
	if claims == nil {
		return false, errors.New("invite token missing claims")
	}

	tokenID := claims.GetTokenId()
	if tokenID == "" {
		return false, errors.New("invite token missing token id")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.dropExpired(now)

	if _, exists := c.entries[tokenID]; exists {
		return false, nil
	}

	c.entries[tokenID] = consumedInviteRecord{
		TokenID:        tokenID,
		ExpiresAtUnix:  claims.GetExpiresAtUnix(),
		ConsumedAtUnix: now.Unix(),
	}

	if err := writeConsumedEntries(c.path, c.entries); err != nil {
		return false, err
	}

	return true, nil
}

func isConsumedExpired(rec consumedInviteRecord, nowUnix int64) bool {
	return rec.ExpiresAtUnix > 0 && rec.ExpiresAtUnix+int64(consumedExpirySkew/time.Second) < nowUnix
}

func (c *ConsumedInvites) dropExpired(now time.Time) {
	nowUnix := now.Unix()
	for tokenID, rec := range c.entries {
		if isConsumedExpired(rec, nowUnix) {
			delete(c.entries, tokenID)
		}
	}
}

func readConsumedEntries(path string, now time.Time) (map[string]consumedInviteRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]consumedInviteRecord{}, nil
		}
		return nil, fmt.Errorf("read consumed invites: %w", err)
	}

	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]consumedInviteRecord{}, nil
	}

	var state consumedInviteState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode consumed invites: %w", err)
	}

	nowUnix := now.Unix()
	entries := make(map[string]consumedInviteRecord, len(state.Invites))
	for _, rec := range state.Invites {
		if rec.TokenID == "" {
			continue
		}
		if isConsumedExpired(rec, nowUnix) {
			continue
		}
		entries[rec.TokenID] = rec
	}

	return entries, nil
}

func writeConsumedEntries(path string, entries map[string]consumedInviteRecord) error {
	state := consumedInviteState{Invites: make([]consumedInviteRecord, 0, len(entries))}
	for _, rec := range entries {
		state.Invites = append(state.Invites, rec)
	}
	sort.Slice(state.Invites, func(i, j int) bool {
		return state.Invites[i].TokenID < state.Invites[j].TokenID
	})

	if err := os.MkdirAll(filepath.Dir(path), directoryPerm); err != nil {
		return fmt.Errorf("create consumed invites directory: %w", err)
	}

	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode consumed invites: %w", err)
	}
	encoded = append(encoded, '\n')

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, encoded, configFilePerm); err != nil {
		return fmt.Errorf("write consumed invites: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace consumed invites: %w", err)
	}

	return nil
}
