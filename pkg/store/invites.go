package store

import (
	"errors"
	"fmt"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
)

var _ auth.InviteConsumer = (*Store)(nil)

const consumedExpirySkew = time.Minute

type consumedInviteEntry struct {
	TokenID        string
	ExpiresAtUnix  int64
	ConsumedAtUnix int64
}

// TryConsume marks an invite token as consumed, returning true if it was
// not previously consumed. Implements auth.InviteConsumer.
func (s *Store) TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error) {
	claims := token.GetClaims()
	if claims == nil {
		return false, errors.New("invite token missing claims")
	}
	tokenID := claims.GetTokenId()
	if tokenID == "" {
		return false, errors.New("invite token missing token id")
	}

	s.mu.Lock()

	s.dropExpiredInvites(now)

	if _, exists := s.consumedInvites[tokenID]; exists {
		s.mu.Unlock()
		return false, nil
	}

	s.consumedInvites[tokenID] = consumedInviteEntry{
		TokenID:        tokenID,
		ExpiresAtUnix:  claims.GetExpiresAtUnix(),
		ConsumedAtUnix: now.Unix(),
	}

	snapshot := s.snapshotStateLocked()
	s.mu.Unlock()

	if err := s.disk.save(snapshot); err != nil {
		return true, fmt.Errorf("persist consumed invite: %w", err)
	}

	return true, nil
}

func (s *Store) dropExpiredInvites(now time.Time) {
	nowUnix := now.Unix()
	for tokenID, entry := range s.consumedInvites {
		if entry.ExpiresAtUnix > 0 && entry.ExpiresAtUnix+int64(consumedExpirySkew/time.Second) < nowUnix {
			delete(s.consumedInvites, tokenID)
		}
	}
}

func loadConsumedInvites(protos []*statev1.ConsumedInvite, now time.Time) map[string]consumedInviteEntry {
	nowUnix := now.Unix()
	out := make(map[string]consumedInviteEntry, len(protos))
	for _, p := range protos {
		tokenID := p.GetTokenId()
		if tokenID == "" {
			continue
		}
		entry := consumedInviteEntry{
			TokenID:        tokenID,
			ExpiresAtUnix:  p.GetExpiryUnix(),
			ConsumedAtUnix: p.GetConsumedUnix(),
		}
		if entry.ExpiresAtUnix > 0 && entry.ExpiresAtUnix+int64(consumedExpirySkew/time.Second) < nowUnix {
			continue
		}
		out[tokenID] = entry
	}
	return out
}
