// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"context"
	"errors"
	"fmt"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

var ErrNotDelegating = errors.New("this node has no delegation authority")

// checkGrantExpiry reports whether the node's grant can no longer
// authenticate it to the mesh. Sessions are re-minted locally from the
// held grant with no round-trip, so the only terminal condition is the
// grant itself passing its deadline or being revoked: past that a
// fresh `pln join <token>` is required. Returns true to signal
// shutdown.
func (s *Service) checkGrantExpiry() bool {
	now := time.Now()
	grant := s.creds.Grant()
	chk := identity.CheckGrant(grant, s.creds.RootPub(), now, nil, nil)

	if dl := grant.GetClaims().GetGrantDeadlineUnix(); dl > 0 {
		s.nodeMetrics.CertExpirySeconds.Record(context.Background(), time.Until(time.Unix(dl, 0)).Seconds())
	}

	if chk.Status.Valid() {
		return false
	}
	s.log.Errorw("node grant no longer valid; rejoin with a fresh `pln join <token>`",
		"status", chk.Status, "reason", chk.Reason)
	return true
}

// publishLocalGrant gossips the local node's grant with a subject
// proof-of-possession signature so peers can evaluate chain-scoped
// policy (deny scoping, downstream cascade) without a direct session.
func (s *Service) publishLocalGrant(grant *identityv1.Grant) {
	if grant == nil || len(s.signPriv) == 0 {
		return
	}
	sig, err := identity.SignGrantSubject(grant, s.signPriv)
	if err != nil {
		s.log.Errorw("sign grant for gossip", "err", err)
		return
	}
	s.forwardEvents(s.store.SetLocalGrant(grant, sig))
}

// IssueGrant mints a child grant for a wire-mode caller under this
// node's grant chain. The node must hold a delegating grant. Admin
// grants carry no horizon (managed infrastructure); delegated tenant
// grants get the default re-bootstrap deadline bounding stolen-key
// exposure.
func (s *Service) IssueGrant(_ context.Context, peerKey types.PeerKey, caps *identityv1.Capabilities, budget *identityv1.Budget) (*identityv1.Grant, error) {
	if caps == nil {
		return nil, errors.New("capabilities must be provided")
	}
	if !s.creds.Grant().GetClaims().GetCapabilities().GetCanDelegate() {
		return nil, ErrNotDelegating
	}
	if budget == nil {
		budget = identity.UnlimitedBudget()
	}

	now := time.Now()
	var grantDeadline time.Time
	if !caps.GetCanAdmit() {
		grantDeadline = now.Add(identity.DefaultGrantDeadlineTTL)
	}
	grant, err := s.creds.IssueGrant(peerKey.Bytes(), caps, budget, now, grantDeadline)
	if err != nil {
		return nil, fmt.Errorf("issue grant: %w", err)
	}
	return grant, nil
}
