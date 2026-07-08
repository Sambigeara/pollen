// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"fmt"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

// ReceiveGrantOffer is the recipient side of an admin-initiated
// capability upgrade. It validates the offered grant against the same
// gate as renewal and first-time enrol before any store mutation; only
// once AdoptGrant has committed the swap and rewritten the on-disk copy
// do the cluster mutations run. A rejected or unpersistable grant never
// leaks half-formed state to peers.
//
// Capability shrink relies on RevokeOwnSpecs; signed tombstones are
// exempt from the per-kind cap check (see admission.authorise).
//
// If RevokeOwnSpecs fails after AdoptGrant has committed, the in
// memory grant is rolled back to the prior value and persisted to
// match. The only way the durable record stays on the new grant is if
// the rollback's own persist also fails; that double failure is
// logged so an operator can reconcile by hand.
func (s *Service) ReceiveGrantOffer(req *meshv1.GrantOfferRequest) *meshv1.GrantOfferResponse {
	newGrant := req.GetGrant()
	if newGrant == nil {
		return &meshv1.GrantOfferResponse{Reason: "grant missing"}
	}

	snap := s.store.Snapshot()
	now := time.Now()
	prevGrant := s.creds.Grant()
	if err := s.creds.AdoptGrant(newGrant, now, snap.DenyChecker()); err != nil {
		return &meshv1.GrantOfferResponse{Reason: err.Error()}
	}

	newCaps := newGrant.GetClaims().GetCapabilities()
	revokeEvents, revokeErr := s.store.RevokeOwnSpecs(newCaps)
	if revokeErr != nil {
		if rbErr := s.creds.AdoptGrant(prevGrant, now, snap.DenyChecker()); rbErr != nil {
			s.log.Errorw("rollback after revoke failure also failed; in-memory grant may be inconsistent",
				"revoke_err", revokeErr, "rollback_err", rbErr)
		}
		return &meshv1.GrantOfferResponse{Reason: fmt.Sprintf("revoke own specs: %s", revokeErr.Error())}
	}
	s.forwardEvents(revokeEvents)
	s.rescheduleExpiryTimer(newGrant)
	s.publishLocalGrant(newGrant)

	ownSubject := identity.PrincipalFromGrant(newGrant).SubjectPub
	s.log.Infow("grant upgraded via mesh push",
		"subject", types.PeerKeyFromBytes(ownSubject).Short(),
		"deadline_unix", newGrant.GetClaims().GetGrantDeadlineUnix(),
		"can_admit", newCaps.GetCanAdmit(),
		"can_delegate", newCaps.GetCanDelegate())
	return &meshv1.GrantOfferResponse{Accepted: true}
}
