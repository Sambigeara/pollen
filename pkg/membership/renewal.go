// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wire"
)

// findRenewalTarget picks a reachable peer that can re-mint this node's
// grant: it must hold delegate authority, not be denied, not be us, and
// advertise a control endpoint. Any such peer works; renewal does not
// depend on the original issuer still being alive, because the server
// re-checks the caller's chain and the denylist before re-issuing.
// Peers are tried in a stable order so a failing attempt retries the
// same target rather than flapping across the cluster. Returns "" when
// no candidate is known.
func findRenewalTarget(snap state.Snapshot, self types.PeerKey) string {
	peers := make([]types.PeerKey, 0, len(snap.Nodes))
	for pk := range snap.Nodes {
		peers = append(peers, pk)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].String() < peers[j].String() })
	for _, pk := range peers {
		if pk == self {
			continue
		}
		nv := snap.Nodes[pk]
		if nv.ControlAddr == "" || snap.IsDenied(pk) {
			continue
		}
		if !identity.PrincipalFromGrant(nv.Grant).CanDelegate() {
			continue
		}
		return nv.ControlAddr
	}
	return ""
}

// renewGrantOnce finds a delegating peer and asks it to re-mint this
// node's grant, returning the fresh grant without installing it.
func (s *Service) renewGrantOnce(ctx context.Context) (*identityv1.Grant, error) {
	addr := findRenewalTarget(s.store.Snapshot(), s.localID)
	if addr == "" {
		return nil, errors.New("no delegating peer with a control endpoint is currently reachable")
	}
	return wire.RenewGrantAt(ctx, addr, s.creds, s.signPriv)
}

// installRenewedGrant validates a freshly issued grant against our own
// root and subject before adopting it, then hot-swaps it into the live
// credentials, persists it, and re-gossips it. The renewal response is
// not trusted blindly: a grant that does not chain to our root, is for
// a different subject, is denied, or is outside its horizon is
// rejected and the node keeps its current grant. SetGrant clears the
// cached session so the next session is minted from the new grant.
func (s *Service) installRenewedGrant(g *identityv1.Grant) error {
	cur := s.creds.Grant()
	if cur == nil {
		return errors.New("no current grant to renew")
	}
	chk := identity.CheckGrant(g, s.creds.RootPub(), time.Now(), cur.GetClaims().GetSubjectPub(), s.store.Snapshot().DenyChecker())
	if !chk.Status.Valid() {
		return fmt.Errorf("renewed grant rejected: %s: %s", chk.Status, chk.Reason)
	}
	s.creds.SetGrant(g)
	if err := identity.SaveCredentials(identity.IdentityPath(s.pollenDir), s.creds); err != nil {
		return fmt.Errorf("persist renewed grant: %w", err)
	}
	s.publishLocalGrant(g)
	return nil
}

// renewLeadWindow is how far before the grant deadline the node starts
// trying to renew. It must comfortably exceed certCheckInterval so a
// node gets many retry ticks against a possibly-unreachable delegating
// peer before the deadline actually bites. One third of the tenant
// horizon gives ~10 days of retry runway on the 30-day default; a
// healthy node renews long before it could ever hard-stop.
const renewLeadWindow = identity.DefaultGrantDeadlineTTL / 3

// grantMaintenanceTick runs one periodic pass: it proactively renews
// the grant once the deadline is within renewLeadWindow, then reports
// whether the node must shut down. The terminal check runs against the
// possibly just-renewed grant, so a node that renews successfully never
// reaches the hard stop; only a node that cannot reach any delegating
// peer for the entire lead window expires. Admin/root grants carry no
// deadline and are neither renewed nor expired here.
func (s *Service) grantMaintenanceTick(ctx context.Context) bool {
	if dl := s.creds.Grant().GetClaims().GetGrantDeadlineUnix(); dl > 0 &&
		time.Until(time.Unix(dl, 0)) < renewLeadWindow {
		s.attemptRenewal(ctx)
	}
	return s.checkGrantExpiry()
}

// attemptRenewal performs one renewal attempt and records its outcome
// for the degraded-state report. A failure is logged and retried on the
// next tick; the node keeps serving on its current grant until the
// deadline actually passes.
func (s *Service) attemptRenewal(ctx context.Context) {
	g, err := s.renewGrantOnce(ctx)
	if err == nil {
		err = s.installRenewedGrant(g)
	}
	s.attemptRecord(err)
	if err != nil {
		s.log.Warnw("proactive grant renewal failed; will retry", "err", err)
		return
	}
	s.log.Infow("grant renewed", "deadline_unix", s.creds.Grant().GetClaims().GetGrantDeadlineUnix())
}

// attemptRecord stores the outcome of a renewal attempt for the
// degraded-state report. Both the production loop and the tests record
// through here so the reported state matches the real path.
func (s *Service) attemptRecord(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewAttemptedAt = time.Now()
	s.renewErr = err
}

// RenewalFailing reports whether the most recent renewal attempt failed.
// It is true only once renewal has been attempted (the node is within
// the lead window) and the last attempt did not succeed, which is the
// truthful "degraded, acting before a hard stop" signal the control
// plane surfaces instead of a post-deadline grace that is never served.
func (s *Service) RenewalFailing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.renewAttemptedAt.IsZero() && s.renewErr != nil
}
