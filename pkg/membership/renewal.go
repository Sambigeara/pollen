// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wire"
)

// findRenewalTargets returns the control endpoints of every peer that may
// re-mint this node's grant: it must hold delegate authority, not be
// denied, not be us, advertise a control endpoint, and sit at or below our
// immediate issuer (the issuer, or a node with the issuer in its chain).
// RenewGrant enforces this same subtree rule server-side; filtering here
// just avoids spending attempts on targets that would reject us. The
// original issuer need not be alive: any node beneath it qualifies.
// Candidates are returned in stable order so renewal retries the same peer.
func findRenewalTargets(snap state.Snapshot, self, issuer types.PeerKey) []string {
	var addrs []string
	for _, pk := range slices.SortedFunc(maps.Keys(snap.Nodes), types.PeerKey.Compare) {
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
		if pk != issuer && !identity.AncestorIn(issuer, nv.Grant) {
			continue
		}
		addrs = append(addrs, nv.ControlAddr)
	}
	return addrs
}

// renewGrantOnce asks a delegating peer to re-mint this node's grant,
// returning the fresh grant without installing it. It tries the known
// delegating peers in stable order and returns the first success; an
// unreachable peer is skipped (RenewGrantAt bounds each attempt) so one
// dead delegate does not strand renewal while another is reachable.
func (s *Service) renewGrantOnce(ctx context.Context) (*identityv1.Grant, error) {
	issuer := types.PeerKeyFromBytes(s.creds.Grant().GetClaims().GetIssuerPub())
	addrs := findRenewalTargets(s.store.Snapshot(), s.localID, issuer)
	if len(addrs) == 0 {
		return nil, errors.New("no delegating peer beneath this grant's issuer has a control endpoint")
	}
	var lastErr error
	for _, addr := range addrs {
		g, err := wire.RenewGrantAt(ctx, addr, s.creds, s.signPriv)
		if err == nil {
			return g, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// installRenewedGrant adopts a freshly issued grant into the live
// credentials (validated against our own root, subject, horizon and
// the cluster denylist by AdoptGrant, which also rewrites the on-disk
// copy as part of the swap), then gossips the new grant via the
// Principal CRDT. A grant that fails validation or persistence is
// rejected and the node keeps its current grant.
func (s *Service) installRenewedGrant(g *identityv1.Grant) error {
	if err := s.creds.AdoptGrant(g, time.Now(), s.store.Snapshot().DenyChecker()); err != nil {
		return err
	}
	s.rescheduleExpiryTimer(g)
	s.publishLocalGrant(g)
	return nil
}

// grantMaintenanceTick runs one periodic pass: it proactively renews
// the grant once it is within the renewal lead window, then reports
// whether the node must shut down. The terminal check runs against the
// possibly just-renewed grant, so a node that renews successfully never
// reaches the hard stop; only a node that cannot reach any delegating
// peer for the entire lead window expires. Admin/root grants carry no
// deadline and are neither renewed nor expired here.
func (s *Service) grantMaintenanceTick(ctx context.Context) bool {
	if identity.GrantRenewDue(s.creds.Grant(), time.Now()) {
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

// rescheduleExpiryTimer arms a one-shot timer that fires shutdown at
// the grant's deadline (plus skew) for non-renewable grants. The
// 5-minute grantCheckInterval is fine for renewable grants: a missed
// renewal has a ~10-day lead window of retries and a five-minute
// polling lag on the eventual hard stop is invisible against the
// 30-day default. A non-renewable grant has no second chance, so the
// daemon must shut down at the actual deadline rather than the next
// tick. Idempotent: callers re-invoke on every grant swap (start,
// renewal, admin upgrade) so the timer always reflects the live grant.
func (s *Service) rescheduleExpiryTimer(grant *identityv1.Grant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	claims := grant.GetClaims()
	if !claims.GetNonRenewable() {
		return
	}
	dl := claims.GetGrantDeadlineUnix()
	if dl == 0 {
		return
	}
	when := max(time.Until(time.Unix(dl, 0).Add(s.expirySkew)), 0)
	s.expiryTimer = time.AfterFunc(when, s.signalExpiryShutdown)
}

// signalExpiryShutdown fires from the deadline timer. Non-blocking
// send: the shutdown channel is buffered to one, the periodic ticker
// is the only other writer, and either reaching the supervisor is
// enough to unwind. A second writer arriving after the buffer fills
// would block this goroutine until the supervisor reads, which it
// already has signal to do.
func (s *Service) signalExpiryShutdown() {
	s.log.Errorw("non-renewable grant deadline passed; shutting down",
		"deadline_unix", s.creds.Grant().GetClaims().GetGrantDeadlineUnix())
	if s.shutdownCh == nil {
		return
	}
	select {
	case s.shutdownCh <- struct{}{}:
	default:
	}
}
