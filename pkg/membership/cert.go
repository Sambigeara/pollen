// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *Service) checkCertExpiry() bool {
	now := time.Now()
	cert := s.creds.Cert()
	expiresAt := auth.CertExpiresAt(cert)
	remaining := time.Until(expiresAt)

	s.nodeMetrics.CertExpirySeconds.Record(context.Background(), remaining.Seconds())

	if auth.IsCertExpired(cert, now) {
		if s.attemptCertRenewal() {
			s.renewalFailed.Store(false)
			return false
		}
		s.renewalFailed.Store(true)

		if !now.Before(expiresAt.Add(s.reconnectWindow)) {
			s.log.Errorw("delegation certificate expired beyond reconnect window, shutting down", "expired_at", expiresAt)
			return true
		}
		s.log.Warnw("delegation certificate expired — running in degraded mode", "expired_at", expiresAt)
		return false
	}

	if remaining <= CertWarnThreshold {
		failed := !s.attemptCertRenewal()
		s.renewalFailed.Store(failed)
		if !failed {
			return false
		}

		if remaining <= CertCriticalThreshold {
			s.log.Warnw("delegation certificate expiring soon — auto-renewal failed", "expires_in", remaining.Truncate(time.Minute))
		} else {
			s.log.Infow("delegation certificate approaching expiry — auto-renewal failed", "expires_in", remaining.Truncate(time.Minute))
		}
	}
	return false
}

func (s *Service) attemptCertRenewal() bool {
	if signer := s.creds.DelegationKey(); signer != nil && signer.IsRoot() {
		return s.attemptRootSelfRenewal()
	}

	connectedPeers := s.mesh.ConnectedPeers()
	if len(connectedPeers) == 0 {
		s.log.Debugw("delegation certificate renewal skipped: no connected peers")
		return false
	}

	s.log.Infow("renewing delegation certificate")

	ctx, cancel := context.WithTimeout(context.Background(), certRenewalTimeout)
	defer cancel()

	for _, peerKey := range connectedPeers {
		newCert, err := s.certs.RequestCertRenewal(ctx, peerKey)
		if err != nil {
			s.log.Debugw("delegation certificate renewal failed", "peer", peerKey.Short(), "err", err)
			continue
		}

		if err := s.applyNewCert(newCert); err != nil {
			s.log.Errorw("delegation certificate renewal failed: invalid cert", "peer", peerKey.Short(), "err", err)
			continue
		}

		s.log.Infow("delegation certificate renewed", "expires_at", auth.CertExpiresAt(newCert))
		s.nodeMetrics.CertRenewals.Add(ctx, 1)
		return true
	}

	connectedSet := make(map[types.PeerKey]struct{}, len(connectedPeers))
	for _, pk := range connectedPeers {
		connectedSet[pk] = struct{}{}
	}

	snap := s.store.Snapshot()
	for pk, nv := range snap.Nodes {
		if pk == s.localID || !nv.AdminCapable {
			continue
		}
		if _, tried := connectedSet[pk]; tried {
			continue
		}
		newCert, err := s.certs.RequestCertRenewal(ctx, pk)
		if err != nil {
			s.log.Debugw("routed cert renewal failed", "admin", pk.Short(), "err", err)
			continue
		}
		if err := s.applyNewCert(newCert); err != nil {
			s.log.Errorw("routed cert renewal: invalid cert", "admin", pk.Short(), "err", err)
			continue
		}
		s.log.Infow("delegation certificate renewed via routed admin", "admin", pk.Short(), "expires_at", auth.CertExpiresAt(newCert))
		s.nodeMetrics.CertRenewals.Add(ctx, 1)
		return true
	}

	s.log.Warnw("delegation certificate renewal failed: all peers and admins refused or returned errors")
	s.nodeMetrics.CertRenewalsFailed.Add(ctx, 1)
	return false
}

// attemptRootSelfRenewal re-issues from the local admin key, preserving
// existing attributes. This path is purely for expiry-driven refresh.
func (s *Service) attemptRootSelfRenewal() bool {
	nodePub := s.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	attrs := s.creds.Cert().GetClaims().GetCapabilities().GetAttributes()

	creds, err := auth.EnsureLocalRootCredentials(
		auth.IdentityPath(s.pollenDir), nodePub, attrs,
		time.Now(), auth.DefaultDelegationTTL,
	)
	if err != nil {
		s.log.Errorw("root cert self-renewal failed", "err", err)
		s.nodeMetrics.CertRenewalsFailed.Add(context.Background(), 1)
		return false
	}

	if err := s.applyNewCert(creds.Cert()); err != nil {
		s.log.Errorw("root cert self-renewal: applyNewCert failed", "err", err)
		s.nodeMetrics.CertRenewalsFailed.Add(context.Background(), 1)
		return false
	}

	s.log.Infow("root cert self-renewed", "expires_at", auth.CertExpiresAt(creds.Cert()))
	s.nodeMetrics.CertRenewals.Add(context.Background(), 1)
	return true
}

func (s *Service) applyNewCert(newCert *admissionv1.DelegationCert) error {
	pubKey := s.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if err := auth.VerifyDelegationCert(newCert, s.creds.RootPub(), time.Now(), pubKey); err != nil {
		return err
	}

	tlsCert, err := transport.GenerateIdentityCert(s.signPriv, newCert, s.tlsIdentityTTL)
	if err != nil {
		return err
	}

	// If the new cert strips publish authority, tombstone every spec
	// this node owns while the old signer is still wired into the store.
	// Without this, services and seeds the peer published as a publisher
	// keep gossiping under their old (still-cryptographically-valid)
	// SpecAuth even after the admin downgrades them. The returned events
	// must be forwarded so the supervisor cascade tears down live
	// runtime state (tunneling bridges, placement claims) alongside
	// the gossip tombstones.
	oldCaps := s.creds.Cert().GetClaims().GetCapabilities()
	if oldCaps.GetCanPublish() && !newCert.GetClaims().GetCapabilities().GetCanPublish() {
		revokeEvents, revokeErr := s.store.RevokeOwnSpecs()
		if revokeErr != nil {
			s.log.Warnw("revoke own specs on cap downgrade", "err", revokeErr)
		}
		s.forwardEvents(revokeEvents)
	}

	s.certs.UpdateMeshCert(tlsCert)
	s.creds.SetCert(newCert)

	// Signers capture their issuer cert by value, so every renewal must
	// rebuild them; otherwise SpecAuth issuance and invite minting keep
	// embedding the about-to-expire cert as publisher and fresh joiners
	// reject those events once the old TTL elapses. Admin→non-admin
	// transitions still happen in the caller, which has the prior
	// capability to decide whether to clear the delegation key.
	switch caps := newCert.GetClaims().GetCapabilities(); {
	case caps.GetCanDelegate():
		signer := auth.NewDelegationSignerFromCert(s.signPriv, newCert)
		s.creds.SetDelegationKey(signer)
		if s.capTransition != nil {
			s.capTransition.UpgradeToAdmin(signer)
		}
	case caps.GetCanPublish():
		signer := auth.NewSpecSignerFromCert(s.signPriv, newCert)
		s.creds.SetSpecSigner(signer)
		// Refresh the store's signer too: cert renewal goes through
		// applyNewCert without touching capTransition, so without this
		// the store keeps the about-to-expire signer.
		s.store.SetLocalSigner(signer)
	}

	if err := auth.SaveNodeCredentials(auth.IdentityPath(s.pollenDir), s.creds); err != nil {
		s.log.Warnw("failed to persist credentials", "err", err)
	}

	// Gossip the new cert so peers can evaluate chain-scoped policy
	// (deny scoping, downstream cascade) without needing a direct TLS
	// session to us.
	s.publishLocalDelegationCert(newCert)

	return nil
}

// publishLocalDelegationCert signs the cert with the local subject
// key and emits the change. The subject signature is what proves to
// other nodes that we (and not some delegated admin re-parenting our
// pub) authored this current cert.
func (s *Service) publishLocalDelegationCert(cert *admissionv1.DelegationCert) {
	if cert == nil || len(s.signPriv) == 0 {
		// Test fixtures and bare-token enrollment paths may invoke
		// us without a signing key wired in yet. Skip gossiping
		// rather than panic; the next renewal/push that reaches us
		// will publish a properly-signed cert.
		return
	}
	sig, err := auth.SignDelegationCertSubject(cert, s.signPriv)
	if err != nil {
		s.log.Errorw("sign delegation cert for gossip", "err", err)
		return
	}
	s.forwardEvents(s.store.SetLocalDelegationCert(cert, sig))
}

func (s *Service) sendCertRenewalResponse(ctx context.Context, to types.PeerKey, resp *meshv1.CertRenewalResponse) {
	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_CertRenewalResponse{CertRenewalResponse: resp},
	}).MarshalVT()
	if err != nil {
		return
	}
	_ = s.routedSender.SendMembershipDatagram(ctx, to, data)
}

func (s *Service) handleCertRenewalRequest(ctx context.Context, from types.PeerKey, req *meshv1.CertRenewalRequest) {
	sendReject := func(reason string) {
		s.sendCertRenewalResponse(ctx, from, &meshv1.CertRenewalResponse{Reason: reason})
	}

	if !bytes.Equal(req.GetPeerPub(), from.Bytes()) {
		sendReject("peer_pub does not match sender")
		return
	}

	signer := s.creds.DelegationKey()
	if signer == nil {
		sendReject("this node is not an admin")
		return
	}

	if slices.Contains(s.store.Snapshot().DeniedPeers(), types.PeerKeyFromBytes(req.GetPeerPub())) {
		sendReject("subject has been denied")
		return
	}

	// Preserve the requester's current capabilities verbatim. Defaulting
	// to leaf and copying only attributes silently downgrades publishers
	// and delegated admins on every renewal, which then trips
	// applyNewCert's downgrade branch and tombstones every spec the
	// node had gossiped — a healthy node vanishes from the mesh on a
	// routine expiry refresh.
	peerCert, ok := s.certs.PeerDelegationCert(from)
	if !ok {
		// Routed renewals come from peers without a direct session, so
		// fall back to the gossiped cert. Inbound admission already
		// verified its chain and subject signature, so it's authoritative.
		if nv, found := s.store.Snapshot().Nodes[from]; found && nv.Cert != nil {
			peerCert = nv.Cert
		}
	}
	if peerCert == nil {
		sendReject("no current cert known for requester")
		return
	}

	peerCaps := peerCert.GetClaims().GetCapabilities()
	caps := &admissionv1.Capabilities{
		CanDelegate: peerCaps.GetCanDelegate(),
		CanAdmit:    peerCaps.GetCanAdmit(),
		CanPublish:  peerCaps.GetCanPublish(),
		MaxDepth:    peerCaps.GetMaxDepth(),
		Attributes:  peerCaps.GetAttributes(),
	}
	ttl := auth.CertTTL(peerCert)
	var accessDeadline time.Time
	if dl, hasDeadline := auth.CertAccessDeadline(peerCert); hasDeadline {
		if time.Now().After(dl) {
			sendReject("access deadline has passed")
			return
		}
		accessDeadline = dl
	}

	now := time.Now()
	notAfter := now.Add(ttl)
	if !accessDeadline.IsZero() && notAfter.After(accessDeadline) {
		notAfter = accessDeadline
	}

	newCert, err := signer.IssueMemberCert(
		req.GetPeerPub(),
		caps,
		now,
		notAfter,
		accessDeadline,
	)
	if err != nil {
		sendReject(err.Error())
		return
	}

	s.sendCertRenewalResponse(ctx, from, &meshv1.CertRenewalResponse{
		Accepted: true,
		Cert:     newCert,
	})
	s.certs.SetPeerDelegationCert(from, newCert)
}

func (s *Service) IssueCert(ctx context.Context, peerKey types.PeerKey, certCaps *admissionv1.Capabilities) error {
	if certCaps == nil {
		return errors.New("cert caps must be provided")
	}
	signer := s.creds.DelegationKey()
	if signer == nil {
		return errors.New("this node has no delegation authority")
	}
	if certCaps.GetCanAdmit() && !signer.IsRoot() {
		return errors.New("only the root admin can issue admin certificates")
	}

	now := time.Now()
	ttl := s.membershipTTL
	if certCaps.GetCanAdmit() {
		ttl = auth.DefaultDelegationTTL
	}
	cert, err := signer.IssueMemberCert(peerKey.Bytes(), certCaps, now, now.Add(ttl), time.Time{})
	if err != nil {
		return fmt.Errorf("issue cert: %w", err)
	}

	if err := s.certs.PushCert(ctx, peerKey, cert); err != nil {
		return err
	}
	// Install into our own cache so subsequent authz evaluations and
	// renewal handlers see ground truth immediately — without waiting
	// for the target's TLS session to us to re-handshake. Without this,
	// a renewal routed through the issuer rebuilds the cert from the
	// stale cache and silently drops the attributes we just granted.
	s.certs.SetPeerDelegationCert(peerKey, cert)
	return nil
}

func (s *Service) handleCertPushRequest(ctx context.Context, from types.PeerKey, req *meshv1.CertPushRequest) {
	sendReject := func(reason string) {
		s.sendCertPushResponse(ctx, from, &meshv1.CertPushResponse{Reason: reason})
	}

	// Capture before applyNewCert overwrites s.creds.Cert.
	oldCanDelegate := s.creds.Cert().GetClaims().GetCapabilities().GetCanDelegate()

	newCert := req.GetCert()
	if err := s.applyNewCert(newCert); err != nil {
		sendReject(err.Error())
		return
	}

	newCaps := newCert.GetClaims().GetCapabilities()
	newCanDelegate := newCaps.GetCanDelegate()
	// The upgrade/regrant case is handled inside applyNewCert; only
	// the downgrade needs the prior capability to know it has to
	// strip the delegation key and clear the admin marker. Publishers
	// keep a spec signer so they can still publish resources.
	if !newCanDelegate {
		s.creds.SetDelegationKey(nil)
		var specSigner *auth.SpecSigner
		if newCaps.GetCanPublish() {
			specSigner = auth.NewSpecSignerFromCert(s.signPriv, newCert)
			s.creds.SetSpecSigner(specSigner)
		} else {
			s.creds.SetSpecSigner(nil)
		}
		s.capTransition.DowngradeFromAdmin(specSigner)
	}

	s.log.Infow("certificate pushed", "from", from.Short(), "can_delegate", newCanDelegate)
	s.sendCertPushResponse(ctx, from, &meshv1.CertPushResponse{Accepted: true})

	// Rotate sessions so peers re-handshake with the new cert. Skip
	// on leaf→admin promotion: AdminCapable propagates via gossip, and
	// closing sessions orphans in-flight invite forwarding. Admin→admin
	// re-grants still rotate so attribute changes reach peers' caches.
	if !oldCanDelegate && newCanDelegate {
		return
	}
	s.rotateSessionsForNewCert(from)
}

// exclude is the cert pusher: closing its session would race the
// accept datagram and fail the admin's PushCert RPC.
func (s *Service) rotateSessionsForNewCert(exclude types.PeerKey) {
	if s.sessionCloser == nil {
		return
	}
	for _, pk := range s.mesh.ConnectedPeers() {
		if pk == exclude {
			continue
		}
		s.sessionCloser.ClosePeerSession(pk, transport.DisconnectCertRotation)
	}
}

func (s *Service) sendCertPushResponse(ctx context.Context, to types.PeerKey, resp *meshv1.CertPushResponse) {
	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_CertPushResponse{CertPushResponse: resp},
	}).MarshalVT()
	if err != nil {
		return
	}
	_ = s.routedSender.SendMembershipDatagram(ctx, to, data)
}

func (s *Service) disconnectExpiredPeers() {
	now := time.Now()
	for _, peerKey := range s.mesh.ConnectedPeers() {
		dc, ok := s.certs.PeerDelegationCert(peerKey)
		if !ok || !auth.IsCertExpired(dc, now) {
			continue
		}
		if now.Before(auth.CertExpiresAt(dc).Add(s.reconnectWindow)) {
			continue
		}
		s.log.Warnw("disconnecting peer with expired delegation cert", "peer", peerKey.Short(), "expired_at", auth.CertExpiresAt(dc))
		s.sessionCloser.ClosePeerSession(peerKey, transport.DisconnectCertExpired)
		s.sendEvent(state.PeerDenied{Key: peerKey})
	}
}
