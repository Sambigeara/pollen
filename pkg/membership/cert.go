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
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"
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
	connectedPeers := s.mesh.ConnectedPeers()
	if len(connectedPeers) == 0 {
		s.log.Warnw("delegation certificate renewal failed: no connected peers")
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
			s.log.Warnw("delegation certificate renewal failed: invalid cert", "peer", peerKey.Short(), "err", err)
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
			s.log.Warnw("routed cert renewal: invalid cert", "admin", pk.Short(), "err", err)
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

func (s *Service) applyNewCert(newCert *admissionv1.DelegationCert) error {
	pubKey := s.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if err := auth.VerifyDelegationCert(newCert, s.creds.RootPub(), time.Now(), pubKey); err != nil {
		return err
	}

	tlsCert, err := transport.GenerateIdentityCert(s.signPriv, newCert, s.tlsIdentityTTL)
	if err != nil {
		return err
	}

	s.certs.UpdateMeshCert(tlsCert)
	s.creds.SetCert(newCert)

	if err := auth.SaveNodeCredentials(s.pollenDir, s.creds); err != nil {
		s.log.Warnw("failed to persist credentials", "err", err)
	}

	return nil
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

	ttl := s.membershipTTL
	caps := auth.LeafCapabilities()
	var accessDeadline time.Time
	if peerCert, ok := s.certs.PeerDelegationCert(from); ok {
		ttl = auth.CertTTL(peerCert)
		caps.Attributes = peerCert.GetClaims().GetCapabilities().GetAttributes()
		if dl, hasDeadline := auth.CertAccessDeadline(peerCert); hasDeadline {
			if time.Now().After(dl) {
				sendReject("access deadline has passed")
				return
			}
			accessDeadline = dl
		}
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
}

func (s *Service) IssueCert(ctx context.Context, peerKey types.PeerKey, admin bool, attributes *structpb.Struct) error {
	signer := s.creds.DelegationKey()
	if signer == nil || !signer.IsRoot() {
		return errors.New("only root admin can push certificates")
	}

	caps := auth.LeafCapabilities()
	if admin {
		caps = auth.FullCapabilities()
	}
	caps.Attributes = attributes

	now := time.Now()
	ttl := s.membershipTTL
	if admin {
		ttl = config.DefaultDelegationTTL
	}
	cert, err := signer.IssueMemberCert(peerKey.Bytes(), caps, now, now.Add(ttl), time.Time{})
	if err != nil {
		return fmt.Errorf("issue cert: %w", err)
	}

	return s.certs.PushCert(ctx, peerKey, cert)
}

func (s *Service) handleCertPushRequest(ctx context.Context, from types.PeerKey, req *meshv1.CertPushRequest) {
	sendReject := func(reason string) {
		s.sendCertPushResponse(ctx, from, &meshv1.CertPushResponse{Reason: reason})
	}

	newCert := req.GetCert()
	if err := s.applyNewCert(newCert); err != nil {
		sendReject(err.Error())
		return
	}

	caps := newCert.GetClaims().GetCapabilities()
	if caps.GetCanDelegate() {
		signer := auth.NewDelegationSignerFromCert(s.signPriv, newCert)
		s.creds.SetDelegationKey(signer)
		s.capTransition.UpgradeToAdmin(signer)
	} else {
		s.creds.SetDelegationKey(nil)
		s.capTransition.DowngradeToLeaf()
	}

	s.log.Infow("certificate pushed", "from", from.Short(), "can_delegate", caps.GetCanDelegate())
	s.sendCertPushResponse(ctx, from, &meshv1.CertPushResponse{Accepted: true})
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
