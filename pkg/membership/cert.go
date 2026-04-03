package membership

import (
	"bytes"
	"context"
	"crypto/ed25519"
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

		if err := s.applyCertRenewal(newCert); err != nil {
			s.log.Warnw("delegation certificate renewal failed: invalid cert", "peer", peerKey.Short(), "err", err)
			continue
		}

		s.log.Infow("delegation certificate renewed", "expires_at", auth.CertExpiresAt(newCert))
		s.nodeMetrics.CertRenewals.Add(ctx, 1)
		return true
	}

	s.log.Warnw("delegation certificate renewal failed: all peers refused or returned errors")
	s.nodeMetrics.CertRenewalsFailed.Add(ctx, 1)
	return false
}

func (s *Service) applyCertRenewal(newCert *admissionv1.DelegationCert) error {
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
		s.log.Warnw("failed to persist renewed credentials", "err", err)
	}

	return nil
}

func (s *Service) handleCertRenewalRequest(ctx context.Context, from types.PeerKey, req *meshv1.CertRenewalRequest) {
	sendReject := func(reason string) {
		data, err := (&meshv1.Envelope{
			Body: &meshv1.Envelope_CertRenewalResponse{
				CertRenewalResponse: &meshv1.CertRenewalResponse{Reason: reason},
			},
		}).MarshalVT()
		if err == nil {
			_ = s.mesh.Send(ctx, from, data)
		}
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
	var accessDeadline time.Time
	if peerCert, ok := s.certs.PeerDelegationCert(from); ok {
		ttl = auth.CertTTL(peerCert)
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
		auth.LeafCapabilities(),
		now,
		notAfter,
		accessDeadline,
	)
	if err != nil {
		sendReject(err.Error())
		return
	}

	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_CertRenewalResponse{CertRenewalResponse: &meshv1.CertRenewalResponse{
			Accepted: true,
			Cert:     newCert,
		}},
	}).MarshalVT()
	if err == nil {
		_ = s.mesh.Send(ctx, from, data)
	}
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
