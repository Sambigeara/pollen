package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
)

// checkCertExpiry checks the local node's delegation cert and logs warnings.
// Returns true if the cert has expired beyond the reconnect window and the
// node should shut down.
func (n *Node) checkCertExpiry() bool {
	now := time.Now()
	expired := auth.IsCertExpired(n.creds.Cert, now)
	remaining := time.Until(auth.CertExpiresAt(n.creds.Cert))
	n.nodeMetrics.CertExpirySeconds.Set(remaining.Seconds())

	if expired {
		// Attempt renewal before giving up.
		if n.attemptCertRenewal() {
			n.renewalFailed.Store(false)
			return false
		}
		n.renewalFailed.Store(true)

		if !auth.IsCertWithinReconnectWindow(n.creds.Cert, now, n.conf.ReconnectWindow) {
			n.log.Errorw("delegation certificate expired beyond reconnect window, shutting down — rejoin the cluster or contact a cluster admin",
				"expired_at", auth.CertExpiresAt(n.creds.Cert))
			return true
		}
		n.log.Warnw("delegation certificate expired — running in degraded mode, will keep retrying renewal",
			"expired_at", auth.CertExpiresAt(n.creds.Cert),
			"reconnect_window_remaining", auth.CertExpiresAt(n.creds.Cert).Add(n.conf.ReconnectWindow).Sub(now).Truncate(time.Minute))
		return false
	}

	if remaining <= certWarnThreshold {
		failed := !n.attemptCertRenewal()
		n.renewalFailed.Store(failed)
		if !failed {
			return false
		}
	}

	switch {
	case remaining <= certCriticalThreshold:
		n.log.Warnw("delegation certificate expiring soon — auto-renewal failed — rejoin the cluster or contact a cluster admin",
			"expires_in", remaining.Truncate(time.Minute))
	case remaining <= certWarnThreshold:
		n.log.Infow("delegation certificate approaching expiry — auto-renewal attempted but failed, will retry",
			"expires_in", remaining.Truncate(time.Minute))
	}
	return false
}

func (n *Node) attemptCertRenewal() bool {
	connectedPeers := n.getConnectedPeers()
	if len(connectedPeers) == 0 {
		n.log.Warnw("delegation certificate renewal failed: no connected peers")
		return false
	}

	n.log.Infow("renewing delegation certificate")

	ctx, cancel := context.WithTimeout(context.Background(), certRenewalTimeout)
	defer cancel()

	for _, peerKey := range connectedPeers {
		newCert, err := n.mesh.RequestCertRenewal(ctx, peerKey)
		if err != nil {
			n.log.Debugw("delegation certificate renewal failed", "peer", peerKey.Short(), "err", err)
			continue
		}

		if err := n.applyCertRenewal(newCert); err != nil {
			n.log.Warnw("delegation certificate renewal failed: invalid cert", "peer", peerKey.Short(), "err", err)
			continue
		}

		n.log.Infow("delegation certificate renewed",
			"expires_at", auth.CertExpiresAt(newCert))
		n.nodeMetrics.CertRenewals.Inc()
		return true
	}

	n.log.Warnw("delegation certificate renewal failed: all peers refused or returned errors")
	n.nodeMetrics.CertRenewalsFailed.Inc()
	return false
}

func (n *Node) applyCertRenewal(newCert *admissionv1.DelegationCert) error {
	now := time.Now()
	pubKey := n.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if err := auth.VerifyDelegationCert(newCert, n.creds.Trust, now, pubKey); err != nil {
		return err
	}

	tlsCert, err := mesh.GenerateIdentityCert(n.signPriv, newCert, n.conf.TLSIdentityTTL)
	if err != nil {
		return err
	}

	n.mesh.UpdateMeshCert(tlsCert)
	n.creds.Cert = newCert

	if err := auth.SaveNodeCredentials(n.pollenDir, n.creds); err != nil {
		n.log.Warnw("failed to persist renewed credentials", "err", err)
	}

	n.queueGossipEvents(n.store.SetLocalCertExpiry(newCert.GetClaims().GetNotAfterUnix()))
	return nil
}

func (n *Node) handleCertRenewalRequest(ctx context.Context, from types.PeerKey, req *meshv1.CertRenewalRequest) {
	sendReject := func(reason string) {
		_ = n.mesh.Send(ctx, from, &meshv1.Envelope{
			Body: &meshv1.Envelope_CertRenewalResponse{
				CertRenewalResponse: &meshv1.CertRenewalResponse{Reason: reason},
			},
		})
	}

	if !bytes.Equal(req.GetSubjectPub(), from.Bytes()) {
		sendReject("subject_pub does not match sender")
		return
	}

	signer := n.creds.DelegationKey
	if signer == nil {
		sendReject("this node is not an admin")
		return
	}

	if n.store.IsDenied(req.GetSubjectPub()) {
		sendReject("subject has been denied")
		return
	}

	ttl := n.conf.MembershipTTL
	var accessDeadline time.Time
	if peerCert, ok := n.mesh.PeerDelegationCert(from); ok {
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

	parentChain := make([]*admissionv1.DelegationCert, 0, 1+len(signer.Issuer.GetChain()))
	parentChain = append(parentChain, signer.Issuer)
	parentChain = append(parentChain, signer.Issuer.GetChain()...)
	newCert, err := auth.IssueDelegationCert(
		signer.Priv,
		parentChain,
		signer.Trust.GetClusterId(),
		req.GetSubjectPub(),
		auth.LeafCapabilities(),
		now.Add(-time.Minute),
		notAfter,
		accessDeadline,
	)
	if err != nil {
		sendReject(err.Error())
		return
	}

	_ = n.mesh.Send(ctx, from, &meshv1.Envelope{
		Body: &meshv1.Envelope_CertRenewalResponse{CertRenewalResponse: &meshv1.CertRenewalResponse{
			Accepted: true,
			Cert:     newCert,
		}},
	})
}

func (n *Node) disconnectExpiredPeers() {
	now := time.Now()
	for _, peerKey := range n.mesh.ConnectedPeers() {
		dc, ok := n.mesh.PeerDelegationCert(peerKey)
		if !ok || dc == nil {
			continue
		}
		if !auth.IsCertExpired(dc, now) {
			continue
		}
		// Allow peers within the reconnect window to stay connected so
		// they can renew their cert.
		if auth.IsCertWithinReconnectWindow(dc, now, n.conf.ReconnectWindow) {
			continue
		}
		n.log.Warnw("disconnecting peer with expired delegation cert beyond reconnect window",
			"peer", peerKey.Short(), "expired_at", auth.CertExpiresAt(dc))
		n.tun.DisconnectPeer(peerKey)
		n.mesh.ClosePeerSession(peerKey, mesh.CloseReasonCertExpired)
		n.handlePeerInput(peer.PeerDisconnected{PeerKey: peerKey, Reason: peer.DisconnectCertExpired})
		n.handlePeerInput(peer.ForgetPeer{PeerKey: peerKey})
	}
}
