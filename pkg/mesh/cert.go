package mesh

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

func (m *impl) UpdateMeshCert(cert tls.Certificate) {
	m.meshCert.Store(&cert)
}

func (m *impl) RequestCertRenewal(ctx context.Context, peerKey types.PeerKey) (*admissionv1.DelegationCert, error) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, fmt.Errorf("no connection to peer %s", peerKey.Short())
	}

	currentCert := m.meshCert.Load()
	var currentCertRaw []byte
	if len(currentCert.Certificate) > 0 {
		currentCertRaw = currentCert.Certificate[0]
	}

	if err := sendEnvelope(s.conn, &meshv1.Envelope{
		Body: &meshv1.Envelope_CertRenewalRequest{
			CertRenewalRequest: &meshv1.CertRenewalRequest{
				SubjectPub:  m.localKey.Bytes(),
				CurrentCert: currentCertRaw,
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("send renewal request: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	select {
	case resp := <-m.renewalCh:
		if !resp.GetAccepted() {
			reason := resp.GetReason()
			if reason == "" {
				reason = "renewal rejected"
			}
			return nil, errors.New(reason)
		}
		return resp.GetCert(), nil
	case <-waitCtx.Done():
		return nil, fmt.Errorf("recv renewal response: %w", waitCtx.Err())
	}
}

func (m *impl) handleInviteRedeem(qc *quic.Conn, peerKey types.PeerKey, req *meshv1.InviteRedeemRequest) (retErr error) {
	defer func() {
		if retErr != nil {
			_ = sendInviteRedeemResponse(qc, nil, retErr)
		}
	}()

	signer := m.inviteSigner
	now := time.Now()
	if err := auth.VerifyInviteToken(req.GetToken(), ed25519.PublicKey(peerKey.Bytes()), now); err != nil {
		return err
	}

	claims := req.GetToken().GetClaims()
	ttl := inviteRedeemTTL
	if remaining := time.Unix(claims.GetExpiresAtUnix(), 0).Sub(now); remaining < ttl {
		ttl = remaining
	}
	if ttl <= 0 {
		return errors.New("invite token expired")
	}

	consumed, err := signer.Consumed.TryConsume(req.GetToken(), now)
	if err != nil {
		return err
	}
	if !consumed {
		return errors.New("invite token already consumed")
	}

	membershipTTL := m.membershipTTL
	var accessDeadline time.Time
	if s := claims.GetMembershipTtlSeconds(); s > 0 {
		accessDeadline = now.Add(time.Duration(s) * time.Second)
	}

	joinToken, err := auth.IssueJoinTokenWithIssuer(
		signer.Priv,
		signer.Trust,
		signer.Issuer,
		ed25519.PublicKey(peerKey.Bytes()),
		claims.GetBootstrap(),
		now,
		ttl,
		membershipTTL,
		accessDeadline,
	)
	if err != nil {
		return err
	}
	return sendInviteRedeemResponse(qc, joinToken, nil)
}

func sendInviteRedeemResponse(qc *quic.Conn, joinToken *admissionv1.JoinToken, redeemErr error) error {
	resp := &meshv1.InviteRedeemResponse{
		Accepted:  redeemErr == nil,
		JoinToken: joinToken,
	}
	if redeemErr != nil {
		resp.Reason = redeemErr.Error()
	}
	return sendEnvelope(qc, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemResponse{InviteRedeemResponse: resp},
	})
}

func redeemInviteOnConn(
	ctx context.Context,
	qc *quic.Conn,
	token *admissionv1.InviteToken,
	subject ed25519.PublicKey,
) (*admissionv1.JoinToken, error) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	if err := sendEnvelope(qc, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemRequest{
			InviteRedeemRequest: &meshv1.InviteRedeemRequest{
				Token:      token,
				SubjectPub: append([]byte(nil), subject...),
			},
		},
	}); err != nil {
		return nil, err
	}

	for {
		env, err := recvEnvelope(waitCtx, qc)
		if err != nil {
			return nil, err
		}
		resp, ok := env.GetBody().(*meshv1.Envelope_InviteRedeemResponse)
		if !ok {
			continue
		}
		if !resp.InviteRedeemResponse.GetAccepted() {
			if reason := resp.InviteRedeemResponse.GetReason(); reason != "" {
				return nil, errors.New(reason)
			}
			return nil, errors.New("invite token rejected")
		}
		return resp.InviteRedeemResponse.GetJoinToken(), nil
	}
}

func recvEnvelope(ctx context.Context, qc *quic.Conn) (*meshv1.Envelope, error) {
	for {
		payload, err := qc.ReceiveDatagram(ctx)
		if err != nil {
			return nil, err
		}
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}
		return env, nil
	}
}

func sendEnvelope(qc *quic.Conn, env *meshv1.Envelope) error {
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}
	return qc.SendDatagram(b)
}
