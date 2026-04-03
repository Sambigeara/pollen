package transport

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/types"
)

func RedeemInvite(ctx context.Context, signPriv ed25519.PrivateKey, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	bareCert, err := GenerateIdentityCert(signPriv, nil, config.DefaultTLSIdentityTTL)
	if err != nil {
		return nil, err
	}

	subjectPub := signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	qt := &quic.Transport{Conn: conn}
	defer func() {
		_ = qt.Close()
		_ = conn.Close()
	}()

	return redeemInviteWithDial(ctx, token, subjectPub, func(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*quic.Conn, error) {
		return qt.Dial(ctx, addr, newInviteDialerTLSConfig(bareCert, expectedPeer), quicConfig())
	})
}

func redeemInviteWithDial(
	ctx context.Context,
	token *admissionv1.InviteToken,
	subjectPub ed25519.PublicKey,
	dial func(context.Context, *net.UDPAddr, types.PeerKey) (*quic.Conn, error),
) (*admissionv1.JoinToken, error) {
	if err := auth.VerifyInviteToken(token, subjectPub, time.Now()); err != nil {
		return nil, err
	}

	var lastErr error
	for _, bootstrap := range token.GetClaims().GetBootstrap() {
		expectedPeer := types.PeerKeyFromBytes(bootstrap.GetPeerPub())
		for _, rawAddr := range bootstrap.GetAddrs() {
			addr, err := net.ResolveUDPAddr("udp", rawAddr)
			if err != nil {
				continue
			}

			qc, err := dial(ctx, addr, expectedPeer)
			if err != nil {
				lastErr = err
				continue
			}

			joinToken, redeemErr := redeemInviteOnConn(ctx, qc, token, subjectPub)
			_ = qc.CloseWithError(0, "invite redeemed")
			if redeemErr != nil {
				lastErr = redeemErr
				continue
			}

			if _, verifyErr := auth.VerifyJoinToken(joinToken, subjectPub, time.Now()); verifyErr != nil {
				lastErr = verifyErr
				continue
			}

			return joinToken, nil
		}
	}

	return nil, fmt.Errorf("failed to redeem invite token: %w", lastErr)
}

func (m *QUICTransport) JoinWithToken(ctx context.Context, token *admissionv1.JoinToken) error {
	claims := token.GetClaims()
	if claims == nil {
		return fmt.Errorf("join token missing claims")
	}

	bootstraps := claims.GetBootstrap()
	if len(bootstraps) == 0 {
		return fmt.Errorf("join token contains no bootstrap peers")
	}

	var lastErr error
	for _, bootstrap := range bootstraps {
		peerKey := types.PeerKeyFromBytes(bootstrap.GetPeerPub())
		resolved := make([]netip.AddrPort, 0, len(bootstrap.GetAddrs()))
		for _, addr := range bootstrap.GetAddrs() {
			ap, err := netip.ParseAddrPort(addr)
			if err != nil {
				continue
			}
			resolved = append(resolved, ap)
		}
		if len(resolved) == 0 {
			continue
		}

		winner, err := m.raceDirectDial(ctx, peerKey, resolved)
		if err != nil {
			lastErr = err
			continue
		}

		m.addPeer(ctx, winner, peerKey)
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("failed to join via token bootstrap peers: %w", lastErr)
	}

	return fmt.Errorf("failed to join via token bootstrap peers")
}

func (m *QUICTransport) JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	joinToken, err := redeemInviteWithDial(ctx, token, ed25519.PublicKey(m.localKey.Bytes()), func(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*quic.Conn, error) {
		return m.mainQT.Dial(ctx, addr, newInviteDialerTLSConfig(m.bareCert, expectedPeer), quicConfig())
	})
	if err != nil {
		return nil, err
	}

	if err := m.JoinWithToken(ctx, joinToken); err != nil {
		return nil, err
	}

	return joinToken, nil
}

func (m *QUICTransport) handleInviteConnection(ctx context.Context, qc *quic.Conn, peerKey types.PeerKey) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	first, err := recvEnvelope(waitCtx, qc)
	if err != nil {
		_ = qc.CloseWithError(0, "recv failed")
		return
	}

	body, ok := first.GetBody().(*meshv1.Envelope_InviteRedeemRequest)
	if !ok {
		_ = qc.CloseWithError(0, "unexpected message on invite connection")
		return
	}

	if err := m.handleInviteRedeem(qc, peerKey, body.InviteRedeemRequest); err != nil {
		m.log.Debugw("rejected invite", "peer", peerKey.Short(), "err", err)
		_ = qc.CloseWithError(0, "invite failed")
	}
}

func (m *QUICTransport) handleInviteRedeem(qc *quic.Conn, peerKey types.PeerKey, req *meshv1.InviteRedeemRequest) (retErr error) {
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

	consumed, err := m.inviteConsumer.TryConsume(req.GetToken(), now)
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

	joinToken, err := signer.IssueJoinToken(
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
				Token:   token,
				PeerPub: append([]byte(nil), subject...),
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

func (m *QUICTransport) RequestCertRenewal(ctx context.Context, peerKey types.PeerKey) (*admissionv1.DelegationCert, error) {
	s, ok := m.getSession(peerKey)
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
				PeerPub:     m.localKey.Bytes(),
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
