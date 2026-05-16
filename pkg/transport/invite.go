// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"time"

	"github.com/quic-go/quic-go"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

const maxInviteEnvelopeSize = 64 * 1024

func RedeemInvite(ctx context.Context, signPriv ed25519.PrivateKey, ticket *identityv1.InviteTicket) (*identityv1.GrantToken, error) {
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

	return redeemInviteWithDial(ctx, ticket, subjectPub, func(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*quic.Conn, error) {
		return qt.Dial(ctx, addr, newInviteDialerTLSConfig(bareCert, expectedPeer), quicConfig())
	})
}

func redeemInviteWithDial(
	ctx context.Context,
	ticket *identityv1.InviteTicket,
	subjectPub ed25519.PublicKey,
	dial func(context.Context, *net.UDPAddr, types.PeerKey) (*quic.Conn, error),
) (*identityv1.GrantToken, error) {
	if _, err := identity.VerifyInviteTicket(ticket, subjectPub, time.Now()); err != nil {
		return nil, err
	}

	var lastErr error
	for _, bootstrap := range ticket.GetClaims().GetBootstrap() {
		expectedPeer := types.PeerKeyFromBytes(bootstrap.GetPeerPub())
		addrs := make([]*net.UDPAddr, 0, len(bootstrap.GetAddrs()))
		for _, rawAddr := range bootstrap.GetAddrs() {
			addr, err := net.ResolveUDPAddr("udp", rawAddr)
			if err != nil {
				continue
			}
			addrs = append(addrs, addr)
		}
		if len(addrs) == 0 {
			continue
		}

		qc, err := raceInviteDials(ctx, addrs, expectedPeer, dial)
		if err != nil {
			lastErr = err
			continue
		}

		// Invite tickets are one-shot — racing redemptions would burn the ticket.
		grantToken, redeemErr := redeemInviteOnConn(ctx, qc, ticket, subjectPub)
		_ = qc.CloseWithError(0, "invite redeemed")
		if redeemErr != nil {
			lastErr = redeemErr
			continue
		}

		if _, verifyErr := identity.VerifyGrantToken(grantToken, subjectPub, time.Now()); verifyErr != nil {
			lastErr = verifyErr
			continue
		}

		return grantToken, nil
	}

	return nil, fmt.Errorf("failed to redeem invite ticket: %w", lastErr)
}

func raceInviteDials(
	ctx context.Context,
	addrs []*net.UDPAddr,
	expectedPeer types.PeerKey,
	dial func(context.Context, *net.UDPAddr, types.PeerKey) (*quic.Conn, error),
) (*quic.Conn, error) {
	dialCtx, cancelDial := context.WithCancel(ctx)
	defer cancelDial()

	type result struct {
		qc  *quic.Conn
		err error
	}
	ch := make(chan result, len(addrs))

	for _, addr := range addrs {
		go func(a *net.UDPAddr) {
			qc, err := dial(dialCtx, a, expectedPeer)
			ch <- result{qc: qc, err: err}
		}(addr)
	}

	var winner *quic.Conn
	var lastErr error
	for range addrs {
		r := <-ch
		switch {
		case r.err != nil:
			if lastErr == nil {
				lastErr = r.err
			}
		case winner == nil:
			winner = r.qc
			cancelDial()
		default:
			_ = r.qc.CloseWithError(0, "invite race lost")
		}
	}

	if winner == nil {
		return nil, lastErr
	}
	return winner, nil
}

// JoinWithGrantToken dials the bootstrap peers carried by a redeemed
// grant token to establish the joiner's first mesh connection. The
// caller must have enrolled the grant (persisted credentials) before
// the transport's mesh certificate can authenticate to these peers.
func (m *QUICTransport) JoinWithGrantToken(ctx context.Context, token *identityv1.GrantToken) error {
	bootstraps := token.GetClaims().GetBootstrap()
	if len(bootstraps) == 0 {
		return fmt.Errorf("grant token contains no bootstrap peers")
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
		return fmt.Errorf("failed to join via grant token bootstrap peers: %w", lastErr)
	}

	return fmt.Errorf("failed to join via grant token bootstrap peers")
}

func (m *QUICTransport) JoinWithInvite(ctx context.Context, ticket *identityv1.InviteTicket) (*identityv1.GrantToken, error) {
	return redeemInviteWithDial(ctx, ticket, ed25519.PublicKey(m.localKey.Bytes()), func(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*quic.Conn, error) {
		return m.mainQT.Dial(ctx, addr, newInviteDialerTLSConfig(m.bareCert, expectedPeer), quicConfig())
	})
}

func (m *QUICTransport) handleInviteConnection(ctx context.Context, qc *quic.Conn, peerKey types.PeerKey) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	stream, err := qc.AcceptStream(waitCtx)
	if err != nil {
		_ = qc.CloseWithError(0, "accept invite stream failed")
		return
	}
	defer stream.Close()

	_ = stream.SetReadDeadline(time.Now().Add(handshakeTimeout))
	first, err := readStreamEnvelope(stream)
	_ = stream.SetReadDeadline(time.Time{})
	if err != nil {
		_ = qc.CloseWithError(0, "read invite stream failed")
		return
	}

	body, ok := first.GetBody().(*meshv1.Envelope_InviteRedeemRequest)
	if !ok {
		_ = qc.CloseWithError(0, "unexpected message on invite connection")
		return
	}

	if err := m.handleInviteRedeem(ctx, stream, peerKey, body.InviteRedeemRequest); err != nil {
		m.log.Debugw("rejected invite", "peer", peerKey.Short(), "err", err)
		_ = qc.CloseWithError(0, "invite failed: "+err.Error())
	}
}

func ProcessInviteRedeem(
	issuer *identity.Credentials,
	consumer identity.InviteConsumer,
	peerKey types.PeerKey,
	req *meshv1.InviteRedeemRequest,
) *meshv1.InviteRedeemResponse {
	now := time.Now()
	ticket := req.GetTicket()
	claims, err := identity.VerifyInviteTicket(ticket, ed25519.PublicKey(peerKey.Bytes()), now)
	if err != nil {
		return &meshv1.InviteRedeemResponse{Reason: err.Error()}
	}

	if issuer == nil || !bytes.Equal(issuer.SubjectPub(), claims.GetIssuerPub()) {
		return &meshv1.InviteRedeemResponse{Reason: "invite ticket issuer is not local issuer"}
	}

	ttl := inviteRedeemTTL
	if remaining := time.Unix(claims.GetExpiresAtUnix(), 0).Sub(now); remaining < ttl {
		ttl = remaining
	}
	if ttl <= 0 {
		return &meshv1.InviteRedeemResponse{Reason: "invite ticket expired"}
	}

	consumed, err := consumer.TryConsume(ticket, now)
	if err != nil {
		return &meshv1.InviteRedeemResponse{Reason: err.Error()}
	}
	if !consumed {
		return &meshv1.InviteRedeemResponse{Reason: "invite ticket already consumed"}
	}

	grantToken, err := issuer.RedeemInvite(ticket, ed25519.PublicKey(peerKey.Bytes()), now, ttl)
	if err != nil {
		return &meshv1.InviteRedeemResponse{Reason: err.Error()}
	}
	return &meshv1.InviteRedeemResponse{Accepted: true, GrantToken: grantToken}
}

type InviteForwarder func(ctx context.Context, peerKey types.PeerKey, req *meshv1.InviteRedeemRequest) (*meshv1.InviteRedeemResponse, error)

func (m *QUICTransport) handleInviteRedeem(ctx context.Context, stream *quic.Stream, peerKey types.PeerKey, req *meshv1.InviteRedeemRequest) error {
	now := time.Now()
	if _, err := identity.VerifyInviteTicket(req.GetTicket(), ed25519.PublicKey(peerKey.Bytes()), now); err != nil {
		return err
	}

	m.inviteHandlerMu.RLock()
	issuer := m.inviteCreds
	consumer := m.inviteConsumer
	forwarder := m.inviteForwarder
	m.inviteHandlerMu.RUnlock()

	issuerPub := req.GetTicket().GetClaims().GetIssuerPub()

	var resp *meshv1.InviteRedeemResponse
	switch {
	case issuer != nil && bytes.Equal(issuer.SubjectPub(), issuerPub):
		resp = ProcessInviteRedeem(issuer, consumer, peerKey, req)
	case forwarder != nil:
		var err error
		resp, err = forwarder(ctx, peerKey, req)
		if err != nil {
			return fmt.Errorf("invite forwarding failed: %w", err)
		}
	case issuer != nil:
		resp = ProcessInviteRedeem(issuer, consumer, peerKey, req)
	default:
		return errors.New("this node is not an issuer and has no forwarding configured")
	}

	if !resp.GetAccepted() {
		reason := resp.GetReason()
		if reason == "" {
			reason = "invite ticket rejected"
		}
		return errors.New(reason)
	}
	return sendInviteRedeemResponse(stream, resp.GetGrantToken())
}

func sendInviteRedeemResponse(stream *quic.Stream, grantToken *identityv1.GrantToken) error {
	return writeStreamEnvelope(stream, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemResponse{InviteRedeemResponse: &meshv1.InviteRedeemResponse{
			Accepted:   true,
			GrantToken: grantToken,
		}},
	})
}

func redeemInviteOnConn(
	ctx context.Context,
	qc *quic.Conn,
	ticket *identityv1.InviteTicket,
	subject ed25519.PublicKey,
) (*identityv1.GrantToken, error) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	stream, err := qc.OpenStreamSync(waitCtx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	if err := writeStreamEnvelope(stream, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemRequest{
			InviteRedeemRequest: &meshv1.InviteRedeemRequest{
				Ticket:  ticket,
				PeerPub: subject,
			},
		},
	}); err != nil {
		return nil, err
	}

	_ = stream.SetReadDeadline(time.Now().Add(handshakeTimeout))
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	for {
		env, err := readStreamEnvelope(stream)
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
			return nil, errors.New("invite ticket rejected")
		}
		return resp.InviteRedeemResponse.GetGrantToken(), nil
	}
}

func readStreamEnvelope(stream *quic.Stream) (*meshv1.Envelope, error) {
	b, err := io.ReadAll(io.LimitReader(stream, maxInviteEnvelopeSize+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxInviteEnvelopeSize {
		return nil, errors.New("invite envelope exceeded size limit")
	}
	env := &meshv1.Envelope{}
	if err := env.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return env, nil
}

func writeStreamEnvelope(stream *quic.Stream, env *meshv1.Envelope) error {
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}
	if _, err := stream.Write(b); err != nil {
		return err
	}
	return stream.Close()
}
