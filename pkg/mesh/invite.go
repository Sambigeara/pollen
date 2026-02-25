package mesh

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/types"
)

func RedeemInvite(ctx context.Context, signPriv ed25519.PrivateKey, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	bareCert, err := generateIdentityCert(signPriv, nil, config.CertTTLs{}.TLSIdentityTTL())
	if err != nil {
		return nil, err
	}

	subjectPub, ok := signPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("identity private key is not ed25519")
	}

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
	verified, err := auth.VerifyInviteToken(token, subjectPub, time.Now())
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, bootstrap := range verified.Claims.GetBootstrap() {
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
