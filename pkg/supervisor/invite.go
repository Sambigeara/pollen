// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	forwardedInviteTimeout        = 10 * time.Second
	maxForwardedInviteMessageSize = 64 * 1024 // TODO(saml) arbitrary, probably not required?
)

func (n *Supervisor) forwardInviteToAdmin(ctx context.Context, joinerKey types.PeerKey, req *meshv1.InviteRedeemRequest) (*meshv1.InviteRedeemResponse, error) {
	issuerPub := req.GetToken().GetClaims().GetIssuer().GetClaims().GetSubjectPub()
	if len(issuerPub) != ed25519.PublicKeySize {
		return nil, errors.New("invite token missing issuer")
	}
	issuerKey := types.PeerKeyFromBytes(issuerPub)

	candidates := n.adminForwardCandidates(issuerKey)
	if len(candidates) == 0 {
		return nil, errors.New("no admin peers available to redeem invite")
	}

	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_ForwardedInviteRequest{ForwardedInviteRequest: &meshv1.ForwardedInviteRedeemRequest{
			Inner:     req,
			JoinerPub: joinerKey.Bytes(),
		}},
	}).MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshal forwarded invite: %w", err)
	}

	var lastErr error
	for _, admin := range candidates {
		waitCtx, cancel := context.WithTimeout(ctx, forwardedInviteTimeout)
		resp, err := n.forwardInviteRequest(waitCtx, admin, joinerKey, data)
		cancel()
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (n *Supervisor) forwardInviteRequest(ctx context.Context, admin, joinerKey types.PeerKey, data []byte) (*meshv1.InviteRedeemResponse, error) {
	stream, err := n.mesh.OpenStream(ctx, admin, transport.StreamTypeMembership)
	if err != nil {
		return nil, fmt.Errorf("invite forward to %s: %w", admin.Short(), err)
	}
	defer stream.Close()

	if _, err := stream.Write(data); err != nil {
		return nil, fmt.Errorf("write invite forward to %s: %w", admin.Short(), err)
	}
	if err := stream.CloseWrite(); err != nil {
		return nil, fmt.Errorf("finish invite forward to %s: %w", admin.Short(), err)
	}

	_ = stream.SetReadDeadline(time.Now().Add(forwardedInviteTimeout))
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	respData, err := io.ReadAll(io.LimitReader(stream, maxForwardedInviteMessageSize+1))
	if err != nil {
		return nil, fmt.Errorf("read invite forward from %s: %w", admin.Short(), err)
	}
	if len(respData) > maxForwardedInviteMessageSize {
		return nil, fmt.Errorf("invite forward from %s exceeded size limit", admin.Short())
	}

	var env meshv1.Envelope
	if err := env.UnmarshalVT(respData); err != nil {
		return nil, fmt.Errorf("unmarshal invite forward from %s: %w", admin.Short(), err)
	}
	body, ok := env.GetBody().(*meshv1.Envelope_ForwardedInviteResponse)
	if !ok {
		return nil, fmt.Errorf("invite forward from %s returned unexpected response", admin.Short())
	}
	if got := types.PeerKeyFromBytes(body.ForwardedInviteResponse.GetJoinerPub()); got != joinerKey {
		return nil, fmt.Errorf("invite forward from %s returned response for wrong joiner", admin.Short())
	}
	return body.ForwardedInviteResponse.GetInner(), nil
}

// Invite tokens are redeemed by their issuer so the resulting join cert stays
// in the issuer's delegation subtree.
func (n *Supervisor) adminForwardCandidates(issuer types.PeerKey) []types.PeerKey {
	snap := n.store.Snapshot()
	if nv, ok := snap.Nodes[issuer]; ok && nv.AdminCapable {
		return []types.PeerKey{issuer}
	}
	return nil
}

func (n *Supervisor) processForwardedInviteRequest(req *meshv1.ForwardedInviteRedeemRequest) *meshv1.ForwardedInviteRedeemResponse {
	signer := n.creds.DelegationKey()
	if signer == nil {
		return &meshv1.ForwardedInviteRedeemResponse{JoinerPub: req.GetJoinerPub(), Inner: &meshv1.InviteRedeemResponse{
			Reason: "this node is not an admin",
		}}
	}

	joinerKey := types.PeerKeyFromBytes(req.GetJoinerPub())
	resp := transport.ProcessInviteRedeem(signer, n.inviteConsumer, n.membershipTTL, joinerKey, req.GetInner())
	return &meshv1.ForwardedInviteRedeemResponse{JoinerPub: req.GetJoinerPub(), Inner: resp}
}

func (n *Supervisor) handleMembershipStream(_ context.Context, stream transport.Stream, from types.PeerKey) {
	defer stream.Close()

	_ = stream.SetReadDeadline(time.Now().Add(forwardedInviteTimeout))
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	data, err := io.ReadAll(io.LimitReader(stream, maxForwardedInviteMessageSize+1))
	if err != nil {
		return
	}
	if len(data) > maxForwardedInviteMessageSize {
		return
	}

	var env meshv1.Envelope
	if err := env.UnmarshalVT(data); err != nil {
		n.log.Debugw("unmarshal membership stream failed", "peer", from.Short(), "err", err)
		return
	}
	body, ok := env.GetBody().(*meshv1.Envelope_ForwardedInviteRequest)
	if !ok {
		n.log.Debugw("unknown membership stream envelope", "peer", from.Short())
		return
	}

	resp := n.processForwardedInviteRequest(body.ForwardedInviteRequest)
	data, err = (&meshv1.Envelope{Body: &meshv1.Envelope_ForwardedInviteResponse{ForwardedInviteResponse: resp}}).MarshalVT()
	if err != nil {
		return
	}
	_, _ = stream.Write(data)
}

type capTransitioner struct {
	mesh               transport.Transport
	store              state.StateStore
	fwd                transport.InviteForwarder
	supervisorConsumer *auth.InviteConsumer // points to Supervisor.inviteConsumer; set once at construction, value immutable after startup
}

func (t *capTransitioner) UpgradeToAdmin(signer *auth.DelegationSigner) {
	t.mesh.SetInviteConsumer(*t.supervisorConsumer)
	t.mesh.SetInviteSigner(signer)
	t.mesh.SetInviteForwarder(t.fwd)
	t.store.SetAdmin()
}

func (t *capTransitioner) DowngradeToLeaf() {
	t.mesh.SetInviteForwarder(t.fwd)
	t.mesh.SetInviteSigner(nil)
	t.store.ClearAdmin()
}
