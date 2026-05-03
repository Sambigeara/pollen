// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const forwardedInviteTimeout = 10 * time.Second

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

	ch := make(chan *meshv1.InviteRedeemResponse, 1)
	n.inviteWaitersMu.Lock()
	n.inviteWaiters[joinerKey] = ch
	n.inviteWaitersMu.Unlock()
	defer func() {
		n.inviteWaitersMu.Lock()
		if n.inviteWaiters[joinerKey] == ch {
			delete(n.inviteWaiters, joinerKey)
		}
		n.inviteWaitersMu.Unlock()
	}()

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
		if err := n.mesh.SendMembershipDatagram(ctx, admin, data); err != nil {
			lastErr = fmt.Errorf("invite forward to %s: %w", admin.Short(), err)
			continue
		}

		waitCtx, cancel := context.WithTimeout(ctx, forwardedInviteTimeout)
		select {
		case resp := <-ch:
			cancel()
			return resp, nil
		case <-waitCtx.Done():
			cancel()
			lastErr = fmt.Errorf("invite forward to %s: %w", admin.Short(), waitCtx.Err())
		}
	}
	return nil, lastErr
}

// Token issuer first, then any other admin-capable peer. Fallback covers
// the case where the issuer is offline but admin has been delegated elsewhere.
func (n *Supervisor) adminForwardCandidates(issuer types.PeerKey) []types.PeerKey {
	snap := n.store.Snapshot()
	candidates := make([]types.PeerKey, 0, len(snap.Nodes))
	if nv, ok := snap.Nodes[issuer]; ok && nv.AdminCapable {
		candidates = append(candidates, issuer)
	}
	for pk, nv := range snap.Nodes {
		if pk == issuer || !nv.AdminCapable {
			continue
		}
		candidates = append(candidates, pk)
	}
	return candidates
}

func (n *Supervisor) handleForwardedInviteRequest(ctx context.Context, from types.PeerKey, req *meshv1.ForwardedInviteRedeemRequest) {
	signer := n.creds.DelegationKey()
	if signer == nil {
		n.sendForwardedInviteResponse(ctx, from, req.GetJoinerPub(), &meshv1.InviteRedeemResponse{
			Reason: "this node is not an admin",
		})
		return
	}

	joinerKey := types.PeerKeyFromBytes(req.GetJoinerPub())
	resp := transport.ProcessInviteRedeem(signer, n.inviteConsumer, n.membershipTTL, joinerKey, req.GetInner())
	n.sendForwardedInviteResponse(ctx, from, req.GetJoinerPub(), resp)
}

func (n *Supervisor) handleForwardedInviteResponse(_ types.PeerKey, resp *meshv1.ForwardedInviteRedeemResponse) {
	joinerKey := types.PeerKeyFromBytes(resp.GetJoinerPub())

	n.inviteWaitersMu.Lock()
	ch, ok := n.inviteWaiters[joinerKey]
	n.inviteWaitersMu.Unlock()

	if ok {
		select {
		case ch <- resp.GetInner():
		default:
		}
	}
}

func (n *Supervisor) sendForwardedInviteResponse(ctx context.Context, to types.PeerKey, joinerPub []byte, resp *meshv1.InviteRedeemResponse) {
	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_ForwardedInviteResponse{ForwardedInviteResponse: &meshv1.ForwardedInviteRedeemResponse{
			Inner:     resp,
			JoinerPub: joinerPub,
		}},
	}).MarshalVT()
	if err != nil {
		return
	}
	_ = n.mesh.SendMembershipDatagram(ctx, to, data)
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
	t.mesh.SetInviteForwarder(nil)
	t.store.SetAdmin()
}

func (t *capTransitioner) DowngradeToLeaf() {
	t.mesh.SetInviteForwarder(t.fwd)
	t.mesh.SetInviteSigner(nil)
	t.store.ClearAdmin()
}
