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

	snap := n.store.Snapshot()
	nv, ok := snap.Nodes[issuerKey]
	if !ok || !nv.AdminCapable {
		return nil, fmt.Errorf("issuing admin %s not available", issuerKey.Short())
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

	if err := n.mesh.SendMembershipDatagram(ctx, issuerKey, data); err != nil {
		return nil, fmt.Errorf("invite forward to %s: %w", issuerKey.Short(), err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, forwardedInviteTimeout)
	defer cancel()

	select {
	case resp := <-ch:
		return resp, nil
	case <-waitCtx.Done():
		return nil, fmt.Errorf("invite forward to %s: %w", issuerKey.Short(), waitCtx.Err())
	}
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
