// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

const inviteExpirySkew = time.Minute

// IssueInviteTicket mints an admin-signed redemption ticket. The
// redeeming host turns it into a grant bounded by the ticket's
// capabilities, budget and horizon. subjectPub, when set, pins the
// ticket to one joiner.
func IssueInviteTicket(
	issuerPriv ed25519.PrivateKey,
	bootstrap []*admissionv1.BootstrapPeer,
	subjectPub ed25519.PublicKey,
	caps *identityv1.Capabilities,
	budget *identityv1.Budget,
	grantDeadline time.Time,
	now time.Time,
	ttl time.Duration,
) (*identityv1.InviteTicket, error) {
	if ttl <= 0 {
		return nil, errors.New("invite ticket ttl must be positive")
	}
	if caps == nil {
		return nil, errors.New("capabilities required")
	}
	if budget == nil {
		return nil, errors.New("budget required")
	}
	if err := ValidateAttributes(caps.GetAttributes()); err != nil {
		return nil, err
	}

	issuerPub := issuerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	claims := &identityv1.InviteTicketClaims{
		TicketId:          uuid.NewString(),
		IssuerPub:         issuerPub,
		Bootstrap:         bootstrap,
		SubjectPub:        subjectPub,
		IssuedAtUnix:      now.Unix(),
		ExpiresAtUnix:     now.Add(ttl).Unix(),
		Capabilities:      caps,
		Budget:            budget,
		GrantDeadlineUnix: grantDeadlineUnix(grantDeadline),
	}
	if err := protovalidate.Validate(claims); err != nil {
		return nil, fmt.Errorf("invite ticket claims invalid: %w", err)
	}
	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	sig, err := SignPayload(issuerPriv, msg, sigContextInviteTicket)
	if err != nil {
		return nil, err
	}
	return &identityv1.InviteTicket{Claims: claims, Signature: sig}, nil
}

func grantDeadlineUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// VerifyInviteTicket authenticates a redemption ticket: signature by
// the named issuer, current window, and (if pinned) the expected
// subject. It does not chain the issuer to root; the redeeming host
// proves issuer authority by holding the matching delegating grant.
func VerifyInviteTicket(ticket *identityv1.InviteTicket, expectedSubject ed25519.PublicKey, now time.Time) (*identityv1.InviteTicketClaims, error) {
	if err := protovalidate.Validate(ticket); err != nil {
		return nil, fmt.Errorf("invite ticket invalid: %w", err)
	}
	claims := ticket.GetClaims()

	msg, err := SignaturePayload(claims)
	if err != nil {
		return nil, err
	}
	if err := VerifyPayload(ed25519.PublicKey(claims.GetIssuerPub()), msg, ticket.GetSignature(), sigContextInviteTicket); err != nil {
		return nil, errors.New("invite ticket signature invalid")
	}

	if len(expectedSubject) > 0 && len(claims.GetSubjectPub()) > 0 && !bytes.Equal(claims.GetSubjectPub(), expectedSubject) {
		return nil, errors.New("invite ticket subject mismatch")
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-TimeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(TimeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return nil, errors.New("invite ticket validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return nil, errors.New("invite ticket expired or not yet valid")
	}

	return claims, nil
}

// RedeemInviteTicket is the host side of a join: verify the ticket,
// mint a child grant for joinerPub bounded by the ticket's
// capabilities, budget and horizon, and wrap it with the cluster
// bootstrap into a short-lived GrantToken the joiner enrols. The
// redeeming host must be the ticket's named issuer and hold the
// matching delegating grant chain.
func RedeemInviteTicket(
	issuerPriv ed25519.PrivateKey,
	parentChain []*identityv1.Grant,
	rootPub ed25519.PublicKey,
	ticket *identityv1.InviteTicket,
	joinerPub ed25519.PublicKey,
	now time.Time,
	tokenTTL time.Duration,
) (*identityv1.GrantToken, error) {
	claims, err := VerifyInviteTicket(ticket, joinerPub, now)
	if err != nil {
		return nil, err
	}

	issuerPub := issuerPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if !bytes.Equal(claims.GetIssuerPub(), issuerPub) {
		return nil, errors.New("invite ticket issuer is not the redeeming host")
	}

	var grantDeadline time.Time
	if d := claims.GetGrantDeadlineUnix(); d > 0 {
		grantDeadline = time.Unix(d, 0)
	}

	childGrant, err := IssueGrant(issuerPriv, parentChain, joinerPub, claims.GetCapabilities(), claims.GetBudget(), now, grantDeadline)
	if err != nil {
		return nil, fmt.Errorf("mint joiner grant: %w", err)
	}

	return IssueGrantToken(issuerPriv, childGrant, claims.GetBootstrap(), rootPub, now, tokenTTL)
}

func EncodeInviteTicket(ticket *identityv1.InviteTicket) (string, error) {
	b, err := ticket.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeInviteTicket(s string) (*identityv1.InviteTicket, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	ticket := &identityv1.InviteTicket{}
	if err := ticket.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return ticket, nil
}

// InviteConsumer enforces one-time redemption of invite tickets,
// keyed by ticket id, and exports the consumed set for gossip so a
// ticket cannot be replayed against another mesh member.
type InviteConsumer interface {
	TryConsume(ticket *identityv1.InviteTicket, now time.Time) (bool, error)
	Export() []*statev1.ConsumedInvite
}

type inviteConsumer struct {
	consumed map[string]int64
	mu       sync.Mutex
}

func NewInviteConsumer(entries []*statev1.ConsumedInvite) InviteConsumer {
	consumed := make(map[string]int64, len(entries))
	for _, e := range entries {
		if e.GetTokenId() != "" {
			consumed[e.GetTokenId()] = e.GetExpiryUnix()
		}
	}
	return &inviteConsumer{consumed: consumed}
}

func (c *inviteConsumer) TryConsume(ticket *identityv1.InviteTicket, now time.Time) (bool, error) {
	claims := ticket.GetClaims()
	if claims == nil {
		return false, errors.New("invite ticket missing claims")
	}
	ticketID := claims.GetTicketId()
	if ticketID == "" {
		return false, errors.New("invite ticket missing ticket id")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.consumed[ticketID]; exists {
		return false, nil
	}

	cutoff := now.Unix() - int64(inviteExpirySkew/time.Second)
	for id, exp := range c.consumed {
		if exp > 0 && exp < cutoff {
			delete(c.consumed, id)
		}
	}

	c.consumed[ticketID] = claims.GetExpiresAtUnix()
	return true, nil
}

func (c *inviteConsumer) Export() []*statev1.ConsumedInvite {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*statev1.ConsumedInvite, 0, len(c.consumed))
	for id, expiry := range c.consumed {
		out = append(out, &statev1.ConsumedInvite{
			TokenId:    id,
			ExpiryUnix: expiry,
		})
	}
	return out
}
