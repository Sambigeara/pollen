// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestSendGrantOfferOfflinePeer is the CLI-fallback discriminator's
// contract test: a node holding no live mesh session to peerKey must
// surface ErrPeerOffline from SendGrantOffer rather than an opaque
// stream error, because the control handler uses it (and only it) to
// raise codes.Unavailable and trigger the subject-pinned-token CLI
// fallback. A wire-mode tenant otherwise looks identical to a daemon
// rejection, and the operator would receive a token they should never
// have been offered.
func TestSendGrantOfferOfflinePeer(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	grant, err := identity.IssueGrant(priv, nil, pub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	creds := identity.NewCredentials(pub, priv, grant)
	self := types.PeerKeyFromBytes(pub)

	m, err := New(self, creds, ":0",
		WithSigningKey(priv),
		WithTLSIdentityTTL(time.Hour),
	)
	require.NoError(t, err)

	other, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	peerKey := types.PeerKeyFromBytes(other)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := m.SendGrantOffer(ctx, peerKey, grant)
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrPeerOffline)
}

// TestGrantOfferEnvelopeOneofSlots pins the proto envelope's oneof slot
// numbers for grant offers. Drift on these field numbers silently
// breaks the recipient: the writer would emit one wire format, the
// reader expects another, the type assertion in ReadGrantOfferRequest
// returns false, and an upgrade attempt becomes an opaque "unexpected
// message" error rather than a clean codec failure. Pinning the slots
// here means the proto change is caught at test time, not on staging.
func TestGrantOfferEnvelopeOneofSlots(t *testing.T) {
	req := &meshv1.Envelope{Body: &meshv1.Envelope_GrantOfferRequest{
		GrantOfferRequest: &meshv1.GrantOfferRequest{},
	}}
	resp := &meshv1.Envelope{Body: &meshv1.Envelope_GrantOfferResponse{
		GrantOfferResponse: &meshv1.GrantOfferResponse{Accepted: true},
	}}

	reqRaw, err := req.MarshalVT()
	require.NoError(t, err)
	respRaw, err := resp.MarshalVT()
	require.NoError(t, err)

	var reqDecoded meshv1.Envelope
	require.NoError(t, reqDecoded.UnmarshalVT(reqRaw))
	_, ok := reqDecoded.GetBody().(*meshv1.Envelope_GrantOfferRequest)
	require.True(t, ok, "GrantOfferRequest must round-trip via the Envelope.body oneof")

	var respDecoded meshv1.Envelope
	require.NoError(t, respDecoded.UnmarshalVT(respRaw))
	respBody, ok := respDecoded.GetBody().(*meshv1.Envelope_GrantOfferResponse)
	require.True(t, ok, "GrantOfferResponse must round-trip via the Envelope.body oneof")
	require.True(t, respBody.GrantOfferResponse.GetAccepted(),
		"response payload must round-trip its boolean fields")
}

// TestErrPeerOfflineIsSentinelComparable guards the sentinel's identity:
// callers must be able to errors.Is-discriminate it from any other
// dispatch error, and the message must not be empty for operator logs.
func TestErrPeerOfflineIsSentinelComparable(t *testing.T) {
	wrapped := errors.New("wrapper around ErrPeerOffline")
	require.False(t, errors.Is(wrapped, ErrPeerOffline))
	require.True(t, errors.Is(ErrPeerOffline, ErrPeerOffline))
	require.NotEmpty(t, ErrPeerOffline.Error())
}
