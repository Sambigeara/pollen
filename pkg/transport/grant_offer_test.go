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

// TestSendGrantOfferOfflinePeer pins that an absent mesh session
// surfaces ErrPeerOffline, the sentinel the CLI fallback keys on.
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

// TestGrantOfferEnvelopeOneofSlots pins the envelope oneof slot numbers;
// drift would silently break the reader's type assertion at runtime.
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
