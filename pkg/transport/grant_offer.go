// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"errors"
	"fmt"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// ErrPeerOffline reports that the local node has no live mesh session
// to the target peer. The upgrade flow surfaces this so the operator
// can either retry once the peer is back, or hand off to a
// subject-pinned invite token for wire-mode peers that never run a
// daemon.
var ErrPeerOffline = errors.New("peer is not reachable over the mesh")

// SendGrantOffer pushes a freshly-minted grant to peerKey over the
// existing mesh peer connection and blocks for the recipient's
// acknowledgement. The recipient adds its own subject PoP before
// gossiping the grant, so the gossip CRDT invariant (subject-PoP on
// Principal updates) holds end-to-end: PoP is never added by the issuer.
//
// Direct-only: callers receive ErrPeerOffline when the issuer holds no
// live session to peerKey, even when a routed path exists. The brief
// reserves routed delivery for tunnel/blob/workload/membership streams;
// upgrade grants are short, infrequent, and the offline path already
// has a clean operator fallback, so the extra route-allow-list surface
// is not worth the added stream type.
func (m *QUICTransport) SendGrantOffer(ctx context.Context, peerKey types.PeerKey, grant *identityv1.Grant) (*meshv1.GrantOfferResponse, error) {
	if _, ok := m.getSession(peerKey); !ok {
		return nil, ErrPeerOffline
	}

	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	stream, err := m.OpenStream(waitCtx, peerKey, StreamTypeGrantOffer)
	if err != nil {
		if errors.Is(err, errUnreachable) {
			return nil, ErrPeerOffline
		}
		return nil, fmt.Errorf("open grant-offer stream: %w", err)
	}
	defer stream.Close() //nolint:errcheck

	if err := writeStreamEnvelope(stream.Stream, &meshv1.Envelope{
		Body: &meshv1.Envelope_GrantOfferRequest{
			GrantOfferRequest: &meshv1.GrantOfferRequest{Grant: grant},
		},
	}); err != nil {
		return nil, fmt.Errorf("send grant offer: %w", err)
	}

	_ = stream.SetReadDeadline(time.Now().Add(handshakeTimeout))
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	env, err := readStreamEnvelope(stream.Stream)
	if err != nil {
		return nil, fmt.Errorf("read grant-offer response: %w", err)
	}
	resp, ok := env.GetBody().(*meshv1.Envelope_GrantOfferResponse)
	if !ok {
		return nil, errors.New("unexpected message on grant-offer stream")
	}
	return resp.GrantOfferResponse, nil
}

// ReadGrantOfferRequest decodes the GrantOfferRequest envelope from a
// stream dispatched to the grant-offer handler. The stream-type byte
// has already been consumed by acceptBidiStreams.
func ReadGrantOfferRequest(stream Stream) (*meshv1.GrantOfferRequest, error) {
	_ = stream.SetReadDeadline(time.Now().Add(handshakeTimeout))
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	env, err := readStreamEnvelope(stream.Stream)
	if err != nil {
		return nil, err
	}
	req, ok := env.GetBody().(*meshv1.Envelope_GrantOfferRequest)
	if !ok {
		return nil, errors.New("unexpected message on grant-offer stream")
	}
	return req.GrantOfferRequest, nil
}

// WriteGrantOfferResponse acks a grant-offer back to the issuer over
// the same stream and closes the writer side.
func WriteGrantOfferResponse(stream Stream, resp *meshv1.GrantOfferResponse) error {
	return writeStreamEnvelope(stream.Stream, &meshv1.Envelope{
		Body: &meshv1.Envelope_GrantOfferResponse{GrantOfferResponse: resp},
	})
}
