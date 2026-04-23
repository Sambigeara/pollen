// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"crypto/ed25519"
	"encoding/hex"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/claims"
	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/types"
)

// verifyPublisherClaim returns an error when a gossip event carries a
// publisher claim whose signature does not verify against the
// originating peer's ed25519 public key.
//
// Events without a claim are accepted unchanged: a nil properties +
// nil signature pair is the "unsigned spec" case and passes through
// claims.Verify untouched. Tampered or forged claims (relay peer
// substitutes properties onto someone else's signed payload) fail.
//
// This is signature-only, the evaluator is never called here. The
// evaluator layer can take time and depends on other subsystems; state
// holds s.mu during ingest and must not block on foreign packages.
// Attribute-based publish policy runs at the RPC boundary
// (spec_publish gate).
func verifyPublisherClaim(pk types.PeerKey, ev *statev1.GossipEvent) error {
	if ev.GetDeleted() {
		return nil
	}
	pub := ed25519.PublicKey(pk.Bytes())
	switch v := ev.GetChange().(type) {
	case *statev1.GossipEvent_BlobSpec:
		c := v.BlobSpec.GetPublisherClaim()
		return claims.Verify(
			pub,
			evaluator.ResourceBlob,
			hex.EncodeToString(v.BlobSpec.GetDigest()),
			c.GetProperties(),
			c.GetSignature(),
		)
	case *statev1.GossipEvent_WorkloadSpec:
		c := v.WorkloadSpec.GetPublisherClaim()
		return claims.Verify(
			pub,
			evaluator.ResourceSeed,
			v.WorkloadSpec.GetHash(),
			c.GetProperties(),
			c.GetSignature(),
		)
	case *statev1.GossipEvent_StaticSpec:
		c := v.StaticSpec.GetPublisherClaim()
		return claims.Verify(
			pub,
			evaluator.ResourceStatic,
			v.StaticSpec.GetName(),
			c.GetProperties(),
			c.GetSignature(),
		)
	}
	return nil
}
