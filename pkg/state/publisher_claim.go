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

// verifyPublisherClaim runs at gossip ingest while s.mu is held — signature
// only, no evaluator call. Attribute-based publish policy runs at the
// spec_publish RPC gate instead.
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
