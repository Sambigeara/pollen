// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"fmt"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/state"
)

// AccountCheck enforces the authority's per-Principal count budget. It is
// pure over the snapshot (no I/O, no locks), so it runs safely inside the
// pipeline under the store's mutate-validate lock.
//
// A zero Budget field means no limit for that dimension (the UnlimitedBudget
// contract). Re-admitting a resource the authority already holds consumes no
// slot, so gossip replay of an accepted spec stays admissible. Services carry
// no budget dimension.
func AccountCheck(snap state.Snapshot, f *factv1.Fact, authGrant *identityv1.Grant) error {
	budget := authGrant.GetClaims().GetBudget()
	usage := snap.UsageByAuthority(f.GetAuthorityPub())
	switch r := f.GetResource().GetBody().(type) {
	case *admissionv1.ResourceID_Seed:
		return checkCount("functions", usage.FunctionNames, r.Seed.GetName(), budget.GetMaxFunctions())
	case *admissionv1.ResourceID_Blob:
		return checkCount("blobs", usage.BlobNames, r.Blob.GetName(), budget.GetMaxBlobs())
	case *admissionv1.ResourceID_Static:
		return checkCount("sites", usage.SiteNames, r.Static.GetName(), budget.GetMaxSites())
	case *admissionv1.ResourceID_Service:
		return nil
	}
	return nil
}

func checkCount(kind string, held map[string]struct{}, name string, limit uint32) error {
	if limit == 0 {
		return nil
	}
	if _, ok := held[name]; ok {
		return nil
	}
	if len(held)+1 > int(limit) {
		return fmt.Errorf("%s budget exhausted: authority holds %d, limit %d", kind, len(held), limit)
	}
	return nil
}
