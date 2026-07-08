// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"bytes"
	"testing"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func authorityPub(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func buildSnap(pub []byte, fns, blobs, sites []string) state.Snapshot {
	owner := types.PeerKeyFromBytes(pub)
	var snap state.Snapshot
	for _, n := range fns {
		snap.SpecsAll = append(snap.SpecsAll,
			state.WorkloadSpecView{Spec: state.WorkloadSpec{Name: n}, Publisher: owner})
	}
	for _, n := range blobs {
		snap.BlobSpecsAll = append(snap.BlobSpecsAll,
			state.BlobSpecView{Spec: state.BlobSpec{Name: n}, Publisher: owner})
	}
	for _, n := range sites {
		snap.StaticSpecsAll = append(snap.StaticSpecsAll,
			state.StaticSpecView{Spec: state.StaticSpec{Name: n}, Publisher: owner})
	}
	return snap
}

func grantWithBudget(b *identityv1.Budget) *identityv1.Grant {
	return &identityv1.Grant{Claims: &identityv1.GrantClaims{Budget: b}}
}

func resourceFact(pub []byte, res *admissionv1.ResourceID) *factv1.Fact {
	return &factv1.Fact{AuthorityPub: pub, Resource: res}
}

func seedRes(name string) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: name}}}
}

func blobRes(name string) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: name}}}
}

func staticRes(name string) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{Name: name}}}
}

func serviceRes(name string) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Service{Service: &admissionv1.ServiceID{Name: name}}}
}

func TestAccountCheck(t *testing.T) {
	pub := authorityPub(0xa1)

	tests := []struct {
		name    string
		snap    state.Snapshot
		fact    *factv1.Fact
		budget  *identityv1.Budget
		wantErr bool
	}{
		{
			name:    "function under limit",
			snap:    buildSnap(pub, []string{"a"}, nil, nil),
			fact:    resourceFact(pub, seedRes("b")),
			budget:  &identityv1.Budget{MaxFunctions: 2},
			wantErr: false,
		},
		{
			name:    "function at limit rejected",
			snap:    buildSnap(pub, []string{"a", "b"}, nil, nil),
			fact:    resourceFact(pub, seedRes("c")),
			budget:  &identityv1.Budget{MaxFunctions: 2},
			wantErr: true,
		},
		{
			name:    "function re-admit existing name is idempotent",
			snap:    buildSnap(pub, []string{"a", "b"}, nil, nil),
			fact:    resourceFact(pub, seedRes("a")),
			budget:  &identityv1.Budget{MaxFunctions: 2},
			wantErr: false,
		},
		{
			name:    "zero budget is unlimited",
			snap:    buildSnap(pub, []string{"a", "b", "c"}, nil, nil),
			fact:    resourceFact(pub, seedRes("d")),
			budget:  &identityv1.Budget{},
			wantErr: false,
		},
		{
			name:    "blob at limit rejected",
			snap:    buildSnap(pub, nil, []string{"x"}, nil),
			fact:    resourceFact(pub, blobRes("y")),
			budget:  &identityv1.Budget{MaxBlobs: 1},
			wantErr: true,
		},
		{
			name:    "site at limit rejected via StaticSpecsAll",
			snap:    buildSnap(pub, nil, nil, []string{"s1"}),
			fact:    resourceFact(pub, staticRes("s2")),
			budget:  &identityv1.Budget{MaxSites: 1},
			wantErr: true,
		},
		{
			name:    "site re-admit existing name is idempotent",
			snap:    buildSnap(pub, nil, nil, []string{"s1"}),
			fact:    resourceFact(pub, staticRes("s1")),
			budget:  &identityv1.Budget{MaxSites: 1},
			wantErr: false,
		},
		{
			name:    "service is unbudgeted",
			snap:    buildSnap(pub, nil, nil, nil),
			fact:    resourceFact(pub, serviceRes("svc")),
			budget:  &identityv1.Budget{MaxFunctions: 1, MaxBlobs: 1, MaxSites: 1},
			wantErr: false,
		},
		{
			name:    "other authority's usage does not count against this one",
			snap:    buildSnap(authorityPub(0xb2), []string{"a", "b"}, nil, nil),
			fact:    resourceFact(pub, seedRes("c")),
			budget:  &identityv1.Budget{MaxFunctions: 1},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := AccountCheck(tc.snap, tc.fact, grantWithBudget(tc.budget))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
