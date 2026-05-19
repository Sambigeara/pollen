// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/state"
)

// unseedStore embeds WorkloadState and overrides only what Unseed
// touches. Snapshot returns the adverse multi-tenant shape that
// state.TestRevokeOwnSpecsIgnoresDedupeWinner proves the real store
// produces: two authorities publish byte-identical content under the
// same name, so SpecsAll carries both per-(authority, name) entries
// while the deduped Specs map collapses to a single outranks winner
// that is the remote co-publisher, not the local node.
type unseedStore struct {
	WorkloadState
	snap    state.Snapshot
	deleted bool
}

func (u *unseedStore) Snapshot() state.Snapshot             { return u.snap }
func (u *unseedStore) ReleaseWorkload(string) []state.Event { return nil }
func (u *unseedStore) DeleteWorkloadSpec(string) ([]state.Event, error) {
	u.deleted = true
	return nil, nil
}

// TestUnseedIgnoresDedupeWinner is the placement-layer companion to
// state.TestRevokeOwnSpecsIgnoresDedupeWinner: the legitimate owner's
// Unseed must reach DeleteWorkloadSpec even when a remote tenant's
// byte-identical content wins the deduped Specs map. The pre-redesign
// guard arbitrated ownership on snap.Specs[hash] and falsely rejected
// the owner with "owned by peer <other>"; the fix scopes the guard to
// LocalPublishesWorkload over the per-(authority, name) SpecsAll, the
// same predicate UnseedStatic, the blob path, and control.UnseedWorkload
// use.
func TestUnseedIgnoresDedupeWinner(t *testing.T) {
	aKey := peerKey(0xaa) // local owner
	bKey := peerKey(0xbb) // remote co-publisher, the adverse dedupe winner
	hash := strings.Repeat("a", 64)
	echo := state.WorkloadSpec{Hash: hash, Name: "echo"}

	fake := &unseedStore{snap: state.Snapshot{
		SpecsAll: []state.WorkloadSpecView{
			{Spec: echo, Publisher: aKey},
			{Spec: echo, Publisher: bKey},
		},
		Specs: map[string]state.WorkloadSpecView{
			hash: {Spec: echo, Publisher: bKey},
		},
	}}
	svc := &Service{localID: aKey, store: fake, manager: newTestManager(t)}

	require.NoError(t, svc.Unseed("echo"))
	require.True(t, fake.deleted,
		"legitimate owner's unseed must reach DeleteWorkloadSpec despite the remote co-publisher winning the deduped Specs map")
}

// TestUnseedRejectsNonPublisher pins the other direction: the fix must
// not turn the ownership guard into a no-op. A node that neither
// publishes nor runs the workload is still rejected.
func TestUnseedRejectsNonPublisher(t *testing.T) {
	aKey := peerKey(0xaa)
	cKey := peerKey(0xcc) // neither publisher nor runner
	hash := strings.Repeat("a", 64)
	echo := state.WorkloadSpec{Hash: hash, Name: "echo"}

	fake := &unseedStore{snap: state.Snapshot{
		SpecsAll: []state.WorkloadSpecView{{Spec: echo, Publisher: aKey}},
		Specs:    map[string]state.WorkloadSpecView{hash: {Spec: echo, Publisher: aKey}},
	}}
	svc := &Service{localID: cKey, store: fake, manager: newTestManager(t)}

	require.ErrorIs(t, svc.Unseed(hash), ErrNotRunning)
	require.False(t, fake.deleted, "a non-publisher must not reach DeleteWorkloadSpec")
}
