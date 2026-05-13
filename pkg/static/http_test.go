// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package static

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func makePub(seed byte) types.PeerKey {
	var k types.PeerKey
	for i := range len(k) { //nolint:gosec
		k[i] = seed
	}
	return k
}

func TestLookupSpec_NoDomain_ExactNameMatch(t *testing.T) {
	pub := makePub(0xaa)
	snap := state.Snapshot{
		StaticSpecs: map[string]state.StaticSpecView{
			"mysite": {Spec: state.StaticSpec{Name: "mysite"}, Publisher: pub},
		},
	}
	svc := &Service{log: zap.NewNop().Sugar()}
	got, ok := svc.lookupSpec(snap, "mysite")
	require.True(t, ok)
	require.Equal(t, "mysite", got.Spec.Name)
}

func TestLookupSpec_WithDomain_DisambiguatesByPubPrefix(t *testing.T) {
	alice := makePub(0x12)
	bob := makePub(0xff)
	snap := state.Snapshot{
		StaticSpecs: map[string]state.StaticSpecView{
			"mysite": {Spec: state.StaticSpec{Name: "mysite"}, Publisher: bob},
		},
		StaticSpecsAll: []state.StaticSpecView{
			{Spec: state.StaticSpec{Name: "mysite", ManifestDigest: "alice"}, Publisher: alice},
			{Spec: state.StaticSpec{Name: "mysite", ManifestDigest: "bob"}, Publisher: bob},
		},
	}
	svc := &Service{log: zap.NewNop().Sugar()}
	svc.SetDomain("pln.sh")

	got, ok := svc.lookupSpec(snap, "mysite-12121212.pln.sh")
	require.True(t, ok)
	require.Equal(t, alice, got.Publisher, "alice's mysite must resolve via her pub prefix")
	require.Equal(t, "alice", got.Spec.ManifestDigest)

	got, ok = svc.lookupSpec(snap, "mysite-ffffffff.pln.sh")
	require.True(t, ok)
	require.Equal(t, bob, got.Publisher)
	require.Equal(t, "bob", got.Spec.ManifestDigest)
}

func TestLookupSpec_WithDomain_ReservedTopLevelFallsThrough(t *testing.T) {
	pub := makePub(0xaa)
	snap := state.Snapshot{
		StaticSpecsAll: []state.StaticSpecView{
			{Spec: state.StaticSpec{Name: "app"}, Publisher: pub},
		},
	}
	svc := &Service{log: zap.NewNop().Sugar()}
	svc.SetDomain("pln.sh")

	for _, host := range []string{"app.pln.sh", "blob.pln.sh", "fn.pln.sh"} {
		_, ok := svc.lookupSpec(snap, host)
		require.False(t, ok, "bare top-level subdomain %q must not match a tenant spec", host)
	}
}

func TestLookupSpec_WithDomain_RejectsMalformedPubSegment(t *testing.T) {
	svc := &Service{log: zap.NewNop().Sugar()}
	svc.SetDomain("pln.sh")
	snap := state.Snapshot{}

	for _, host := range []string{
		"mysite-tooshort.pln.sh",
		"mysite-uvwxyzab.pln.sh",  // non-hex
		"mysite-1234567.pln.sh",   // 7 chars
		"mysite-123456789.pln.sh", // 9 chars
		"mysite.example.com",      // wrong suffix
	} {
		_, ok := svc.lookupSpec(snap, host)
		require.False(t, ok, "%q must not match", host)
	}
}

func TestServeHTTP_404OnUnknownHost(t *testing.T) {
	svc := &Service{log: zap.NewNop().Sugar(), store: &fakeStore{}}
	svc.SetDomain("pln.sh")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "blob.pln.sh"
	rec := httptest.NewRecorder()
	svc.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
