// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// staticVisible reports whether snap carries a live static spec
// published by publisher under name.
func staticVisible(snap state.Snapshot, publisher types.PeerKey, name string) bool {
	for _, sv := range snap.StaticSpecsAll {
		if sv.Publisher == publisher && sv.Spec.Name == name {
			return true
		}
	}
	return false
}

func staticResource(name string, digest []byte) *admissionv1.ResourceID {
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           name,
		ManifestDigest: digest,
	}}}
}

// defaultRelayKey is the relay peer key every wire-relay test uses
// unless it specifically needs to vary it.
var defaultRelayKey = types.PeerKeyFromBytes([]byte{0x09})

// relayHarness encodes the cluster shape every wire-relay durability
// test needs: a cluster root, a publisher P with a root-issued grant
// and its signing key, and factories that materialise fresh P / R
// stores under that identity. Tests express only what they vary (who
// seeds, who unseeds, whether R restarts), not the setup.
type relayHarness struct {
	rootPub ed25519.PublicKey
	pKey    types.PeerKey
	grantP  *identityv1.Grant
	sigP    []byte
	signer  *fact.Signer
}

func newRelayHarness(t *testing.T) *relayHarness {
	t.Helper()
	rootPub, rootPriv := keyPair(t)
	now := time.Now()
	pPub, pPriv := keyPair(t)
	grantP, err := identity.IssueGrant(rootPriv, nil, pPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)
	sigP, err := identity.SignGrantSubject(grantP, pPriv)
	require.NoError(t, err)
	return &relayHarness{
		rootPub: rootPub,
		pKey:    types.PeerKeyFromBytes(pPub),
		grantP:  grantP,
		sigP:    sigP,
		signer:  fact.NewSigner(pPriv),
	}
}

// publisher returns a fresh daemon-mode P store with signer and grant
// wired, ready to call SetStaticSpec / DeleteStaticSpec.
func (h *relayHarness) publisher(t *testing.T) state.StateStore {
	t.Helper()
	p := validatedStore(t, h.pKey, h.rootPub)
	p.SetLocalSigner(h.signer)
	p.SetLocalGrant(h.grantP, h.sigP)
	return p
}

// relay returns a fresh wire-mode store under self with P's grant
// pre-registered, ready to accept presigned facts.
func (h *relayHarness) relay(t *testing.T, self types.PeerKey) state.StateStore {
	t.Helper()
	r := validatedStore(t, self, h.rootPub)
	r.RegisterPeerGrant(h.pKey, h.grantP, h.sigP)
	return r
}

// bareRelay returns a fresh wire-mode store under self with no grant
// pre-registered, ready to rehydrate via RestoreFromDisk.
func (h *relayHarness) bareRelay(t *testing.T, self types.PeerKey) state.StateStore {
	t.Helper()
	return validatedStore(t, self, h.rootPub)
}

// staticFact mints a presigned static-spec Fact under P's signer.
// deleted=true makes it a tombstone.
func (h *relayHarness) staticFact(t *testing.T, name string, digest []byte, deleted bool) *factv1.Fact {
	t.Helper()
	body := &statev1.StaticSpecChange{Name: name, ManifestDigest: digest}
	f, err := h.signer.IssueFact(staticResource(name, digest), body, nil, deleted)
	require.NoError(t, err)
	return f
}

// TestWireRelayedStaticSurvivesOfflinePublisher mirrors the live staging
// scenario: a tenant publishes a static site over the wire against an
// edge relay while the publisher's own daemon is offline. The spec must
// surface in the relay's snapshot, or the orphan-blob janitor evicts
// the content the relay just pulled and the public URL 404s.
func TestWireRelayedStaticSurvivesOfflinePublisher(t *testing.T) {
	h := newRelayHarness(t)
	digest := bytes.Repeat([]byte{0xbb}, 32)
	spec := state.StaticSpec{Name: "swlock", ManifestDigest: hex.EncodeToString(digest)}

	r := h.relay(t, defaultRelayKey)
	_, err := r.SetStaticSpecPresigned(spec, h.staticFact(t, "swlock", digest, false))
	require.NoError(t, err, "admission accepts the presigned spec")

	snap := r.Snapshot()
	require.False(t, snap.IsDenied(h.pKey))
	require.True(t, staticVisible(snap, h.pKey, "swlock"),
		"wire-relayed static spec must survive with the publisher offline")
}

// TestReseedIdenticalBytesAfterUnseedRepublishes: a publisher unseeds a
// static site and re-seeds the byte-identical content, and the re-seed
// must win. Tombstone and re-seed land in different slots (publisher's
// own carries the unseed; the relay's carries the wire re-seed), so seq
// supersession (not body_hash) lets the higher-seq re-seed revive the
// name.
func TestReseedIdenticalBytesAfterUnseedRepublishes(t *testing.T) {
	h := newRelayHarness(t)
	digest := bytes.Repeat([]byte{0xbb}, 32)
	spec := state.StaticSpec{Name: "swlock", ManifestDigest: hex.EncodeToString(digest)}

	// P seeds (seq 1) then unseeds (seq 2) in its own slot.
	p := h.publisher(t)
	_, err := p.SetStaticSpec(spec, nil)
	require.NoError(t, err)
	_, err = p.DeleteStaticSpec("swlock")
	require.NoError(t, err)

	// R takes P's tombstoned state via gossip.
	r := h.bareRelay(t, defaultRelayKey)
	_, _, err = r.ApplyDelta(p.EncodeFull())
	require.NoError(t, err)
	require.False(t, staticVisible(r.Snapshot(), h.pKey, "swlock"))

	// P re-seeds identical bytes (seq 3) into R's slot; tombstone (seq
	// 2) and re-seed (seq 3) now coexist in different slots.
	_, err = r.SetStaticSpecPresigned(spec, h.staticFact(t, "swlock", digest, false))
	require.NoError(t, err)
	require.True(t, staticVisible(r.Snapshot(), h.pKey, "swlock"),
		"re-seeding identical bytes after an unseed must republish")
}

// TestHigherSeqUnseedSuppressesStaleLiveAcrossSlots: an unseed that
// lands in one slot still suppresses a stale, lower-seq live copy
// carried in another slot, because the tombstone holds the higher seq.
// This is the cross-slot suppression that body_hash matching used to
// provide and seq supersession must keep.
func TestHigherSeqUnseedSuppressesStaleLiveAcrossSlots(t *testing.T) {
	h := newRelayHarness(t)
	digest := bytes.Repeat([]byte{0xcc}, 32)
	spec := state.StaticSpec{Name: "site", ManifestDigest: hex.EncodeToString(digest)}

	// A stale live seed (seq 1) reaches R over the wire.
	r := h.relay(t, defaultRelayKey)
	_, err := r.SetStaticSpecPresigned(spec, h.staticFact(t, "site", digest, false))
	require.NoError(t, err)
	require.True(t, staticVisible(r.Snapshot(), h.pKey, "site"))

	// P unseeds in its own slot at a later seq; R takes the tombstone
	// via gossip.
	p := h.publisher(t)
	_, err = p.SetStaticSpec(spec, nil)
	require.NoError(t, err)
	_, err = p.DeleteStaticSpec("site")
	require.NoError(t, err)
	_, _, err = r.ApplyDelta(p.EncodeFull())
	require.NoError(t, err)

	require.False(t, staticVisible(r.Snapshot(), h.pKey, "site"),
		"a higher-seq unseed in another slot must suppress the stale live copy")
}

// TestDaemonUnseedRemovesWireRelayedStatic closes the cross-mode unseed
// gap: a publisher wire-publishes a static (the presigned spec lands in
// the relay's slot), then unseeds it in daemon mode. The daemon delete
// must locate the live spec in the relay's slot and mint a superseding
// tombstone. Before the cross-slot lookup it scanned only the
// publisher's own slot, found nothing, and silently no-opped.
func TestDaemonUnseedRemovesWireRelayedStatic(t *testing.T) {
	h := newRelayHarness(t)
	digest := bytes.Repeat([]byte{0xbb}, 32)
	spec := state.StaticSpec{Name: "swlock", ManifestDigest: hex.EncodeToString(digest)}

	// P wire-publishes against R: the presigned spec lands only in R's
	// slot.
	r := h.relay(t, defaultRelayKey)
	_, err := r.SetStaticSpecPresigned(spec, h.staticFact(t, "swlock", digest, false))
	require.NoError(t, err)

	// P's own daemon learns the spec only by gossip from R.
	p := h.publisher(t)
	_, _, err = p.ApplyDelta(r.EncodeFull())
	require.NoError(t, err)
	require.True(t, staticVisible(p.Snapshot(), h.pKey, "swlock"))

	// Daemon-mode unseed must mint a superseding tombstone, not no-op.
	events, err := p.DeleteStaticSpec("swlock")
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.False(t, staticVisible(p.Snapshot(), h.pKey, "swlock"))

	// Idempotent: the tombstone now sits in P's own slot.
	events, err = p.DeleteStaticSpec("swlock")
	require.NoError(t, err)
	require.Empty(t, events, "repeated unseed is a no-op")

	// A wire re-seed at a higher seq must be unseeable again from the
	// daemon: the stale tombstone in P's slot must not mask the newer
	// live copy held in R's slot.
	_, err = r.SetStaticSpecPresigned(spec, h.staticFact(t, "swlock", digest, false))
	require.NoError(t, err)
	_, _, err = p.ApplyDelta(r.EncodeFull())
	require.NoError(t, err)
	require.True(t, staticVisible(p.Snapshot(), h.pKey, "swlock"))

	events, err = p.DeleteStaticSpec("swlock")
	require.NoError(t, err)
	require.NotEmpty(t, events, "daemon unseed must suppress a newer re-seed, not no-op on a stale tombstone")
	require.False(t, staticVisible(p.Snapshot(), h.pKey, "swlock"))
}

// TestWireUnseedRemovesDaemonSeededStatic is the reverse direction: a
// publisher daemon-seeds a static (the spec lands in its own slot),
// then unseeds it over the wire while offline via a presigned tombstone
// applied by a relay. The presigned-tombstone path already scans every
// slot, so it finds the daemon-seeded body in the publisher's slot and
// suppresses it.
func TestWireUnseedRemovesDaemonSeededStatic(t *testing.T) {
	h := newRelayHarness(t)
	digest := bytes.Repeat([]byte{0xdd}, 32)
	spec := state.StaticSpec{Name: "swlock", ManifestDigest: hex.EncodeToString(digest)}

	// P daemon-seeds.
	p := h.publisher(t)
	_, err := p.SetStaticSpec(spec, nil)
	require.NoError(t, err)

	// R learns the daemon-seed by gossip.
	r := h.relay(t, defaultRelayKey)
	_, _, err = r.ApplyDelta(p.EncodeFull())
	require.NoError(t, err)
	require.True(t, staticVisible(r.Snapshot(), h.pKey, "swlock"))

	// P unseeds over the wire while offline; R applies the tombstone
	// and the daemon-seeded spec is suppressed.
	_, err = r.DeleteStaticSpecPresigned("swlock", h.staticFact(t, "swlock", digest, true))
	require.NoError(t, err)
	require.False(t, staticVisible(r.Snapshot(), h.pKey, "swlock"))
}

// TestRelaySelfSlotTombstoneSurvivesRestore pins the restore-path
// self-slot contract. P daemon-seeds, R learns by gossip, P wire-
// unseeds via R (tombstone in R's own slot keyed (static, name, P)),
// R restarts. The tombstone must survive: routing it through the live
// self-conflict guard on restore would strip it (the !ev.Deleted check
// is sound for peer-supplied events, unsound for own-disk replay).
func TestRelaySelfSlotTombstoneSurvivesRestore(t *testing.T) {
	h := newRelayHarness(t)
	digest := bytes.Repeat([]byte{0xbb}, 32)
	spec := state.StaticSpec{Name: "swlock", ManifestDigest: hex.EncodeToString(digest)}

	p := h.publisher(t)
	_, err := p.SetStaticSpec(spec, nil)
	require.NoError(t, err)

	r := h.relay(t, defaultRelayKey)
	_, _, err = r.ApplyDelta(p.EncodeFull())
	require.NoError(t, err)
	require.True(t, staticVisible(r.Snapshot(), h.pKey, "swlock"))

	_, err = r.DeleteStaticSpecPresigned("swlock", h.staticFact(t, "swlock", digest, true))
	require.NoError(t, err)
	require.False(t, staticVisible(r.Snapshot(), h.pKey, "swlock"))

	r2 := h.bareRelay(t, defaultRelayKey)
	require.NoError(t, r2.RestoreFromDisk(r.EncodeFull()))
	require.False(t, staticVisible(r2.Snapshot(), h.pKey, "swlock"),
		"self-slot tombstone survives the restart")
}
