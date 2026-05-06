// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"crypto/ed25519"
	"crypto/rand"
	"slices"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// denyScopingFixture spins up a 4-node trust graph for testing
// chain-bounded deny semantics:
//
//	root  ──admits──▶  adminA  ──admits──▶  leafA
//	root  ──admits──▶  adminB  ──admits──▶  leafB
//
// Each "node" gets a real signing key and a delegation cert chained
// through its admin to root, so cert verification + chain walks
// behave the same as in production.
type denyScopingFixture struct {
	rootPriv  ed25519.PrivateKey
	rootKey   types.PeerKey
	adminA    nodeIdentity
	adminB    nodeIdentity
	leafA     nodeIdentity
	leafB     nodeIdentity
	rootCert  *admissionv1.DelegationCert
	adminAC   *admissionv1.DelegationCert
	adminBC   *admissionv1.DelegationCert
	leafACert *admissionv1.DelegationCert
	leafBCert *admissionv1.DelegationCert
}

type nodeIdentity struct {
	priv ed25519.PrivateKey
	pub  ed25519.PublicKey
	key  types.PeerKey
}

func newIdentity(t *testing.T) nodeIdentity {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return nodeIdentity{priv: priv, pub: pub, key: types.PeerKeyFromBytes(pub)}
}

func newDenyScopingFixture(t *testing.T) *denyScopingFixture {
	t.Helper()
	rootPub, rootPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	expires := now.Add(time.Hour)

	rootCert, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), expires, time.Time{})
	require.NoError(t, err)

	mkAdmin := func(id nodeIdentity) *admissionv1.DelegationCert {
		c, err := auth.IssueDelegationCert(rootPriv, []*admissionv1.DelegationCert{rootCert}, id.pub, auth.FullCapabilities(), now.Add(-time.Minute), expires, time.Time{})
		require.NoError(t, err)
		return c
	}
	mkLeaf := func(adminPriv ed25519.PrivateKey, parentChain []*admissionv1.DelegationCert, id nodeIdentity) *admissionv1.DelegationCert {
		c, err := auth.IssueDelegationCert(adminPriv, parentChain, id.pub, auth.LeafCapabilities(), now.Add(-time.Minute), expires, time.Time{})
		require.NoError(t, err)
		return c
	}

	f := &denyScopingFixture{
		rootPriv: rootPriv,
		rootKey:  types.PeerKeyFromBytes(rootPub),
		adminA:   newIdentity(t),
		adminB:   newIdentity(t),
		leafA:    newIdentity(t),
		leafB:    newIdentity(t),
		rootCert: rootCert,
	}
	f.adminAC = mkAdmin(f.adminA)
	f.adminBC = mkAdmin(f.adminB)
	f.leafACert = mkLeaf(f.adminA.priv, []*admissionv1.DelegationCert{f.adminAC, rootCert}, f.leafA)
	f.leafBCert = mkLeaf(f.adminB.priv, []*admissionv1.DelegationCert{f.adminBC, rootCert}, f.leafB)
	return f
}

func (f *denyScopingFixture) rootPub() []byte { return f.rootPriv.Public().(ed25519.PublicKey) }

// pushCert simulates `peer` gossipping its current delegation cert.
// signKey is the key used to produce the subject signature; pass the
// cert's actual subject priv for a legitimate publication, or an
// unrelated key to simulate a forged subject signature (the apply-time
// gate must reject).
func pushCert(t *testing.T, s StateStore, peer types.PeerKey, cert *admissionv1.DelegationCert, signKey ed25519.PrivateKey) {
	t.Helper()
	var sig []byte
	if signKey != nil {
		// Sign without the helper's "matches subject" sanity check so
		// tests can also construct intentionally-bad signatures.
		msg, err := (proto.MarshalOptions{Deterministic: true}).Marshal(cert.GetClaims())
		require.NoError(t, err)
		sig, err = signKey.Sign(nil, msg, &ed25519.Options{Context: "pollen.delegation.subject.v1"})
		require.NoError(t, err)
	}
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peer.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_DelegationCert{DelegationCert: &statev1.DelegationCertChange{
			Cert:             cert,
			SubjectSignature: sig,
		}},
	})
}

// pushDeny simulates `issuer` gossipping a deny event against `subject`.
func pushDeny(t *testing.T, s StateStore, issuer, subject types.PeerKey) {
	t.Helper()
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  issuer.String(),
		Counter: 1,
		Change:  &statev1.GossipEvent_Deny{Deny: &statev1.DenyChange{PeerPub: subject.Bytes()}},
	})
}

func denied(snap Snapshot, pk types.PeerKey) bool {
	return slices.Contains(snap.DeniedKeys, pk)
}

func TestDenyScoping_RootCanDenyAnyone(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	pushCert(t, s, f.adminA.key, f.adminAC, f.adminA.priv)
	pushCert(t, s, f.leafA.key, f.leafACert, f.leafA.priv)
	pushDeny(t, s, f.rootKey, f.leafA.key)

	require.True(t, denied(s.Snapshot(), f.leafA.key), "root deny on leafA should take effect")
}

func TestDenyScoping_AdminCanDenyOwnSubtree(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	pushCert(t, s, f.adminA.key, f.adminAC, f.adminA.priv)
	pushCert(t, s, f.leafA.key, f.leafACert, f.leafA.priv)
	pushDeny(t, s, f.adminA.key, f.leafA.key)

	require.True(t, denied(s.Snapshot(), f.leafA.key), "adminA must be able to deny its own leaf")
}

func TestDenyScoping_AdminCannotDenyForeignSubtree(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	pushCert(t, s, f.adminA.key, f.adminAC, f.adminA.priv)
	pushCert(t, s, f.adminB.key, f.adminBC, f.adminB.priv)
	pushCert(t, s, f.leafB.key, f.leafBCert, f.leafB.priv)
	pushDeny(t, s, f.adminA.key, f.leafB.key)

	require.False(t, denied(s.Snapshot(), f.leafB.key),
		"adminA must not be able to deny leafB (in adminB's subtree)")
}

func TestDenyScoping_RevokingIntermediateAdminCascades(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	pushCert(t, s, f.adminA.key, f.adminAC, f.adminA.priv)
	pushCert(t, s, f.leafA.key, f.leafACert, f.leafA.priv)
	pushCert(t, s, f.adminB.key, f.adminBC, f.adminB.priv)
	pushCert(t, s, f.leafB.key, f.leafBCert, f.leafB.priv)

	// root revokes adminA. Should cascade: adminA + leafA denied; adminB + leafB unaffected.
	events := applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  f.rootKey.String(),
		Counter: 1,
		Change:  &statev1.GossipEvent_Deny{Deny: &statev1.DenyChange{PeerPub: f.adminA.key.Bytes()}},
	})

	snap := s.Snapshot()
	require.True(t, denied(snap, f.adminA.key), "denied admin should be in denied set")
	require.True(t, denied(snap, f.leafA.key), "leaves of denied admin should cascade")
	require.False(t, denied(snap, f.adminB.key), "unrelated admin should not be denied")
	require.False(t, denied(snap, f.leafB.key), "unrelated subtree should not be denied")

	// Cascade victims must surface as PeerDenied events so downstream
	// consumers (supervisor) can evict sessions, refresh routes, and
	// drop placements without waiting for an unrelated trigger.
	deniedEvents := make(map[types.PeerKey]bool)
	for _, ev := range events {
		if pd, ok := ev.(PeerDenied); ok {
			deniedEvents[pd.Key] = true
		}
	}
	require.True(t, deniedEvents[f.adminA.key], "PeerDenied should fire for the directly-revoked admin")
	require.True(t, deniedEvents[f.leafA.key], "PeerDenied should fire for cascade victim leafA")
	require.False(t, deniedEvents[f.adminB.key], "no PeerDenied for unrelated admin")
	require.False(t, deniedEvents[f.leafB.key], "no PeerDenied for unrelated leaf")
}

func TestDenyScoping_DenyPendingUntilCertArrives(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	// adminA denies leafA before leafA's cert is gossiped — deny is pending,
	// not yet effective (we can't verify adminA is in leafA's chain).
	pushDeny(t, s, f.adminA.key, f.leafA.key)
	require.False(t, denied(s.Snapshot(), f.leafA.key), "deny is pending without subject cert")

	// Once leafA's cert arrives, the deny becomes authorized + effective.
	pushCert(t, s, f.adminA.key, f.adminAC, f.adminA.priv)
	pushCert(t, s, f.leafA.key, f.leafACert, f.leafA.priv)
	require.True(t, denied(s.Snapshot(), f.leafA.key), "deny resolves once cert chain is known")
}

// TestDenyScoping_RejectsDelegatedAdminForgingForeignSubject covers
// the canonical bypass attempt: a delegated admin issues a *valid*
// cert chain for a victim peer's pub (re-parenting victim into the
// admin's subtree), then publishes it as the victim's cert event.
// Without subject proof-of-possession the victim becomes deniable by
// the malicious admin. With it, the apply-time gate drops the cert
// because the subject signature can't be forged.
func TestDenyScoping_RejectsDelegatedAdminForgingForeignSubject(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	// Set up the legitimate cluster shape: leafB belongs to adminB.
	pushCert(t, s, f.adminA.key, f.adminAC, f.adminA.priv)
	pushCert(t, s, f.adminB.key, f.adminBC, f.adminB.priv)
	pushCert(t, s, f.leafB.key, f.leafBCert, f.leafB.priv)

	// adminA mints a "leafB" cert under its own subtree using its
	// signer privileges. Chain verifies; subject == leafB's pub.
	forged, err := auth.IssueDelegationCert(
		f.adminA.priv,
		[]*admissionv1.DelegationCert{f.adminAC, f.rootCert},
		f.leafB.pub,
		auth.LeafCapabilities(),
		time.Now().Add(-time.Minute),
		time.Now().Add(time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// adminA tries to publish the forged cert AS leafB but signs the
	// "subject signature" with adminA's own key (the only key it has).
	// The store must reject because subject_signature doesn't verify
	// against cert.subject_pub == leafB.pub.
	pushCert(t, s, f.leafB.key, forged, f.adminA.priv)

	// adminA then issues a deny against leafB. If the forged cert
	// had taken effect, leafB's chain would now contain adminA and
	// the deny would be authorized. It must not be.
	pushDeny(t, s, f.adminA.key, f.leafB.key)

	require.False(t, denied(s.Snapshot(), f.leafB.key),
		"a delegated admin must not be able to deny a foreign-subtree peer by re-parenting their pub")
}

func TestDenyScoping_RejectsForgedCertChain(t *testing.T) {
	f := newDenyScopingFixture(t)
	observer := genKey(t)
	s := newTestStore(t, observer)
	s.SetRootPub(f.rootPub())

	// Attacker key that isn't in the trust graph.
	bogus := newIdentity(t)
	bogusCert, err := auth.IssueDelegationCert(bogus.priv, nil, bogus.pub, auth.FullCapabilities(),
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour), time.Time{})
	require.NoError(t, err)

	// Forged cert: signed by attacker, not chained to root.
	pushCert(t, s, bogus.key, bogusCert, bogus.priv)

	// The cert should have been rejected at apply time, so the bogus
	// pub is unknown to the cert graph and a self-deny is the only
	// thing it could ever take effect against.
	pushDeny(t, s, bogus.key, f.leafA.key)
	require.False(t, denied(s.Snapshot(), f.leafA.key),
		"deny issued by an unrooted cert holder must not affect anyone else")
}
