// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type captureState struct {
	blobState
	wrappings []*factv1.BlobWrapping
}

func (c *captureState) Snapshot() state.Snapshot { return state.Snapshot{} }

func (c *captureState) SetBlobWrapping(w *factv1.BlobWrapping) []state.Event {
	c.wrappings = append(c.wrappings, w)
	return nil
}

func newWrapTestService(t *testing.T, self types.PeerKey) (*Service, *captureState) {
	t.Helper()
	signPub, signPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cs := &captureState{}
	return &Service{
		self:     self,
		state:    cs,
		signer:   fact.NewSigner(signPriv),
		signPriv: signPriv,
		signPub:  signPub,
		dekCache: make(map[string][]byte),
	}, cs
}

func hashHex(b byte) string {
	raw := make([]byte, 32)
	raw[0] = b
	return hex.EncodeToString(raw)
}

// Recipients are addressed by ed25519 pub key, so WrapDEK parses them
// as curve points; only generated keys round-trip.
func realPeerKey(t *testing.T) types.PeerKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return types.PeerKeyFromBytes(pub)
}

// IssueWrappingsFor is the seed-time wrap-to-set primitive: every
// (hash, recipient) pair gossips a wrapping except where recipient is
// self (Put's self-wrap already covers it). The product across a list
// of digests and the serving set must come out at the expected
// cardinality with the self entry dropped.
func TestIssueWrappingsFor_ProductWithSelfSkipped(t *testing.T) {
	self := realPeerKey(t)
	a := realPeerKey(t)
	b := realPeerKey(t)
	svc, cs := newWrapTestService(t, self)
	h1, h2 := hashHex(0xaa), hashHex(0xbb)
	svc.cacheDEK(h1, make([]byte, 32))
	svc.cacheDEK(h2, make([]byte, 32))

	require.NoError(t, svc.IssueWrappingsFor([]string{h1, h2}, []types.PeerKey{self, a, b}))
	require.Len(t, cs.wrappings, 4, "two hashes by two non-self recipients")

	pairs := make(map[string]map[types.PeerKey]struct{})
	for _, w := range cs.wrappings {
		hash := hex.EncodeToString(w.GetBlobHash())
		if pairs[hash] == nil {
			pairs[hash] = make(map[types.PeerKey]struct{})
		}
		pairs[hash][types.PeerKeyFromBytes(w.GetRecipientPub())] = struct{}{}
	}
	require.Equal(t, map[types.PeerKey]struct{}{a: {}, b: {}}, pairs[h1])
	require.Equal(t, map[types.PeerKey]struct{}{a: {}, b: {}}, pairs[h2])
}

// Wrapping issuance reads the DEK via localDEK; if a hash has no
// cached DEK and no gossiped wrapping the issue fails. Returning early
// surfaces the first cause to the caller rather than silently
// half-completing the fanout.
func TestIssueWrappingsFor_StopsOnFirstError(t *testing.T) {
	self := realPeerKey(t)
	a := realPeerKey(t)
	svc, cs := newWrapTestService(t, self)
	good := hashHex(0xcc)
	missing := hashHex(0xdd)
	svc.cacheDEK(good, make([]byte, 32))

	err := svc.IssueWrappingsFor([]string{good, missing}, []types.PeerKey{a})
	require.Error(t, err)
	require.True(t, errors.Is(err, errNoWrapping), "expect propagated errNoWrapping, got %v", err)
	require.Len(t, cs.wrappings, 1, "first hash succeeds; loop halts before second")
}

// Empty input is a no-op rather than a degenerate error: an empty
// serving set is the caller's policy decision (and is gated by the
// seed handler itself), and an empty digest list is meaningless but
// harmless.
func TestIssueWrappingsFor_EmptyInputsAreNoOp(t *testing.T) {
	svc, cs := newWrapTestService(t, realPeerKey(t))
	require.NoError(t, svc.IssueWrappingsFor(nil, []types.PeerKey{realPeerKey(t)}))
	require.NoError(t, svc.IssueWrappingsFor([]string{hashHex(0xee)}, nil))
	require.Empty(t, cs.wrappings)
}

// hasStore embeds the blobStore interface and only implements Has.
// Get is exercised through paths that exit on Has=false or on
// errNoWrapping before touching store.Get / store.Remove; any future
// caller that escapes those branches will nil-panic, which is the
// signal that the test fixture needs to grow.
type hasStore struct {
	blobStore
	has bool
}

func (h *hasStore) Has(string) bool { return h.has }

// Get differentiates missing bytes from missing wrapping: prior to
// this change the byte path silently masqueraded as a wrapping error,
// which is exactly how the user's bug report ("no DEK wrapping for
// blob") read even though it could have been either failure mode.
// Reorder pins the contract: ErrNotLocal first, errNoWrapping only when
// bytes are present.
func TestGet_OrdersByteCheckBeforeWrapping(t *testing.T) {
	svc, _ := newWrapTestService(t, realPeerKey(t))

	t.Run("missing bytes return ErrNotLocal even with no wrapping", func(t *testing.T) {
		svc.store = &hasStore{has: false}
		_, err := svc.Get(hashHex(0xa1))
		require.ErrorIs(t, err, ErrNotLocal)
	})

	t.Run("bytes present but no wrapping returns errNoWrapping", func(t *testing.T) {
		svc.store = &hasStore{has: true}
		_, err := svc.Get(hashHex(0xa2))
		require.ErrorIs(t, err, errNoWrapping)
	})
}

type ciphertextStore struct {
	blobStore
	bytes []byte
}

func (c *ciphertextStore) GetCiphertext(string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(c.bytes)), nil
}

type discardStream struct {
	io.Reader
}

func (d *discardStream) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardStream) Close() error                { return nil }

// Late-joiner safety net: after fanoutWrappingsForServingSet has run,
// a node that flips to CanServeStatic later still has to gain a
// wrapping. The brief keeps the lazy-wrap inside blobs.Serve as the
// only mechanism for that case, so a future refactor of Serve that
// drops the issueWrappingFor call would silently lose the safety net.
// This test pins that Serve still wraps for the requester after a
// successful stream.
func TestServe_LazyWrapsForRequester(t *testing.T) {
	self := realPeerKey(t)
	requester := realPeerKey(t)
	svc, cs := newWrapTestService(t, self)
	svc.store = &ciphertextStore{bytes: []byte("payload-bytes")}
	hash := hashHex(0xf0)
	svc.cacheDEK(hash, make([]byte, 32))

	svc.Serve(&discardStream{Reader: bytes.NewReader(nil)}, hash, requester)

	require.Len(t, cs.wrappings, 1, "Serve must lazy-wrap exactly once for the requester")
	require.Equal(t, requester.Bytes(), cs.wrappings[0].GetRecipientPub())
}

// A requester equal to self must not produce a redundant wrapping;
// Put already self-wraps, so re-issuing would be wasted gossip churn.
func TestServe_NoLazyWrapForSelfRequester(t *testing.T) {
	self := realPeerKey(t)
	svc, cs := newWrapTestService(t, self)
	svc.store = &ciphertextStore{bytes: []byte("payload-bytes")}
	hash := hashHex(0xf1)
	svc.cacheDEK(hash, make([]byte, 32))

	svc.Serve(&discardStream{Reader: bytes.NewReader(nil)}, hash, self)

	require.Empty(t, cs.wrappings, "self-wrap is Put's job, not Serve's")
}

// A failed ciphertext fetch must not lazy-wrap. Wrapping a hash
// nobody can fetch from this node is misleading: the recipient would
// learn it can decrypt bytes it can never obtain here.
type missingCiphertextStore struct {
	blobStore
}

func (m *missingCiphertextStore) GetCiphertext(string) (io.ReadCloser, error) {
	return nil, cas.ErrNotFound
}

func TestServe_NoLazyWrapOnMissingCiphertext(t *testing.T) {
	self := realPeerKey(t)
	svc, cs := newWrapTestService(t, self)
	svc.store = &missingCiphertextStore{}
	svc.Serve(&discardStream{Reader: bytes.NewReader(nil)}, hashHex(0xf2), realPeerKey(t))
	require.Empty(t, cs.wrappings)
}
