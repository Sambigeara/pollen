// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTestRouteAuth(t *testing.T) (*routeAuth, types.PeerKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key := types.PeerKeyFromBytes(pub)
	ra, err := newRouteAuth(priv)
	require.NoError(t, err)
	return ra, key
}

// sealHeader builds a routed header as the origin would and seals it toward
// dest for the given frame domain. source is written verbatim so tests can
// forge it.
func sealHeader(t *testing.T, origin *routeAuth, dest, source types.PeerKey, domain, innerType byte, payload []byte) []byte {
	t.Helper()
	return sealHeaderAt(t, origin, dest, source, domain, innerType, payload, uint64(time.Now().UnixMilli()))
}

// sealHeaderAt is sealHeader with an explicit message timestamp, for tests
// that exercise the freshness and epoch-monotonicity logic.
func sealHeaderAt(t *testing.T, origin *routeAuth, dest, source types.PeerKey, domain, innerType byte, payload []byte, nowMs uint64) []byte {
	t.Helper()
	header := make([]byte, routeHeaderSize)
	writeRouteHeader(header, dest, source, innerType)
	require.NoError(t, origin.sealAt(domain, header, payload, nowMs))
	return header
}

func TestRouteAuthRoundTrip(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	payload := []byte("hello over the relay")
	header := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), payload)

	got, ok := dest.verify(routeDomainDatagram, header, payload)
	require.True(t, ok)
	require.Equal(t, originKey, got)
}

func TestRouteAuthStreamRoundTrip(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	header := sealHeader(t, origin, destKey, originKey, routeDomainStream, byte(StreamTypeMembership), nil)
	got, ok := dest.verify(routeDomainStream, header, nil)
	require.True(t, ok)
	require.Equal(t, originKey, got)
}

// A peer that forges a victim's key as the route source cannot produce a
// MAC the destination accepts, because the destination derives the key
// from the claimed source and only the real victim holds its private key.
func TestRouteAuthSpoofedSourceRejected(t *testing.T) {
	attacker, _ := newTestRouteAuth(t)
	_, victimKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	payload := []byte("forged")
	header := sealHeader(t, attacker, destKey, victimKey, routeDomainDatagram, byte(DatagramTypeTunnel), payload)

	_, ok := dest.verify(routeDomainDatagram, header, payload)
	require.False(t, ok)
}

// A source whose MAC does not verify must leave no entry in the key or
// replay caches, so a flood of forged sources cannot grow them unbounded.
func TestRouteAuthFailedVerifyLeavesNoResidue(t *testing.T) {
	attacker, _ := newTestRouteAuth(t)
	_, victimKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	header := sealHeader(t, attacker, destKey, victimKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("forged"))
	_, ok := dest.verify(routeDomainDatagram, header, []byte("forged"))
	require.False(t, ok)

	dest.mu.Lock()
	_, keyed := dest.keys[victimKey]
	_, replayed := dest.replays[victimKey]
	dest.mu.Unlock()
	require.False(t, keyed, "a forged source must not be cached")
	require.False(t, replayed, "a forged source must not enter the replay window")
}

// The stream and datagram inner-type enums overlap numerically, so the MAC
// binds the frame class: a header sealed for one class must not verify on
// the other, else an on-path relay could reinterpret a stream header as an
// empty datagram (and vice versa).
func TestRouteAuthFrameDomainSeparation(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	streamHdr := sealHeader(t, origin, destKey, originKey, routeDomainStream, byte(StreamTypeTunnel), nil)
	_, ok := dest.verify(routeDomainStream, streamHdr, nil)
	require.True(t, ok)
	_, ok = dest.verify(routeDomainDatagram, streamHdr, nil)
	require.False(t, ok, "a stream header must not verify as a datagram")

	dgramHdr := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), nil)
	_, ok = dest.verify(routeDomainDatagram, dgramHdr, nil)
	require.True(t, ok)
	_, ok = dest.verify(routeDomainStream, dgramHdr, nil)
	require.False(t, ok, "a datagram header must not verify as a stream")
}

func TestRouteAuthTamperRejected(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	payload := []byte("authentic payload")
	header := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), payload)

	tampered := append([]byte(nil), payload...)
	tampered[0] ^= 0xff
	_, ok := dest.verify(routeDomainDatagram, header, tampered)
	require.False(t, ok)

	header[roInnerType] ^= 0xff
	_, ok = dest.verify(routeDomainDatagram, header, payload)
	require.False(t, ok)
}

func TestRouteAuthReplayRejected(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	h1 := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("one"))
	_, ok := dest.verify(routeDomainDatagram, h1, []byte("one"))
	require.True(t, ok)
	_, ok = dest.verify(routeDomainDatagram, h1, []byte("one"))
	require.False(t, ok, "replay of identical message must be rejected")

	h2 := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("two"))
	_, ok = dest.verify(routeDomainDatagram, h2, []byte("two"))
	require.True(t, ok)
}

func TestRouteAuthStaleRejected(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	payload := []byte("stale")
	now := uint64(time.Now().UnixMilli())
	skew := uint64((routeFreshness + time.Minute) / time.Millisecond)

	header := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), payload)
	_, ok := dest.verifyAt(routeDomainDatagram, header, payload, now)
	require.True(t, ok)

	stale := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), payload)
	_, ok = dest.verifyAt(routeDomainDatagram, stale, payload, now+skew)
	require.False(t, ok, "a timestamp too far in the past is rejected")

	ahead := sealHeader(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), payload)
	_, ok = dest.verifyAt(routeDomainDatagram, ahead, payload, now-skew)
	require.False(t, ok, "a timestamp too far in the future is rejected")
}

// An origin restart mints a new epoch and resets its counter; the
// destination must accept the new epoch's low counter despite having seen
// a higher counter under the old epoch. The restart's first message is
// newer than anything sent before it, since a restart takes real time.
func TestRouteAuthEpochResetAllowsRestart(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	now := uint64(time.Now().UnixMilli())
	h1 := sealHeaderAt(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("pre"), now)
	_, ok := dest.verifyAt(routeDomainDatagram, h1, []byte("pre"), now)
	require.True(t, ok)

	restarted, err := newRouteAuth(origin.signPriv)
	require.NoError(t, err)
	h2 := sealHeaderAt(t, restarted, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("post"), now+1000)
	_, ok = dest.verifyAt(routeDomainDatagram, h2, []byte("post"), now+1000)
	require.True(t, ok)
}

// A message captured under one epoch must not replay after a newer-epoch
// message has been accepted: the window reset on epoch change is gated on
// the timestamp advancing, so a stale pre-restart capture is rejected.
func TestRouteAuthEpochResetRejectsStaleReplay(t *testing.T) {
	origin, originKey := newTestRouteAuth(t)
	dest, destKey := newTestRouteAuth(t)

	now := uint64(time.Now().UnixMilli())
	old := sealHeaderAt(t, origin, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("old"), now)

	restarted, err := newRouteAuth(origin.signPriv)
	require.NoError(t, err)
	fresh := sealHeaderAt(t, restarted, destKey, originKey, routeDomainDatagram, byte(DatagramTypeTunnel), []byte("fresh"), now+1000)

	_, ok := dest.verifyAt(routeDomainDatagram, fresh, []byte("fresh"), now+1000)
	require.True(t, ok)
	_, ok = dest.verifyAt(routeDomainDatagram, old, []byte("old"), now+1000)
	require.False(t, ok, "a captured older-epoch message must not replay after a newer-epoch one")
}

func TestRouteAuthKeySymmetry(t *testing.T) {
	a, aKey := newTestRouteAuth(t)
	b, bKey := newTestRouteAuth(t)

	ka, err := a.macKey(bKey)
	require.NoError(t, err)
	kb, err := b.macKey(aKey)
	require.NoError(t, err)
	require.Equal(t, ka, kb)
}

func TestReplayFilter(t *testing.T) {
	var f replayFilter
	require.True(t, f.validate(1))
	require.False(t, f.validate(1))
	require.True(t, f.validate(2))
	require.True(t, f.validate(5))
	require.True(t, f.validate(3))
	require.False(t, f.validate(3))
	require.True(t, f.validate(4))
	require.True(t, f.validate(10000))
	require.False(t, f.validate(1))
	require.False(t, f.validate(10000))
}
