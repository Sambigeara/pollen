// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
)

// Routed-header byte layout (after the leading stream/datagram type byte
// has been stripped). The MAC covers every field except ttl, which each
// relay decrements, and the mac field itself.
const (
	roDest      = 0
	roSource    = 32
	roTTL       = 64
	roInnerType = 65
	roEpoch     = 66
	roCounter   = 74
	roTimestamp = 82
	roMAC       = 90

	routeHeaderSize = 106
	routeEpochSize  = 8
	routeMACSize    = 16

	routeAuthKDFInfo = "pollen.route-auth.v1"
	routeFreshness   = 2 * time.Minute
)

// routeAuth authenticates the origin of relayed streams and datagrams.
// The origin and destination derive an identical MAC key from their
// static Ed25519 identities (X25519 ECDH then HKDF), so the destination
// can confirm the route header's source is the real origin rather than a
// spoofed key. Relays cannot read or forge the tag and forward it
// opaquely; only the destination verifies.
type routeAuth struct {
	keys     map[types.PeerKey][]byte
	replays  map[types.PeerKey]*replayState
	signPriv ed25519.PrivateKey
	counter  atomic.Uint64
	mu       sync.Mutex
	epoch    [routeEpochSize]byte
}

func newRouteAuth(signPriv ed25519.PrivateKey) (*routeAuth, error) {
	ra := &routeAuth{
		signPriv: signPriv,
		keys:     make(map[types.PeerKey][]byte),
		replays:  make(map[types.PeerKey]*replayState),
	}
	if _, err := rand.Read(ra.epoch[:]); err != nil {
		return nil, fmt.Errorf("route auth epoch: %w", err)
	}
	return ra, nil
}

// writeRouteHeader fills the routing fields an origin sets: dest, source,
// the initial ttl and innerType. seal then stamps the authenticated fields
// over the result.
func writeRouteHeader(header []byte, dest, source types.PeerKey, innerType byte) {
	copy(header[roDest:], dest[:])
	copy(header[roSource:], source[:])
	header[roTTL] = defaultRouteTTL
	header[roInnerType] = innerType
}

// macKey returns the MAC key shared with remote, deriving and caching it
// on first use. Used on the send path, where remote is a destination we
// chose to address.
func (ra *routeAuth) macKey(remote types.PeerKey) ([]byte, error) {
	if k, ok := ra.cachedKey(remote); ok {
		return k, nil
	}
	key, err := ra.deriveKey(remote)
	if err != nil {
		return nil, err
	}
	ra.storeKey(remote, key)
	return key, nil
}

func (ra *routeAuth) cachedKey(remote types.PeerKey) ([]byte, bool) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	k, ok := ra.keys[remote]
	return k, ok
}

// deriveKey computes the shared MAC key without caching it.
func (ra *routeAuth) deriveKey(remote types.PeerKey) ([]byte, error) {
	secret, err := identity.StaticSharedSecret(ra.signPriv, ed25519.PublicKey(remote[:]))
	if err != nil {
		return nil, err
	}
	return hkdf.Key(sha256.New, secret, nil, routeAuthKDFInfo, sha256.Size)
}

func (ra *routeAuth) storeKey(remote types.PeerKey, key []byte) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.keys[remote] = key
}

// seal stamps epoch, counter, timestamp and MAC into header, which must
// already carry dest, source and innerType (see writeRouteHeader). domain
// is the frame class (routeDomainStream or routeDomainDatagram); payload
// is the datagram body, nil for streams whose body the MAC does not cover.
func (ra *routeAuth) seal(domain byte, header, payload []byte) error {
	return ra.sealAt(domain, header, payload, uint64(time.Now().UnixMilli()))
}

func (ra *routeAuth) sealAt(domain byte, header, payload []byte, nowMs uint64) error {
	var dest types.PeerKey
	copy(dest[:], header[roDest:roSource])
	key, err := ra.macKey(dest)
	if err != nil {
		return err
	}
	copy(header[roEpoch:], ra.epoch[:])
	binary.BigEndian.PutUint64(header[roCounter:], ra.counter.Add(1))
	binary.BigEndian.PutUint64(header[roTimestamp:], nowMs)
	copy(header[roMAC:], routeMAC(key, domain, header, payload))
	return nil
}

// verify checks the MAC and replay window of a routed message addressed
// to us, returning the authenticated source on success.
func (ra *routeAuth) verify(domain byte, header, payload []byte) (types.PeerKey, bool) {
	return ra.verifyAt(domain, header, payload, uint64(time.Now().UnixMilli()))
}

func (ra *routeAuth) verifyAt(domain byte, header, payload []byte, nowMs uint64) (types.PeerKey, bool) {
	var source types.PeerKey
	copy(source[:], header[roSource:roTTL])

	counter := binary.BigEndian.Uint64(header[roCounter:])
	if counter == 0 {
		return source, false
	}
	ts := binary.BigEndian.Uint64(header[roTimestamp:])
	if absU64(nowMs, ts) > uint64(routeFreshness/time.Millisecond) {
		return source, false
	}
	// source is attacker-controlled, so derive the key without caching and
	// admit it to the key cache and replay window only after the MAC proves
	// source holds the shared secret. A forged source fails the compare and
	// leaves no residue, so a flood cannot grow either map.
	key, cached := ra.cachedKey(source)
	if !cached {
		var err error
		if key, err = ra.deriveKey(source); err != nil {
			return source, false
		}
	}
	if subtle.ConstantTimeCompare(routeMAC(key, domain, header, payload), header[roMAC:roMAC+routeMACSize]) != 1 {
		return source, false
	}
	if !cached {
		ra.storeKey(source, key)
	}
	var epoch [routeEpochSize]byte
	copy(epoch[:], header[roEpoch:roEpoch+routeEpochSize])
	if !ra.checkReplay(source, epoch, counter, ts) {
		return source, false
	}
	return source, true
}

// Frame-class domain tags fold into the MAC so a routed stream header
// cannot be reinterpreted as a routed datagram or vice versa: the stream
// and datagram inner-type enums overlap numerically, so the class must be
// bound explicitly. The tag is supplied by the receiving path and never
// travels on the wire.
const (
	routeDomainStream   byte = 0
	routeDomainDatagram byte = 1
)

func routeMAC(key []byte, domain byte, header, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte{domain})
	mac.Write(header[roDest:roTTL]) // dest + source
	mac.Write(header[roInnerType:roEpoch])
	mac.Write(header[roEpoch:roMAC]) // epoch + counter + timestamp
	mac.Write(payload)
	return mac.Sum(nil)[:routeMACSize]
}

// checkReplay enforces the per-source RFC 6479 window. An origin restart
// mints a new epoch and resets its counter, so a window reset on epoch
// change is allowed only for a message newer than any already accepted;
// otherwise a captured pre-restart message could reopen the window after a
// newer one. ts is the message's freshness-checked timestamp.
func (ra *routeAuth) checkReplay(source types.PeerKey, epoch [routeEpochSize]byte, counter, ts uint64) bool {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	rs, ok := ra.replays[source]
	if !ok {
		rs = &replayState{}
		ra.replays[source] = rs
	}
	if !rs.has || rs.epoch != epoch {
		if rs.has && ts <= rs.lastTS {
			return false
		}
		rs.has = true
		rs.epoch = epoch
		rs.filter = replayFilter{}
	}
	if !rs.filter.validate(counter) {
		return false
	}
	if ts > rs.lastTS {
		rs.lastTS = ts
	}
	return true
}

func absU64(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

type replayState struct {
	filter replayFilter
	epoch  [routeEpochSize]byte
	lastTS uint64
	has    bool
}

// replayFilter is the RFC 6479 anti-replay sliding window: a ring of
// 64-bit blocks tracking which counters within the window have been seen.
const (
	replayBlockLog   = 6
	replayBlockBits  = 1 << replayBlockLog
	replayRingBlocks = 32
	replayBlockMask  = replayRingBlocks - 1
	replayBitMask    = replayBlockBits - 1
	replayWindow     = (replayRingBlocks - 1) * replayBlockBits
)

type replayFilter struct {
	last uint64
	ring [replayRingBlocks]uint64
}

func (f *replayFilter) validate(counter uint64) bool {
	indexBlock := counter >> replayBlockLog
	if counter > f.last {
		cur := f.last >> replayBlockLog
		diff := min(indexBlock-cur, replayRingBlocks)
		for i := cur + 1; i <= cur+diff; i++ {
			f.ring[i&replayBlockMask] = 0
		}
		f.last = counter
	} else if f.last-counter > replayWindow {
		return false
	}
	idx := indexBlock & replayBlockMask
	bit := uint64(1) << (counter & replayBitMask)
	if f.ring[idx]&bit != 0 {
		return false
	}
	f.ring[idx] |= bit
	return true
}
