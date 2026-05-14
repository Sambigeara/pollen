// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"crypto/sha256"
	"encoding/base32"
)

// SlugLen is the URL-facing slug length: 12 base32 digits = 60 bits, so
// grinding a preimage against a target slug takes ~2^60 SHA-256 trials.
const SlugLen = 12

// ReservedBearerSlug is the path segment under which the HTTP gateway
// serves token-bearer URLs. PublisherSlug never produces this value
// because the slug alphabet excludes "_".
const ReservedBearerSlug = "_"

// crockfordAlphabet is Crockford base32: 0-9 then A-Z minus I L O U.
// Lowercase for URL aesthetics; the encoding is the routing primitive,
// not user-typeable, so visual unambiguity is the only constraint.
const crockfordAlphabet = "0123456789abcdefghjkmnpqrstvwxyz"

var slugEncoding = base32.NewEncoding(crockfordAlphabet).WithPadding(base32.NoPadding)

// PublisherSlug derives a stable, collision-resistant URL slug from an
// ed25519 subject public key. The slug is the first SlugLen characters
// of base32(sha256(subjectPub)). Hashing decouples slug grinding from
// pubkey grinding: an attacker cannot select a target pubkey such that
// its slug collides with a victim's without inverting SHA-256.
func PublisherSlug(subjectPub []byte) string {
	sum := sha256.Sum256(subjectPub)
	return slugEncoding.EncodeToString(sum[:])[:SlugLen]
}

// Slug returns the publisher slug for this peer key.
func (pk PeerKey) Slug() string {
	return PublisherSlug(pk[:])
}

// IsValidSlug reports whether s is the right shape to be a slug:
// SlugLen characters from the Crockford alphabet. It does not verify
// that the slug corresponds to a known publisher.
func IsValidSlug(s string) bool {
	if len(s) != SlugLen {
		return false
	}
	for i := range s {
		if !isCrockfordChar(s[i]) {
			return false
		}
	}
	return true
}

func isCrockfordChar(c byte) bool {
	for i := range len(crockfordAlphabet) {
		if crockfordAlphabet[i] == c {
			return true
		}
	}
	return false
}
