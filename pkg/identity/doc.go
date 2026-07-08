// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package identity defines the authority tier: Grant, Session and the
// resolved Principal. A Grant is the long-lived, root-rooted,
// denylist-revocable authority horizon and the only durability bound. A
// Session is a short-lived liveness proof self-minted from a held Grant
// with no issuer round-trip. A Principal is the resolved view of a
// subject's valid Grant, denial status and budget.
package identity
