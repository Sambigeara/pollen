// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"errors"
	"time"
)

// ErrOverloaded is returned when a node refuses a call because admission
// would breach the local memory budget. Distinct from ErrAlreadyRunning
// / ErrNotRunning: overload is node-wide and retryable on a different
// claimant, so callers fall through to the next replica rather than
// surfacing the failure to the user.
var ErrOverloaded = errors.New("placement: node overloaded")

// retryAfterDefault is how long callers should wait before retrying an
// overloaded node — used by canRetry to decide whether the remaining
// context deadline can still absorb a retry.
const retryAfterDefault = 100 * time.Millisecond

// OverloadError wraps an overload sentinel with a human-readable reason.
// errors.Is(err, ErrOverloaded) detects the cause; the Reason aids
// debugging without bleeding into program logic.
type OverloadError struct {
	Sentinel error
	Reason   string
}

func (e *OverloadError) Error() string {
	if e.Reason != "" {
		return e.Reason + ": " + e.Sentinel.Error()
	}
	return e.Sentinel.Error()
}

func (e *OverloadError) Unwrap() error { return e.Sentinel }

func newOverload(sentinel error, reason string) *OverloadError {
	return &OverloadError{
		Sentinel: sentinel,
		Reason:   reason,
	}
}
