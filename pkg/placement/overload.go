// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import "time"

// retryAfterDefault is the suggested wait we attach to gate / admission
// rejections when nothing more specific is known. Roughly the time a
// short call takes — long enough that the rejecting node has space again
// for steady-state workloads, short enough that callers don't sit on a
// retry budget for human-noticeable durations.
const retryAfterDefault = 100 * time.Millisecond

// OverloadError wraps a backpressure sentinel (ErrAtCapacity /
// ErrOverloaded) with retry-after metadata. Callers detect the cause
// with errors.Is(err, ErrAtCapacity) and read the retry hint with
// errors.As(err, &target).
type OverloadError struct {
	Sentinel   error
	Reason     string
	RetryAfter time.Duration
}

func (e *OverloadError) Error() string {
	if e.Reason != "" {
		return e.Reason + ": " + e.Sentinel.Error()
	}
	return e.Sentinel.Error()
}

func (e *OverloadError) Unwrap() error { return e.Sentinel }

// newOverload wraps a sentinel into an OverloadError with the package
// default retry hint. Call sites with a better heuristic can construct
// OverloadError directly.
func newOverload(sentinel error, reason string) *OverloadError {
	return &OverloadError{
		Sentinel:   sentinel,
		Reason:     reason,
		RetryAfter: retryAfterDefault,
	}
}
