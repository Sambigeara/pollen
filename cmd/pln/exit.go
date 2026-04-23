// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
)

// Exit codes for scripting consumers. 0 = success, 1 = generic.
// Sub-categories let scripts react to common failure shapes without
// parsing error text.
const (
	exitSuccess     = 0
	exitGeneric     = 1
	exitNotFound    = 3
	exitPermission  = 4
	exitAmbiguous   = 5
	exitUnreachable = 6
)

// exitError tags an error with a non-default exit code. Construct with
// notFoundErr / ambiguousErr / permissionErr / unreachableErr.
type exitError struct {
	err  error
	code int
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

func notFoundErr(format string, args ...any) error {
	return &exitError{code: exitNotFound, err: fmt.Errorf(format, args...)}
}

func ambiguousErr(format string, args ...any) error {
	return &exitError{code: exitAmbiguous, err: fmt.Errorf(format, args...)}
}

func permissionErr(msg string) error {
	return &exitError{code: exitPermission, err: errors.New(msg)}
}

func unreachableErr(msg string) error {
	return &exitError{code: exitUnreachable, err: errors.New(msg)}
}

// wrapExit attaches a code to an arbitrary error, preserving its
// chain. Useful when wrapping a third-party error (e.g. blob lookup
// helper) without losing its original message.
func wrapExit(code int, err error) error {
	if err == nil {
		return nil
	}
	return &exitError{code: code, err: err}
}

func exitCodeOf(err error) int {
	if err == nil {
		return exitSuccess
	}
	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code
	}
	return exitGeneric
}
