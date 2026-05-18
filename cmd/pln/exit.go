// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

const (
	exitSuccess     = 0
	exitGeneric     = 1
	exitNotFound    = 3
	exitPermission  = 4
	exitAmbiguous   = 5
	exitUnreachable = 6
)

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

// errorLine renders err for the operator as "Error: <message>". The
// connect client surfaces a daemon status as a *connect.Error whose
// Error() is "<code>: <message>" (e.g. "failed_precondition: admission:
// ..."); the gRPC code is transport detail the operator did not ask
// for, so it is dropped to the daemon's own message. Local errors,
// which carry no such envelope, print verbatim.
func errorLine(err error) string {
	msg := err.Error()
	var ce *connect.Error
	if errors.As(err, &ce) {
		msg = ce.Message()
	}
	return "Error: " + msg
}
