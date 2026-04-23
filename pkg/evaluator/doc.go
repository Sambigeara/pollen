// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package evaluator is Pollen's authorisation layer. Every gateable
// primitive routes an authorisation Request through Router.Allow at its
// single dispatch site; domain services never carry a gate reference
// themselves.
//
// Request and Decision marshal to the conventional subject/action/
// resource/context shape used by policy decision-point vendors, so a
// PDP seed receives json.Marshal(Request) as-is.
//
// Evaluator extension happens via seeds, not a Go plugin SDK. The
// router holds a factory registry keyed by evaluator kind; external
// packages plug in via WithFactory.
package evaluator
