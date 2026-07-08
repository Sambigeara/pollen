// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package view projects a state snapshot through a calling Principal so
// reads return only what that Principal's authority grants. The serving
// node's own identity is never the implicit lens.
package view
