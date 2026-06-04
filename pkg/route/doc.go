// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package route selects a holder for a Fact by locality, keeping
// authority and source orthogonal. One Selector carries the locality
// metric and selection policy so blob fetch and workload routing agree
// on what "nearest" means: it prefers the routing layer's path cost to
// reach a holder (which prices in relay detours) and falls back to
// straight-line Vivaldi distance when the router has no route.
package route
