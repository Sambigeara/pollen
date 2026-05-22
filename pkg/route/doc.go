// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package route resolves the holder set for a Fact and selects a holder
// by locality, keeping authority and source orthogonal. The locality
// metric and selection policy are lifted verbatim from placement
// dispatch so blob fetch and workload routing agree on what "nearest"
// means.
package route
