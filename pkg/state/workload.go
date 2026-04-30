// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import "time"

// WorkloadSpec crosses package boundaries by value; the proto representation
// stays internal to state. Timeout is a time.Duration in-domain but uint32
// ms on the wire — sub-millisecond or out-of-range values are clamped at
// the translation boundary.
type WorkloadSpec struct {
	Hash        string
	Name        string
	MemoryBytes uint64
	Timeout     time.Duration
	MinReplicas uint32
	Spread      float32
}

// NodeResources is the per-node resource snapshot gossiped via
// SetLocalResources.
type NodeResources struct {
	MemTotalBytes uint64
	CPUPercent    uint32
	MemPercent    uint32
	NumCPU        uint32
}

type StaticSpec struct {
	Name           string
	ManifestDigest string
}

// BlobSpec names a content-addressed blob so callers can refer to it by
// label rather than digest. Entries are keyed by (publisher, digest) —
// re-publishing the same digest under a new name overwrites the previous
// name for that publisher.
type BlobSpec struct {
	Name   string
	Digest string
}
