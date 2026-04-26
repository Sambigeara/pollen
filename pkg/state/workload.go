// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import "time"

// WorkloadSpec crosses package boundaries by value; the proto representation
// stays internal to state. Timeout and LatencySLO are time.Duration in-domain
// but uint32 ms on the wire — sub-millisecond or out-of-range values are
// clamped at the translation boundary.
type WorkloadSpec struct {
	Hash        string
	Name        string
	MemoryBytes uint64
	Timeout     time.Duration
	LatencySLO  time.Duration
	MinReplicas uint32
	Spread      float32
}

// NodeResources is the per-node resource snapshot gossiped via
// SetLocalResources. CPU and memory percent are uint32 to match
// sysinfo.Sample at source and the proto on the wire — no more
// float64→uint32 casting noise at call sites.
type NodeResources struct {
	MemTotalBytes    uint64
	CPUPercent       uint32
	MemPercent       uint32
	NumCPU           uint32
	CPUBudgetPercent uint32
	MemBudgetPercent uint32
}

// SeedMetrics is the per-seed per-node telemetry bundle, gossiped as one
// snapshot. OriginRate is calls/sec entering the cluster at this node — the
// authoritative demand signal for placement. ParkedMs is mean blocked time
// inside pollen_request, used by adaptive gate sizing to separate compute
// from waiting. Zero ComputeCostMs in an otherwise non-zero entry means
// "no observation", not a real 0 ms sample.
type SeedMetrics struct {
	ServedRate       float32
	OriginRate       float32
	ComputeCostMs    float32
	SLOSatisfiedRate float32
	SLOBurnedRate    float32
	ParkedMs         float32
	GateWaitMs       uint32
}

// IsZero is true when SetSeedMetrics should omit the entry from gossip;
// non-zero→zero transitions on any single field still pass the dead-band check.
func (m SeedMetrics) IsZero() bool {
	return m.ServedRate == 0 && m.OriginRate == 0 &&
		m.ComputeCostMs == 0 && m.SLOSatisfiedRate == 0 &&
		m.SLOBurnedRate == 0 && m.ParkedMs == 0 &&
		m.GateWaitMs == 0
}

type StaticSpec struct {
	Name           string
	ManifestDigest string
	MinReplicas    uint32
}

// BlobSpec names a content-addressed blob so callers can refer to it by
// label rather than digest. Entries are keyed by (publisher, digest) —
// re-publishing the same digest under a new name overwrites the previous
// name for that publisher.
type BlobSpec struct {
	Name   string
	Digest string
}
