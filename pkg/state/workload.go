// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"time"

	"github.com/sambigeara/pollen/pkg/claims"
)

// WorkloadSpec is the domain-side representation of a user's workload
// declaration. It crosses package boundaries by value — the underlying
// proto (statev1.WorkloadSpecChange) stays internal to state, used
// only for gossip serialization via seedMetrics/workloadSpec helpers.
//
// Timeout and LatencySLO are time.Duration in-domain but uint32 ms on
// the wire. Round-tripping through the proto is lossless only for
// whole-millisecond, in-uint32-range durations; anything finer or
// larger is clamped/truncated at the translation boundary.
type WorkloadSpec struct {
	Claim       *claims.PublisherClaim
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

// SeedMetrics is the per-seed per-node telemetry bundle. All fields are
// gossiped together so consumers (Prometheus, the reconciler's
// cluster-wide aggregates) read a single coherent snapshot per seed.
//
// Rate fields end in "Rate" to distinguish them from counts; cost and
// wait-time fields carry unit suffixes instead because they aren't
// rates. A zero ComputeCostMs inside an otherwise-nonzero entry means
// "no compute-cost observation", not a real 0 ms sample.
//
// OriginRate is calls/sec entering the cluster at this node (whether
// the node hosts the seed or forwards the call elsewhere). Placement
// scoring treats the cluster-wide distribution of OriginRate as the
// authoritative demand signal.
//
// ParkedMs is the mean time per invocation spent blocked inside
// pollen_request. It's the signal adaptive gate sizing uses to separate
// "CPU work" from "waiting on downstream" without hard-coding a ratio.
type SeedMetrics struct {
	ServedRate       float32
	OriginRate       float32
	ComputeCostMs    float32
	SLOSatisfiedRate float32
	SLOBurnedRate    float32
	ParkedMs         float32
	GateWaitMs       uint32
}

// IsZero reports whether every field is zero. Used by SetSeedMetrics to
// decide whether a hash entry is worth gossiping — an all-zero bundle
// means the seed has no active telemetry on this node and the entry is
// omitted. A non-zero→zero transition on any single field (e.g. gate
// wait clearing while traffic continues) is a material change and still
// travels through the dead-band check.
func (m SeedMetrics) IsZero() bool {
	return m.ServedRate == 0 && m.OriginRate == 0 &&
		m.ComputeCostMs == 0 && m.SLOSatisfiedRate == 0 &&
		m.SLOBurnedRate == 0 && m.ParkedMs == 0 &&
		m.GateWaitMs == 0
}

type StaticSpec struct {
	Claim          *claims.PublisherClaim
	Name           string
	ManifestDigest string
	MinReplicas    uint32
}

// BlobSpec names a content-addressed blob so callers can refer to it by
// label rather than digest. Entries are keyed by (publisher, digest) —
// re-publishing the same digest under a new name overwrites the previous
// name for that publisher.
type BlobSpec struct {
	Claim  *claims.PublisherClaim
	Name   string
	Digest string
}
