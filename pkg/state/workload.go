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
	AdmissionState   AdmissionState
}

// AdmissionState is the local backpressure picture for routing peers.
// Values mirror the proto enum.
type AdmissionState uint8

const (
	AdmissionUnspecified AdmissionState = iota
	AdmissionOpen
	AdmissionDegraded
	AdmissionClosed
)

// SeedMetrics is the per-seed per-node telemetry bundle, gossiped as one
// snapshot. OriginRate is calls/sec entering the cluster at this node — the
// authoritative demand signal for placement. ParkedMs is mean blocked time
// inside pollen_request, subtracted from ComputeCostMs in the autoscaler
// so capacity sizing tracks active CPU work rather than wall-clock time
// (a chain workload spending 90% parked needs 10× fewer replicas than a
// CPU-bound leaf at the same observed wall time). Zero ComputeCostMs in
// an otherwise non-zero entry means "no observation", not a real 0 ms
// sample. OriginRateFast / OriginRateSlow flank OriginRate as short- and
// long-window EWMAs for burst detection; RejectRate counts admissions
// rejected at this node and forces immediate scale-up when non-zero.
type SeedMetrics struct {
	ServedRate       float32
	OriginRate       float32
	OriginRateFast   float32
	OriginRateSlow   float32
	RejectRate       float32
	ComputeCostMs    float32
	SLOSatisfiedRate float32
	SLOBurnedRate    float32
	ParkedMs         float32
}

// IsZero is true when SetSeedMetrics should omit the entry from gossip;
// non-zero→zero transitions on any single field still pass the dead-band check.
func (m SeedMetrics) IsZero() bool {
	return m.ServedRate == 0 && m.OriginRate == 0 &&
		m.OriginRateFast == 0 && m.OriginRateSlow == 0 &&
		m.RejectRate == 0 &&
		m.ComputeCostMs == 0 && m.SLOSatisfiedRate == 0 &&
		m.SLOBurnedRate == 0 && m.ParkedMs == 0
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
