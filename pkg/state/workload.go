// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import "time"

type WorkloadSpec struct {
	Hash        string
	Name        string
	MemoryBytes uint64
	Timeout     time.Duration
	MinReplicas uint32
	Spread      float32
}

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

type BlobSpec struct {
	Name   string
	Digest string
}
