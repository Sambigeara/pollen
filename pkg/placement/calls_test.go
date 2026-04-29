// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type callsRecorder struct {
	snapshots []map[string]uint64
}

func (r *callsRecorder) emit(counts map[string]uint64) {
	r.snapshots = append(r.snapshots, counts)
}

func TestCallTracker_RecordsLocalCalls(t *testing.T) {
	r := &callsRecorder{}
	tr := newCallTracker(time.Hour, r.emit)

	tr.RecordCall("seed-a")
	tr.RecordCall("seed-a")
	tr.RecordCall("seed-b")
	tr.flush()

	require.Len(t, r.snapshots, 1)
	require.Equal(t, uint64(2), r.snapshots[0]["seed-a"])
	require.Equal(t, uint64(1), r.snapshots[0]["seed-b"])
}

func TestCallTracker_FlushResetsLocal(t *testing.T) {
	r := &callsRecorder{}
	tr := newCallTracker(time.Hour, r.emit)

	tr.RecordCall("seed-a")
	tr.flush()
	tr.RecordCall("seed-a")
	tr.flush()

	require.Len(t, r.snapshots, 2)
	require.Equal(t, uint64(1), r.snapshots[0]["seed-a"])
	require.Equal(t, uint64(1), r.snapshots[1]["seed-a"])
}

func TestCallTracker_FlushSkipsEmptyWindow(t *testing.T) {
	r := &callsRecorder{}
	tr := newCallTracker(time.Hour, r.emit)

	tr.flush()
	require.Empty(t, r.snapshots)
}

func TestCallTracker_LocalCount(t *testing.T) {
	tr := newCallTracker(time.Hour, func(_ map[string]uint64) {})

	require.Equal(t, uint64(0), tr.localCount("seed-a"))

	tr.RecordCall("seed-a")
	tr.RecordCall("seed-a")
	require.Equal(t, uint64(2), tr.localCount("seed-a"))

	tr.flush()
	require.Equal(t, uint64(0), tr.localCount("seed-a"))
}
